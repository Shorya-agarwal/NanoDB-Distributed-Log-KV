#pragma once
#include "crc32.hpp"
#include <string>
#include <vector>
#include <mutex>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include <fcntl.h>
#include <unistd.h>

struct LogEntry {
    std::string key;
    std::string value;
};

// On-disk frame layout (all integers are raw native-endian binary, which is
// fine here since we only ever read what we write on the same architecture):
//
//   [CRC32: 4B][KeySize: 8B][Key: N][ValueSize: 8B][Value: N]
//
// CRC32 covers everything after itself (KeySize..Value). On replay, any frame
// whose checksum doesn't match is treated as a torn/corrupted write -- almost
// always the tail of the file after a crash mid-write -- and replay stops
// there rather than trusting garbage bytes as the next frame's length.
class WALLogger {
private:
    std::string filename;
    int fd = -1;
    std::mutex log_mutex;

    void open_for_append() {
        fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            throw std::runtime_error("WALLogger: failed to open " + filename + " for append");
        }
    }

    static void write_all(int fd, const void* buf, size_t len) {
        const char* p = static_cast<const char*>(buf);
        size_t written = 0;
        while (written < len) {
            ssize_t n = ::write(fd, p + written, len - written);
            if (n < 0) throw std::runtime_error("WALLogger: write() failed");
            written += static_cast<size_t>(n);
        }
    }

public:
    explicit WALLogger(const std::string& fname) : filename(fname) {
        open_for_append();
    }

    ~WALLogger() {
        if (fd >= 0) ::close(fd);
    }

    // Not copyable (owns an fd); movable if ever needed.
    WALLogger(const WALLogger&) = delete;
    WALLogger& operator=(const WALLogger&) = delete;

    void log_operation(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(log_mutex);

        uint64_t k_len = key.size();
        uint64_t v_len = value.size();

        // Build the payload (everything the checksum covers) contiguously so
        // we checksum exactly what we write, in one pass.
        std::string payload;
        payload.reserve(sizeof(k_len) + k_len + sizeof(v_len) + v_len);
        payload.append(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        payload.append(key);
        payload.append(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
        payload.append(value);

        uint32_t crc = CRC32::compute(payload.data(), payload.size());

        write_all(fd, &crc, sizeof(crc));
        write_all(fd, payload.data(), payload.size());

        // fsync (not just flush): flush only pushes libc/OS buffers into the
        // page cache, which survives a process crash but NOT a power loss or
        // kernel panic. fsync forces the page cache to physical disk, which
        // is the actual durability guarantee a WAL exists to provide.
        if (::fsync(fd) != 0) {
            throw std::runtime_error("WALLogger: fsync() failed");
        }
    }

    // Replays the log from disk. Stops (without throwing) at the first frame
    // that fails its checksum or is truncated -- that's expected behavior
    // after a crash mid-write, not an error to propagate.
    std::vector<LogEntry> read_all_logs() {
        std::vector<LogEntry> entries;

        int rfd = ::open(filename.c_str(), O_RDONLY);
        if (rfd < 0) return entries; // file doesn't exist yet -- fine, fresh DB

        auto read_exact = [&](void* buf, size_t len) -> bool {
            char* p = static_cast<char*>(buf);
            size_t total = 0;
            while (total < len) {
                ssize_t n = ::read(rfd, p + total, len - total);
                if (n <= 0) return false; // EOF or error => stop replay here
                total += static_cast<size_t>(n);
            }
            return true;
        };

        size_t corrupted_at = 0;
        bool hit_corruption = false;

        while (true) {
            uint32_t stored_crc;
            if (!read_exact(&stored_crc, sizeof(stored_crc))) break; // clean EOF

            uint64_t k_len;
            if (!read_exact(&k_len, sizeof(k_len))) { hit_corruption = true; break; }

            std::string key(k_len, '\0');
            if (k_len > 0 && !read_exact(&key[0], k_len)) { hit_corruption = true; break; }

            uint64_t v_len;
            if (!read_exact(&v_len, sizeof(v_len))) { hit_corruption = true; break; }

            std::string value(v_len, '\0');
            if (v_len > 0 && !read_exact(&value[0], v_len)) { hit_corruption = true; break; }

            std::string payload;
            payload.reserve(sizeof(k_len) + k_len + sizeof(v_len) + v_len);
            payload.append(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            payload.append(key);
            payload.append(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            payload.append(value);

            uint32_t computed_crc = CRC32::compute(payload.data(), payload.size());
            if (computed_crc != stored_crc) {
                hit_corruption = true;
                break;
            }

            entries.push_back({std::move(key), std::move(value)});
            corrupted_at = entries.size();
        }

        ::close(rfd);

        if (hit_corruption) {
            std::cerr << "WALLogger: stopped replay after " << corrupted_at
                      << " valid entries -- remaining bytes failed checksum "
                      << "(likely a torn write from a crash). Ignoring the rest."
                      << std::endl;
        }

        return entries;
    }

    // Atomically replaces the WAL with a compacted log containing only
    // `live_entries` (the current in-memory state). Writes to a temp file,
    // fsyncs its contents AND the containing directory, then rename()s over
    // the original. rename() is atomic on POSIX -- a reader/crash never sees
    // a half-written compacted file under the real filename.
    void compact(const std::vector<LogEntry>& live_entries) {
        std::lock_guard<std::mutex> lock(log_mutex);

        std::string tmp_name = filename + ".compacting";
        int tmp_fd = ::open(tmp_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (tmp_fd < 0) {
            throw std::runtime_error("WALLogger: failed to open compaction temp file");
        }

        for (const auto& e : live_entries) {
            uint64_t k_len = e.key.size();
            uint64_t v_len = e.value.size();

            std::string payload;
            payload.reserve(sizeof(k_len) + k_len + sizeof(v_len) + v_len);
            payload.append(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            payload.append(e.key);
            payload.append(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            payload.append(e.value);

            uint32_t crc = CRC32::compute(payload.data(), payload.size());
            write_all(tmp_fd, &crc, sizeof(crc));
            write_all(tmp_fd, payload.data(), payload.size());
        }

        if (::fsync(tmp_fd) != 0) {
            ::close(tmp_fd);
            throw std::runtime_error("WALLogger: fsync on compacted file failed");
        }
        ::close(tmp_fd);

        // Close our current append handle before swapping the underlying file.
        ::close(fd);

        if (::rename(tmp_name.c_str(), filename.c_str()) != 0) {
            // Reopen the old file so the process can keep running even if
            // compaction failed to swap in.
            open_for_append();
            throw std::runtime_error("WALLogger: rename() during compaction failed");
        }

        // fsync the directory too -- on POSIX, the rename itself isn't
        // guaranteed durable until the directory entry change is synced.
        // (Real databases do this; it's an easy detail to miss.)
        size_t slash = filename.find_last_of('/');
        std::string dir = (slash == std::string::npos) ? "." : filename.substr(0, slash);
        int dir_fd = ::open(dir.c_str(), O_RDONLY);
        if (dir_fd >= 0) {
            ::fsync(dir_fd);
            ::close(dir_fd);
        }

        open_for_append(); // reopen append handle pointing at the new file
    }
};