#pragma once
#include "wal_logger.hpp"
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <functional>
#include <iostream>

// A unique string that acts as our "Delete Marker"
// In a production system, we would use a binary flag, but this is perfect for a dev version.
const std::string TOMBSTONE = "||__TOMBSTONE__||";

struct Shard {
    std::unordered_map<std::string, std::string> data;
    mutable std::shared_mutex mutex;
};

class ShardedKVStore {
private:
    size_t shard_count;
    std::vector<Shard> shards;
    WALLogger logger;

    size_t get_shard_index(const std::string& key) const {
        std::hash<std::string> hasher;
        return hasher(key) % shard_count;
    }

public:
    ShardedKVStore(size_t count = 16, const std::string& filename = "wal.log")
        : shard_count(count), shards(count), logger(filename) {

        auto entries = logger.read_all_logs();
        for (const auto& entry : entries) {
            size_t idx = get_shard_index(entry.key);
            if (entry.value == TOMBSTONE) {
                shards[idx].data.erase(entry.key);
            } else {
                shards[idx].data[entry.key] = entry.value;
            }
        }
        std::cout << "Recovered " << entries.size() << " log entries from disk." << std::endl;
    }

    void put(const std::string& key, const std::string& value) {
        logger.log_operation(key, value);
        size_t idx = get_shard_index(key);
        std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
        shards[idx].data[key] = value;
    }

    bool get(const std::string& key, std::string& out_value) const {
        size_t idx = get_shard_index(key);
        std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
        auto it = shards[idx].data.find(key);
        if (it != shards[idx].data.end()) {
            out_value = it->second;
            return true;
        }
        return false;
    }

    void del(const std::string& key) {
        logger.log_operation(key, TOMBSTONE);
        size_t idx = get_shard_index(key);
        std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
        shards[idx].data.erase(key);
    }

    // Total number of live keys across all shards (for stats/monitoring).
    size_t size() const {
        size_t total = 0;
        for (const auto& s : shards) {
            std::shared_lock<std::shared_mutex> lock(s.mutex);
            total += s.data.size();
        }
        return total;
    }

    // Rewrites the WAL to contain only the current live key-value pairs,
    // dropping historical overwrites and tombstoned deletes. Locks every
    // shard (always in ascending index order, to avoid deadlock against any
    // other multi-shard operation added later), snapshots the live data, then
    // hands it to the logger for an atomic on-disk swap.
    //
    // Blocks writers for the duration of the snapshot copy (not the disk
    // I/O) -- see the compaction design notes for why a copy-then-release
    // approach is used instead of holding locks through the write to disk.
    void compact() {
        std::vector<LogEntry> live_entries;
        live_entries.reserve(size());

        std::vector<std::unique_lock<std::shared_mutex>> locks;
        locks.reserve(shard_count);
        for (size_t i = 0; i < shard_count; ++i) {
            locks.emplace_back(shards[i].mutex);
        }

        for (size_t i = 0; i < shard_count; ++i) {
            for (const auto& [k, v] : shards[i].data) {
                live_entries.push_back({k, v});
            }
        }

        locks.clear(); // release all shard locks before doing disk I/O

        logger.compact(live_entries);
        std::cout << "Compaction complete: " << live_entries.size()
                  << " live keys written to fresh log." << std::endl;
    }
};