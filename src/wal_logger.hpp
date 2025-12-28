#include <iostream>
#include <fstream>
#include <mutex>
#include <vector>
#include <string>

// A struct to hold restored data
struct LogEntry {
    std::string key;
    std::string value;
};

class WALLogger {
private:
    std::fstream log_file;
    std::string filename;
    std::mutex log_mutex;

public:
    WALLogger(const std::string& fname) : filename(fname) {
        open_file();
    }

    // Opens file in append mode for writing
    void open_file() {
        // std::ios::app ensures we don't overwrite existing data
        log_file.open(filename, std::ios::out | std::ios::app | std::ios::binary);
    }

    void log_operation(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(log_mutex);
        
        size_t k_len = key.size();
        size_t v_len = value.size();

        // Write Key Size -> Key -> Value Size -> Value
        log_file.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        log_file.write(key.c_str(), k_len);
        log_file.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
        log_file.write(value.c_str(), v_len);
        
        log_file.flush(); // Ensure it hits the disk
    }

    // New Function: Replay the log to restore state
    std::vector<LogEntry> read_all_logs() {
        std::vector<LogEntry> entries;
        
        // Open specifically for reading
        std::ifstream infile(filename, std::ios::binary);
        if (!infile.is_open()) return entries; // File might not exist yet, that's fine

        while (infile.peek() != EOF) {
            size_t k_len;
            size_t v_len;
            
            // Read Key Size
            if (!infile.read(reinterpret_cast<char*>(&k_len), sizeof(k_len))) break;
            
            // Read Key
            std::string key(k_len, '\0');
            infile.read(&key[0], k_len);

            // Read Value Size
            infile.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
            
            // Read Value
            std::string value(v_len, '\0');
            infile.read(&value[0], v_len);

            entries.push_back({key, value});
        }
        return entries;
    }
};
