#include <fstream>
#include <mutex>

class WALLogger {
private:
    std::fstream log_file;
    std::mutex log_mutex; // Sequential writes to disk

public:
    WALLogger(const std::string& filename) {
        // Open for appending and binary mode
        log_file.open(filename, std::ios::app | std::ios::binary | std::ios::out);
    }

    void log_operation(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(log_mutex);
        if (!log_file.is_open()) return;

        // Simple binary format: [KeySize][Key][ValueSize][Value]
        size_t k_len = key.size();
        size_t v_len = value.size();

        log_file.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        log_file.write(key.c_str(), k_len);
        log_file.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
        log_file.write(value.c_str(), v_len);
        
        // FLUSH is critical for durability
        log_file.flush(); 
    }
};
