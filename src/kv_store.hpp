#include "wal_logger.hpp" // Include your new logger
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <functional>

struct Shard {
    std::unordered_map<std::string, std::string> data;
    mutable std::shared_mutex mutex; 
};

class ShardedKVStore {
private:
    std::vector<Shard> shards;
    size_t shard_count;
    WALLogger logger; // Instance of the logger

    size_t get_shard_index(const std::string& key) const {
        std::hash<std::string> hasher;
        return hasher(key) % shard_count;
    }

public:
    // Constructor: Initialize shards AND restore data from disk
    ShardedKVStore(size_t count = 16, const std::string& filename = "wal.log") 
        : shard_count(count), shards(count), logger(filename) {
        
        // RECOVERY STEP
        auto entries = logger.read_all_logs();
        for (const auto& entry : entries) {
            // We use direct access here to avoid double-logging during recovery
            size_t idx = get_shard_index(entry.key);
            shards[idx].data[entry.key] = entry.value;
        }
        std::cout << "Recovered " << entries.size() << " records from disk." << std::endl;
    }

    void put(const std::string& key, const std::string& value) {
        // 1. Log to Disk FIRST (Durability)
        logger.log_operation(key, value);

        // 2. Update Memory
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
};
