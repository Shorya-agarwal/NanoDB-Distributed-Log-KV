#include "wal_logger.hpp"
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <functional>

// A unique string that acts as our "Delete Marker"
// In a production system, we would use a binary flag, but this is perfect for a prototype.
const std::string TOMBSTONE = "||__TOMBSTONE__||";

struct Shard {
    std::unordered_map<std::string, std::string> data;
    mutable std::shared_mutex mutex; 
};

class ShardedKVStore {
private:
    std::vector<Shard> shards;
    size_t shard_count;
    WALLogger logger; 

    size_t get_shard_index(const std::string& key) const {
        std::hash<std::string> hasher;
        return hasher(key) % shard_count;
    }

public:
    ShardedKVStore(size_t count = 16, const std::string& filename = "wal.log") 
        : shard_count(count), shards(count), logger(filename) {
        
        // RECOVERY: Replay the log to restore state
        auto entries = logger.read_all_logs();
        for (const auto& entry : entries) {
            size_t idx = get_shard_index(entry.key);
            
            // IF we see a Tombstone, remove the key from memory
            if (entry.value == TOMBSTONE) {
                shards[idx].data.erase(entry.key);
            } else {
                shards[idx].data[entry.key] = entry.value;
            }
        }
        std::cout << "Recovered " << entries.size() << " log entries from disk." << std::endl;
    }

    void put(const std::string& key, const std::string& value) {
        logger.log_operation(key, value); // Persist
        
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

    // NEW: The Delete Function
    void del(const std::string& key) {
        // 1. Log the Tombstone to disk
        logger.log_operation(key, TOMBSTONE);

        // 2. Remove from Memory
        size_t idx = get_shard_index(key);
        std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
        shards[idx].data.erase(key);
    }
};
