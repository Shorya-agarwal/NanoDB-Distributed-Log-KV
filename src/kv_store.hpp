#include <iostream>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <functional>

// A single shard containing a map and a lock
struct Shard {
    std::unordered_map<std::string, std::string> data;
    mutable std::shared_mutex mutex; // Allows multiple readers, one writer
};

class ShardedKVStore {
private:
    std::vector<Shard> shards;
    size_t shard_count;

    // Hash function to determine which shard a key belongs to
    size_t get_shard_index(const std::string& key) const {
        std::hash<std::string> hasher;
        return hasher(key) % shard_count;
    }

public:
    ShardedKVStore(size_t count = 16) : shard_count(count), shards(count) {}

    void put(const std::string& key, const std::string& value) {
        size_t idx = get_shard_index(key);
        // Unique lock for WRITING (blocks everyone else for this specific shard only)
        std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
        shards[idx].data[key] = value;
    }

    bool get(const std::string& key, std::string& out_value) const {
        size_t idx = get_shard_index(key);
        // Shared lock for READING (allows other readers)
        std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
        
        auto it = shards[idx].data.find(key);
        if (it != shards[idx].data.end()) {
            out_value = it->second;
            return true;
        }
        return false;
    }
};
