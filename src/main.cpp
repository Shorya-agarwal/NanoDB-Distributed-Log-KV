#include "kv_store.hpp"
#include <iostream>

int main() {
    // 1. Initialize DB (it will auto-recover if file exists)
    ShardedKVStore db(16, "wal.log");

    std::string val;
    // Check if data from a PREVIOUS run exists
    if (db.get("user1", val)) {
        std::cout << "Found existing data for user1: " << val << std::endl;
    } else {
        std::cout << "No data found. Writing new data..." << std::endl;
        db.put("user1", "JohnDoe");
        db.put("session_id", "xyz789");
    }

    return 0;
}
