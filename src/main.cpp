// src/main.cpp
#include "kv_store.hpp"
#include <thread>
#include <vector>
#include <iostream>

void writer_task(ShardedKVStore& db, int start, int end) {
    for (int i = start; i < end; ++i) {
        db.put("key" + std::to_string(i), "value" + std::to_string(i));
    }
}

int main() {
    ShardedKVStore db(16); // 16 Shards
    std::cout << "Starting 4 threads to write 100k keys..." << std::endl;

    std::vector<std::thread> threads;
    // Launch 4 concurrent writers
    for(int i = 0; i < 4; i++) {
        threads.emplace_back(writer_task, std::ref(db), i*25000, (i+1)*25000);
    }

    for(auto& t : threads) t.join();

    std::cout << "Finished. Verifying..." << std::endl;
    std::string val;
    if(db.get("key50000", val)) {
        std::cout << "Found key50000: " << val << std::endl;
    }

    return 0;
}
