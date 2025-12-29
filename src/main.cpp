#include "kv_store.hpp"
#include <iostream>
#include <sstream>
#include <string>
#include <algorithm>

// Helper to print the prompt
void print_prompt() {
    std::cout << "nanodb> ";
}

void print_help() {
    std::cout << "Commands:\n"
              << "  PUT <key> <value>   : Insert or update a key-value pair\n"
              << "  GET <key>           : Retrieve a value by key\n"
              << "  DEL <key>           : Delete a key\n"
              << "  EXIT                : Save and quit\n"
              << "  HELP                : Show this message\n";
}

int main() {
    std::cout << "Initializing NanoDB (Distributed-Log-KV)..." << std::endl;
    
    // Initialize DB (This triggers the Recovery/Replay you built earlier)
    ShardedKVStore db(16, "wal.log");

    std::cout << "Database Ready. Type 'HELP' for commands." << std::endl;

    std::string line;
    print_prompt();

    // Main Command Loop
    while (std::getline(std::cin, line)) {
        if (line.empty()) {
            print_prompt();
            continue;
        }

        std::stringstream ss(line);
        std::string command;
        ss >> command;

        // Convert command to uppercase for case-insensitivity
        std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "EXIT") {
            std::cout << "Shutting down safely..." << std::endl;
            break;
        } 
        else if (command == "HELP") {
            print_help();
        } 
        else if (command == "PUT") {
            std::string key, value;
            ss >> key;
            
            // Read the rest of the line as the value (allows spaces in value)
            std::string remainder;
            std::getline(ss, remainder);
            
            // Trim leading whitespace from the value
            size_t first = remainder.find_first_not_of(" \t");
            if (first != std::string::npos) {
                value = remainder.substr(first);
                db.put(key, value);
                std::cout << "OK" << std::endl;
            } else {
                std::cout << "Error: Usage: PUT <key> <value>" << std::endl;
            }
        } 
        else if (command == "GET") {
            std::string key;
            if (ss >> key) {
                std::string value;
                if (db.get(key, value)) {
                    std::cout << value << std::endl;
                } else {
                    std::cout << "(nil)" << std::endl;
                }
            } else {
                std::cout << "Error: Usage: GET <key>" << std::endl;
            }
        }
        else if (command == "DEL") {
            std::string key;
            if (ss >> key) {
                db.del(key);
                std::cout << "OK (Deleted)" << std::endl;
            } else {
                std::cout << "Error: Usage: DEL <key>" << std::endl;
            }
        } 
        else {
            std::cout << "Unknown command: " << command << std::endl;
        }

        print_prompt();
    }

    return 0;
}
