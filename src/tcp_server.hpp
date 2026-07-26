#pragma once
#include "kv_store.hpp"
#include "resp_protocol.hpp"
#include <thread>
#include <atomic>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>

class TCPServer {
private:
    ShardedKVStore& db;
    int port;
    int server_fd = -1;
    std::atomic<bool> running{false};

    static std::string upper(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        return s;
    }

    void handle_client(int client_fd) {
        RESPConnection conn(client_fd);
        while (running) {
            auto tokens = conn.read_command();
            if (tokens.empty()) break;

            std::string cmd = upper(tokens[0]);

            if (cmd == "PING") {
                conn.send_simple_string("PONG");
            } else if (cmd == "SET" && tokens.size() >= 3) {
                db.put(tokens[1], tokens[2]);
                conn.send_simple_string("OK");
            } else if (cmd == "GET" && tokens.size() >= 2) {
                std::string value;
                if (db.get(tokens[1], value)) conn.send_bulk_string(value);
                else conn.send_nil();
            } else if ((cmd == "DEL" || cmd == "DELETE") && tokens.size() >= 2) {
                db.del(tokens[1]);
                conn.send_integer(1);
            } else if (cmd == "COMPACT") {
                db.compact();
                conn.send_simple_string("OK");
            } else if (cmd == "DBSIZE") {
                conn.send_integer(static_cast<long long>(db.size()));
            } else if (cmd == "QUIT") {
                conn.send_simple_string("OK");
                break;
            } else {
                conn.send_error("unknown command '" + tokens[0] + "'");
            }
        }
        close(client_fd);
    }

public:
    TCPServer(ShardedKVStore& store, int p) : db(store), port(p) {}

    void start() {
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return; }
        if (listen(server_fd, 128) < 0) { perror("listen"); return; }

        running = true;
        std::cout << "NanoDB TCP server listening on port " << port << std::endl;

        while (running) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            int client_fd = accept(server_fd, (sockaddr*)&client_addr, &len);
            if (client_fd < 0) { if (running) perror("accept"); continue; }
            std::thread(&TCPServer::handle_client, this, client_fd).detach();
        }
    }

    void stop() {
        running = false;
        if (server_fd >= 0) close(server_fd);
    }
};