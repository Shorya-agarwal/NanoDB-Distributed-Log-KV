#pragma once
#include <string>
#include <vector>
#include <sstream>
#include <sys/socket.h>

// Parses the RESP wire protocol (what redis-cli / redis-py actually speak):
// requests arrive as arrays of bulk strings, e.g.
//   *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
// Also accepts plain inline commands (space-separated, newline-terminated)
// so you can still `nc`/`telnet` into it by hand.
class RESPConnection {
private:
    int client_fd;
    std::string buffer;

    bool fill_buffer() {
        char temp[4096];
        ssize_t n = recv(client_fd, temp, sizeof(temp), 0);
        if (n <= 0) return false;
        buffer.append(temp, n);
        return true;
    }

    bool read_line(std::string& out) {
        size_t pos;
        while ((pos = buffer.find("\r\n")) == std::string::npos) {
            if (!fill_buffer()) return false;
        }
        out = buffer.substr(0, pos);
        buffer.erase(0, pos + 2);
        return true;
    }

    bool read_exact(size_t len, std::string& out) {
        while (buffer.size() < len + 2) {
            if (!fill_buffer()) return false;
        }
        out = buffer.substr(0, len);
        buffer.erase(0, len + 2); // strip value + trailing \r\n
        return true;
    }

public:
    explicit RESPConnection(int fd) : client_fd(fd) {}

    // Empty vector => client disconnected or sent something unparseable.
    std::vector<std::string> read_command() {
        std::string line;
        if (!read_line(line)) return {};
        if (line.empty()) return read_command(); // skip blank lines

        if (line[0] == '*') {
            long num_args = std::stol(line.substr(1));
            if (num_args <= 0) return {};
            std::vector<std::string> tokens;
            for (long i = 0; i < num_args; ++i) {
                std::string len_line;
                if (!read_line(len_line) || len_line.empty() || len_line[0] != '$') return {};
                size_t len = std::stoul(len_line.substr(1));
                std::string value;
                if (!read_exact(len, value)) return {};
                tokens.push_back(std::move(value));
            }
            return tokens;
        }

        std::vector<std::string> tokens;
        std::istringstream iss(line);
        std::string tok;
        while (iss >> tok) tokens.push_back(tok);
        return tokens;
    }

    void send_raw(const std::string& data) { send(client_fd, data.c_str(), data.size(), 0); }
    void send_simple_string(const std::string& s) { send_raw("+" + s + "\r\n"); }
    void send_error(const std::string& s)         { send_raw("-ERR " + s + "\r\n"); }
    void send_integer(long long i)                { send_raw(":" + std::to_string(i) + "\r\n"); }
    void send_nil()                                { send_raw("$-1\r\n"); }
    void send_bulk_string(const std::string& s) {
        send_raw("$" + std::to_string(s.size()) + "\r\n" + s + "\r\n");
    }
};