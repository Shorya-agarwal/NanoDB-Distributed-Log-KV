#pragma once
#include <cstdint>
#include <cstddef>
#include <array>

// Standard CRC-32 (IEEE 802.3 polynomial, reflected). Same algorithm used by
// zlib/gzip/PNG. Table is computed once at static-init time.
class CRC32 {
private:
    static std::array<uint32_t, 256> build_table() {
        std::array<uint32_t, 256> table{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        return table;
    }

    static const std::array<uint32_t, 256>& table() {
        static const std::array<uint32_t, 256> t = build_table();
        return t;
    }

public:
    static uint32_t compute(const void* data, size_t len) {
        const auto& t = table();
        uint32_t crc = 0xFFFFFFFFu;
        const unsigned char* p = static_cast<const unsigned char*>(data);
        for (size_t i = 0; i < len; ++i) {
            crc = t[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFFu;
    }
};