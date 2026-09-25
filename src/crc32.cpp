#include "crc32.h"

namespace polea {

uint32_t crc32(const uint8_t* data, size_t len, uint32_t seed) {
    // Tabla del CRC-32 IEEE 802.3.
    static uint32_t table[256] = {};
    static bool initd = false;
    if (!initd) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        initd = true;
    }

    uint32_t crc = seed ^ 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

} // namespace polea