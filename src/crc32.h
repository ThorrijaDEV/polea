/// CRC-32 (IEEE 802.3, polinomio 0xEDB88320) de tabla, en C++.
///
/// Misma familia que la usada en cask, pero implementada aquí de forma
/// autocontenida para que polea no dependa de nada más que de la libc.

#ifndef POLEA_CRC32_H
#define POLEA_CRC32_H

#include <cstddef>
#include <cstdint>

namespace polea {

/// CRC-32 incremental sobre un búfer. `seed` permite encadenar llamadas.
uint32_t crc32(const uint8_t* data, size_t len, uint32_t seed = 0u);

} // namespace polea

#endif // POLEA_CRC32_H