#include "protocol.h"

#include <cstdio>
#include <cstring>
#include <unistd.h>

#include "crc32.h"

namespace polea {

namespace {
void put16(std::vector<uint8_t>& v, uint16_t x) {
    v.push_back(static_cast<uint8_t>(x & 0xFF));
    v.push_back(static_cast<uint8_t>(x >> 8));
}
void put32(std::vector<uint8_t>& v, uint32_t x) {
    for (int i = 0; i < 4; ++i) v.push_back(static_cast<uint8_t>((x >> (8 * i)) & 0xFF));
}
uint16_t get16(const uint8_t* p) {
    return static_cast<uint16_t>(p[0] | (p[1] << 8));
}
uint32_t get32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}
uint64_t get64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}
} // namespace

uint64_t Frame::msg_id() const {
    if (payload.size() < 8) return 0;
    return get64(payload.data());
}

DecodeResult decode_frame(const uint8_t* buf, size_t len) {
    DecodeResult r;
    if (len < HEADER_SIZE) return r; // NeedMore por defecto

    if (std::memcmp(buf, MAGIC, 4) != 0) {
        r.err = ProtoError::BadMagic;
        return r;
    }
    if (buf[4] != PROTO_VERSION) {
        r.err = ProtoError::BadVersion;
        return r;
    }
    r.frame.type = static_cast<Type>(buf[5]);
    r.frame.flags = buf[6];
    uint16_t topic_len = get16(buf + 7);
    uint32_t payload_len = get32(buf + 9);

    if (topic_len > MAX_TOPIC || payload_len > MAX_PAYLOAD) {
        r.err = ProtoError::BadLength;
        return r;
    }
    if (topic_len + HEADER_SIZE + payload_len + CRC_SIZE > len) return r; // NeedMore

    const uint8_t* topic_start = buf + HEADER_SIZE;
    const uint8_t* payload_start = topic_start + topic_len;
    const uint8_t* crc_at = payload_start + payload_len;

    uint32_t expected = get32(crc_at);
    uint32_t actual = crc32(buf + 4, HEADER_SIZE - 4 + topic_len + payload_len);
    if (expected != actual) {
        r.err = ProtoError::BadCrc;
        return r;
    }

    r.frame.topic.assign(reinterpret_cast<const char*>(topic_start), topic_len);
    r.frame.payload.assign(payload_start, payload_start + payload_len);
    r.ok = true;
    r.consumed = HEADER_SIZE + topic_len + payload_len + CRC_SIZE;
    return r;
}

std::vector<uint8_t> encode_frame(Type type, uint8_t flags,
                                  const std::string& topic,
                                  const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> v;
    v.insert(v.end(), MAGIC, MAGIC + 4);
    v.push_back(PROTO_VERSION);
    v.push_back(static_cast<uint8_t>(type));
    v.push_back(flags);
    put16(v, static_cast<uint16_t>(topic.size()));
    put32(v, static_cast<uint32_t>(payload.size()));
    v.insert(v.end(), topic.begin(), topic.end());
    v.insert(v.end(), payload.begin(), payload.end());
    uint32_t crc = crc32(v.data() + 4, v.size() - 4);
    put32(v, crc);
    return v;
}

std::vector<uint8_t> encode_frame(Type type, uint8_t flags,
                                  const std::string& topic,
                                  const std::string& payload) {
    return encode_frame(type, flags, topic,
                        std::vector<uint8_t>(payload.begin(), payload.end()));
}

std::string make_name(const char* opt) {
    if (opt && *opt) return opt;
    char buf[64];
    std::snprintf(buf, sizeof(buf), "cliente-%d", static_cast<int>(getpid()));
    return buf;
}

const char* protocol_error_text(ProtoError e) {
    switch (e) {
        case ProtoError::NeedMore: return "trama incompleta";
        case ProtoError::BadMagic: return "magic incorrecto";
        case ProtoError::BadVersion: return "versión de protocolo no soportada";
        case ProtoError::BadLength: return "longitud ilegal";
        case ProtoError::BadCrc: return "crc inválido";
        case ProtoError::Invalid: return "trama inválida";
    }
    return "desconocido";
}

} // namespace polea