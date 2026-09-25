/// Protocolo de polea: tramas binarias con delimitación por longitud,
/// cabecera fija, sumas CRC-32 y validación estricta.
///
/// Formato de cada trama (little-endian):
///
/// ```text
///  +--------+---------+-------+--------+------------+---------------+-------+
///  | magic  | version | type  | flags  | topic_len  |  payload_len  | topic |
///  | 4 B    | 1 B     | 1 B   | 1 B    | 2 B        |  4 B          | N B   |
///  +--------+---------+-------+--------+------------+---------------+-------+
///  | payload (P B)                       | crc32 (4 B sobre version..payload) |
///  +-------------------------------------+------------------------------------+
/// ```
///
/// El CRC cubre desde `version` hasta el final del payload (excluye `magic`
/// y el propio CRC, que sirve como marcador de sincronización de trama).

#ifndef POLEA_PROTOCOL_H
#define POLEA_PROTOCOL_H

#include <cstdint>
#include <string>
#include <vector>

namespace polea {

/// Tamaño de cabecera fija (sin topic/payload/crc).
constexpr size_t HEADER_SIZE = 13;
/// Tamaño de la cola CRC.
constexpr size_t CRC_SIZE = 4;
/// Longitud máxima de un tema (topic).
constexpr size_t MAX_TOPIC = 64;
/// Longitud máxima de un payload.
constexpr uint32_t MAX_PAYLOAD = 1024u * 1024u; // 1 MiB

/// Versión del protocolo comercializada.
constexpr uint8_t PROTO_VERSION = 1;

/// Marcador mágico de inicio de trama.
constexpr uint8_t MAGIC[4] = {0xA7, 0x11, 0xB5, 0x7E};

/// Tipos de trama.
enum class Type : uint8_t {
    Hello = 0x01,      // cliente -> servidor; payload = nombre público
    HelloAck = 0x02,   // servidor -> cliente; payload = id asignada (8 B LE)
    Sub = 0x03,        // cliente -> servidor; topic en el campo topic
    Unsub = 0x04,      // cliente -> servidor; topic en el campo topic
    Pub = 0x05,        // cliente -> servidor; topic + payload
    Deliver = 0x06,    // servidor -> cliente; topic + payload: [8 B id LE][datos]
    PubAck = 0x07,     // servidor -> cliente (si ACK); payload = [8 B id LE]
    List = 0x08,       // cliente -> servidor; lista de topics
    ListReply = 0x09,  // servidor -> cliente; payload = topics "\n"
    Error = 0x0A,      // servidor -> cliente; payload = mensaje
    Ping = 0x0B,       // keepalive (usado por los clientes)
    Pong = 0x0C,
    StatsReq = 0x0D,   // cliente -> servidor
    StatsReply = 0x0E, // servidor -> cliente; payload = texto plano
};

/// Banderas de la cabecera.
constexpr uint8_t FLAG_ACK = 0x01; // solicitar PubAck en un Pub

/// Una trama decodificada (ya validada y con CRC comprobado).
struct Frame {
    Type type{};
    uint8_t flags{};
    std::string topic;
    std::vector<uint8_t> payload;

    /// Lee el id de mensaje de los primeros 8 bytes LE del payload.
    uint64_t msg_id() const;
};

/// Errores de protocolo.
enum class ProtoError {
    NeedMore,   // faltan bytes para completar la trama
    BadMagic,   // sincronización perdida
    BadVersion,
    BadLength,  // longitud ilegal (topic/payload fuera de rango)
    BadCrc,     // suma de verificación incorrecta
    Invalid,    // tipo/contenido no acorde
};

/// Decodifica una trama desde `buf[0..buf.size())`.
/// Devuelve la trama y el número de bytes consumidos, o el error.
/// Con `ProtoError::NeedMore`, no se consume nada.
struct DecodeResult {
    bool ok = false;
    Frame frame;
    size_t consumed = 0;
    ProtoError err = ProtoError::NeedMore;
};
DecodeResult decode_frame(const uint8_t* buf, size_t len);

/// Texto diagnóstico para un error de protocolo.
const char* protocol_error_text(ProtoError e);

/// Codifica una trama a bytes listos para el socket (con CRC incluido).
std::vector<uint8_t> encode_frame(Type type, uint8_t flags,
                                  const std::string& topic,
                                  const std::vector<uint8_t>& payload);

std::vector<uint8_t> encode_frame(Type type, uint8_t flags,
                                  const std::string& topic,
                                  const std::string& payload);

/// Verificación de turnos al conectarse: presentación del peer.
std::string make_name(const char* opt);

} // namespace polea

#endif // POLEA_PROTOCOL_H