/// Cliente de polea: encapsula un socket TCP, la presentación (Hello) y el
/// bucle de recepción con rebanado de tramas y validación de CRC.

#ifndef POLEA_CLIENT_H
#define POLEA_CLIENT_H

#include <string>
#include <vector>

#include "protocol.h"

namespace polea {

class Client {
public:
    Client() = default;
    ~Client() { close(); }

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /// Conecta al broker (host:port), envía Hello y espera el HelloAck.
    /// Devuelve el id de par asignado por el servidor (0 si falla).
    uint16_t connect(const std::string& host, uint16_t port,
                     const std::string& name);

    bool subscribe(const std::string& topic);
    bool unsubscribe(const std::string& topic);
    bool publish(const std::string& topic, const std::string& payload,
                 bool ack = false);
    bool request_list();
    bool request_stats();

    /// Espera hasta `timeout_ms`. Devuelve:
    ///  >0  nº de tramas recibidas (depositadas en `out`)
    ///   0  timeout sin datos
    ///  -1  error / EOF de la conexión
    int recv(int timeout_ms, std::vector<Frame>& out);

    /// Envía cualquier trama ya construida.
    bool send_frame(const Frame& f);

    int fd() const { return fd_; }
    bool connected() const { return fd_ >= 0; }
    void close();
    uint16_t peer_id() const { return peer_id_; }

private:
    int fd_ = -1;
    uint16_t peer_id_ = 0;
    std::vector<uint8_t> in_;
};

} // namespace polea

#endif // POLEA_CLIENT_H