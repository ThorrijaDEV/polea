/// Servidor de polea: un broker pub/sub de un solo hilo sobre poll(2).
///
/// Mantiene un tema (topic) por grupo de suscriptores, reenvía cada
/// publicación a todas las colas de los suscriptores (una trama Deliver por
/// cliente, ordenada y con id de mensaje) y aplica backpressure con colas
/// acotadas: cuando una cola se llena, se descarta el mensaje más antiguo.
///
/// Un único hilo procesa lectura, escritura y reenvíos; esto garantiza un
/// orden global determinista para todos los pares.

#ifndef POLEA_SERVER_H
#define POLEA_SERVER_H

#include <cstdint>
#include <atomic>
#include <deque>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "protocol.h"

namespace polea {

struct ServerOptions {
    uint16_t port = 4590;      // 0 = puerto efímero (útil para tests)
    bool quiet = false;
    bool debug = false;
    size_t queue_cap = 1024;   // máx. mensajes en cola por cliente
    int idle_secs = 60;        // 0 = sin expiración
    int peer_sndbuf = 0;       // SO_SNDBUF por par; solo para diagnosticar colas
};

class Peer;

struct Peer;
using PeerPtr = std::shared_ptr<Peer>;

class Server {
public:
    explicit Server(const ServerOptions& opt);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    /// Crea el socket de escucha. Devuelve false si falla (p. ej. puerto
    /// ocupado).
    bool start();

    /// Puerto real que quedó enlazado (tras puerto efímero).
    uint16_t bound_port() const;

    /// Bucle de eventos hasta `stop()`. `max_poll_ms` limita cada espera de
    /// poll (útil para que los tests avancen).
    void loop(int max_poll_ms = 200);

    void stop();

    /// Bandera externa para cortar el bucle (p. ej. un manejador de señales).
    void set_stop_flag(std::atomic<bool>* flag) { external_stop_ = flag; }

    // Contadores (para tests y STATS).
    uint64_t messages_routed() const { return routed_; }
    uint64_t messages_dropped() const { return dropped_; }
    size_t peer_count() const { return peers_.size(); }
    uint64_t msg_id() const { return msg_id_; }
    bool running() const { return running_; }

private:
    void accept_one();
    void handle_hello(const PeerPtr& p, const Frame& f);
    void handle_sub(const PeerPtr& p, const Frame& f);
    void handle_unsub(const PeerPtr& p, const Frame& f);
    void handle_pub(const PeerPtr& p, const Frame& f);
    void handle_list(const PeerPtr& p);
    void handle_stats(const PeerPtr& p);
    void enqueue(const PeerPtr& p, const Frame& f);
    void flush_peer(const PeerPtr& p);
    void drop_peer(const PeerPtr& p, const std::string& why);
    std::string stats_text() const;
    void send_error(const PeerPtr& p, const std::string& msg);

    ServerOptions opt_;
    int listen_fd_ = -1;
    bool running_ = false;
    std::atomic<bool>* external_stop_ = nullptr;
    int next_peer_id_ = 1;
    uint64_t msg_id_ = 0;
    uint64_t routed_ = 0;
    uint64_t dropped_ = 0;
    std::deque<PeerPtr> peers_;
    std::map<std::string, uint64_t> topic_msgs_; // topic -> mensajes
};

} // namespace polea

#endif // POLEA_SERVER_H