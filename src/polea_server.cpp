#include "polea_server.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "crc32.h"

namespace polea {

namespace {
// Devuelve el instante en segundos desde un origen arbitrario (para idle).
int64_t now_secs() {
    using namespace std::chrono;
    return duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
}

bool set_nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    return fl >= 0 && fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0;
}
} // namespace

// Un cliente conectado: búfer de entrada, cola de salida acotada y su
// estado de suscripciones.
struct Peer {
    int fd = -1;
    int id = 0;
    std::string name;
    std::vector<uint8_t> in;
    std::vector<uint8_t> pending;      // bytes de la trama en curso de envío
    std::deque<Frame> out;             // tramas completas salientes
    std::set<std::string> subs;
    int64_t last_seen = now_secs();
    uint64_t sent = 0;                 // tramas entregadas a este cliente
    uint64_t dropped = 0;              // mensajes descartados por cola llena
    bool write_error = false;
};

Server::Server(const ServerOptions& opt) : opt_(opt) {}
Server::~Server() {
    stop();
    for (auto& p : peers_) {
        if (p->fd >= 0) ::close(p->fd);
        p->fd = -1;
    }
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

bool Server::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

    int yes = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(opt_.port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    if (listen(listen_fd_, 32) != 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    set_nonblock(listen_fd_);
    running_ = true;
    return true;
}

uint16_t Server::bound_port() const {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return 0;
    return ntohs(addr.sin_port);
}

void Server::stop() {
    running_ = false;
}

void Server::accept_one() {
    for (;;) {
        int c = accept(listen_fd_, nullptr, nullptr);
        if (c < 0) break;
        set_nonblock(c);
        if (opt_.peer_sndbuf > 0) {
            setsockopt(c, SOL_SOCKET, SO_SNDBUF, &opt_.peer_sndbuf,
                       static_cast<socklen_t>(sizeof(int)));
        }
        auto p = std::make_shared<Peer>();
        p->fd = c;
        p->id = next_peer_id_++;
        peers_.push_back(p);
        if (!opt_.quiet) {
            std::printf("  + conexión #%d (esperando Hello)\n", p->id);
        }
    }
}

void Server::drop_peer(const PeerPtr& p, const std::string& why) {
    if (p->fd < 0) return;
    if (!opt_.quiet || !why.empty()) {
        std::printf("  - #%d (%s) cerrado: %s\n", p->id, p->name.c_str(), why.c_str());
    }
    ::close(p->fd);
    p->fd = -1;
    peers_.erase(std::remove(peers_.begin(), peers_.end(), p), peers_.end());
}

void Server::send_error(const PeerPtr& p, const std::string& msg) {
    Frame f;
    f.type = Type::Error;
    f.payload.assign(msg.begin(), msg.end());
    enqueue(p, f);
}

void Server::enqueue(const PeerPtr& p, const Frame& f) {
    if (p->out.size() >= opt_.queue_cap) {
        p->out.pop_front();
        p->dropped++;
        dropped_++;
    }
    p->out.push_back(f);
}

void Server::flush_peer(const PeerPtr& p) {
    while (!p->out.empty() || !p->pending.empty()) {
        if (p->pending.empty()) {
            p->pending = encode_frame(p->out.front().type, p->out.front().flags,
                                      p->out.front().topic, p->out.front().payload);
            p->out.pop_front();
        }
        ssize_t n = send(p->fd, p->pending.data(), p->pending.size(), MSG_DONTWAIT);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            p->write_error = true;
            return;
        }
        if (static_cast<size_t>(n) < p->pending.size()) {
            p->pending.erase(p->pending.begin(), p->pending.begin() + n);
            return;
        }
        p->pending.clear();
        p->sent++;
    }
}

void Server::handle_hello(const PeerPtr& p, const Frame& f) {
    if (!p->name.empty()) {
        send_error(p, "Hello duplicado");
        drop_peer(p, "protocolo: Hello duplicado");
        return;
    }
    std::string name(f.payload.begin(), f.payload.end());
    if (name.empty() || name.size() > 64) {
        drop_peer(p, "protocolo: nombre inválido");
        return;
    }
    p->name = name;
    std::vector<uint8_t> ack;
    for (int i = 0; i < 8; ++i)
        ack.push_back(static_cast<uint8_t>((static_cast<uint64_t>(p->id) >> (8 * i)) & 0xFF));
    Frame g;
    g.type = Type::HelloAck;
    g.payload = ack;
    enqueue(p, g);
    if (!opt_.quiet) {
        std::printf("  = #%d registrado como «%s»\n", p->id, p->name.c_str());
    }
}

void Server::handle_sub(const PeerPtr& p, const Frame& f) {
    if (f.topic.empty() || f.topic.size() > MAX_TOPIC) {
        send_error(p, "topic inválido");
        return;
    }
    p->subs.insert(f.topic);
    if (opt_.debug) std::printf("    #%d suscrito a «%s»\n", p->id, f.topic.c_str());
}

void Server::handle_unsub(const PeerPtr& p, const Frame& f) {
    p->subs.erase(f.topic);
    if (opt_.debug) std::printf("    #%d desuscrito de «%s»\n", p->id, f.topic.c_str());
}

void Server::handle_pub(const PeerPtr& p, const Frame& f) {
    if (f.topic.empty() || f.topic.size() > MAX_TOPIC) {
        send_error(p, "topic inválido");
        return;
    }
    // Un suscriptor también recibe lo que publica si está suscrito: se añade
    // a la cola como cualquier otro reenvío.
    uint64_t id = ++msg_id_;
    std::vector<uint8_t> idb;
    for (int i = 0; i < 8; ++i) idb.push_back(static_cast<uint8_t>((id >> (8 * i)) & 0xFF));
    std::vector<uint8_t> body = idb;
    body.insert(body.end(), f.payload.begin(), f.payload.end());
    Frame d;
    d.type = Type::Deliver;
    d.topic = f.topic;
    d.payload = body;
    for (auto& q : peers_) {
        if (q->subs.count(f.topic)) enqueue(q, d);
    }
    routed_ += 1;
    topic_msgs_[f.topic] += 1;
    if (opt_.debug) {
        std::printf("    «%s» <- %s (%llu B, id %llu)\n", f.topic.c_str(),
                    p->name.c_str(), (unsigned long long)f.payload.size(),
                    (unsigned long long)id);
    }
    if (f.flags & FLAG_ACK) {
        Frame ack;
        ack.type = Type::PubAck;
        ack.payload = idb;
        enqueue(p, ack);
    }
}

void Server::handle_list(const PeerPtr& p) {
    std::string out;
    for (auto& [topic, n] : topic_msgs_) {
        out += topic;
        out += '\n';
        (void)n;
    }
    Frame g;
    g.type = Type::ListReply;
    g.payload.assign(out.begin(), out.end());
    enqueue(p, g);
}

void Server::handle_stats(const PeerPtr& p) {
    std::string s = stats_text();
    Frame g;
    g.type = Type::StatsReply;
    g.payload.assign(s.begin(), s.end());
    enqueue(p, g);
}

std::string Server::stats_text() const {
    std::string s;
    s += "peers " + std::to_string(peers_.size()) + "\n";
    s += "topics " + std::to_string(topic_msgs_.size()) + "\n";
    s += "msgs " + std::to_string(routed_) + "\n";
    s += "drops " + std::to_string(dropped_) + "\n";
    s += "next_id " + std::to_string(msg_id_) + "\n";
    return s;
}

void Server::loop(int max_poll_ms) {
    while (running_ && !(external_stop_ && external_stop_->load())) {
        std::vector<pollfd> fds;
        fds.push_back({listen_fd_, POLLIN, 0});
        for (auto& p : peers_) {
            fds.push_back({p->fd,
                           static_cast<short>(POLLIN | (p->out.empty() ? 0 : POLLOUT)),
                           0});
        }
        // Copia de seguridad: los peers pueden caerse mientras se procesan y
        // desplazarse dentro de `peers_`; la captura mantiene la correspondencia
        // con `fds` (1:1 con el estado previo al poll).
        std::vector<PeerPtr> snapshot(peers_.begin(), peers_.end());

        int n = poll(fds.data(), fds.size(), max_poll_ms);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        if (fds[0].revents & POLLIN) accept_one();

        for (size_t i = 0; i < snapshot.size(); ++i) {
            auto p = snapshot[i];
            if (p->fd < 0) continue; // ya caído en esta pasada
            short ev = fds[i + 1].revents;
            if (ev & (POLLERR | POLLHUP | POLLNVAL)) {
                drop_peer(p, "socket");
                continue;
            }
            if (ev & POLLOUT) flush_peer(p);
            if (p->write_error) {
                drop_peer(p, "escritura");
                continue;
            }
            if (ev & POLLIN) {
                uint8_t buf[4096];
                for (;;) {
                    ssize_t r = recv(p->fd, buf, sizeof(buf), 0);
                    if (r > 0) {
                        p->in.insert(p->in.end(), buf, buf + r);
                        if (p->in.size() > MAX_PAYLOAD + HEADER_SIZE + CRC_SIZE + MAX_TOPIC + 4096) {
                            drop_peer(p, "trama excesiva");
                        }
                        break;
                    }
                    if (r == 0) {
                        drop_peer(p, "EOF");
                        break;
                    }
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    if (errno == EINTR) continue;
                    drop_peer(p, "lectura");
                    break;
                }
            }
            // Procesa todas las tramas completas ya recibidas.
            if (p->fd >= 0) {
                for (;;) {
                    auto d = decode_frame(p->in.data(), p->in.size());
                    if (!d.ok) {
                        if (d.err == ProtoError::NeedMore) break;
                        send_error(p, "protocolo");
                        drop_peer(p, protocol_error_text(d.err));
                        break;
                    }
                    if (d.ok) {
                        p->in.erase(p->in.begin(), p->in.begin() + d.consumed);
                        p->last_seen = now_secs();
                    }
                    switch (d.frame.type) {
                        case Type::Hello: handle_hello(p, d.frame); break;
                        case Type::Sub: handle_sub(p, d.frame); break;
                        case Type::Unsub: handle_unsub(p, d.frame); break;
                        case Type::Pub: handle_pub(p, d.frame); break;
                        case Type::List: handle_list(p); break;
                        case Type::StatsReq: handle_stats(p); break;
                        case Type::Ping: {
                            Frame po;
                            po.type = Type::Pong;
                            enqueue(p, po);
                            break;
                        }
                        default:
                            drop_peer(p, "tipo desconocido");
                    }
                    flush_peer(p);
                }
            }
        }

        // Expiración por inactividad.
        if (opt_.idle_secs > 0) {
            int64_t now = now_secs();
            for (auto it = peers_.begin(); it != peers_.end();) {
                if (now - (*it)->last_seen > opt_.idle_secs) {
                    std::printf("  - #%d expirado por inactividad\n", (*it)->id);
                    ::close((*it)->fd);
                    (*it)->fd = -1;
                    it = peers_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
}

} // namespace polea