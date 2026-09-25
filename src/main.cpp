/// Interfaz de línea de comandos de polea.
///
/// Uso:
///   polea serve      [-p puerto] [-q] [-d] [-t cola] [-i idle]
///   polea listen     <host:puerto> <topic...> [-n nombre] [-q]
///   polea say        <host:puerto> <topic> <texto> [-a] [-n nombre]
///   polea status     <host:puerto> [-n nombre]

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "polea_client.h"
#include "polea_server.h"

using namespace polea;

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) {
    g_stop.store(true);
}

bool parse_hostport(const std::string& s, std::string& host, uint16_t& port) {
    auto pos = s.rfind(':');
    if (pos == std::string::npos) return false;
    host = s.substr(0, pos);
    try {
        port = static_cast<uint16_t>(std::stoi(s.substr(pos + 1)));
    } catch (...) {
        return false;
    }
    return !host.empty() && port > 0;
}

int cmd_serve(const std::vector<std::string>& args) {
    ServerOptions opt;
    for (size_t i = 0; i < args.size(); ++i) {
        auto& a = args[i];
        auto next = [&]() -> std::string {
            return i + 1 < args.size() ? args[++i] : "";
        };
        if (a == "-p") opt.port = static_cast<uint16_t>(std::stoi(next()));
        else if (a == "-q") opt.quiet = true;
        else if (a == "-d") opt.debug = true;
        else if (a == "-t") opt.queue_cap = static_cast<size_t>(std::stoi(next()));
        else if (a == "-i") opt.idle_secs = std::stoi(next());
        else {
            std::fprintf(stderr, "polea serve: opción desconocida `%s`\n", a.c_str());
            return 2;
        }
    }

    Server srv(opt);
    if (!srv.start()) {
        std::fprintf(stderr, "polea: no se pudo enlazar el puerto %u\n", opt.port);
        return 1;
    }
    std::printf("polea serve — broker pub/sub en 127.0.0.1:%u (cola=%zu, idle=%ds)\n",
                srv.bound_port(), opt.queue_cap, opt.idle_secs);
    std::printf("  Ctrl-C para detener.\n");

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    srv.set_stop_flag(&g_stop);

    while (!g_stop.load()) {
        srv.loop(200);
    }
    srv.stop();

    std::printf("\nresumen: %zu peers, %llu mensajes encaminados, %llu descartados\n",
                srv.peer_count(), (unsigned long long)srv.messages_routed(),
                (unsigned long long)srv.messages_dropped());
    return 0;
}

int cmd_listen(const std::vector<std::string>& args) {
    std::string host, name;
    uint16_t port = 0;
    bool quiet = false;
    std::vector<std::string> topics;
    for (size_t i = 0; i < args.size(); ++i) {
        auto& a = args[i];
        if (a == "-n" && i + 1 < args.size()) name = args[++i];
        else if (a == "-q") quiet = true;
        else if (a.find(':') != std::string::npos) {
            if (!parse_hostport(a, host, port)) {
                std::fprintf(stderr, "polea listen: `%s` no es host:puerto\n", a.c_str());
                return 2;
            }
        } else {
            topics.push_back(a);
        }
    }
    if (host.empty() || topics.empty()) {
        std::fprintf(stderr, "uso: polea listen <host:puerto> <topic...> [-n nombre]\n");
        return 2;
    }

    Client c;
    uint16_t id = c.connect(host, port, make_name(name.c_str()));
    if (id == 0) {
        std::fprintf(stderr, "polea: no se pudo conectar a %s:%u\n", host.c_str(), port);
        return 1;
    }
    if (!quiet) std::printf("[polea] conectado; id=%u\n", id);
    for (auto& t : topics) {
        if (!c.subscribe(t)) return 1;
        if (!quiet) std::printf("[polea] suscrito a «%s»\n", t.c_str());
    }
    std::vector<Frame> frames;
    for (;;) {
        int n = c.recv(200, frames);
        if (n < 0) {
            if (!quiet) std::fprintf(stderr, "[polea] conexión terminada\n");
            return 1;
        }
        for (auto& f : frames) {
            switch (f.type) {
                case Type::Deliver: {
                    if (f.payload.size() < 8) continue;
                    std::string body(f.payload.begin() + 8, f.payload.end());
                    std::printf("[%s] %s\n", f.topic.c_str(), body.c_str());
                    std::fflush(stdout);
                    break;
                }
                case Type::Error:
                    std::printf("[polea] servidor: %s\n",
                                std::string(f.payload.begin(), f.payload.end()).c_str());
                    break;
                default:
                    break;
            }
        }
        frames.clear();
    }
    return 0;
}

int cmd_say(const std::vector<std::string>& args) {
    std::string host, topic, text, name;
    uint16_t port = 0;
    bool ack = false;
    for (size_t i = 0; i < args.size(); ++i) {
        auto& a = args[i];
        if (a == "-a") ack = true;
        else if (a == "-n" && i + 1 < args.size()) name = args[++i];
        else if (a.find(':') != std::string::npos) {
            if (!parse_hostport(a, host, port)) return 2;
        } else if (topic.empty()) topic = a;
        else text = a;
    }
    if (host.empty() || topic.empty()) {
        std::fprintf(stderr, "uso: polea say <host:puerto> <topic> <texto> [-a]\n");
        return 2;
    }
    Client c;
    if (c.connect(host, port, make_name(name.c_str())) == 0) {
        std::fprintf(stderr, "polea: no se pudo conectar a %s:%u\n", host.c_str(), port);
        return 1;
    }
    if (!c.publish(topic, text, ack)) {
        std::fprintf(stderr, "polea: error al publicar\n");
        return 1;
    }
    if (ack) {
        std::vector<Frame> frames;
        int n = c.recv(2000, frames);
        for (auto& f : frames) {
            if (f.type == Type::PubAck && f.payload.size() >= 8) {
                uint64_t mid = f.msg_id();
                std::printf("ack id=%llu\n", (unsigned long long)mid);
                return 0;
            }
        }
        (void)n;
    }
    return 0;
}

int cmd_status(const std::vector<std::string>& args) {
    std::string host, name;
    uint16_t port = 0;
    for (size_t i = 0; i < args.size(); ++i) {
        auto& a = args[i];
        if (a == "-n" && i + 1 < args.size()) name = args[++i];
        else if (a.find(':') != std::string::npos) {
            if (!parse_hostport(a, host, port)) return 2;
        }
    }
    if (host.empty()) {
        std::fprintf(stderr, "uso: polea status <host:puerto>\n");
        return 2;
    }
    Client c;
    if (c.connect(host, port, make_name(name.c_str())) == 0) {
        std::fprintf(stderr, "polea: no se pudo conectar a %s:%u\n", host.c_str(), port);
        return 1;
    }
    c.request_stats();
    c.request_list();
    std::vector<Frame> frames;
    bool got_stats = false, got_list = false;
    while (!(got_stats && got_list)) {
        int n = c.recv(1000, frames);
        if (n < 0) return 1;
        for (auto& f : frames) {
            if (f.type == Type::StatsReply) {
                std::string s(f.payload.begin(), f.payload.end());
                std::printf("-- estado --\n%s", s.c_str());
                got_stats = true;
            } else if (f.type == Type::ListReply) {
                std::string s(f.payload.begin(), f.payload.end());
                std::printf("-- temas --\n");
                if (s.empty()) std::printf("(ninguno)\n");
                else std::printf("%s", s.c_str());
                got_list = true;
            }
        }
        frames.clear();
    }
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    if (argc < 2) {
        std::printf(
            "polea — broker pub/sub con protocolo binario propio (CRC32)\n\n"
            "Uso:\n"
            "  polea serve   [-p puerto] [-q] [-d] [-t cola] [-i idle_secs]\n"
            "  polea listen  <host:puerto> <topic...> [-n nombre]\n"
            "  polea say     <host:puerto> <topic> <texto> [-a] [-n nombre]\n"
            "  polea status  <host:puerto> [-n nombre]\n");
        return 2;
    }
    std::vector<std::string> args(argv + 2, argv + argc);
    std::string cmd = argv[1];
    if (cmd == "serve") return cmd_serve(args);
    if (cmd == "listen") return cmd_listen(args);
    if (cmd == "say") return cmd_say(args);
    if (cmd == "status") return cmd_status(args);
    std::fprintf(stderr, "polea: comando desconocido `%s`\n", cmd.c_str());
    return 2;
}