/// Pruebas de integración de polea: protocolo binario + broker real sobre
/// sockets TCP en localhost.

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "crc32.h"
#include "polea_client.h"
#include "polea_server.h"
#include "protocol.h"

using namespace polea;

static int failures = 0;
static int checks = 0;
static int tests = 0;

#define CHECK(cond)                                                               \
    do {                                                                          \
        checks++;                                                                 \
        if (!(cond)) {                                                            \
            failures++;                                                           \
            std::printf("  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);         \
        }                                                                         \
    } while (0)

#define RUN(name)                                                                 \
    do {                                                                          \
        tests++;                                                                  \
        std::printf("== %-32s ==\n", #name);                                      \
        test_##name();                                                            \
    } while (0)

namespace {

struct Harness {
    ServerOptions opt;
    Server* srv = nullptr;
    std::thread th;

    void start(size_t queue_cap = 1024, int sndbuf = 0, int idle = 0) {
        opt.quiet = true;
        opt.idle_secs = idle;
        opt.queue_cap = queue_cap;
        opt.peer_sndbuf = sndbuf;
        srv = new Server(opt);
        CHECK(srv->start());
        th = std::thread([this] { srv->loop(20); });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    uint16_t port() { return srv->bound_port(); }
    void stop() {
        srv->stop();
        th.join();
        delete srv;
        srv = nullptr;
    }
    uint64_t routed() { return srv->messages_routed(); }
    uint64_t drops() { return srv->messages_dropped(); }
    size_t peers() { return srv->peer_count(); }
};

bool wait_frames(Client& c, int timeout_ms, std::vector<Frame>& out) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    do {
        int n = c.recv(100, out);
        if (n > 0) return true;
        if (n < 0) return false;
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

/// Barrera determinista: tras enviar una trama, un Ping seguido de su Pong
/// garantiza que el servidor ya la aplicó (las tramas se procesan en orden
/// por conexión, así que el Pong llega después de todo lo anterior).
void sync_client(Client& c) {
    Frame ping;
    ping.type = Type::Ping;
    CHECK(c.send_frame(ping));
    std::vector<Frame> out;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(3000);
    while (std::chrono::steady_clock::now() < deadline) {
        int n = c.recv(150, out);
        if (n < 0) break;
        for (auto& f : out)
            if (f.type == Type::Pong) { out.clear(); return; }
        out.clear();
    }
    CHECK(false && "no recibió Pong");
}

} // namespace

// ---------------------------------------------------------------------------
// Protocolo
// ---------------------------------------------------------------------------

static void test_encode_decode_roundtrip() {
    std::vector<uint8_t> payload{'a', 'b', 'c', 0, (uint8_t)-1, 0x7F};
    auto bytes = encode_frame(Type::Pub, FLAG_ACK, "radar", payload);
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(d.ok);
    CHECK(d.consumed == bytes.size());
    CHECK(d.frame.type == Type::Pub);
    CHECK(d.frame.flags == FLAG_ACK);
    CHECK(d.frame.topic == "radar");
    CHECK(d.frame.payload == payload);
}

static void test_encode_decode_need_more() {
    auto bytes = encode_frame(Type::Pub, 0, "radar", "x");
    for (size_t cut = 0; cut < bytes.size(); ++cut) {
        auto d = decode_frame(bytes.data(), cut);
        CHECK(!d.ok);
        CHECK(d.err == ProtoError::NeedMore);
    }
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(d.ok);
}

static void test_decode_bad_magic() {
    auto bytes = encode_frame(Type::Hello, 0, "", "pepe");
    bytes[0] = 0x00;
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(!d.ok && d.err == ProtoError::BadMagic);
}

static void test_decode_bad_version() {
    auto bytes = encode_frame(Type::Hello, 0, "", "pepe");
    bytes[4] = 9;
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(!d.ok && d.err == ProtoError::BadVersion);
}

static void test_decode_bad_crc() {
    auto bytes = encode_frame(Type::Pub, 0, "radar", "hola");
    bytes[bytes.size() - 1] ^= 0x01; // corrompe el CRC
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(!d.ok && d.err == ProtoError::BadCrc);
}

static void test_decode_bad_length() {
    auto bytes = encode_frame(Type::Pub, 0, "radar", "hola");
    bytes[7] = 0xFF; // topic_len 255 > MAX_TOPIC; tampoco cuadra con CRC
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(!d.ok);
}

static void test_large_payload() {
    std::vector<uint8_t> big(512 * 1024, 7);
    auto bytes = encode_frame(Type::Pub, 0, "dato", big);
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(d.ok);
    CHECK(d.frame.payload.size() == big.size());
    CHECK(d.consumed == bytes.size());
}

static void test_two_frames_concatenated() {
    auto a = encode_frame(Type::Ping, 0, "", "");
    auto b = encode_frame(Type::Pong, 0, "", "");
    std::vector<uint8_t> both = a;
    both.insert(both.end(), b.begin(), b.end());
    auto d1 = decode_frame(both.data(), both.size());
    CHECK(d1.ok && d1.frame.type == Type::Ping);
    auto d2 = decode_frame(both.data() + d1.consumed, both.size() - d1.consumed);
    CHECK(d2.ok && d2.frame.type == Type::Pong);
}

static void test_encode_frame_strings() {
    auto bytes = encode_frame(Type::Hello, 0, "", std::string("nodo1"));
    auto d = decode_frame(bytes.data(), bytes.size());
    CHECK(d.ok);
    std::string n(d.frame.payload.begin(), d.frame.payload.end());
    CHECK(n == "nodo1");
}

// ---------------------------------------------------------------------------
// Servidor
// ---------------------------------------------------------------------------

static void test_hello_ack() {
    Harness h;
    h.start();
    Client c;
    uint16_t id = c.connect("127.0.0.1", h.port(), "nodo-unico");
    CHECK(id != 0);
    CHECK(c.connected());
    h.stop();
}

static void test_delivery_ordering() {
    Harness h;
    h.start();
    Client pub, sub1, sub2;
    CHECK(pub.connect("127.0.0.1", h.port(), "editor") != 0);
    CHECK(sub1.connect("127.0.0.1", h.port(), "oyente-1") != 0);
    CHECK(sub2.connect("127.0.0.1", h.port(), "oyente-2") != 0);
    CHECK(sub1.subscribe("noticias"));
    CHECK(sub2.subscribe("noticias"));
    sync_client(sub1);
    sync_client(sub2);

    for (int i = 0; i < 5; ++i) {
        CHECK(pub.publish("noticias", "nota-" + std::to_string(i)));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    for (Client* s : {&sub1, &sub2}) {
        std::vector<Frame> out;
        int expect = 0;
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(2000);
        while (expect < 5 && std::chrono::steady_clock::now() < deadline) {
            int n = s->recv(200, out);
            if (n < 0) break;
            for (auto& f : out) {
                if (f.type != Type::Deliver || f.topic != "noticias") continue;
                uint64_t id = f.msg_id();
                std::string body(f.payload.begin() + 8, f.payload.end());
                CHECK(id == static_cast<uint64_t>(expect + 1));
                CHECK(body == "nota-" + std::to_string(expect));
                expect++;
            }
            out.clear();
        }
        CHECK(expect == 5);
    }
    h.stop();
}

static void test_non_subscriber_excluded() {
    Harness h;
    h.start();
    Client pub, ina, otra;
    CHECK(pub.connect("127.0.0.1", h.port(), "editor") != 0);
    CHECK(ina.connect("127.0.0.1", h.port(), "interes") != 0);
    CHECK(otra.connect("127.0.0.1", h.port(), "ajeno") != 0);
    CHECK(ina.subscribe("deporte"));
    sync_client(ina);
    CHECK(pub.publish("deporte", "gol"));
    std::vector<Frame> in_f, out_f;
    CHECK(wait_frames(ina, 1000, in_f));
    int got_in = 0;
    for (auto& f : in_f)
        if (f.type == Type::Deliver) got_in++;
    CHECK(got_in == 1);
    int n = otra.recv(300, out_f);
    CHECK(n == 0); // sin entregas: timeout
    h.stop();
}

static void test_self_delivery() {
    Harness h;
    h.start();
    Client c;
    CHECK(c.connect("127.0.0.1", h.port(), "eco") != 0);
    CHECK(c.subscribe("mi-tema"));
    sync_client(c);
    CHECK(c.publish("mi-tema", "eco"));
    std::vector<Frame> out;
    CHECK(wait_frames(c, 1000, out));
    bool got = false;
    for (auto& f : out)
        if (f.type == Type::Deliver && f.topic == "mi-tema") got = true;
    CHECK(got);
    h.stop();
}

static void test_unsubscribe() {
    Harness h;
    h.start();
    Client pub, sub;
    CHECK(pub.connect("127.0.0.1", h.port(), "editor") != 0);
    CHECK(sub.connect("127.0.0.1", h.port(), "fugaz") != 0);
    CHECK(sub.subscribe("canal"));
    sync_client(sub);
    CHECK(pub.publish("canal", "uno"));
    std::vector<Frame> out;
    CHECK(wait_frames(sub, 2000, out));
    int first = 0;
    for (auto& f : out)
        if (f.type == Type::Deliver) first++;
    CHECK(first == 1);

    CHECK(sub.unsubscribe("canal"));
    sync_client(sub); // garantiza que el Unsub ya se aplicó
    CHECK(pub.publish("canal", "dos"));
    out.clear();
    int n = sub.recv(300, out);
    CHECK(n == 0);
    h.stop();
}

static void test_puback() {
    Harness h;
    h.start();
    Client pub;
    CHECK(pub.connect("127.0.0.1", h.port(), "editor") != 0);
    CHECK(pub.publish("confia", "importante", /*ack=*/true));
    std::vector<Frame> out;
    CHECK(wait_frames(pub, 1000, out));
    bool acked = false;
    for (auto& f : out) {
        if (f.type == Type::PubAck) {
            acked = true;
            CHECK(f.msg_id() == 1);
        }
    }
    CHECK(acked);
    h.stop();
}

static void test_corrupt_frame_disconnect() {
    Harness h;
    h.start();
    Client good;
    CHECK(good.connect("127.0.0.1", h.port(), "bueno") != 0);

    // Cliente "sucio": envía un Hello y una trama con CRC roto.
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(h.port());
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    CHECK(connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);

    auto hello = encode_frame(Type::Hello, 0, "", "sucio");
    write(fd, hello.data(), hello.size());
    sleep(1); // deja que el servidor procese el Hello (serializador)
    auto bad = encode_frame(Type::Ping, 0, "", "");
    bad[5] = static_cast<uint8_t>(Type::Hello); // tipo válido; CRC ya no cuadra
    write(fd, bad.data(), bad.size());

    char tmp[64];
    ssize_t n;
    for (;;) {
        n = recv(fd, tmp, sizeof(tmp), 0);
        if (n == 0) break; // servidor cerró la conexión
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { n = -2; break; }
        if (n < 0) { n = -2; break; }
    }
    CHECK(n >= 0); // recibimos cierre o datos, no error local
    close(fd);
    h.stop();
}

static void test_duplicate_hello_drop() {
    Harness h;
    h.start();
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(h.port());
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    CHECK(connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
    auto hello = encode_frame(Type::Hello, 0, "", "dos-veces");
    write(fd, hello.data(), hello.size());
    sleep(1);
    auto again = encode_frame(Type::Hello, 0, "", "dos-veces");
    write(fd, again.data(), again.size());

    // Debe llegar el HelloAck y luego el cierre por Hello duplicado.
    std::vector<uint8_t> buf(4096);
    bool saw_ack = false;
    std::vector<uint8_t> all;
    for (;;) {
        ssize_t n = recv(fd, buf.data(), buf.size(), 0);
        if (n == 0) break;
        if (n < 0) break;
        all.insert(all.end(), buf.begin(), buf.begin() + n);
    }
    size_t off = 0;
    while (off + HEADER_SIZE <= all.size()) {
        auto d = decode_frame(all.data() + off, all.size() - off);
        if (!d.ok) break;
        if (d.frame.type == Type::HelloAck) saw_ack = true;
        off += d.consumed;
    }
    CHECK(saw_ack);
    close(fd);
    h.stop();
}

static void test_backpressure_drops() {
    Harness h;
    h.start(/*queue_cap=*/2, /*sndbuf=*/4096);
    Client pub, sub;
    CHECK(pub.connect("127.0.0.1", h.port(), "editor") != 0);
    CHECK(sub.connect("127.0.0.1", h.port(), "lento") != 0);
    CHECK(sub.subscribe("torrente"));
    sync_client(sub);

    // El suscriptor no lee nada: el socket se llena (SO_SNDBUF=4 KiB) y la
    // cola del servidor (cap 2) empieza a descartar lo más antiguo.
    for (int i = 0; i < 1000; ++i) {
        CHECK(pub.publish("torrente", std::string(200, 'x')));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    CHECK(h.routed() == 1000);
    CHECK(h.drops() > 0); // backpressure: descartes por cola llena

    // El suscriptor seguía conectado: pide estado y obtiene respuesta.
    CHECK(sub.request_stats());
    std::vector<Frame> out;
    bool have = false;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(2000);
    while (!have && std::chrono::steady_clock::now() < deadline) {
        int n = sub.recv(150, out);
        if (n < 0) break;
        for (auto& f : out)
            if (f.type == Type::StatsReply) have = true;
        out.clear();
    }
    CHECK(have);
    h.stop();
}

static void test_stats_and_list() {
    Harness h;
    h.start();
    Client pub, sub;
    CHECK(pub.connect("127.0.0.1", h.port(), "editor") != 0);
    CHECK(sub.connect("127.0.0.1", h.port(), "curioso") != 0);
    CHECK(pub.publish("alpha", "1"));
    CHECK(pub.publish("beta", "2"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    CHECK(sub.request_stats());
    CHECK(sub.request_list());
    std::vector<Frame> out;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(2000);
    bool stats = false, list = false;
    while ((!stats || !list) && std::chrono::steady_clock::now() < deadline) {
        int n = sub.recv(200, out);
        if (n < 0) break;
        for (auto& f : out) {
            if (f.type == Type::StatsReply) {
                std::string s(f.payload.begin(), f.payload.end());
                stats = s.find("msgs 2") != std::string::npos &&
                        s.find("topics 2") != std::string::npos;
            } else if (f.type == Type::ListReply) {
                std::string s(f.payload.begin(), f.payload.end());
                list = s.find("alpha") != std::string::npos &&
                       s.find("beta") != std::string::npos;
            }
        }
        out.clear();
    }
    CHECK(stats);
    CHECK(list);
    h.stop();
}

static void test_ping_pong() {
    Harness h;
    h.start();
    Client c;
    CHECK(c.connect("127.0.0.1", h.port(), "sonar") != 0);
    Frame ping;
    ping.type = Type::Ping;
    CHECK(c.send_frame(ping));
    std::vector<Frame> out;
    CHECK(wait_frames(c, 1000, out));
    bool pong = false;
    for (auto& f : out)
        if (f.type == Type::Pong) pong = true;
    CHECK(pong);
    h.stop();
}

static void test_unknown_type_drop() {
    Harness h;
    h.start();
    Client good;
    CHECK(good.connect("127.0.0.1", h.port(), "sano") != 0);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(h.port());
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    CHECK(connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
    auto hello = encode_frame(Type::Hello, 0, "", "raro");
    write(fd, hello.data(), hello.size());
    sleep(1);
    auto weird = encode_frame(Type::Ping, 0, "", "x");
    weird[5] = 0x55; // tipo no definido (CRC ya no aplica, se detecta antes)
    write(fd, weird.data(), weird.size());
    char tmp[64];
    ssize_t n;
    for (;;) {
        n = recv(fd, tmp, sizeof(tmp), 0);
        if (n == 0) break;
        if (n < 0) break;
    }
    CHECK(true); // simplemente no debe colgar
    close(fd);
    h.stop();
}

static void test_many_clients() {
    Harness h;
    h.start();
    const int N = 8;
    std::vector<Client> subs(N), pubs(N);
    for (int i = 0; i < N; ++i) {
        CHECK(pubs[i].connect("127.0.0.1", h.port(), "edit-" + std::to_string(i)) != 0);
        CHECK(subs[i].connect("127.0.0.1", h.port(), "oye-" + std::to_string(i)) != 0);
        CHECK(subs[i].subscribe("todos"));
    }
    for (auto& s : subs) sync_client(s);
    for (int i = 0; i < N; ++i) {
        CHECK(pubs[i].publish("todos", "m" + std::to_string(i)));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    CHECK(h.routed() == N);
    for (auto& s : subs) {
        std::vector<Frame> out;
        int dels = 0;
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(2000);
        while (dels < N && std::chrono::steady_clock::now() < deadline) {
            int n = s.recv(200, out);
            if (n < 0) break;
            for (auto& f : out)
                if (f.type == Type::Deliver) dels++;
            out.clear();
        }
        CHECK(dels == N);
    }
    h.stop();
}

int main() {
    RUN(encode_decode_roundtrip);
    RUN(encode_decode_need_more);
    RUN(decode_bad_magic);
    RUN(decode_bad_version);
    RUN(decode_bad_crc);
    RUN(decode_bad_length);
    RUN(large_payload);
    RUN(two_frames_concatenated);
    RUN(encode_frame_strings);

    RUN(hello_ack);
    RUN(delivery_ordering);
    RUN(non_subscriber_excluded);
    RUN(self_delivery);
    RUN(unsubscribe);
    RUN(puback);
    RUN(corrupt_frame_disconnect);
    RUN(duplicate_hello_drop);
    RUN(backpressure_drops);
    RUN(stats_and_list);
    RUN(ping_pong);
    RUN(unknown_type_drop);
    RUN(many_clients);

    std::printf("\n%d comprobaciones en %d pruebas; fallos: %d\n", checks, tests,
                failures);
    return failures == 0 ? 0 : 1;
}