#include "polea_client.h"

#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace polea {

void Client::close() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    in_.clear();
}

uint16_t Client::connect(const std::string& host, uint16_t port, const std::string& name) {
    close();
    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    std::string portstr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portstr.c_str(), &hints, &res) != 0 || !res) {
        return 0;
    }
    fd_ = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd_ < 0) {
        freeaddrinfo(res);
        return 0;
    }
    int ok = ::connect(fd_, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    if (ok != 0) {
        close();
        return 0;
    }

    Frame hello;
    hello.type = Type::Hello;
    hello.payload.assign(name.begin(), name.end());
    if (!send_frame(hello)) {
        close();
        return 0;
    }
    std::vector<Frame> out;
    int n = recv(2000, out);
    if (n <= 0) {
        close();
        return 0;
    }
    for (auto& f : out) {
        if (f.type == Type::HelloAck && f.payload.size() >= 8) {
            peer_id_ = static_cast<uint16_t>(f.payload[0] | (f.payload[1] << 8));
            return peer_id_;
        }
        if (f.type == Type::Error) {
            close();
            return 0;
        }
    }
    close();
    return 0;
}

bool Client::send_frame(const Frame& f) {
    if (fd_ < 0) return false;
    std::vector<uint8_t> bytes =
        encode_frame(f.type, f.flags, f.topic, f.payload);
    size_t off = 0;
    while (off < bytes.size()) {
        ssize_t n = ::send(fd_, bytes.data() + off, bytes.size() - off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool Client::subscribe(const std::string& topic) {
    Frame f;
    f.type = Type::Sub;
    f.topic = topic;
    return send_frame(f);
}
bool Client::unsubscribe(const std::string& topic) {
    Frame f;
    f.type = Type::Unsub;
    f.topic = topic;
    return send_frame(f);
}
bool Client::publish(const std::string& topic, const std::string& payload, bool ack) {
    Frame f;
    f.type = Type::Pub;
    f.flags = ack ? FLAG_ACK : 0;
    f.topic = topic;
    f.payload.assign(payload.begin(), payload.end());
    return send_frame(f);
}
bool Client::request_list() {
    Frame f;
    f.type = Type::List;
    return send_frame(f);
}
bool Client::request_stats() {
    Frame f;
    f.type = Type::StatsReq;
    return send_frame(f);
}

int Client::recv(int timeout_ms, std::vector<Frame>& out) {
    if (fd_ < 0) return -1;
    struct pollfd pfd {fd_, POLLIN, 0};
    int r = poll(&pfd, 1, timeout_ms);
    if (r == 0) return 0;
    if (r < 0) return -1;
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) return -1;

    uint8_t buf[8192];
    for (;;) {
        ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
        if (n > 0) {
            in_.insert(in_.end(), buf, buf + n);
            break;
        }
        if (n == 0) return -1;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        if (errno == EINTR) continue;
        return -1;
    }

    size_t before = out.size();
    for (;;) {
        auto d = decode_frame(in_.data(), in_.size());
        if (!d.ok) {
            if (d.err == ProtoError::NeedMore) break;
            return -1;
        }
        in_.erase(in_.begin(), in_.begin() + d.consumed);
        out.push_back(std::move(d.frame));
    }
    return static_cast<int>(out.size() - before);
}

} // namespace polea