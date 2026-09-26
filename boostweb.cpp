#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <array>
#include <memory>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <charconv>
#include <cstdlib>
#include<algorithm>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

namespace net = boost::asio;
namespace ssl = net::ssl;
namespace websocket = boost::beast::websocket;
using tcp = net::ip::tcp;

// =========================================================================
// CPU RELAX MACRO FOR LOW-LATENCY BUSY SPIN
// =========================================================================
#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CPU_RELAX() _mm_pause()
#elif defined(__aarch64__)
#define CPU_RELAX() asm volatile("yield" ::: "memory")
#else
#define CPU_RELAX() std::this_thread::yield()
#endif


enum class MessageType :uint8_t{
    Trade,
    BookTicker
};

struct MarketMessage{
    double price;
    double quantity;
    MessageType type;
};
// =========================================================================
// TEMPLATED LOCK-FREE RING BUFFER
// =========================================================================
template <size_t Capacity>
class FixedRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2!");

    struct Slot {
        MarketMessage message;
        std::chrono::steady_clock::time_point timestamp;
    };

    const size_t capacity_ = Capacity;
    const size_t mask_ = Capacity - 1;

    std::unique_ptr<Slot[]> buffer_;

    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};

public:
    FixedRingBuffer() : buffer_(std::make_unique<Slot[]>(Capacity)) {}

    bool try_push(const MarketMessage& message, std::chrono::steady_clock::time_point timestamp){
        

        size_t current_head = head_.load(std::memory_order_relaxed);
        size_t next_head = (current_head + 1) & mask_;

        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        auto& slot = buffer_[current_head];
        slot.message=message;
        slot.timestamp=timestamp;
        

        head_.store(next_head, std::memory_order_release);
        return true;
    }

    bool try_pop(MarketMessage& out_message, std::chrono::steady_clock::time_point& out_ts) {

        size_t current_tail = tail_.load(std::memory_order_relaxed);

        if (current_tail == head_.load(std::memory_order_acquire)){ 
            return false;
        }
        const auto& slot = buffer_[current_tail];
        out_message = slot.message;
        out_ts = slot.timestamp;
        return true;
    }

    void consume() {
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = (current_tail + 1) & mask_;
        tail_.store(next_tail, std::memory_order_release);
    }
};

// =========================================================================
// ZERO-ALLOCATION PARSING HELPERS (MACOS SAFE)
// =========================================================================
inline std::string_view extract_json_field(std::string_view json, std::string_view key) {
    size_t key_pos = json.find(key);
    if (key_pos == std::string_view::npos) return {};

    size_t val_start = key_pos + key.length();
    size_t val_end = json.find('"', val_start);
    if (val_end == std::string_view::npos) return {};

    return json.substr(val_start, val_end - val_start);
}

inline double fast_atof(std::string_view sv) {
    char buf[32];
    size_t len = std::min(sv.size(), sizeof(buf) - 1);
    std::memcpy(buf, sv.data(), len);
    buf[len] = '\0';
    
    char* end;
    return std::strtod(buf, &end);
}
bool parse_market_message(
    std::string_view json,
    MarketMessage& message)
{
    std::string_view price_str =
        extract_json_field(json, R"("p":")");

    std::string_view qty_str =
        extract_json_field(json, R"("q":")");

    if (price_str.empty() || qty_str.empty())
        return false;

    message.price = fast_atof(price_str);
    message.quantity = fast_atof(qty_str);

    message.type = MessageType::Trade;

    return true;
}

// =========================================================================
// WEBSOCKET CLIENT
// =========================================================================
class LowLatencyBinance : public std::enable_shared_from_this<LowLatencyBinance> {
    tcp::resolver resolver_;
    websocket::stream<ssl::stream<tcp::socket>> ws_;
    boost::beast::flat_buffer buffer_;
    FixedRingBuffer<1024>& ring_buffer_;
    std::atomic<uint64_t>& dropped_messages_;
    std::string host_ = "stream.binance.com";
    std::string port_ = "9443";

public:
    LowLatencyBinance(net::io_context& ioc, ssl::context& ctx, FixedRingBuffer<1024>& ring_buffer,std::atomic<uint64_t>&dropped_messages)
        : resolver_(ioc), ws_(ioc, ctx), ring_buffer_(ring_buffer),dropped_messages_(dropped_messages) {}

    void start() {
        resolver_.async_resolve(host_, port_,
            std::bind(&LowLatencyBinance::on_resolve, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
    }

private:
    void on_resolve(boost::system::error_code ec, tcp::resolver::results_type results) {
        if (ec) return;
        net::async_connect(ws_.next_layer().next_layer(), results,
            std::bind(&LowLatencyBinance::on_connect, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
    }

    void on_connect(boost::system::error_code ec, tcp::endpoint) {
        if (ec) return;
        ws_.next_layer().async_handshake(ssl::stream_base::client,
            std::bind(&LowLatencyBinance::on_ssl_handshake, shared_from_this(), std::placeholders::_1));
    }

    void on_ssl_handshake(boost::system::error_code ec) {
        if (ec) return;
        ws_.async_handshake(host_, "/ws",
            std::bind(&LowLatencyBinance::on_ws_handshake, shared_from_this(), std::placeholders::_1));
    }

    void on_ws_handshake(boost::system::error_code ec) {
        if (ec) return;

        auto sub = std::make_shared<std::string>(R"({
            "method": "SUBSCRIBE",
            "params": [
               "btcusdt@trade",
                "ethusdt@trade",
                "solusdt@trade"
            ],
            "id": 1
        })");

        ws_.async_write(net::buffer(*sub), [self = shared_from_this(), sub](boost::system::error_code ec, std::size_t) {
            if (!ec) self->do_read();
        });
    }


    void do_read() {
        ws_.async_read(
            buffer_,
            [self = shared_from_this()](boost::system::error_code ec, std::size_t bytes_transferred) {
                if (!ec) {
                    //std::cout<<"raw"<<boost::beast::buffers_to_string(self->buffer_.data())<<"\n";

                    auto data = self->buffer_.data();

                    std::string_view json(
                        static_cast<const char*>(data.data()),
                        bytes_transferred
                    );

                    MarketMessage message;

                    if (parse_market_message(json, message)) {

                        auto timestamp =std::chrono::steady_clock::now();

                        if(!self->ring_buffer_.try_push(message,timestamp)){
    
                            self->dropped_messages_.fetch_add(1,std::memory_order_relaxed);
                        }
                    }
                

                    self->buffer_.consume(bytes_transferred);

                    self->do_read();
                
                }
            }   
    
    );
}


};
    


// =========================================================================
// MAIN LOGIC
// =========================================================================
int main() {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tls_client);
    std::atomic<uint64_t> total_messages {0};
    std::atomic<uint64_t> dropped_messages{0};
    std::atomic<uint64_t> total_latency_ns{0};
    std::atomic<uint64_t> max_latency_ns{0};

    FixedRingBuffer<1024> ring_buffer;
    std::vector<uint64_t>latency_samples;
    latency_samples.reserve(1'000'000);

    std::make_shared<LowLatencyBinance>(ioc, ctx, ring_buffer,dropped_messages)->start();

    std::atomic<bool> running{true};


    auto start_time = std::chrono::high_resolution_clock::now();

    std::thread processor_thread([&]() {
        MarketMessage message;
        std::chrono::steady_clock::time_point push_ts;

        while (running.load(std::memory_order_relaxed)) {
            if (ring_buffer.try_pop(message, push_ts)) {
                auto pop_ts = std::chrono::steady_clock::now();
                uint64_t lat = std::chrono::duration_cast<std::chrono::nanoseconds>(pop_ts - push_ts).count();

                uint64_t current_msg_count = total_messages.fetch_add(1,std::memory_order_relaxed)+1;
                if (current_msg_count > 10) { // Warmup period
                    total_latency_ns.fetch_add(lat, std::memory_order_relaxed);
                    latency_samples.push_back(lat);

                    uint64_t current_max = max_latency_ns.load(std::memory_order_relaxed);
                    while (lat > current_max && !max_latency_ns.compare_exchange_weak(current_max, lat,std::memory_order_relaxed)) {}
                }

                if(current_msg_count<=5){
                    std::cout
                    <<"[parsed trade] price:"
                    <<message.price
                    <<"qty:"
                    <<message.quantity
                    <<"\n";
                }               

                ring_buffer.consume();
            } else {
                CPU_RELAX();
            }
        }
    });

    std::thread io_thread([&ioc]() { ioc.run(); });

    for (int i = 0; i < 24; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto now = std::chrono::high_resolution_clock::now();
        double elapsed_sec = std::chrono::duration<double>(now - start_time).count();
        uint64_t count = total_messages.load(std::memory_order_relaxed);

        double msg_per_sec = count / elapsed_sec;
        uint64_t tracked_count = count > 10 ? count - 10 : 0;
        double avg_lat = tracked_count > 0 ? static_cast<double>(
            total_latency_ns.load(std::memory_order_relaxed)
        )/tracked_count:0.0;
    
      
        std::cout << "================ METRICS ================\n";
        std::cout << " Total Messages Received : " << count << "\n";
        std::cout << " dropped.                : " <<dropped_messages.load() <<"\n";
        std::cout << " Throughput              : " << msg_per_sec << " msg/sec\n";
        std::cout << " Avg Queue Latency       : " << avg_lat << " ns (" << (avg_lat / 1000.0) << " us)\n";
        std::cout << " Max Queue Latency       : " << max_latency_ns.load() << " ns (" << (max_latency_ns.load() / 1000.0) << " us)\n";
        std::cout << "=========================================\n\n";
    }
        
    ioc.stop();
    running = false;
    if (io_thread.joinable()) io_thread.join();
    if (processor_thread.joinable()) processor_thread.join();
    std::sort(latency_samples.begin(),latency_samples.end());
    auto percentile=[&](double p)->uint64_t{
        if(latency_samples.empty()){
            return 0;
        }
        size_t index=static_cast<size_t>(
            p*(latency_samples.size()-1)
        );
        return latency_samples[index];
        };

    uint64_t p50=percentile(0.50);
    uint64_t p99=percentile(0.99);
    uint64_t p999=percentile(0.999);
    std::cout<<"p50.   :   "<<"ns ("<<p50/1000.0<<"us)\n";
    std::cout<<"p99.   :   "<<"ns("<<p99/1000.0<<"us)\n";
    std::cout<<"p999.  :  "<<"ns("<<p999/1000.0<<"us)\n";

    return 0;
}