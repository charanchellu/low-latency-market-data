# Low-Latency C++ Market Data Feed Handler

**A C++20 asynchronous market-data ingestion and processing system built with Boost.Asio, Boost.Beast, OpenSSL, and a custom lock-free SPSC ring buffer.**

The project focuses on building a low-latency pipeline for receiving live cryptocurrency market data, parsing incoming WebSocket messages, and transferring them to a dedicated processing thread with minimal synchronization overhead.

Beyond implementing the feed handler, the project explores performance engineering at the hardware and operating-system levels, including cache-line alignment, false sharing, atomic memory ordering, CPU affinity, and Linux CPU isolation.

---

## 1. System Architecture

The application separates network ingestion from market-data processing using a producer-consumer architecture.

```text
             Binance WebSocket Streams
                       |
                       v
              DNS Resolution
                       |
                       v
               TCP Connection
                       |
                       v
                TLS Handshake
                  (OpenSSL)
                       |
                       v
             WebSocket Handshake
                (Boost.Beast)
                       |
                       v
              Asynchronous Read
                (Boost.Asio)
                       |
                       v
               JSON Parsing
                       |
                       v
              SPSC Ring Buffer
                       |
                       v
              Processing Thread
                       |
                       v
             Market Data Processing
                       |
                       v
               Latency Metrics
```

### Threading architecture

| Thread | Responsibility |
|---|---|
| I/O thread | WebSocket communication, asynchronous reads, message parsing and queue production |
| Processing thread | Consumes messages from the SPSC ring buffer and measures queue latency |

The two threads communicate without using a conventional mutex on the SPSC handoff path.

---

## 2. Asynchronous Networking with Boost.Asio and Boost.Beast

The networking layer uses Boost.Asio and Boost.Beast to establish and maintain asynchronous WebSocket connections.

### Technologies

- **Boost.Asio:** Event-driven asynchronous networking and I/O execution.
- **Boost.Beast:** WebSocket protocol implementation.
- **OpenSSL:** TLS encryption for secure WebSocket connections.
- **Boost.JSON:** JSON message parsing.

### Connection lifecycle

The feed handler follows an asynchronous connection sequence:

1. DNS resolution using an Asio resolver.
2. TCP connection establishment.
3. TLS handshake using OpenSSL.
4. WebSocket handshake using Boost.Beast.
5. Subscription to market-data streams.
6. Continuous asynchronous message reception.

### Important Boost.Asio concepts

| Component | Purpose |
|---|---|
| `io_context` | Executes asynchronous operations and completion handlers |
| `async_resolve` | Resolves the exchange hostname |
| `async_connect` | Establishes the TCP connection |
| `ssl::stream` | Provides TLS transport |
| `websocket::stream` | Implements WebSocket communication |
| `async_handshake` | Establishes the WebSocket session |
| `async_read` | Receives incoming market-data messages |
| `buffer` | Provides message buffer sequences |
| `strand` | Serializes handlers when shared execution requires it |
| `executor_work_guard` | Keeps the event loop alive while work is outstanding |

The I/O thread runs the Asio event loop, allowing network operations to proceed through asynchronous completion handlers rather than blocking on each message.

---

## 3. Custom Lock-Free SPSC Ring Buffer

One of the central components is a custom Single-Producer Single-Consumer (SPSC) ring buffer.

Instead of passing market-data messages through a mutex-protected queue, the feed handler uses atomic producer and consumer indices.

### Why SPSC?

The architecture has exactly one producer and one consumer:

- Producer: Network I/O thread.
- Consumer: Processing thread.

This allows a simpler synchronization design than a general-purpose multi-producer, multi-consumer queue.

### Head and tail management

The ring buffer uses two atomic indices:

- `head`: Producer position.
- `tail`: Consumer position.

The producer writes a message into the current head slot and publishes the updated head.

The consumer checks the head, reads the published message, processes it, and advances the tail.

Conceptually:

```cpp
alignas(64) std::atomic<size_t> head{0};
alignas(64) std::atomic<size_t> tail{0};
```

### Producer operation

The producer follows this sequence:

1. Load the current head using relaxed memory ordering.
2. Calculate the next slot.
3. Check the consumer's tail using acquire ordering.
4. Return failure if the ring buffer is full.
5. Copy the incoming message into the slot.
6. Update the slot metadata.
7. Publish the new head using release ordering.

Conceptually:

```cpp
auto current_head = head.load(std::memory_order_relaxed);

auto next_head = (current_head + 1) & mask;

if (next_head == tail.load(std::memory_order_acquire)) {
    return false;
}

// Write message into the current slot.

head.store(next_head, std::memory_order_release);
```

### Consumer operation

The consumer:

1. Loads the current tail using relaxed ordering.
2. Checks the producer's head using acquire ordering.
3. Detects whether the queue is empty.
4. Obtains a view of the message.
5. Processes the message.
6. Advances the tail using release ordering.

Conceptually:

```cpp
auto current_tail = tail.load(std::memory_order_relaxed);

if (current_tail == head.load(std::memory_order_acquire)) {
    return false;
}

// Access the message in the current slot.

tail.store((current_tail + 1) & mask,
           std::memory_order_release);
```

The acquire-release relationship ensures that the consumer sees the producer's completed slot writes before reading the message.

---

## 4. Cache-Line Alignment and False Sharing

A critical optimization in the ring buffer is cache-line separation of the producer and consumer indices.

### The problem: False sharing

Modern CPUs maintain cache coherence between cores.

When two threads frequently modify different variables located on the same cache line, cache-coherence traffic can occur even though the variables are logically independent.

This is known as **false sharing**.

In an SPSC queue:

- The producer repeatedly updates `head`.
- The consumer repeatedly updates `tail`.

If both atomic variables occupy the same cache line, their independent updates can cause unnecessary cache-line invalidations.

### The solution: `alignas(64)`

The head and tail are separately cache-line aligned:

```cpp
alignas(64) std::atomic<size_t> head{0};
alignas(64) std::atomic<size_t> tail{0};
```

The intention is to place the producer and consumer indices on separate 64-byte boundaries, reducing false sharing on the target hardware.

This is especially relevant when the producer and consumer execute on different CPU cores.

**Optimization objective:** Reduce cache-coherence interference between the producer and consumer threads.

---

## 5. Message Storage and Consumer Access

The ring buffer uses individual slots to hold incoming market-data messages.

Each slot maintains message storage and its size.

Conceptually:

```cpp
struct Slot {
    std::vector<char> storage;
    size_t size = 0;
};
```

The slots are initially allocated with 4096 bytes of storage.

The consumer accesses the message through a `std::string_view` into the slot's storage instead of creating another message copy.

This reduces copying on the consumer side.

The consumer must finish processing the message before advancing the tail and allowing the producer to reuse that slot.

The ring buffer uses a one-empty-slot convention to distinguish full from empty states. With a capacity of 1024, the usable capacity is 1023 slots.

---

## 6. Memory Ordering

The implementation uses C++ atomic memory-ordering operations rather than relying on mutexes for producer-consumer synchronization.

| Operation | Memory ordering | Purpose |
|---|---|---|
| Producer head load | Relaxed | Access its own index |
| Producer tail load | Acquire | Observe consumer progress |
| Producer head store | Release | Publish completed slot writes |
| Consumer tail load | Relaxed | Access its own index |
| Consumer head load | Acquire | Observe published producer data |
| Consumer tail store | Release | Publish completion of consumption |

The release/acquire relationship is essential to ensuring that the consumer does not read a slot before the producer has published its contents.

---

## 7. Busy-Polling and Processing

The consumer thread continuously checks for available messages instead of sleeping and waiting on a conventional condition variable.

On x86 systems, the polling loop uses the CPU pause instruction:

```cpp
_mm_pause();
```

This provides a processor hint during spin-waiting.

The approach avoids introducing sleep/wakeup latency into the consumer path, at the cost of keeping a CPU core busy.

---

## 8. CPU Affinity and Linux CPU Isolation

After implementing the networking and queue architecture, I investigated how operating-system scheduling affects queue latency.

### Thread placement

| Thread | Logical CPU |
|---|---:|
| Network I/O | CPU 0 |
| Processing | CPU 2 |

Thread affinity is configured using:

```cpp
pthread_setaffinity_np();
```

### Linux kernel parameters

The experiment uses:

```text
isolcpus=0,2
nohz_full=0,2
rcu_nocbs=0,2
```

These configure scheduler isolation, tick reduction where supported, and RCU callback offloading for the selected CPUs.

The objective is to reduce interference from unrelated scheduled tasks and selected kernel housekeeping activity.

---

## 9. Performance Benchmarking

The system measures latency between the producer timestamp and the consumer timestamp.

The benchmark tracks:

- Average queue latency
- P50 latency
- P99 latency
- P99.9 latency
- Maximum observed latency
- Message throughput
- Dropped messages

### Live benchmark: Baseline vs. CPU affinity and isolation

Both configurations were evaluated over approximately two-minute live market-data sessions.

| Metric | Baseline | Affinity + Isolation |
|---|---:|---:|
| P50 | 191 ns | 174 ns |
| P99 | 193.616 µs | 299 ns |
| P99.9 | 501.097 µs | 613 ns |
| Maximum observed | 539.917 µs | 1.077 µs |
| Average queue latency | 4.458 µs | 181 ns |
| Throughput | 140.02 msg/s | 163.38 msg/s |
| Dropped messages | 0 | 0 |

### Observations

The largest observed differences were in tail latency:

- P99 latency decreased by approximately 99.85%.
- P99.9 latency decreased by approximately 99.88%.
- Maximum observed latency decreased by approximately 99.80%.
- Average queue latency decreased by approximately 95.9%.

These results are encouraging observations from individual live runs. They do not isolate the contribution of each optimization, and repeated controlled experiments are needed to establish reproducibility.

**Important:** These figures measure internal producer-to-consumer queue latency, not exchange-to-application or end-to-end execution latency.

---

## 10. Project Technology Stack

| Category | Technology |
|---|---|
| Language | C++20 |
| Networking | Boost.Asio |
| WebSocket | Boost.Beast |
| TLS | OpenSSL |
| JSON | Boost.JSON |
| Concurrency | C++ atomics, pthreads |
| Data structure | Custom SPSC ring buffer |
| CPU optimization | CPU affinity, cache-line alignment |
| Operating system | Ubuntu Linux |
| Performance analysis | Nanosecond-resolution latency metrics |

---

## 11. Build and Execution

### Dependencies

- C++20-compatible compiler
- Boost libraries (Asio, Beast and JSON)
- OpenSSL development libraries
- CMake, if using the provided CMake build configuration

### Build

Build commands depend on the repository's actual CMake configuration. Update this section with the verified build command and executable name.

### Run

```bash
./feed_handler
```

For a CPU-restricted systemd scope:

```bash
sudo systemd-run --scope \
  --unit=feed-handler \
  -p AllowedCPUs=0,2 \
  ./feed_handler
```

---

## 12. Future Improvements

The project is an ongoing exploration of low-latency systems engineering.

Planned areas of investigation:

- Cache-line alignment and false-sharing profiling
- Cache misses and hardware performance counters
- CPU migrations and context-switch analysis
- Network IRQ and softirq affinity
- More efficient message storage and allocation strategies
- Ring-buffer backpressure and overflow handling
- Improved WebSocket reconnection and resilience
- Order book construction
- Controlled synthetic benchmarking
- End-to-end market-data processing measurements

---

## 13. Engineering Objective

This project explores the engineering principles behind latency-sensitive market-data infrastructure.

The objective is to understand how networking, concurrency, memory layout, cache coherence, CPU scheduling, and operating-system behavior interact in a real C++ application.

**The focus is not simply on reducing average latency. It is on understanding and improving latency predictability.**

*Built as an independent systems-programming and performance-engineering project.*
