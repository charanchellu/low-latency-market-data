# Low-Latency C++ Market Data Feed Handler

A low-latency C++20 market-data pipeline that connects to Binance WebSocket trade streams, parses incoming market data, and transfers normalized messages through a cache-line-aligned SPSC ring buffer for low-overhead producer-consumer communication.

## Overview

This project focuses on the systems engineering challenges involved in building a low-latency market-data pipeline:

```text
Binance WebSocket
       │
       ▼
Boost.Asio / Beast
       │
       ▼
TLS / OpenSSL
       │
       ▼
JSON Message Parsing
       │
       ▼
Timestamp
       │
       ▼
SPSC Ring Buffer
       │
       ▼
Consumer Thread
       │
       ▼
Latency Measurement
```

The current implementation subscribes to Binance trade streams for:

* BTCUSDT
* ETHUSDT
* SOLUSDT

## Key Features

* C++20 asynchronous networking
* Binance WebSocket market-data ingestion
* TLS using OpenSSL
* Boost.Asio asynchronous I/O
* Boost.Beast WebSocket implementation
* Lock-free SPSC ring buffer
* Cache-line-aligned producer/consumer indices
* Atomic operations with acquire/release memory ordering
* Busy-spin consumer
* Architecture-specific CPU relaxation
* Producer-to-consumer queue latency measurement
* P50, P99, P99.9 and maximum latency tracking

## SPSC Ring Buffer

The core communication mechanism is a fixed-size **Single Producer Single Consumer (SPSC)** ring buffer.

```text
             Producer
                │
                │ try_push()
                ▼
      ┌─────────────────────┐
      │                     │
      │     SPSC Buffer     │
      │                     │
      │  [ ][ ][ ][ ][ ]   │
      │                     │
      └─────────────────────┘
                │
                │ try_pop()
                ▼
             Consumer
```

The producer owns the `head` index and the consumer owns the `tail` index.

The indices are implemented using atomics with acquire/release memory ordering to establish the required producer-consumer synchronization without using a mutex.

## Cache-Line Alignment

The producer and consumer indices are cache-line aligned to reduce the possibility of **false sharing** between the two threads.

```cpp
alignas(64) std::atomic<size_t> head_;
alignas(64) std::atomic<size_t> tail_;
```

This keeps frequently modified synchronization variables separated across cache lines.

## CPU Relaxation

The consumer uses architecture-specific relaxation while waiting for new messages.

### x86-64

```cpp
_mm_pause();
```

### ARM64

```asm
yield
```

### Fallback

```cpp
std::this_thread::yield();
```

This allows the consumer to busy-spin without simply executing the polling loop at maximum aggressiveness.

## Latency Measurement

The benchmark measures **producer-to-consumer queue residence time**.

The timestamp is captured immediately before inserting a message into the SPSC queue:

```text
Producer
   │
   │ timestamp
   ▼
try_push()
   │
   ▼
SPSC Queue
   │
   ▼
try_pop()
   │
   ▼
Consumer
   │
   ▼
latency = consumer_time - producer_timestamp
```

This measurement does **not** represent end-to-end network latency. Network transmission, WebSocket processing, and JSON parsing before the timestamp are outside the measured queue latency.

## Live Binance Benchmark

A 2-minute live Binance trade-stream run produced:

| Metric |                Result |
| ------ | --------------------: |
| P50    | **125 ns (0.125 µs)** |
| P99    | **417 ns (0.417 µs)** |
| P99.9  |          **8.125 µs** |

The P99.9 result shows a substantially longer latency tail compared with the median and P99 measurements, motivating further investigation into scheduling, CPU migration, and system-level effects.

## Benchmark Roadmap

The project is being benchmarked in stages:

### 1. Live Market-Data Baseline

Measure the complete Binance ingestion pipeline under a real market-data workload.

### 2. Synthetic SPSC Benchmark

Remove network and parsing overhead and measure the SPSC queue independently.

Planned measurements:

* Maximum throughput
* P50 latency
* P99 latency
* P99.9 latency
* Maximum latency
* Message drops

### 3. CPU Affinity Experiment

Compare unrestricted scheduling against producer/consumer CPU affinity.

```text
Baseline

Producer ──► Scheduler ──► CPU
Consumer ──► Scheduler ──► CPU


Affinity

Producer ───────────────► CPU X
Consumer ───────────────► CPU Y
```

The goal is to investigate the effect of CPU scheduling and migration on latency, particularly tail latency.

## Technologies

* **C++20**
* **Boost.Asio**
* **Boost.Beast**
* **OpenSSL**
* **WebSockets**
* **TCP/IP**
* **STL Atomics**
* **CMake**
* **Git**

```text
```

## Purpose

This project is designed as a systems-oriented exploration of:

* Low-latency C++ programming
* Lock-free data structures
* Memory ordering
* CPU/cache behavior
* Asynchronous networking
* Market-data ingestion
* Latency and throughput benchmarking

The long-term goal is to extend the pipeline toward a more complete low-latency trading infrastructure stack.
