# Low-Latency C++ Market Data Feed Handler

A C++20 market-data ingestion and processing system focused on asynchronous networking, lock-free inter-thread communication, CPU affinity, Linux CPU isolation, and nanosecond-resolution queue-latency measurement.

The project explores systems-level performance optimization techniques relevant to low-latency trading infrastructure and HFT engineering.

## Overview

The application connects to live market-data WebSocket streams, receives and parses incoming messages, and transfers them from the network I/O thread to a dedicated processing thread through a fixed-capacity SPSC ring buffer.

The primary objective is to investigate how thread placement and Linux CPU isolation influence latency and scheduling consistency.

## Architecture

```text
       Live Market Data
              |
              v
    Boost.Asio / Beast
       WebSocket I/O
              |
              v
       Message Parsing
              |
              v
    Lock-Free SPSC Ring
              |
              v
      Processing Thread
              |
              v
       Latency Metrics
    P50 / P99 / P99.9 / Max
```

### Threading model

| Component | CPU | Responsibility |
|---|---:|---|
| I/O thread | 0 | WebSocket communication, message reception and queue production |
| Processing thread | 2 | Queue consumption and message processing |

The threads communicate through a single-producer, single-consumer ring buffer.

## Technology Stack

- **C++20** – systems programming
- **Boost.Asio** – asynchronous I/O
- **Boost.Beast** – WebSocket communication
- **Linux pthreads** – thread management and CPU affinity
- **SPSC ring buffer** – lock-free producer-consumer communication
- **Linux kernel parameters** – CPU isolation and housekeeping optimization
- **CMake / compiler toolchain** – build environment, where configured

## Performance Engineering

The project investigates several low-level optimization techniques.

### 1. CPU affinity

Worker threads are pinned to specific logical CPUs using:

```cpp
pthread_setaffinity_np()
```

Current placement:

- I/O thread → CPU 0
- Processing thread → CPU 2

This provides explicit control over worker-thread placement.

### 2. Linux CPU isolation

Kernel boot parameters used in the experiment:

```text
isolcpus=0,2
nohz_full=0,2
rcu_nocbs=0,2
```

These configure scheduler-domain isolation, tick reduction where supported, and RCU callback offloading for the selected CPUs.

### 3. Lock-free SPSC communication

The network thread produces messages while the processing thread consumes them.

The ring buffer avoids conventional mutex-based synchronization on the producer-consumer handoff path.

### 4. Latency measurement

Queue latency is measured from the producer-side timestamp to the consumer-side timestamp.

The benchmark reports:

- Average queue latency
- Maximum observed queue latency
- P50 latency
- P99 latency
- P99.9 latency
- Message throughput
- Dropped messages

**Measurement scope:** These figures represent internal producer-to-consumer queue latency, not end-to-end network latency.

## Benchmark Results

### Experimental setup

Two configurations were evaluated using approximately two-minute live market-data sessions.

- Baseline: normal Linux CPU scheduling.
- Optimized: CPU affinity and Linux CPU isolation.

### Results

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

The most notable differences appeared in the latency tail:

- P99 latency was approximately 99.85% lower.
- P99.9 latency was approximately 99.88% lower.
- Maximum observed latency was approximately 99.80% lower.
- Average queue latency was approximately 95.9% lower.

These are encouraging observations from individual live runs, not proof that CPU isolation alone caused the entire improvement. Repeated controlled experiments are needed to establish reproducibility.

Throughput is dependent on incoming live market-data activity and should not be interpreted as maximum processing capacity.

## Linux Environment

The optimization experiments were conducted on an Intel-based laptop running Ubuntu Linux.

The implementation uses Linux-specific thread affinity and kernel boot parameters.

To inspect isolated CPUs:

```bash
cat /sys/devices/system/cpu/isolated
```

To launch the application with a CPU restriction using systemd:

```bash
sudo systemd-run --scope \
  --unit=feed-handler \
  -p AllowedCPUs=0,2 \
  ./feed_handler
```

Note: CPU affinity, scheduler isolation, and cgroup CPU restrictions are separate mechanisms.

## Current Development Focus

- Cache-line alignment and false-sharing analysis
- CPU migration and context-switch profiling
- Network interrupt and softirq analysis
- Improved latency measurement methodology
- Controlled synthetic benchmarking
- Order book construction and market-data processing
- Further Linux performance optimization

## Project Motivation

This project is an exploration of the engineering principles behind latency-sensitive C++ systems.

The focus is not simply on achieving a low latency number, but on understanding the interactions between application architecture, CPU topology, thread scheduling, synchronization, and operating-system behavior.

**Measure. Profile. Optimize. Repeat.**

## Disclaimer

This is an educational and experimental market-data processing project. The benchmark figures are environment-specific observations and are not representative of production HFT infrastructure or exchange-to-execution latency.
