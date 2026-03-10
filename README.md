# Monolith-Arb-RS

A high-performance, asynchronous triangular arbitrage bot built in Rust.

## Phase 1 Objectives
- [ ] Real-time WebSocket ingestion (Binance).
- [ ] Thread-safe in-memory OrderBook Weaver.
- [ ] Directed Graph representation of market pairs.
- [ ] Bellman-Ford cycle detection.

## Tech Stack
- **Language:** Rust 2024 Edition
- **Runtime:** Tokio (Async I/O)
- **Networking:** tokio-tungstenite (WebSockets)
- **Math:** Petgraph (Graph Theory)
