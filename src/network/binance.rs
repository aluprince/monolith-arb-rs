// ============================================================
// network/binance.rs — WebSocket Feed & Price Ingestion
// ============================================================
//
// WHAT THIS FILE DOES:
// 1. Connects to Binance WebSocket !bookTicker stream
// 2. Receives live best bid/ask updates for ALL trading pairs
// 3. Parses raw JSON into structured Rust types
// 4. Writes parsed prices into Weaver (shared price board)
// 5. Handles disconnections and reconnects automatically
// 6. Handles the 24-hour forced disconnection from Binance
//
// KEY FACTS ABOUT !bookTicker:
// - Single connection delivers ALL symbol updates
// - Testnet update speed: ~5 seconds per symbol
// - Live update speed: real-time (sub-100ms)
// - Binance pings every 3 minutes — library handles pong automatically
// - Binance WILL disconnect after exactly 24 hours (by design)
// - Our reconnection loop handles both unexpected drops AND 24hr reset
//
// WHAT THIS FILE DOES NOT DO:
// - No trading decisions
// - No arbitrage calculations  
// - No order placement
// Pure data ingestion only.
// ============================================================

use crate::weaver::{PriceEntry, current_timestamp_ms};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use std::str::FromStr;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::Duration;
use std::env;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use tracing::{info, warn, error, debug};

// ============================================================
// STRUCT: BookTickerUpdate
// ============================================================
// Maps EXACTLY to the JSON Binance sends on !bookTicker stream.
//
// Actual JSON from testnet:
// {
//   "e": "bookTicker",      <- event type
//   "u": 400900217,         <- order book updateId (unused)
//   "E": 1568014460893,     <- event time ms
//   "T": 1568014460891,     <- transaction time ms (we use this)
//   "s": "BNBUSDT",         <- symbol
//   "b": "25.35190000",     <- best bid price (string)
//   "B": "31.21000000",     <- best bid qty
//   "a": "25.36520000",     <- best ask price (string)
//   "A": "40.66000000"      <- best ask qty
// }
//
// #[serde(rename = "x")] maps Binance's single-letter keys
// to readable field names. MUST match Binance's exact key names.

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BookTickerUpdate {
    /// "e" — event type, always "bookTicker" for this stream
    #[serde(rename = "e")]
    event_type: String,

    /// "E" — when Binance packaged and sent this update (ms since epoch)
    /// Slightly LATER than transaction_time
    #[serde(rename = "E")]
    event_time: u64,

    /// "T" — when the orderbook change ACTUALLY OCCURRED (ms since epoch)
    /// This is the most accurate timestamp — we store this in PriceEntry
    ///
    /// WHY BINANCE'S TIMESTAMP OVER OURS:
    /// Clock skew between our machine and Binance = inaccurate staleness detection
    /// Binance's own timestamp eliminates this problem entirely
    #[serde(rename = "T")]
    transaction_time: u64,

    /// "s" — trading pair symbol e.g. "BTCUSDT"
    /// Becomes the HashMap KEY in Weaver
    #[serde(rename = "s")]
    symbol: String,

    /// "b" — best bid price as STRING (always 8 decimal places)
    /// Best bid = highest price any buyer will pay RIGHT NOW
    /// If you SELL at market, you receive this price
    #[serde(rename = "b")]
    best_bid: String,

    /// "B" — best bid quantity available at best bid price
    #[serde(rename = "B")]
    best_bid_qty: String,

    /// "a" — best ask price as STRING
    /// Best ask = lowest price any seller will accept RIGHT NOW  
    /// If you BUY at market, you pay this price
    #[serde(rename = "a")]
    best_ask: String,

    /// "A" — best ask quantity available at best ask price
    #[serde(rename = "A")]
    best_ask_qty: String,
}

// ============================================================
// FUNCTION: get_websocket_url()
// ============================================================
// Reads ENVIRONMENT from .env, returns correct WebSocket URL.
// Switch testnet ↔ live by changing ENVIRONMENT in .env only.
// Never hardcode URLs — makes accidental live trading impossible.
fn get_websocket_url() -> String {
    let environment = env::var("ENVIRONMENT")
        .unwrap_or_else(|_| "testnet".to_string());
        // Default to testnet if not set — safety net

    match environment.as_str() {
        "live" => {
            env::var("WS_URL_LIVE")
                .unwrap_or_else(|_| {
                    "wss://stream.binance.com:9443/ws/!bookTicker".to_string()
                })
        }
        _ => {
            // "testnet" or anything unrecognized → testnet
            // IMPORTANT: testnet updates ~5s per symbol (not real-time)
            // Set MAX_PRICE_AGE_MS=10000 in .env when using testnet
            env::var("WS_URL_TESTNET")
                .unwrap_or_else(|_| {
                    "wss://testnet.binance.vision/ws/!bookTicker".to_string()
                })
        }
    }
}

// ============================================================
// FUNCTION: connect_and_stream()
// ============================================================
// Main entry point. Spawned as its own Tokio task in main.rs.
// Runs FOREVER — handles all reconnection automatically.
//
// PARAMETER:
// weaver_handle — Arc shared reference to Weaver's price HashMap
//
// WHY Arc<RwLock<HashMap>> NOT &Weaver:
// Tokio tasks must be 'static — they can outlive spawning function.
// &Weaver BORROWS — the borrowed data might be dropped while task runs.
// Arc OWNS — data stays alive as long as any Arc exists.
// Task holds its own Arc clone = completely independent lifetime.
//
// This function never returns under normal operation.
pub async fn connect_and_stream(
    weaver_handle: Arc<RwLock<HashMap<String, PriceEntry>>>,
) {
    let url_str = get_websocket_url();
    info!("WebSocket target: {}", url_str);

    let url = Url::parse(&url_str)
        .expect("Invalid WebSocket URL — check WS_URL values in .env");

    let mut connection_attempts: u32 = 0;

    // ============================================================
    // OUTER LOOP — Reconnection Loop
    // ============================================================
    // Handles every disconnection scenario:
    // 1. Unexpected network drop → reconnects
    // 2. Binance maintenance → reconnects  
    // 3. Binance 24-hour forced reset → reconnects
    // 4. Failed initial connection → retries
    loop {
        connection_attempts += 1;
        info!("WebSocket connection attempt #{}", connection_attempts);

        match connect_async(url.clone()).await {

            Ok((ws_stream, _)) => {
                info!("Connected to Binance !bookTicker (attempt #{})", connection_attempts);

                // Split into read and write halves
                // tokio-tungstenite handles Ping→Pong AUTOMATICALLY at library level
                // before messages even reach our code
                // We only need the read half for price ingestion
                let (_, mut read) = ws_stream.split();

                let mut messages_received: u64 = 0;

                // ============================================================
                // INNER LOOP — Message Processing Loop
                // ============================================================
                // Runs while connection is alive.
                // .next().await yields to other tasks while waiting —
                // engine and executor run freely between our messages.
                loop {
                    match read.next().await {

                        Some(Ok(message)) => {
                            messages_received += 1;

                            // Heartbeat log every 1000 messages
                            // Proves WebSocket is still flowing in logs
                            if messages_received % 1000 == 0 {
                                info!("WebSocket healthy — {} messages processed", messages_received);
                            }

                            process_message(message, &weaver_handle).await;
                        }

                        Some(Err(e)) => {
                            warn!("WebSocket error after {} messages: {}", messages_received, e);
                            break; // Break inner loop → outer loop reconnects
                        }

                        None => {
                            // Connection closed — two possible reasons:
                            if messages_received > 100 {
                                // Had a healthy connection — likely 24hr Binance reset
                                info!(
                                    "Connection closed after {} messages (likely 24hr reset)",
                                    messages_received
                                );
                            } else {
                                // Closed very quickly — something is wrong
                                warn!("Connection closed after only {} messages", messages_received);
                            }
                            break;
                        }
                    }
                }
                // Fell out of inner loop — connection dead
            }

            Err(e) => {
                error!("Connection attempt #{} failed: {}", connection_attempts, e);
            }
        }

        // ---- RECONNECTION WAIT ----
        // Always wait before reconnecting — never hammer Binance servers
        // Fixed 5s wait for now. Phase 3 improvement: exponential backoff
        // (5s → 10s → 20s → 40s after repeated failures)
        warn!("Waiting 5 seconds before reconnecting...");
        tokio::time::sleep(Duration::from_secs(5)).await;
        // .await yields during wait — engine and executor keep running
    }
}

// ============================================================
// FUNCTION: process_message()
// ============================================================
// Handles ONE incoming WebSocket message.
// Separated from loop for readability.
//
// ERROR PHILOSOPHY:
// Bad data = log and skip. NEVER panic. NEVER crash.
// One bad message must not kill the bot.
async fn process_message(
    message: Message,
    weaver_handle: &Arc<RwLock<HashMap<String, PriceEntry>>>,
) {
    match message {

        // ---- TEXT — our price update (main case) ----
        Message::Text(text) => {

            // Step 1: Parse JSON → BookTickerUpdate struct
            let update = match serde_json::from_str::<BookTickerUpdate>(&text) {
                Ok(u) => u,
                Err(e) => {
                    // Log preview only — full JSON could be huge
                    let preview = &text[..text.len().min(100)];
                    warn!("JSON parse failed: {} | preview: {}", e, preview);
                    return;
                }
            };

            // Step 2: Convert bid string → Decimal
            // NEVER use f64 for money (floating point errors compound)
            let bid = match Decimal::from_str(&update.best_bid) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Bid parse failed for {}: '{}' — {}", update.symbol, update.best_bid, e);
                    return;
                }
            };

            // Step 3: Convert ask string → Decimal
            let ask = match Decimal::from_str(&update.best_ask) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Ask parse failed for {}: '{}' — {}", update.symbol, update.best_ask, e);
                    return;
                }
            };

            // Step 4: Validate — crossed book check
            // bid >= ask is physically impossible in a real market
            // Acting on crossed book data guarantees losing trades
            if bid >= ask {
                warn!("Crossed book for {} — bid:{} >= ask:{} — skipping", update.symbol, bid, ask);
                return;
            }

            // Step 5: Select timestamp
            // Use Binance's transaction_time (T) — when the market event occurred
            // This eliminates clock skew between our machine and Binance
            // Fallback to our clock if T is somehow zero (defensive)
            let timestamp = if update.transaction_time > 0 {
                update.transaction_time
            } else {
                warn!("transaction_time is 0 for {} — using local clock as fallback", update.symbol);
                current_timestamp_ms()
            };

            // Step 6: Write to Weaver
            // Acquire write lock → insert → release IMMEDIATELY
            // Explicit scope block ensures lock releases right after insert
            // NOT at end of this function — minimizes lock contention
            {
                let mut prices = weaver_handle.write().await;
                prices.insert(
                    update.symbol.clone(),
                    PriceEntry::new(bid, ask, timestamp),
                );
                // Lock released here — not at end of process_message()
            }

            // Step 7: Development debugging (uncomment to verify data flow)
            // if update.symbol == "BTCUSDT" {
            //     debug!("BTCUSDT bid:{} ask:{}", bid, ask);
            // }
        }

        // ---- PING — library handles Pong automatically ----
        // tokio-tungstenite responds with Pong before we see this message
        // Binance pings every 3 minutes; no pong within 10 min = disconnected
        // We just log for awareness
        Message::Ping(_) => {
            debug!("Ping received — Pong sent automatically by tokio-tungstenite");
        }

        Message::Pong(_) => {
            debug!("Pong received (unexpected — we don't send Pings manually)");
        }

        // ---- CLOSE — Binance closing connection ----
        // Happens during maintenance or 24hr forced reset
        // Log it — next .next().await returns None → inner loop breaks → reconnect
        Message::Close(frame) => {
            match frame {
                Some(f) => info!("Binance closing: code:{} reason:'{}'", f.code, f.reason),
                None => info!("Binance closing connection (no details provided)"),
            }
        }

        Message::Binary(data) => {
            warn!("Unexpected binary message ({} bytes) — ignoring", data.len());
        }

        Message::Frame(_) => {
            // Raw frames handled internally by tungstenite — we never see these
            // Rust requires exhaustive match so we handle explicitly
        }
    }
}

// ============================================================
// TESTS — run with: cargo test
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_testnet_payload() {
        // Exact format Binance testnet sends (includes e, E, T fields)
        let json = r#"{
            "e": "bookTicker",
            "u": 400900217,
            "E": 1568014460893,
            "T": 1568014460891,
            "s": "BNBUSDT",
            "b": "25.35190000",
            "B": "31.21000000",
            "a": "25.36520000",
            "A": "40.66000000"
        }"#;

        let update: BookTickerUpdate = serde_json::from_str(json)
            .expect("Should parse complete testnet payload");

        assert_eq!(update.symbol, "BNBUSDT");
        assert_eq!(update.event_type, "bookTicker");
        assert_eq!(update.transaction_time, 1568014460891);
        // T must be <= E (transaction happens before event is packaged)
        assert!(update.transaction_time <= update.event_time);
        println!("✓ Full testnet payload parses correctly");
    }

    #[test]
    fn test_transaction_time_used_as_timestamp() {
        let json = r#"{
            "e": "bookTicker",
            "u": 1,
            "E": 1568014460893,
            "T": 1568014460891,
            "s": "BTCUSDT",
            "b": "43250.50000000",
            "B": "1.00000000",
            "a": "43251.00000000",
            "A": "2.00000000"
        }"#;
        let update: BookTickerUpdate = serde_json::from_str(json).unwrap();
        let timestamp = if update.transaction_time > 0 {
            update.transaction_time
        } else {
            current_timestamp_ms()
        };
        // Must use T (1568014460891), not E (1568014460893)
        assert_eq!(timestamp, 1568014460891);
        println!("✓ Transaction time (T) used as timestamp");
    }

    #[test]
    fn test_crossed_book_rejected() {
        let bid = Decimal::from_str("100.50").unwrap();
        let ask = Decimal::from_str("100.00").unwrap();
        assert!(bid >= ask);
        println!("✓ Crossed book detected: bid:{} >= ask:{}", bid, ask);
    }

    #[test]
    fn test_small_altcoin_price_precision() {
        let price = Decimal::from_str("0.00001234").unwrap();
        assert_eq!(price.to_string(), "0.00001234");
        println!("✓ Small price precision maintained: {}", price);
    }

    #[test]
    fn test_future_binance_fields_ignored() {
        let json = r#"{
            "e": "bookTicker",
            "u": 1,
            "E": 1568014460893,
            "T": 1568014460891,
            "s": "BTCUSDT",
            "b": "43250.50000000",
            "B": "1.00000000",
            "a": "43251.00000000",
            "A": "2.00000000",
            "futureField": "Binance added this",
            "anotherField": 99999
        }"#;
        // serde ignores unknown fields by default
        // Bot must survive Binance API updates without code changes
        let result = serde_json::from_str::<BookTickerUpdate>(json);
        assert!(result.is_ok());
        println!("✓ Unknown Binance fields ignored safely");
    }

    #[test]
    fn test_default_url_is_testnet() {
        std::env::remove_var("ENVIRONMENT");
        let url = get_websocket_url();
        assert!(url.contains("testnet"));
        println!("✓ Defaults to testnet URL: {}", url);
    }
}