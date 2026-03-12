// ============================================================
// weaver.rs — The Shared Price Board (Central Nervous System)
// ============================================================
//
// WHAT THIS FILE DOES:
// Weaver is the single source of truth for all live prices.
// The WebSocket task WRITES prices into it.
// The Engine task READS prices from it.
// Nothing else in the system owns price data — only Weaver.
//
// WHY IT'S DESIGNED THIS WAY:
// Two tasks running simultaneously cannot share a plain HashMap.
// Rust's compiler will refuse to compile that — it's a data race.
// Weaver wraps the HashMap in an Arc<RwLock<>> which makes
// simultaneous access safe without corrupting data.
//
// THE MENTAL MODEL:
// Think of Weaver as a live scoreboard in a sports stadium.
// Many people (tasks) can look at it simultaneously (read).
// Only the official updater (WebSocket task) can change it (write).
// While the updater is changing a score, readers wait a microsecond.
// The moment the update is done, everyone can read again.
// ============================================================

// --- IMPORTS ---
// We bring in only what we need. Every import has a reason.

use std::collections::HashMap;
// HashMap — the core data structure. Key: pair symbol ("BTCUSDT").
// Value: PriceEntry (bid, ask, timestamp).

use std::sync::Arc;
// Arc = Atomic Reference Counted.
// Lets MULTIPLE tasks own a reference to the SAME data.
// "Atomic" means the reference count updates are thread-safe.
// Cloning an Arc does NOT copy the data — it just adds one more owner.
// When ALL owners drop their Arc, the data is cleaned up automatically.

use tokio::sync::RwLock;
// RwLock = Read-Write Lock (Tokio's async version).
// IMPORTANT: We use tokio::sync::RwLock, NOT std::sync::RwLock.
// Why? Because std::RwLock BLOCKS the entire thread while waiting.
// tokio::RwLock YIELDS to other async tasks while waiting — much better.
//
// RwLock rules:
// - Many tasks can READ simultaneously (no blocking between readers)
// - Only ONE task can WRITE at a time (blocks all readers while writing)
// - Write locks are held for MICROSECONDS — price updates are tiny/fast

use rust_decimal::Decimal;
// Decimal = precise fixed-point arithmetic.
// NEVER use f64 for money. f64 has floating point errors that compound.
// Example of why f64 is dangerous:
//   0.1 + 0.2 in f64 = 0.30000000000000004 (not 0.3!)
// Decimal handles this correctly. Always use it for prices and profits.

use std::time::{SystemTime, UNIX_EPOCH};
// Used to get current timestamp in milliseconds.
// We store WHEN each price was last updated so the engine can
// reject stale prices (older than 500ms) before acting on them.

// ============================================================
// STRUCT: PriceEntry
// ============================================================
// This is what we store for EACH trading pair in the HashMap.
// One PriceEntry per pair (BTCUSDT, ETHUSDT, BNBUSDT, etc.)

#[derive(Debug, Clone)]
#[allow(dead_code)]
// Debug — lets us print this struct with {:?} for logging/debugging
// Clone — lets us copy this struct cheaply when the engine reads prices
pub struct PriceEntry {
    /// The highest price a buyer is willing to pay RIGHT NOW.
    /// If you want to SELL immediately, you get this price.
    /// Example: bid = 43250.50 means someone will buy BTC at $43,250.50
    pub best_bid: Decimal,

    /// The lowest price a seller is willing to accept RIGHT NOW.
    /// If you want to BUY immediately, you pay this price.
    /// Example: ask = 43251.00 means cheapest BTC available is $43,251.00
    pub best_ask: Decimal,

    /// Unix timestamp in MILLISECONDS when this price was last updated.
    /// The engine uses this to reject prices older than MAX_PRICE_AGE_MS.
    /// Stale prices = phantom opportunities = losing trades.
    pub updated_at: u64,
}

#[allow(dead_code)]
impl PriceEntry {
    /// Creates a new PriceEntry.
    /// Called by update_price() every time Binance sends us a price update.
    pub fn new(best_bid: Decimal, best_ask: Decimal, updated_at: u64) -> Self {
        Self {
            best_bid,
            best_ask,
            updated_at,
        }
    }

    /// Returns how old this price data is in milliseconds.
    ///
    /// HOW IT WORKS:
    /// Gets current time, subtracts when this price was last updated.
    /// Result is how many milliseconds have passed since the last update.
    ///
    /// WHY THIS MATTERS:
    /// If a price hasn't updated in 800ms, it might be stale.
    /// The engine will call this before acting on any opportunity.
    /// Stale bid/ask = the real market has moved = your trade loses money.
    pub fn age_ms(&self) -> u64 {
        // Get current time since Unix epoch in milliseconds
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            // unwrap() is safe here — system time is always after Unix epoch
            // unless someone set their system clock to before 1970
            .unwrap()
            .as_millis() as u64;

        // Saturating subtraction — if now < updated_at (clock skew edge case)
        // this returns 0 instead of panicking with overflow
        now.saturating_sub(self.updated_at)
    }

    /// Returns true if this price is fresh enough to trade on.
    /// max_age_ms comes from your .env MAX_PRICE_AGE_MS setting (500ms default).
    ///
    /// The engine calls this on every opportunity before executing.
    /// If any of the 3 legs have stale prices → abort the opportunity.
    pub fn is_fresh(&self, max_age_ms: u64) -> bool {
        self.age_ms() < max_age_ms
    }
}

// ============================================================
// STRUCT: Weaver
// ============================================================
// The main shared state container.
// One Weaver instance exists for the entire lifetime of the bot.
// Multiple tasks hold an Arc handle to it — not copies of it.

pub struct Weaver {
    /// The live price map.
    ///
    /// Arc — multiple tasks (WebSocket, Engine) can own this simultaneously.
    /// RwLock — safe concurrent access: many readers OR one writer at a time.
    /// HashMap — maps "BTCUSDT" → PriceEntry { bid, ask, updated_at }
    ///
    /// Why wrap in Arc<RwLock<>> instead of just HashMap?
    /// Without Arc: only one task can own the data (Rust ownership rules)
    /// Without RwLock: concurrent read/write = data race = compiler error
    /// With Arc<RwLock<>>: many tasks, safe access, Rust is happy
    prices: Arc<RwLock<HashMap<String, PriceEntry>>>,
}

#[allow(dead_code)]
impl Weaver {
    // ============================================================
    // METHOD: new()
    // ============================================================
    // Creates a fresh Weaver with an empty price map.
    // Called ONCE in main.rs at startup before any tasks are spawned.
    //
    // RETURNS: Weaver instance ready to receive prices
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            // Arc::new() — wraps our HashMap in an Arc (starts ref count at 1)
            // RwLock::new() — wraps HashMap in a lock for safe concurrent access
            // HashMap::new() — starts empty, WebSocket will fill it immediately
        }
    }

    // ============================================================
    // METHOD: get_handle()
    // ============================================================
    // Returns a CLONE of the Arc — NOT a clone of the data.
    //
    // This is how you share Weaver between tasks.
    // In main.rs you'll do:
    //   let weaver = Weaver::new();
    //   let ws_handle = weaver.get_handle();   // give to WebSocket task
    //   let engine_handle = weaver.get_handle(); // give to Engine task
    //
    // Both handles point to the EXACT SAME HashMap in memory.
    // Arc's reference count goes from 1 → 3 (original + 2 handles).
    // No data is copied. Memory usage stays constant.
    //
    // RETURNS: Arc<RwLock<HashMap>> — a shared reference to the price map
    pub fn get_handle(&self) -> Arc<RwLock<HashMap<String, PriceEntry>>> {
        Arc::clone(&self.prices)
        // Arc::clone() increments reference count by 1
        // Does NOT clone the underlying HashMap
        // Both the original Arc and this clone point to same memory
    }

    // ============================================================
    // METHOD: update_price()
    // ============================================================
    // Called by the WebSocket task every time Binance sends a price update.
    // This happens THOUSANDS of times per second across all pairs.
    //
    // CRITICAL PERFORMANCE RULE:
    // This method acquires a WRITE lock. While the write lock is held,
    // NO OTHER TASK can read or write the price map.
    // Therefore: do the absolute minimum work inside this method.
    // We do: acquire lock → update one HashMap entry → release lock.
    // We do NOT: calculate anything, log anything, or do any math here.
    //
    // PARAMETERS:
    // symbol — the trading pair e.g. "BTCUSDT" (always uppercase)
    // best_bid — highest buy price as Decimal
    // best_ask — lowest sell price as Decimal
    // timestamp — milliseconds since Unix epoch when Binance sent this update
    //
    // async — this method is async because write() is async in tokio::RwLock
    // It YIELDS (not blocks) while waiting for the write lock to be available
    pub async fn update_price(
        &self,
        symbol: String,
        best_bid: Decimal,
        best_ask: Decimal,
        timestamp: u64,
    ) {
        // Acquire the write lock asynchronously.
        // .await means: "if lock isn't available, pause this task and let
        // other tasks run until the lock is free, then resume here"
        // This is the KEY difference from std::RwLock which would BLOCK.
        let mut prices = self.prices.write().await;

        // Insert or update the entry for this symbol.
        // If "BTCUSDT" already exists → overwrites it (most common case)
        // If "BTCUSDT" doesn't exist yet → creates new entry
        prices.insert(
            symbol,
            PriceEntry::new(best_bid, best_ask, timestamp),
        );

        // Write lock is automatically released here when `prices` goes out
        // of scope. This is Rust's RAII — no manual unlock needed.
        // The lock is held for MICROSECONDS — just long enough to update one entry.
    }

    // ============================================================
    // METHOD: get_all_prices()
    // ============================================================
    // Called by the Engine every 100ms to get a snapshot of all prices.
    //
    // CRITICAL DESIGN DECISION — WHY WE CLONE:
    // We could give the engine a read lock reference and let it run
    // Bellman-Ford while holding the lock. DON'T DO THIS.
    //
    // If we hold the read lock during Bellman-Ford (which takes real time),
    // the WebSocket task cannot write new prices for that entire duration.
    // Prices go stale. Opportunities are missed.
    //
    // Instead: acquire read lock → clone entire HashMap → release lock → 
    // give clone to engine → engine runs Bellman-Ford on the clone freely.
    //
    // Cloning ~400 PriceEntry structs takes ~microseconds.
    // Bellman-Ford on 400 nodes takes ~milliseconds.
    // We hold the lock for microseconds, not milliseconds. 
    //
    // RETURNS: A cloned snapshot of the entire price map.
    // The caller owns this clone — modifying it does NOT affect Weaver.
    pub async fn get_all_prices(&self) -> HashMap<String, PriceEntry> {
        // Acquire read lock asynchronously (yields if write lock is active)
        let prices = self.prices.read().await;

        // Clone the entire HashMap.
        // This copies all keys (strings) and values (PriceEntry structs).
        // After this line, we have a completely independent copy.
        prices.clone()

        // Read lock automatically released here — as fast as possible.
        // Engine now works on its own copy with zero impact on Weaver.
    }

    // ============================================================
    // METHOD: get_price()
    // ============================================================
    // Gets the current price for a SINGLE specific pair.
    // Useful for checking a specific pair without cloning everything.
    // The executor uses this to verify prices right before placing orders.
    //
    // PARAMETERS:
    // symbol — e.g. "BTCUSDT"
    //
    // RETURNS: Option<PriceEntry>
    // Some(PriceEntry) — if we have price data for this pair
    // None — if this pair doesn't exist in our map yet
    pub async fn get_price(&self, symbol: &str) -> Option<PriceEntry> {
        let prices = self.prices.read().await;

        // .get() returns Option<&PriceEntry> — a reference.
        // .cloned() converts Option<&PriceEntry> to Option<PriceEntry>
        // so we can return owned data after the lock is released.
        prices.get(symbol).cloned()
    }

    // ============================================================
    // METHOD: price_count()
    // ============================================================
    // Returns how many pairs are currently in the price map.
    // Used in main.rs for startup validation:
    // "Don't start the engine until we have at least N prices"
    //
    // On startup the WebSocket takes a few seconds to fill the map.
    // We want to wait until we have meaningful coverage before
    // running Bellman-Ford on an incomplete graph.
    pub async fn price_count(&self) -> usize {
        let prices = self.prices.read().await;
        prices.len()
        // Lock released automatically here
    }
}

// ============================================================
// HELPER FUNCTION: current_timestamp_ms()
// ============================================================
// Returns the current Unix timestamp in milliseconds.
// Used by the WebSocket task when Binance doesn't provide a timestamp
// (some message types omit it — we generate our own in that case).
//
// Made public so network/binance.rs can import and use it.
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ============================================================
// TESTS
// ============================================================
// Run with: cargo test
// These verify Weaver works correctly before we connect real WebSocket data.
// ALWAYS write tests for financial logic. Silent bugs cost real money.

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Test that a new Weaver starts empty
    #[tokio::test]
    async fn test_weaver_starts_empty() {
        let weaver = Weaver::new();
        assert_eq!(weaver.price_count().await, 0);
        println!("✓ Weaver initializes with empty price map");
    }

    // Test that update_price correctly stores and retrieves a price
    #[tokio::test]
    async fn test_update_and_retrieve_price() {
        let weaver = Weaver::new();

        weaver.update_price(
            "BTCUSDT".to_string(),
            dec!(43250.50), // bid
            dec!(43251.00), // ask
            current_timestamp_ms(),
        ).await;

        // Verify it was stored
        let entry = weaver.get_price("BTCUSDT").await;
        assert!(entry.is_some(), "Price should exist after update");

        let entry = entry.unwrap();
        assert_eq!(entry.best_bid, dec!(43250.50));
        assert_eq!(entry.best_ask, dec!(43251.00));
        println!("✓ Price stored and retrieved correctly");
    }

    // Test that price_count increments correctly
    #[tokio::test]
    async fn test_price_count() {
        let weaver = Weaver::new();

        weaver.update_price("BTCUSDT".to_string(), dec!(43250), dec!(43251), current_timestamp_ms()).await;
        weaver.update_price("ETHUSDT".to_string(), dec!(2251), dec!(2252), current_timestamp_ms()).await;
        weaver.update_price("BNBUSDT".to_string(), dec!(245), dec!(246), current_timestamp_ms()).await;

        assert_eq!(weaver.price_count().await, 3);
        println!("✓ Price count tracks correctly");
    }

    // Test that get_handle() shares the SAME data (not a copy)
    #[tokio::test]
    async fn test_handle_shares_same_data() {
        let weaver = Weaver::new();
        let handle = weaver.get_handle();

        // Write through the weaver
        weaver.update_price(
            "SOLUSDT".to_string(),
            dec!(95.50),
            dec!(95.55),
            current_timestamp_ms(),
        ).await;

        // Read through the handle — should see the same data
        let prices = handle.read().await;
        assert!(prices.contains_key("SOLUSDT"), "Handle should see data written through weaver");
        println!("✓ Handle shares same underlying data as weaver");
    }

    // Test that get_all_prices returns a clone (not a reference)
    // Modifying the clone should NOT affect the original
    #[tokio::test]
    async fn test_get_all_prices_is_clone() {
        let weaver = Weaver::new();

        weaver.update_price("BTCUSDT".to_string(), dec!(43250), dec!(43251), current_timestamp_ms()).await;

        let mut snapshot = weaver.get_all_prices().await;

        // Modify the clone
        snapshot.remove("BTCUSDT");

        // Original Weaver should be unaffected
        assert_eq!(weaver.price_count().await, 1, "Original weaver should be unchanged");
        println!("✓ get_all_prices returns independent clone");
    }

    // Test that is_fresh() correctly identifies stale prices
    #[tokio::test]
    async fn test_price_freshness() {
        // A price updated right now should be fresh
        let fresh_price = PriceEntry::new(
            dec!(100),
            dec!(101),
            current_timestamp_ms(),
        );
        assert!(fresh_price.is_fresh(500), "Price updated now should be fresh");

        // A price updated 1 second ago should be stale at 500ms threshold
        let stale_timestamp = current_timestamp_ms().saturating_sub(1000);
        let stale_price = PriceEntry::new(
            dec!(100),
            dec!(101),
            stale_timestamp,
        );
        assert!(!stale_price.is_fresh(500), "Price updated 1s ago should be stale");
        println!("✓ Price freshness detection works correctly");
    }
}