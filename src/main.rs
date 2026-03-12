// ============================================================
// main.rs — System Entry Point & Task Orchestration
// ============================================================
//
// WHAT THIS FILE DOES:
// 1. Boots the entire system in correct order
// 2. Creates shared state (Weaver)
// 3. Spawns all concurrent tasks
// 4. Monitors tasks and restarts them if they crash
// 5. Keeps the process alive indefinitely
//
// WHAT THIS FILE DOES NOT DO:
// No price processing. No arbitrage logic. No order placement.
// Pure orchestration only — starts things, watches things.
//
// ARCHITECTURE REMINDER:
// main.rs
//   ├── spawns → network::binance::connect_and_stream() [WebSocket task]
//   ├── spawns → engine::graph::run_engine_loop()       [Engine task - stub]
//   └── spawns → safety::executor::run_executor_loop()  [Executor task - stub]
// All three share Weaver via Arc<RwLock<HashMap>>
// Engine→Executor communicate via mpsc channel
// ============================================================

// Declare modules — tells Rust these folders/files exist
// Without these, the compiler ignores everything in src/ except main.rs
mod weaver;
mod network;
mod engine;    // stub — not fully built yet
mod safety;    // stub — not fully built yet
mod logger;    // stub — not fully built yet

use weaver::Weaver;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
// mpsc = multi-producer single-consumer channel
// Engine (producer) sends opportunities → Executor (consumer) receives them
// "multi-producer" means multiple senders possible (useful later)
// "single-consumer" means one receiver (the executor)

use tracing::{info, warn, error};
use tracing_subscriber::EnvFilter;
// EnvFilter — controls which log levels appear based on environment
// Set RUST_LOG=debug in .env to see debug logs
// Set RUST_LOG=info for production (less verbose)

// ============================================================
// CONSTANT: Minimum prices before engine starts
// ============================================================
// We wait until Weaver has at least this many pairs before
// starting the engine. On testnet, prices trickle in slowly.
// On live, this fills in seconds.
// 50 is enough to have meaningful triangular paths to evaluate.
const MIN_PRICES_TO_START: usize = 50;

// How long to wait between startup checks
const STARTUP_CHECK_INTERVAL_MS: u64 = 2000; // check every 2 seconds

// How long to wait before restarting a crashed task
const TASK_RESTART_DELAY_MS: u64 = 3000;

#[tokio::main]
// #[tokio::main] — transforms our async main into a real main()
// Sets up the Tokio async runtime under the hood
// "full" feature in Cargo.toml enables multi-threaded scheduler
// Tasks genuinely run in parallel on multiple CPU cores
async fn main() {
    // ============================================================
    // STEP 1: Load .env file
    // ============================================================
    // Reads .env and pushes all KEY=VALUE pairs into environment
    // Must happen BEFORE anything reads env::var()
    // .ok() — if .env doesn't exist, continue anyway (uses system env)
    dotenv::dotenv().ok();

    // ============================================================
    // STEP 2: Initialize logging
    // ============================================================
    // Set up the tracing subscriber that formats and outputs logs
    // RUST_LOG in .env controls verbosity:
    //   RUST_LOG=debug  → see everything including debug!() calls
    //   RUST_LOG=info   → see info!(), warn!(), error!() only
    //   RUST_LOG=warn   → see warn!() and error!() only
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
                // If RUST_LOG not set → default to "info" level
        )
        .with_target(false)  // don't show module path in logs (cleaner output)
        .init();

    info!("========================================");
    info!("  monolith-arb-rs starting up");
    info!("========================================");

    // ============================================================
    // STEP 3: Create Weaver (shared price board)
    // ============================================================
    // One Weaver instance for the entire bot lifetime
    // All tasks share it via Arc handles (never the Weaver directly)
    let weaver = Weaver::new();
    info!("Weaver initialized — price map ready");

    // Get the raw Arc handle that we'll clone for each task
    // weaver.get_handle() returns Arc<RwLock<HashMap<String, PriceEntry>>>
    let weaver_arc = weaver.get_handle();

    // ============================================================
    // STEP 4: Create channel for Engine → Executor communication
    // ============================================================
    // Engine finds opportunities and SENDS them through this channel
    // Executor RECEIVES opportunities and executes them
    //
    // Buffer size 100 — if executor is busy, engine can queue 100 opportunities
    // before it starts blocking. In practice we process much faster than this.
    // If buffer fills up, the engine's .send() will wait — this is intentional
    // backpressure preventing opportunity queue from growing unbounded.
    //
    // OpportunitySignal is defined in engine module (stub type for now)
    // We use () as placeholder until engine module is built
    let (opportunity_tx, mut opportunity_rx) = mpsc::channel::<()>(100);
    // TODO: Replace () with actual OpportunitySignal type when engine is built

    info!("Inter-task channel created (capacity: 100)");

    // ============================================================
    // STEP 5: Spawn WebSocket Task
    // ============================================================
    // This task runs connect_and_stream() from network/binance.rs
    // It lives forever — handles its own reconnection internally
    //
    // We wrap it in a supervision task that restarts it if it ever panics
    // (connect_and_stream loops forever so this is a safety net)
    let ws_weaver = Arc::clone(&weaver_arc);
    // Arc::clone() — gives the WebSocket task its own ownership handle
    // Does NOT copy the underlying HashMap — just increments reference count
    // Both ws_weaver and weaver_arc point to the exact same HashMap in memory

    tokio::spawn(async move {
        // Supervision loop — if the WebSocket task somehow exits, restart it
        loop {
            info!("Starting WebSocket price feed task...");

            // Clone handle for this specific run of the task
            // We need a new clone each iteration because the previous
            // one was moved into connect_and_stream() and consumed
            let handle = Arc::clone(&ws_weaver);

            // Spawn the actual WebSocket task and wait for it to finish
            // (it should never finish — it loops forever internally)
            let result = tokio::spawn(
                network::binance::connect_and_stream(handle)
            ).await;

            // If we reach here, the task exited — this is unexpected
            match result {
                Ok(_) => warn!("WebSocket task exited cleanly — restarting"),
                Err(e) => error!("WebSocket task panicked: {} — restarting", e),
            }

            // Wait before restarting to avoid rapid restart loops
            tokio::time::sleep(Duration::from_millis(TASK_RESTART_DELAY_MS)).await;
        }
    });

    info!("WebSocket task spawned");

    // ============================================================
    // STEP 6: Wait for price map to populate
    // ============================================================
    // Don't start the engine until we have enough prices to build
    // a meaningful currency graph. On testnet this takes 30-120 seconds
    // because updates come every ~5 seconds per symbol.
    // On live this takes 5-15 seconds.
    info!("Waiting for price map to populate (need {} pairs)...", MIN_PRICES_TO_START);

    loop {
        // Check how many prices Weaver currently has
        let count = weaver.price_count().await;

        if count >= MIN_PRICES_TO_START {
            info!("Price map ready — {} pairs loaded, starting engine", count);
            break;
        }

        // Not enough yet — log progress and wait
        info!("Price map: {}/{} pairs loaded, waiting...", count, MIN_PRICES_TO_START);
        tokio::time::sleep(Duration::from_millis(STARTUP_CHECK_INTERVAL_MS)).await;
    }

    // ============================================================
    // STEP 7: Spawn Engine Task (STUB)
    // ============================================================
    // The engine reads prices from Weaver every 100ms,
    // builds the currency graph, runs Bellman-Ford,
    // and sends opportunities through opportunity_tx.
    //
    // NOT BUILT YET — placeholder spawn so architecture compiles.
    // Will be replaced in the next milestone with engine/graph.rs logic.
    let _engine_weaver = Arc::clone(&weaver_arc);
    let _engine_tx = opportunity_tx.clone();

    tokio::spawn(async move {
        // Supervision loop for engine task
        loop {
            info!("Engine task starting (STUB — Bellman-Ford not yet implemented)");

            // STUB: just log and sleep until engine is built
            // Real implementation: engine::graph::run_engine_loop(engine_weaver, engine_tx)
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                // TODO: Replace this loop with actual engine call:
                // engine::graph::run_engine_loop(Arc::clone(&engine_weaver), engine_tx.clone()).await;
            }
        }
    });

    info!("Engine task spawned (stub)");

    // ============================================================
    // STEP 8: Spawn Executor Task (STUB)
    // ============================================================
    // The executor receives opportunities from the engine via channel,
    // validates them (freshness, profitability after fees),
    // and executes FOK orders for all three legs.
    //
    // NOT BUILT YET — placeholder so architecture compiles.
    tokio::spawn(async move {
        // Supervision loop for executor task
        loop {
            info!("Executor task starting (STUB — FOK execution not yet implemented)");

            // STUB: drain the channel so it doesn't fill up during development
            // Real implementation: safety::executor::run_executor_loop(opportunity_rx)
            while let Some(_opportunity) = opportunity_rx.recv().await {
                // TODO: Replace with actual execution logic
                info!("Opportunity received (stub — not executing)");
            }

            warn!("Executor channel closed — restarting executor");
            tokio::time::sleep(Duration::from_millis(TASK_RESTART_DELAY_MS)).await;
        }
    });

    info!("Executor task spawned (stub)");

    // ============================================================
    // STEP 9: Keep main alive + periodic system health log
    // ============================================================
    // main() returning = process exits = all tasks die
    // We loop forever, printing a health report every 60 seconds
    // so you can see the bot is alive in your terminal
    info!("All tasks running — bot is live");
    info!("Press Ctrl+C to stop");

    let mut tick: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        tick += 1;

        // Health report every 60 seconds
        let price_count = weaver.price_count().await;
        info!(
            "[Health tick #{}] Uptime: {}min | Prices in Weaver: {}",
            tick,
            tick * 60 / 60,
            price_count
        );
    }
}