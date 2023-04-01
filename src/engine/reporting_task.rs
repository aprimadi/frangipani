use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::engine::EngineState;

pub(super) fn start_reporting_thread(
    state: Arc<EngineState>,
    stop_tx: broadcast::Sender<()>,
) -> JoinHandle<()> {
    let mut stop_rx = stop_tx.subscribe();
    tokio::spawn(async move {
        'run: loop {
            let sleep = tokio::time::sleep(std::time::Duration::from_secs(60));
            tokio::pin!(sleep);

            tokio::select! {
                res = stop_rx.recv() => {
                    if let Ok(_) = res {
                        break 'run;
                    }
                }
                _ = &mut sleep => {
                    let crawled = state.stats.total_crawled();
                    let processed = state.stats.total_processed();
                    let cpm = state.stats.crawled_per_minute();
                    let ppm = state.stats.processed_per_minute();
                    log::info!(
                        "{} crawled at {} pages/minute, {} processed at {} items/minute",
                        crawled,
                        cpm,
                        processed,
                        ppm,
                    );
                }
            }
        }
    })
}
