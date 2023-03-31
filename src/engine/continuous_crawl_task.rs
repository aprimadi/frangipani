use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::scheduler::{DEFAULT_PRIORITY, Scheduler, SchedulerItem};

use super::EngineState;

pub(super) fn start_continuous_crawl_thread<Sched>(
    state: Arc<EngineState<Sched>>,
    stop_tx: broadcast::Sender<()>,
) -> JoinHandle<()>
where
    // TODO: Not sure how to fix this lifetime issue without using static
    Sched: 'static + Scheduler + Send,
{
    let mut stop_rx = stop_tx.subscribe();
    tokio::spawn(async move {
        'run: loop {
            let duration = std::time::Duration::from_secs(60 * state.config.continuous_crawl_interval_mins);
            let sleep = tokio::time::sleep(duration);
            tokio::pin!(sleep);

            tokio::select! {
                res = stop_rx.recv() => {
                    if let Ok(_) = res {
                        break 'run;
                    }
                }
                _ = &mut sleep => {
                    let mut scheduler = state.scheduler.lock().unwrap();
                    let num_items = scheduler.size();
                    let priority;
                    if num_items > 0 {
                        if scheduler.max_priority() < 255 {
                            priority = scheduler.max_priority() + 1;
                        } else {
                            priority = scheduler.max_priority();
                        }
                    } else {
                        priority = DEFAULT_PRIORITY;
                    }

                    log::info!(
                        "Recrawl, num items: {}, priority: {}",
                        num_items,
                        priority
                    );
                    for (name, spider) in state.spiders.iter() {
                        let start_urls = spider.start_urls();
                        for start_url in start_urls {
                            let item = SchedulerItem {
                                spider_name: name.clone(),
                                url: start_url,
                                priority,
                                force: true,
                                retry: 0,
                                last_retry: None,
                            };
                            scheduler.enqueue_item(&spider.name(), item);
                        }
                    }
                }
            }
        }
    })
}
