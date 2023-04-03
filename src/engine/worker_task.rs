use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::seq::SliceRandom;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::scheduler::SchedulerItem;
use crate::util;

use super::EngineState;
use super::thread_state::ThreadStatus;
use super::worker_pool::WorkerState;

const DEBUG_LOCK: bool = false;

#[macro_export(local_inner_macros)]
macro_rules! lock_debug {
    // debug!("a {} event", "log")
    ($($arg:tt)+) => {
        if DEBUG_LOCK {
            log::debug!($($arg)+);
        }
    };
}

pub(super) fn start_worker_thread(
    worker_id: u32,
    state: Arc<EngineState>,
    worker_states: Arc<Mutex<Vec<WorkerState>>>,
    stop_tx: broadcast::Sender<()>,
) -> JoinHandle<()> {
    log::debug!("[worker-{}] start", worker_id);
    let mut stop_rx = stop_tx.subscribe();
    tokio::spawn(async move {
        // Set worker states to running
        {
            let mut worker_states = worker_states.lock().unwrap();
            let idx = (worker_id - 1) as usize;
            assert_eq!(worker_states[idx], WorkerState::Initialized);
            worker_states[idx] = WorkerState::Running;
        }


        'run: loop {
            if let Ok(_) = stop_rx.try_recv() {
                break 'run;
            }

            // Get next item
            let item;
            {
                let mut rng = rand::thread_rng();
                let spider_names: Vec<&String> = state.spiders.keys().collect();
                let chosen_spider = *spider_names.choose(&mut rng).unwrap();

                lock_debug!(
                    "[worker-{}] scheduler lock 1 waiting on lock",
                    worker_id
                );
                let mut scheduler = state.scheduler.lock().unwrap();
                lock_debug!("[worker-{}] scheduler lock 1 acquired", worker_id);
                item = scheduler.next_item(chosen_spider);
            }
            lock_debug!("[worker-{}] scheduler lock 1 released", worker_id);

            match item {
                Some(item) => {
                    log::info!("[worker-{}] {}", worker_id, &item.url);

                    state.thread_state.set_thread_status(worker_id, ThreadStatus::Busy);

                    // Get response from the downloader
                    let downloader = state.downloader_pool.get_downloader(&item.url);
                    let response = downloader.get(&item.url).await;
                    if let Err(e) = response {
                        // TODO: More advanced error handling on request error
                        // Perhaps retry later depending on the actual error.
                        log::error!("[worker-{}] {:?}", worker_id, e);
                        continue;
                    }
                    let response = response.unwrap();
                    state.stats.incr_total_crawled();

                    // Route response to the spider
                    let spider = state.spiders.get(&item.spider_name).unwrap();
                    let base_url = response.get_url().to_owned();
                    let (num_processed, urls) = spider.parse(response).await;
                    let urls = normalize_urls(&base_url, urls);
                    state.stats.add_total_processed(num_processed);

                    // Enqueue back urls from the spider
                    {
                        let mut scheduler = state.scheduler.lock().unwrap();
                        lock_debug!("[worker-{}] scheduler lock 2 acquired", worker_id);
                        scheduler.mark_visited(&item.url);
                        for url in urls {
                            if !state.guard_robot.is_allowed(&url) {
                                continue;
                            }

                            let new_item = SchedulerItem {
                                spider_name: item.spider_name.clone(),
                                url,
                                priority: item.priority,
                                force: false,
                                retry: 0,
                                last_retry: None,
                            };
                            scheduler.enqueue_item(&item.spider_name, new_item);
                        }
                    }
                    lock_debug!("[worker-{}] scheduler lock 2 released", worker_id);
                }
                None => {
                    {
                        state.thread_state.set_thread_status(worker_id, ThreadStatus::Idle);

                        if state.thread_state.is_all_idle() {
                            if state.config.continuous_crawl {
                                state.stats.reset();
                            } else {
                                // Crawl finished, exit.
                                let _res = stop_tx.send(());
                                break 'run;
                            }
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    })
}

fn normalize_urls(base_url: &str, urls: Vec<String>) -> Vec<String> {
    let mut res = vec![];
    for url in urls {
        let absolute_url = util::join_url(base_url, &url);
        let mut req_url = reqwest::Url::parse(&absolute_url).unwrap();
        req_url.set_fragment(None);
        if req_url.scheme() != "http" && req_url.scheme() != "https" {
            continue;
        }
        res.push(req_url.to_string());
    }
    res
}
