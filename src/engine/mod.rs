use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::seq::SliceRandom;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

mod downloader_pool;
mod guard_robot;
mod thread_state;

use crate::Config;
use crate::downloader::Downloader;
use crate::scheduler::{DEFAULT_PRIORITY, Scheduler, SchedulerItem};
use crate::spider::Spider;
use crate::stats::Stats;
use crate::util;

use downloader_pool::DownloaderPool;
use guard_robot::GuardRobot;
use thread_state::{ThreadState, ThreadStatus};

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

struct EngineState<Sched> 
where
    Sched: Scheduler + Send,
{
    config: Arc<Config>,
    scheduler: Arc<Mutex<Sched>>,
    spiders: HashMap<String, Box<dyn Spider + Send + Sync>>,
    downloader_pool: DownloaderPool,
    guard_robot: GuardRobot,
    thread_state: ThreadState,

    stats: Stats,
}

impl<Sched> EngineState<Sched>
where
    Sched: Scheduler + Send
{
    pub fn new(
        config: Config, 
        scheduler: Sched, 
        spiders: Vec<Box<dyn Spider + Send + Sync>>
    ) -> Self {
        let config = Arc::new(config);
        let mut spiders_ = HashMap::new();
        for spider in spiders {
            let name = spider.name();
            spiders_.insert(name, spider);
        }
        Self {
            config: config.clone(),
            scheduler: Arc::new(Mutex::new(scheduler)),
            spiders: spiders_,
            downloader_pool: DownloaderPool::new(config.clone()),
            guard_robot: GuardRobot::new(config.clone()),
            thread_state: ThreadState::new(config.concurrent_requests as usize),
            stats: Stats::new(),
        }
    }
}

// Note that since `config`, `spiders`, and `downloaders` are read-only after
// initialization, it doesn't need to be protected by mutex.
pub struct Engine<Sched>
where 
    Sched: Scheduler + Send,
{
    state: Arc<EngineState<Sched>>,
}

impl<Sched> Engine<Sched> 
where
    Sched: 'static + Scheduler + Send,
{
    pub fn new(
        config: Config,
        scheduler: Sched,
        spiders: Vec<Box<dyn Spider + Send + Sync>>,
    ) -> Self {
        let state = EngineState::new(config, scheduler, spiders);
        Self { state: Arc::new(state) }
    }

    
    pub async fn start(&mut self) {
        let config = &self.state.config;
        config.sanity_check();

        if self.state.spiders.len() == 0 {
            panic!("No spiders set");
        }

        let (stop_tx, _) = broadcast::channel::<()>(32);
        let tx = stop_tx.clone();
        ctrlc::set_handler(move || {
            tx.send(()).unwrap();
        }).unwrap();

        let mut join_handles = vec![];
        
        // Spawn multiple downloaders
        let mut downloaders = vec![];
        log::debug!("concurrent requests: {}", config.concurrent_requests);
        for _ in 0..config.concurrent_requests {
            let mut downloader = Downloader::new(config.download_delay);
            let mut handles = downloader.start(stop_tx.clone());
            let downloader = Arc::new(downloader);
            downloaders.push(downloader);
            join_handles.append(&mut handles);
        }
        self.state.downloader_pool.set_downloaders(downloaders);
        
        // Puts each spider start urls to scheduler
        {
            let mut scheduler = self.state.scheduler.lock().unwrap();
            for (name, spider) in self.state.spiders.iter() {
                let start_urls = spider.start_urls();
                for start_url in start_urls {
                    let item = SchedulerItem {
                        spider_name: name.clone(),
                        url: start_url,
                        priority: DEFAULT_PRIORITY,
                        force: true,
                        retry: 0,
                        last_retry: None,
                    };
                    scheduler.enqueue_item(&spider.name(), item);
                }
            }
        }

        // Start reporting thread
        {
            let h = start_reporting_thread(self.state.clone(), stop_tx.clone());
            join_handles.push(h);
        }
        
        // Start processing
        for i in 0..config.concurrent_requests {
            let handle = start_processing_thread(
                i+1,
                self.state.clone(),
                stop_tx.clone(),
            );
            join_handles.push(handle);
        }

        if config.continuous_crawl {
            let h = start_continuous_crawl_thread(
                self.state.clone(), 
                stop_tx.clone()
            );
            join_handles.push(h);
        }

        for h in join_handles {
            h.await.unwrap();
        }

        log::info!("Exit gracefully");
    }
}

fn start_processing_thread<Sched>(
    thread_id: u32,
    state: Arc<EngineState<Sched>>,
    stop_tx: broadcast::Sender<()>,
) -> JoinHandle<()> 
where
    // TODO: Not sure how to fix this lifetime issue without using static
    Sched: 'static + Scheduler + Send,
{
    log::debug!("[thread-{}] start", thread_id);
    let mut stop_rx = stop_tx.subscribe();
    tokio::spawn(async move {
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
                    "[thread-{}] scheduler lock 1 waiting on lock", 
                    thread_id
                );
                let mut scheduler = state.scheduler.lock().unwrap();
                lock_debug!("[thread-{}] scheduler lock 1 acquired", thread_id);
                item = scheduler.next_item(chosen_spider);
            }
            lock_debug!("[thread-{}] scheduler lock 1 released", thread_id);

            match item {
                Some(item) => {
                    log::info!("[thread-{}] {}", thread_id, &item.url);

                    state.thread_state.set_thread_status(thread_id, ThreadStatus::Busy);

                    // Get response from the downloader
                    let downloader = state.downloader_pool.get_downloader(&item.url);
                    let response = downloader.get(&item.url).await;
                    if let Err(e) = response {
                        // TODO: More advanced error handling on request error
                        // Perhaps retry later depending on the actual error.
                        log::error!("[thread-{}] {:?}", thread_id, e);
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
                        lock_debug!("[thread-{}] scheduler lock 2 acquired", thread_id);
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
                    lock_debug!("[thread-{}] scheduler lock 2 released", thread_id);
                }
                None => {
                    {
                        state.thread_state.set_thread_status(thread_id, ThreadStatus::Idle);

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

fn start_reporting_thread<Sched>(
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

fn start_continuous_crawl_thread<Sched>(
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
