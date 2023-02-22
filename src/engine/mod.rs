use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::seq::SliceRandom;
use texting_robots::Robot;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use var_bitmap::Bitmap;

mod downloader_pool;

use crate::Config;
use crate::downloader::Downloader;
use crate::scheduler::{DEFAULT_PRIORITY, Scheduler, SchedulerItem};
use crate::spider::Spider;
use crate::stats::Stats;
use crate::util;

use downloader_pool::DownloaderPool;

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
    scheduler: Mutex<Sched>,
    spiders: HashMap<String, Box<dyn Spider + Send + Sync>>,
    downloader_pool: DownloaderPool,

    // Map from host -> robot
    //
    // Note that this is part of the engine because the scheduler's job is
    // only to schedule item, whether the item is added or visited.
    //
    // Obeying robots.txt is at a url level. Although, one can argue that it 
    // should be part of the scheduler, we want to keep the scheduler interface
    // as clean as possible, dealing only with added or visited items. This is 
    // to ensure that one can extend the scheduler and provides other 
    // implementations.
    robots: Mutex< HashMap<String, Option<Robot>> >,

    // Processing thread status either idle `1` or busy `0`
    idle_process: Mutex<Bitmap>,

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
            scheduler: Mutex::new(scheduler),
            spiders: spiders_,
            downloader_pool: DownloaderPool::new(config.clone()),
            robots: Mutex::new(HashMap::new()),
            idle_process: Mutex::new(Bitmap::new()),
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
        {
            let mut idle_process = self.state.idle_process.lock().unwrap();
            for _ in 0..config.concurrent_requests {
                idle_process.push(false);
            }
        }
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

                    {
                        let mut idle_process = state.idle_process.lock().unwrap();
                        let process_idx = (thread_id - 1) as usize;
                        idle_process.set(process_idx, false);
                    }

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
                            if !is_url_allowed_by_robot(state.clone(), &url) {
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
                        let mut idle_process = state.idle_process.lock().unwrap();
                        let process_idx = (thread_id - 1) as usize;
                        idle_process.set(process_idx, true);

                        let mut all_idle = true;
                        for process_idx in 0..idle_process.size() {
                            all_idle &= idle_process.get(process_idx);
                        }

                        if all_idle {
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

fn is_url_allowed_by_robot<Sched>(
    state: Arc<EngineState<Sched>>,
    url: &str,
) -> bool 
where
    Sched: 'static + Scheduler + Send,
{
    if !state.config.robotstxt_obey {
        return true;
    }

    let host = util::get_host(&url);
    if host.is_none() { // robot rules doesn't apply
        return true;
    }
    let host = host.unwrap();

    let robot_url = util::get_robot_url(&url);
    if robot_url.is_none() { // no robot url
        return true;
    }
    let robot_url = robot_url.unwrap();

    let mut robots = state.robots.lock().unwrap();

    // Populate robot
    if !robots.contains_key(&host) {
        if let Ok(response) = ureq::get(&robot_url).call() {
            let text = response.into_string().unwrap();
            let robot = Robot::new(&state.config.bot_name, text.as_bytes()).unwrap();
            robots.insert(host.clone(), Some(robot));
        } else {
            robots.insert(host.clone(), None);
        }
    }

    // Check if url obey robots.txt
    if let Some(Some(robot)) = robots.get(&host) {
        robot.allowed(&url)
    } else { // No robot found, return true
        true
    }
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
