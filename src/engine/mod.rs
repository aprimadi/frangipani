use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

mod continuous_crawl_task;
mod downloader_pool;
mod guard_robot;
mod processing_task;
mod reporting_task;
mod thread_state;

use crate::Config;
use crate::scheduler::{DEFAULT_PRIORITY, Scheduler, SchedulerItem};
use crate::spider::Spider;
use crate::stats::Stats;

use continuous_crawl_task::start_continuous_crawl_thread;
use downloader_pool::DownloaderPool;
use guard_robot::GuardRobot;
use processing_task::start_processing_thread;
use reporting_task::start_reporting_thread;
use thread_state::ThreadState;

pub struct EngineState
{
    config: Arc<Config>,
    scheduler: Arc<Mutex<Box<dyn Scheduler + Send>>>,
    spiders: HashMap<String, Box<dyn Spider + Send + Sync>>,
    downloader_pool: DownloaderPool,
    guard_robot: GuardRobot,
    thread_state: ThreadState,

    stats: Stats,
}

impl EngineState {
    pub fn new(
        config: Config,
        scheduler: Box<dyn Scheduler + Send>,
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
pub struct Engine {
    pub(crate) state: Arc<EngineState>,
}

impl Engine {
    pub fn new(
        config: Config,
        scheduler: Box<dyn Scheduler + Send>,
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

        // Start the downloader pool. This will spawn multiple downloaders.
        let mut handles = self.state.downloader_pool.start(stop_tx.clone());
        join_handles.append(&mut handles);

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

        // Start processing threads
        for i in 0..config.concurrent_requests {
            let handle = start_processing_thread(
                i+1,
                self.state.clone(),
                stop_tx.clone(),
            );
            join_handles.push(handle);
        }

        // Start continuous crawl thread
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
