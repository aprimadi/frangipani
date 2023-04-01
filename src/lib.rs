use std::sync::Arc;

mod config;
mod downloader;
mod engine;
mod spider;
mod stats;

pub mod scheduler;
pub mod util;

// (Re) Exports
pub use config::Config;
pub use engine::{Engine, EngineState};
pub use scheduler::{Scheduler, SchedulerItem};
pub use spider::Spider;

pub type Response = ureq::Response;

use scheduler::mempq::MempqScheduler;

pub fn engine(
    spiders: Vec<Box<dyn Spider + Send + Sync>>,
) -> Engine {
    let config = Config::default();
    let scheduler = MempqScheduler::new(&config);
    Engine::new(config, Box::new(scheduler), spiders)
}

pub struct EngineBuilder
{
    config: Option<Config>,
    scheduler: Option<Box<dyn Scheduler + Send>>,
    spiders: Vec<Box<dyn Spider + Send + Sync>>,
}

impl EngineBuilder
{
    pub fn new() -> Self {
        Self {
            config: None,
            scheduler: None,
            spiders: vec![],
        }
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_scheduler(mut self, scheduler: Box<dyn Scheduler + Send>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn add_spider(mut self, spider: Box<dyn Spider + Send + Sync>) -> Self {
        self.spiders.push(spider);
        self
    }

    pub fn build(self) -> Engine {
        let config = match self.config {
            Some(c) => c,
            None => Config::default(),
        };
        let scheduler = match self.scheduler {
            Some(s) => s,
            None => {
                let s = MempqScheduler::new(&config);
                let boxed = Box::new(s) as Box<dyn Scheduler + Send>;
                boxed
            }
        };
        let spiders = self.spiders;

        let state = EngineState::new(config, scheduler, spiders);
        Engine { state: Arc::new(state) }
    }
}
