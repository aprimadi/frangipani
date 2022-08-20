mod config;
mod downloader;
mod engine;
mod spider;
mod stats;

pub mod scheduler;
pub mod util;

// (Re) Exports
pub use config::Config;
pub use engine::Engine;
pub use scheduler::{Scheduler, SchedulerItem};
pub use spider::Spider;

pub type Response = ureq::Response;

use scheduler::mempq::MempqScheduler;

pub fn engine(
    spiders: Vec<Box<dyn Spider + Send + Sync>>,
) -> Engine<MempqScheduler> {
    let config = Config::default();
    let scheduler = MempqScheduler::new(&config);
    Engine::new(config, scheduler, spiders)
}

pub fn engine_with_config(
    config: Config,
    spiders: Vec<Box<dyn Spider + Send + Sync>>,
) -> Engine<MempqScheduler> {
    let scheduler = MempqScheduler::new(&config);
    Engine::new(config, scheduler, spiders)
}

