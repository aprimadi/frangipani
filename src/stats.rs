use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{NaiveDateTime, Utc};

// Needed stats:
// - crawl / minutes
// - processed / minutes
// - total processed
// - total crawled
// - runtime for single cycle crawl

pub struct Stats {
    total_crawled: AtomicU64,
    total_processed: AtomicU64,
    start_time: Mutex<NaiveDateTime>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            total_crawled: AtomicU64::new(0),
            total_processed: AtomicU64::new(0),
            start_time: Mutex::new(Utc::now().naive_utc()),
        }
    }

    pub fn reset(&self) {
        let mut start_time = self.start_time.lock().unwrap();
        self.total_crawled.store(0, Ordering::Relaxed);
        self.total_processed.store(0, Ordering::Relaxed);
        *start_time = Utc::now().naive_utc();
    }

    pub fn incr_total_crawled(&self) {
        self.total_crawled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_total_processed(&self, value: u64) {
        self.total_processed.fetch_add(value, Ordering::Relaxed);
    }

    pub fn total_crawled(&self) -> u64 {
        self.total_crawled.load(Ordering::Relaxed)
    }

    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }

    pub fn crawled_per_minute(&self) -> u64 {
        let crawled = self.total_crawled();
        let elapsed = (self.elapsed_time() / 60) as u64;
        if elapsed > 0 {
            crawled / elapsed
        } else {
            0
        }
    }

    pub fn processed_per_minute(&self) -> u64 {
        let processed = self.total_processed();
        let elapsed = (self.elapsed_time() / 60) as u64;
        if elapsed > 0 {
            processed / elapsed
        } else {
            0
        }
    }
    
    /// Elapsed time for this crawl cycle in seconds
    pub fn elapsed_time(&self) -> i64 {
        let start_time = self.start_time.lock().unwrap();
        let now = Utc::now().naive_utc();
        let elapsed = now - *start_time;
        elapsed.num_seconds()
    }
}

