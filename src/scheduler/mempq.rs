use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;

use chrono::{Duration, Utc};

use crate::config::Config;

use super::{Scheduler, SchedulerItem};

/// In-memory priority-queue based scheduler.
///
/// Schedules higher priority items first.
//
// TODO: For now we implement in-memory priority queue scheduler but this will
// crash the system if we crawl large websites. Ideally, this writes to disk
// and only bring the necessary "page" to memory.
//
// Also, since the scheduler only returns url to crawl next. It's okay to
// protect it using a single mutex. This eliminates the need for complicated
// buffer manager since the "page" is only accessed by the scheduler that is
// protected behind a mutex.
pub struct MempqScheduler {
    config: Config,

    current_priority: HashMap<String, u8>,

    queues: HashMap<String, HashMap<u8, VecDeque<SchedulerItem>>>,
    // Number of items enqueued
    num_items: usize,

    added_urls: HashSet<String>,
    visited_urls: Arc<rocksdb::DB>,
}

impl MempqScheduler {
    pub fn new(config: &Config) -> Self {
        let visited_urls_path = Path::new(&config.data_dir).join("visited_urls");
        Self {
            config: config.clone(),
            current_priority: HashMap::new(),
            queues: HashMap::new(),
            num_items: 0,
            added_urls: HashSet::new(),
            visited_urls: Arc::new(rocksdb::DB::open_default(visited_urls_path.to_str().unwrap()).unwrap()),
        }
    }
}

impl Scheduler for MempqScheduler {
    fn next_item(&mut self, spider_name: &str) -> Option<SchedulerItem> {
        if !self.queues.contains_key(spider_name) {
            self.queues.insert(spider_name.to_owned(), HashMap::new());
        }
        let queues = self.queues.get_mut(spider_name).unwrap();

        if !self.current_priority.contains_key(spider_name) {
            self.current_priority.insert(spider_name.to_owned(), 0);
        }
        let current_priority = self.current_priority.get_mut(spider_name).unwrap();

        // Update current priority
        while *current_priority > 0 {
            let queue = queues.get(current_priority);
            match queue {
                Some(q) => {
                    if q.len() == 0 {
                        *current_priority -= 1;
                    } else {
                        break;
                    }
                }
                _ => {
                    *current_priority -= 1;
                }
            }
        }

        let res = match queues.get_mut(current_priority) {
            Some(queue) => queue.pop_front(),
            _ => None,
        };

        if res.is_some() {
            self.num_items -= 1;
        }

        res
    }

    fn enqueue_item(&mut self, spider_name: &str, item: SchedulerItem) -> bool {
        if !self.queues.contains_key(spider_name) {
            self.queues.insert(spider_name.to_owned(), HashMap::new());
        }
        let queues = self.queues.get_mut(spider_name).unwrap();

        if !self.current_priority.contains_key(spider_name) {
            self.current_priority.insert(spider_name.to_owned(), 0);
        }
        let current_priority = self.current_priority.get_mut(spider_name).unwrap();

        let now = Utc::now().naive_utc().timestamp();
        let visited_url = self.visited_urls.get(&item.url).unwrap().map(|x| {
            let x: &mut &[u8] = &mut x.as_ref();
            read_be_i64(x)
        });
        let visited_or_added =
            (visited_url.is_some() && visited_url.unwrap() > now) ||
            self.added_urls.contains(&item.url);
        if !visited_or_added || item.force {
            if item.priority > *current_priority {
                *current_priority = item.priority;
            }
            if !queues.contains_key(&item.priority) {
                queues.insert(item.priority, VecDeque::new());
            }
            let queue = queues.get_mut(&item.priority).unwrap();
            self.added_urls.insert(item.url.clone());
            queue.push_back(item);
            self.num_items += 1;
            true
        } else {
            false
        }
    }

    fn mark_visited(&mut self, url: &str) {
        // Default to expire in 100 years
        let expired_interval = self
            .config
            .visited_url_expired_interval_days
            .unwrap_or(100 * 365);

        let expired_at = Utc::now().naive_utc() + Duration::days(expired_interval as i64);
        self.added_urls.remove(url);
        self.visited_urls
            .put(url.to_owned(), expired_at.timestamp().to_be_bytes())
            .unwrap();
    }

    fn size(&self) -> usize {
        self.num_items
    }

    fn max_priority(&self) -> u8 {
        let mut priority = 0;
        for (_, curp) in self.current_priority.iter() {
            if priority < *curp {
                priority = *curp;
            }
        }
        priority
    }
}

fn read_be_i64(input: &mut &[u8]) -> i64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    *input = rest;
    i64::from_be_bytes(int_bytes.try_into().unwrap())
}

#[derive(Clone, Debug)]
pub struct MempqSchedulerBuilder {
    config: Option<Config>,
    db: Option<Arc<rocksdb::DB>>,
}

impl MempqSchedulerBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            db: None,
        }
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_db(mut self, db: Arc<rocksdb::DB>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn build(&self) -> MempqScheduler {
        let config = match self.config.clone() {
            Some(c) => c,
            None => Config::default(),
        };

        let db = match self.db.clone() {
            Some(d) => d,
            None => {
                let path = Path::new(&config.data_dir).join("visited_urls");
                let d = rocksdb::DB::open_default(path.to_str().unwrap()).unwrap();
                Arc::new(d)
            }
        };

        MempqScheduler {
            config,
            current_priority: HashMap::new(),
            queues: HashMap::new(),
            num_items: 0,
            added_urls: HashSet::new(),
            visited_urls: db,
        }
    }
}
