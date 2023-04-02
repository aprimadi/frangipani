use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fasthash::FastHash;
use fasthash::xx::Hash64;
use rand::{RngCore, SeedableRng};
use rand::seq::SliceRandom;
use rand_chacha::ChaCha8Rng;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::{Config, util};
use crate::downloader::Downloader;

#[derive(Clone)]
pub struct DownloaderPool {
    config: Arc<Config>,
    inner: Arc<Mutex<DownloaderPoolInner>>,
}

struct DownloaderPoolInner {
    downloaders: Vec<Arc<Downloader>>,
    domain_downloaders: HashMap<String, Vec<usize>>,
}

impl DownloaderPool {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(DownloaderPoolInner {
                downloaders: vec![],
                domain_downloaders: HashMap::new(),
            }))
        }
    }

    pub fn start(&self, stop_tx: broadcast::Sender<()>) -> Vec<JoinHandle<()>> {
        let mut join_handles = vec![];
        let mut inner = self.inner.lock().unwrap();
        for _ in 0..self.config.concurrent_requests {
            let mut downloader = Downloader::new(self.config.download_delay);
            let handle = downloader.start(stop_tx.clone());
            join_handles.push(handle);

            let downloader = Arc::new(downloader);
            inner.downloaders.push(downloader.clone());
        }
        join_handles
    }

    pub fn get_downloader(&self, url: &str) -> Arc<Downloader> {
        let mut rng = rand::thread_rng();
        let mut inner = self.inner.lock().unwrap();
        if self.config.concurrent_requests_per_domain == 0 {
            // Concurrent requests per domain is disabled, simply choose from
            // all downloaders
            let downloader = inner.downloaders.choose(&mut rng).unwrap();
            downloader.clone()
        } else {
            let domain = util::get_domain(url);
            if !inner.domain_downloaders.contains_key(&domain) {
                // Build indices
                let seed = Hash64::hash(domain.as_bytes());
                let mut cha_rng = ChaCha8Rng::seed_from_u64(seed);
                let mut indices = vec![];
                let sz = self.config.concurrent_requests_per_domain as usize;
                while indices.len() < sz {
                    let idx = cha_rng.next_u32() as usize % inner.downloaders.len();
                    if !indices.contains(&idx) {
                        indices.push(idx);
                    }
                }
                inner.domain_downloaders.insert(domain.clone(), indices);
            }

            let indices = inner.domain_downloaders.get(&domain).unwrap();
            let idx = indices.choose(&mut rng).unwrap();
            let downloader = inner.downloaders.get(*idx).unwrap();
            downloader.clone()
        }
    }
}
