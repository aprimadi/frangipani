pub struct Config {
    /// Bot name / user agent
    pub bot_name: String,
    /// The maximum number of concurrent requests that will be performed by
    /// the downloader
    pub concurrent_requests: u32,
    /// The maximum number of concurrent requests that will be performed to
    /// any single domain. A value of 0 means that this setting will be 
    /// ignored.
    pub concurrent_requests_per_domain: u32,
    /// If set to true, the engine will re-add starting urls every defined 
    /// interval. Default to false.
    pub continuous_crawl: bool,
    /// Interval before the engine re-add starting urls in minutes.
    pub continuous_crawl_interval_mins: u64,
    /// Directory to store crawler state.
    pub data_dir: String,
    /// The amount of time (in secs) that the downloader should wait before
    /// downloading consecutive pages from the same website.
    pub download_delay: f32,
    /// If enabled, Harvester will respect robots.txt policies.
    pub robotstxt_obey: bool,
}

impl Config {
    pub fn sanity_check(&self) {
        if self.concurrent_requests == 0 {
            panic!("config.concurrent_requests cannot be zero");
        }
        if self.concurrent_requests_per_domain > self.concurrent_requests {
            panic!("config.concurrent_requests_per_domain must be greater than config.concurrent_requests");
        }
        if self.download_delay < 0.0 {
            panic!("config.download_delay must be positive");
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bot_name: "harvesterbot".to_owned(),
            concurrent_requests: 16,
            concurrent_requests_per_domain: 1,
            continuous_crawl: false,
            continuous_crawl_interval_mins: 60,
            data_dir: "db".to_owned(),
            download_delay: 2.0,
            robotstxt_obey: true,
        }
    }
}
