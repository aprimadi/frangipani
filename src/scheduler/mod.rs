use chrono::NaiveDateTime;

pub mod mempq;

pub const DEFAULT_PRIORITY: u8 = 10;

pub struct SchedulerItem {
    pub spider_name: String,
    pub url: String,
    pub priority: u8,

    // This flag tells the scheduler to whitelist the url, for example when
    // adding back starting urls for re-crawling. If this flag is not set, the
    // url will be treated as visited and ignored by the scheduler.
    pub force: bool,

    // These fields are for exponential backoff algorithm
    pub retry: u8, // Number of retry
    pub last_retry: Option<NaiveDateTime>,
}

pub trait Scheduler {
    fn next_item(&mut self, spider_name: &str) -> Option<SchedulerItem>;

    /// Enqueue item to be scheduled lates.
    ///
    /// Returns whether the item is enqueued.
    fn enqueue_item(&mut self, spider_name: &str, item: SchedulerItem) -> bool;

    fn mark_visited(&mut self, url: &str);

    /// Returns the number of items enqueued in the scheduler.
    fn size(&self) -> usize;

    /// Returns the maximum priority of items enqueued.
    fn max_priority(&self) -> u8;
}
