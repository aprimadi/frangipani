use std::sync::{Arc, Mutex};

use var_bitmap::Bitmap;

/// This is used to keep track of which threads are idle or busy
///
/// Note that thread id should be numbered between 1 to number of threads.
#[derive(Clone)]
pub struct ThreadState {
    inner: Arc<Mutex<ProcessStateInner>>,
}

pub enum ThreadStatus {
    Idle,
    Busy,
}

/// Idle process are marked as `1` and busy process is marked as `0`.
struct ProcessStateInner {
    idle_map: Bitmap,
}

impl ThreadState {
    pub fn new(num_threads: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProcessStateInner { 
                idle_map: Bitmap::with_size(num_threads),
            }))
        }
    }
    
    pub fn set_thread_status(&self, thread_id: u32, status: ThreadStatus) {
        let mut inner = self.inner.lock().unwrap();
        let idx = (thread_id - 1) as usize;
        match status {
            ThreadStatus::Busy => inner.idle_map.set(idx, false),
            ThreadStatus::Idle => inner.idle_map.set(idx, true),
        }
    }
    
    pub fn is_all_idle(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        let mut all_idle = true;
        for idx in 0..inner.idle_map.size() {
            all_idle &= inner.idle_map.get(idx);
        }
        all_idle
    }
}
