use std::sync::{Arc, Mutex};

use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;

use crate::Config;

use super::EngineState;
use super::worker_task::start_worker_thread;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerState {
    Initialized,
    Running,
    Panicked,
    Finished,
}

pub struct WorkerPool {
    config: Arc<Config>,
    state: Arc<EngineState>,
    worker_states: Arc<Mutex<Vec<WorkerState>>>,
    join_handle: Option<JoinHandle<()>>,
}

impl WorkerPool {
    pub fn new(config: Arc<Config>, state: Arc<EngineState>) -> Self {
        Self {
            config,
            state,
            worker_states: Arc::new(Mutex::new(vec![])),
            join_handle: None,
        }
    }

    pub fn start(&mut self, stop_tx: broadcast::Sender<()>) {
        let monitor_notify = Arc::new(Notify::new());
        let worker_notify = monitor_notify.clone();

        // First run a monitor thread that listens to messages from
        // worker thread
        let worker_states = self.worker_states.clone();
        let state = self.state.clone();
        let monitor_stop_tx = stop_tx.clone();
        let monitor_handle = tokio::spawn(async move {
            loop {
                // Wait for notification from worker monitor thread
                monitor_notify.notified().await;

                // Restart all panicked workers
                {
                    let mut worker_guard = worker_states.lock().unwrap();
                    for (idx, ws) in worker_guard.iter_mut().enumerate() {
                        match ws {
                            WorkerState::Panicked => {
                                // Set worker states to initialized
                                *ws = WorkerState::Initialized;

                                spawn_worker_and_monitor_thread(
                                    (idx+1) as u32,
                                    state.clone(),
                                    worker_states.clone(),
                                    monitor_stop_tx.clone(),
                                    monitor_notify.clone(),
                                );
                            }
                            _ => { /* Do nothing */ },
                        }
                    }
                }

                // If all workers are completed, stop this thread
                {
                    let worker_states = worker_states.lock().unwrap();
                    let finished = worker_states
                        .iter()
                        .all(|x| x.clone() == WorkerState::Finished);
                    if finished {
                        break;
                    }
                }
            }
        });
        self.join_handle = Some(monitor_handle);

        // Initialize worker states
        {
            let mut worker_states = self.worker_states.lock().unwrap();
            for _ in 0..self.config.concurrent_requests {
                worker_states.push(WorkerState::Initialized);
            }
        }

        // Spawn workers
        for i in 0..self.config.concurrent_requests {
            spawn_worker_and_monitor_thread(
                i+1,
                self.state.clone(),
                self.worker_states.clone(),
                stop_tx.clone(),
                worker_notify.clone(),
            )
        }
    }

    // Wait for all worker threads to complete
    pub async fn join(&mut self) {
        let handle = std::mem::replace(&mut self.join_handle, None);
        if let Some(h) = handle {
            h.await.unwrap();
        }
    }
}

fn spawn_worker_and_monitor_thread(
    worker_id: u32,
    state: Arc<EngineState>,
    worker_states: Arc<Mutex<Vec<WorkerState>>>,
    stop_tx: broadcast::Sender<()>,
    monitor_notify: Arc<Notify>,
) {
    let handle = start_worker_thread(
        worker_id,
        state.clone(),
        worker_states.clone(),
        stop_tx.clone(),
    );

    // Spawns a worker monitor thread that monitors the status of the
    // threads.
    tokio::spawn(async move {
        let result = handle.await;

        if let Err(e) = result {
            // Worker thread panicked

            log::error!("{}", e);

            // Set the worker state
            let worker_idx = (worker_id - 1) as usize;
            let mut worker_states = worker_states.lock().unwrap();
            assert_eq!(worker_states[worker_idx], WorkerState::Running);
            worker_states[worker_idx] = WorkerState::Panicked;

            // Notify monitor that worker states has changed
            monitor_notify.notify_one();
        } else {
            // Worker thread completed successfully

            // Set the worker state
            let worker_idx = (worker_id - 1) as usize;
            let mut worker_states = worker_states.lock().unwrap();
            assert_eq!(worker_states[worker_idx], WorkerState::Running);
            worker_states[worker_idx] = WorkerState::Finished;

            // Notify monitor that worker states has changed
            monitor_notify.notify_one();
        }
    });
}
