use chrono::Utc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub enum DownloaderError {
    RequestError(ureq::Error),
    Killed,
}

#[derive(Debug)]
enum DownloaderRequestIpc {
    Get { url: String, tx: mpsc::Sender<DownloaderResponseIpc> },
}

#[derive(Debug)]
enum DownloaderResponseIpc {
    Get(Result<ureq::Response, DownloaderError>),
}

#[derive(Clone)]
pub struct Downloader {
    download_delay: f32,
    request_tx: Option<mpsc::Sender<DownloaderRequestIpc>>,
}

impl Downloader {
    pub fn new(download_delay: f32) -> Self {
        Self {
            download_delay,
            request_tx: None,
        }
    }
    
    pub fn start(
        &mut self, 
        stop_tx: broadcast::Sender<()>
    ) -> Vec<JoinHandle<()>> {
        let (request_tx, request_rx) = mpsc::channel::<DownloaderRequestIpc>(32);
        self.request_tx = Some(request_tx.clone());
        
        let mut handles = vec![];
        {
            let h = start_processing_thread(
                self.download_delay, 
                request_rx,
                stop_tx,
            );
            handles.push(h);
        }
        handles
    }
    
    // Note this is thread-safe since it doesn't modify any underlying data.
    pub async fn get(
        &self, 
        url: &str
    ) -> Result<ureq::Response, DownloaderError> {
        let request_tx = self.request_tx.clone().unwrap();
        
        let (response_tx, mut response_rx) = 
            mpsc::channel::<DownloaderResponseIpc>(1);
        let req = DownloaderRequestIpc::Get { 
            url: url.to_owned(), 
            tx: response_tx 
        };
        request_tx.send(req).await.unwrap();
        
        if let Some(ipc) = response_rx.recv().await {
            match ipc {
                DownloaderResponseIpc::Get(resp) => resp
            }
        } else {
            Err(DownloaderError::Killed)
        }
    }
}

fn start_processing_thread(
    download_delay: f32,
    mut rx: mpsc::Receiver<DownloaderRequestIpc>,
    stop_tx: broadcast::Sender<()>,
) -> JoinHandle<()> {
    let mut stop_rx = stop_tx.subscribe();
    tokio::spawn(async move {
        // Timestamp since epoch in millis
        let mut last_fetch = 0;
        let download_delay_millis = (download_delay * 1000.0) as i64;

        'run: loop {
            let sleep = tokio::time::sleep(std::time::Duration::from_millis(1));
            tokio::pin!(sleep);

            tokio::select! {
                res = stop_rx.recv() => {
                    if let Ok(_) = res {
                        break 'run;
                    }
                }
                _ = &mut sleep => {
                    let now = Utc::now().naive_utc().timestamp_millis();
                    if now < last_fetch + download_delay_millis  {
                        continue;
                    }

                    if let Ok(msg) = rx.try_recv() {
                        last_fetch = Utc::now().naive_utc().timestamp_millis();
                        match msg {
                            DownloaderRequestIpc::Get { url, tx } => {
                                //println!("reqwest::get may never return");
                                let response = ureq::get(&url).call();
                                //println!("reqwest::get actually return");
                                match response {
                                    Ok(response) => {
                                        let resp = DownloaderResponseIpc::Get(Ok(response));
                                        tx.send(resp).await.unwrap();
                                    }
                                    Err(e) => {
                                        log::error!("{}", e);
                                        let err = DownloaderError::RequestError(e);
                                        let resp = DownloaderResponseIpc::Get(Err(err));
                                        tx.send(resp).await.unwrap();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

