use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use frangipani::{Response, Spider};
use frangipani::util::join_url;
use scraper::{Html, Selector};

pub struct DexcodeSpider {
    pub url_count: Arc<Mutex<u64>>,
}

#[async_trait]
impl Spider for DexcodeSpider {
    fn name(&self) -> String {
        "dexcode-spider".to_owned()
    }
    
    fn start_urls(&self) -> Vec<String> {
        vec![
            "https://dexcode.com/".to_owned(),
        ]
    }
    
    async fn parse(&self, response: Response) -> (u64, Vec<String>) {
        if response.content_type() != "text/html" {
            return (0, vec![]);
        }
        let url = response.get_url().to_owned();
        let text = response.into_string().unwrap();
        let mut urls = vec![];
        {
            let document = Html::parse_document(&text);
            let link_selector = Selector::parse("a").unwrap();
            for link in document.select(&link_selector) {
                if let Some(relative_url) = link.value().attr("href") {
                    let join_url = join_url(&url, relative_url);
                    let req_url = reqwest::Url::parse(&join_url).unwrap();
                    if req_url.scheme() != "http" && req_url.scheme() != "https" {
                        continue;
                    }
                    if req_url.domain().unwrap().ends_with("dexcode.com") {
                        // Only push url with `dexcode.com` domain
                        urls.push(relative_url.to_string());
                    }
                }
            }
        }
        let mut url_count = self.url_count.lock().unwrap();
        *url_count += 1;
        (1, urls)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let url_count = Arc::new(Mutex::new(0));
    let spider = DexcodeSpider { url_count: url_count.clone() };
    let spiders: Vec<Box<dyn Spider + Send + Sync>> = vec![
        Box::new(spider),
    ];
    let mut engine = frangipani::engine(spiders);
    engine.start().await;

    let url_count = url_count.lock().unwrap();
    println!("url count: {}", url_count);
}

