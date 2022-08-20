use async_trait::async_trait;
use frangipani::{Config, Response, Spider};
use frangipani::util::join_url;
use scraper::{Html, Selector};

pub struct DexcodeSpider {
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
                        urls.push(req_url.to_string());
                    }
                }
            }

            let title_selector = Selector::parse("title").unwrap();
            let title = match document.select(&title_selector).next() {
                Some(el) => el.inner_html(),
                None => "".to_owned(),
            };
            println!("{},{}", url, title);
        }
        
        (1, urls)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let spiders: Vec<Box<dyn Spider + Send + Sync>> = vec![
        Box::new(DexcodeSpider {}),
    ];

    let mut config = Config::default();
    config.continuous_crawl = true;
    config.continuous_crawl_interval_mins = 15;
    let mut engine = frangipani::engine_with_config(config, spiders);
    engine.start().await;
}

