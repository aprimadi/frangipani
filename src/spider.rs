use async_trait::async_trait;

/// Spider interface
#[async_trait]
pub trait Spider {
    /// Get spider name. 
    ///
    /// This is used to route response to the correct spider so make sure that
    /// it's unique for each spider.
    fn name(&self) -> String;
    
    /// Returns a list of starting urls.
    fn start_urls(&self) -> Vec<String>;
    
    /// Parse response
    ///
    /// Returns (num_processed, urls)
    ///     num_processed - the number of items processed
    ///     urls - list of candidate urls to crawl
    async fn parse(&self, response: ureq::Response) -> (u64, Vec<String>);
}

