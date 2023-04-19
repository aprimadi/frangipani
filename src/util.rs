use reqwest::Url;

/// Join base url with another (possibly relative) url
pub fn join_url(current_url: &str, url: &str) -> String {
    let mut u = reqwest::Url::parse(current_url).unwrap();
    u = u.join(url).unwrap();
    u.to_string()
}

pub fn get_domain(url: &str) -> String {
    let url_ = Url::parse(url).unwrap();
    url_.domain().unwrap().to_owned()
}

pub fn get_host(url: &str) -> Option<String> {
    let url_ = Url::parse(url).unwrap();
    url_.host_str().map(|x| x.to_owned())
}

pub fn get_robot_url(url: &str) -> Option<String> {
    let mut url_ = Url::parse(url).unwrap();
    if url_.scheme() != "http" && url_.scheme() != "https" {
        return None;
    }
    url_.set_path("/robots.txt");
    url_.set_query(None);
    url_.set_fragment(None);
    Some(url_.to_string())
}
