/// Join base url with another (possibly relative) url
pub fn join_url(current_url: &str, url: &str) -> String {
    let mut u = reqwest::Url::parse(current_url).unwrap();
    u = u.join(url).unwrap();
    u.to_string()
}

