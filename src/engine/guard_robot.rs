use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use texting_robots::Robot;

use crate::Config;
use crate::util;

#[derive(Clone)]
pub struct GuardRobot {
    config: Arc<Config>,
    inner: Arc<Mutex<GuardRobotInner>>,
}

struct GuardRobotInner {
    robots: HashMap<String, Option<Robot>>,
}

impl GuardRobot {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(GuardRobotInner {
                robots: HashMap::new(),
            })),
        }
    }
    pub fn is_allowed(&self, url: &str) -> bool {
        if !self.config.robotstxt_obey {
            return true;
        }
        
        let host = util::get_host(&url);
        if host.is_none() { // robot rules doesn't apply
            return true;
        }
        let host = host.unwrap();
        
        let robot_url = util::get_robot_url(&url);
        if robot_url.is_none() { // no robot url
            return true;
        }
        let robot_url = robot_url.unwrap();
        
        // Populate robot
        let mut inner = self.inner.lock().unwrap();
        if !inner.robots.contains_key(&host) {
            if let Ok(response) = ureq::get(&robot_url).call() {
                let text = response.into_string().unwrap();
                let robot = Robot::new(&self.config.bot_name, text.as_bytes()).unwrap();
                inner.robots.insert(host.clone(), Some(robot));
            } else {
                inner.robots.insert(host.clone(), None);
            }
        }
    
        // Check if url obey robots.txt
        if let Some(Some(robot)) = inner.robots.get(&host) {
            robot.allowed(&url)
        } else { // No robot found, return true
            true
        }
    }
}
