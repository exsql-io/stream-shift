use std::time::Duration;

pub mod kafka;
pub mod rendering;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
