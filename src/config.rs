use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database_url: String,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_per_second")]
    pub rate_limit_per_second: u64,
    #[serde(default = "default_burst")]
    pub rate_limit_burst: u32,
    #[serde(default)]
    pub open_signups: bool,
    // Data older than this is eligible for reaping (messages, empty rooms, stale
    // invites and join requests).
    #[serde(default = "default_retention_days")]
    pub retention_days: i32,
    // How often the reaper runs, in seconds.
    #[serde(default = "default_reap_interval_secs")]
    pub reap_interval_secs: u64,
    pub admin_username: String,
    pub admin_credential: String,
}

fn default_bind() -> String {
    "0.0.0.0:3000".into()
}
fn default_per_second() -> u64 {
    4
}
fn default_burst() -> u32 {
    10
}
fn default_retention_days() -> i32 {
    30
}
fn default_reap_interval_secs() -> u64 {
    3600
}

impl Config {
    pub fn from_env() -> Result<Self, envy::Error> {
        envy::from_env()
    }
}
