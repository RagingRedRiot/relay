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

impl Config {
    pub fn from_env() -> Result<Self, envy::Error> {
        envy::from_env()
    }
}
