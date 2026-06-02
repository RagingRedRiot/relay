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
    // Largest chunk *payload* (file bytes, excluding the CHUNK_HEADER_LEN frame
    // header) the server accepts in one binary upload frame, and the value
    // GetMaxChunkSize reports back to clients. Defaults to the websocket
    // framework's own max message size so an unconfigured server behaves exactly
    // like axum/tungstenite out of the box; raise it to cut round trips on large
    // files, lower it to bound per-chunk memory. The transport's frame and message
    // caps are both pinned to this value + header (see handler::ws_handler), so the
    // advertised number is the real limit with no smaller hidden frame cap beneath.
    #[serde(default = "default_max_chunk_bytes")]
    pub max_chunk_bytes: usize,
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
fn default_max_chunk_bytes() -> usize {
    // Mirror the framework's own default message size so an unconfigured server
    // matches axum/tungstenite exactly, minus the frame header since the advertised
    // limit is the payload the client controls, not the whole frame. unwrap_or
    // guards the (current) None == "unlimited" case in a future tungstenite.
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size
        .unwrap_or(64 << 20)
        .saturating_sub(crate::attachment::CHUNK_HEADER_LEN)
}

impl Config {
    pub fn from_env() -> Result<Self, envy::Error> {
        envy::from_env()
    }
}
