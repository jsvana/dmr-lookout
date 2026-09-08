use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub feed: FeedConfig,
    pub push: Option<PushConfig>,
    #[serde(default)]
    pub rules: RulesConfig,
    #[serde(default)]
    pub db: DbConfig,
}

#[derive(Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

#[derive(Deserialize, Clone)]
pub struct FeedConfig {
    #[serde(default = "default_feed_url")]
    pub url: String,
}

#[derive(Deserialize, Clone)]
pub struct PushConfig {
    pub key_path: String,
    pub key_id: String,
    pub team_id: String,
    pub bundle_id: String,
}

#[derive(Deserialize, Clone)]
pub struct RulesConfig {
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: i64,
    #[serde(default = "default_max_pushes")]
    pub max_pushes_per_hour: usize,
    #[serde(default = "default_freshness")]
    pub freshness_secs: i64,
    #[serde(default = "default_grace")]
    pub connect_grace_secs: i64,
}

#[derive(Deserialize, Clone)]
pub struct DbConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

fn default_bind() -> String {
    "127.0.0.1:8084".into()
}

fn default_feed_url() -> String {
    "wss://api.brandmeister.network/lh/socket.io/?EIO=4&transport=websocket".into()
}

fn default_cooldown() -> i64 {
    1800
}

fn default_max_pushes() -> usize {
    12
}

fn default_freshness() -> i64 {
    120
}

fn default_grace() -> i64 {
    10
}

fn default_db_path() -> String {
    "data/lookout.db".into()
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind: default_bind(),
        }
    }
}

impl Default for FeedConfig {
    fn default() -> Self {
        FeedConfig {
            url: default_feed_url(),
        }
    }
}

impl Default for RulesConfig {
    fn default() -> Self {
        RulesConfig {
            cooldown_secs: default_cooldown(),
            max_pushes_per_hour: default_max_pushes(),
            freshness_secs: default_freshness(),
            connect_grace_secs: default_grace(),
        }
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            path: default_db_path(),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config {path}: {e}"))?;
        let config: Config = toml::from_str(&text)?;
        Ok(config)
    }
}

/// The single shared API bearer token; required.
pub fn api_token() -> anyhow::Result<String> {
    std::env::var("LOOKOUT_API_TOKEN")
        .map_err(|_| anyhow::anyhow!("LOOKOUT_API_TOKEN must be set"))
}
