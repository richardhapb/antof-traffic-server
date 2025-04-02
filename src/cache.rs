/// # Memcache traits implementation
/// Implement all the necessary cache serialization and deserialization
use std::io;
use std::io::Write;
use std::sync::{Arc, OnceLock};

use crate::errors::{CacheError, UpdateError};
use crate::models::alerts::{Alert, AlertType, AlertsDataGroup, AlertsGroup, AlertsGrouper};
use memcache::{CommandError, FromMemcacheValue, MemcacheError, ToMemcacheValue};

pub type MemcacheValue<T> = Result<T, MemcacheError>;

pub const MEMCACHE_URI: &str = "memcache://127.0.0.1:11211";

// Global state for cache, this ensure an stable unique connection
static CACHE_SERVICE: OnceLock<Arc<CacheService>> = OnceLock::new();

pub const ALERTS_CACHE_KEY: &str = "alerts_data";
pub const ALERTS_CACHE_EXP: u32 = 604800; // One week

pub const ALERTS_GROUPER_CACHE_KEY: &str = "alerts_grouper";
pub const ALERTS_GROUPER_CACHE_EXP: u32 = 604800; // One week 

/// Handles the cache service throught the application
pub struct CacheService {
    pub client: memcache::Client,
}

impl CacheService {
    pub fn new(client: memcache::Client) -> Self {
        Self { client }
    }

    /// Initialize the cache instance, ensuring it is initialized only once
    pub async fn init_cache() -> Arc<CacheService> {
        CACHE_SERVICE
            .get_or_init(|| {
                Arc::new(CacheService::new(
                    memcache::Client::connect(MEMCACHE_URI).unwrap(),
                ))
            })
            .clone()
    }

    /// Get the content of a key from the cache or return the default
    ///
    /// # Params
    /// * `key`: Key stored in cache
    /// * `default`: Default value
    ///
    /// # Returns
    /// * Retrieved data or default
    pub fn get_or_default<T>(&self, key: &str, default: T) -> T
    where
        T: ToMemcacheValue<Vec<u8>> + FromMemcacheValue,
    {
        match self.client.get::<T>(key) {
            Ok(Some(value)) => value,
            Ok(None) | Err(_) => default,
        }
    }

    /// Get the content of a key from the cache, return the default or map a CacheError
    ///
    /// # Params
    /// * `key`: Key stored in cache
    ///
    /// # Returns
    /// * Result with retrieved data, default or [`CacheError`]
    pub fn get_or_cache_err<T>(&self, key: &str) -> Result<T, CacheError>
    where
        T: ToMemcacheValue<Vec<u8>> + FromMemcacheValue,
    {
        match self.client.get::<T>(key) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(CacheError::NotFound(MemcacheError::CommandError(
                CommandError::KeyNotFound,
            ))),
            Err(e) => {
                tracing::error!("Failed to retrieve alerts from cache: {}", e);
                Err(CacheError::Request(e))
            }
        }
    }

    /// Store alerts in cache
    ///
    /// # Parameters
    /// * `alerts`: Alerts to be stored
    ///
    /// # Returns
    /// `Result` instance with Ok or error if there is an error
    pub fn store_alerts(&self, alerts: &AlertsDataGroup) -> Result<(), UpdateError> {
        self.client
            .set(ALERTS_CACHE_KEY, alerts, ALERTS_CACHE_EXP)
            .map_err(|e| {
                tracing::error!("Error setting alerts data in cache: {}", e);
                UpdateError::Cache(CacheError::Request(e))
            })
    }

    /// Remove a key from the cache
    pub fn remove_key(&self, key: &str) -> Result<bool, CacheError> {
        self.client.delete(key).map_err(CacheError::Request)
    }
}

enum Flags {
    Bytes = 0,
}

// AlertType implementation

impl<W: Write> ToMemcacheValue<W> for &AlertType {
    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        self.as_str().len()
    }

    fn write_to(&self, stream: &mut W) -> io::Result<()> {
        stream.write_all(self.as_str().as_bytes())
    }
}

impl FromMemcacheValue for AlertType {
    fn from_memcache_value(value: Vec<u8>, _: u32) -> MemcacheValue<Self> {
        let s = String::from_utf8(value)?;
        match s.as_str() {
            "accident" => Ok(AlertType::Accident),
            "construction" => Ok(AlertType::Construction),
            "hazard" => Ok(AlertType::Hazard),
            "jam" => Ok(AlertType::Jam),
            "misc" => Ok(AlertType::Misc),
            "road_closed" => Ok(AlertType::RoadClosed),
            _ => Err(MemcacheError::CommandError(CommandError::InvalidArguments)),
        }
    }
}

// Alert implementation

impl<W: Write> ToMemcacheValue<W> for &Alert {
    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        serde_json::to_string(self).map_or(0, |s| s.len())
    }
    fn write_to(&self, stream: &mut W) -> io::Result<()> {
        stream.write_all(serde_json::to_string(self)?.as_bytes())
    }
}

impl FromMemcacheValue for Alert {
    fn from_memcache_value(value: Vec<u8>, _: u32) -> MemcacheValue<Self> {
        serde_json::from_slice(&value)
            .map_err(|_| MemcacheError::CommandError(CommandError::InvalidArguments))
    }
}

// AlertsGroup implementation

impl<W: Write> ToMemcacheValue<W> for &AlertsGroup {
    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |v| v.len())
    }

    fn write_to(&self, stream: &mut W) -> io::Result<()> {
        let json = serde_json::to_vec(self)?;
        stream.write_all(&json)
    }
}

impl FromMemcacheValue for AlertsGroup {
    fn from_memcache_value(value: Vec<u8>, _: u32) -> MemcacheValue<Self> {
        serde_json::from_slice(&value)
            .map_err(|_| MemcacheError::CommandError(CommandError::InvalidArguments))
    }
}

// AlertsDataGroup implementation

impl<W: Write> ToMemcacheValue<W> for &AlertsDataGroup {
    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |v| v.len())
    }

    fn write_to(&self, stream: &mut W) -> io::Result<()> {
        let json = serde_json::to_vec(self)?;
        stream.write_all(&json)
    }
}

impl<W: Write> ToMemcacheValue<W> for AlertsDataGroup {
    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |v| v.len())
    }

    fn write_to(&self, stream: &mut W) -> io::Result<()> {
        let json = serde_json::to_vec(self)?;
        stream.write_all(&json)
    }
}

impl FromMemcacheValue for AlertsDataGroup {
    fn from_memcache_value(value: Vec<u8>, _: u32) -> MemcacheValue<Self> {
        serde_json::from_slice(&value)
            .map_err(|_| MemcacheError::CommandError(CommandError::InvalidArguments))
    }
}

// AlertsGrouper implementation

impl FromMemcacheValue for AlertsGrouper {
    fn from_memcache_value(value: Vec<u8>, _: u32) -> MemcacheValue<Self> {
        serde_json::from_slice(&value)
            .map_err(|_| MemcacheError::CommandError(CommandError::InvalidArguments))
    }
}

impl<W: Write> ToMemcacheValue<W> for &AlertsGrouper {
    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |v| v.len())
    }

    fn write_to(&self, stream: &mut W) -> io::Result<()> {
        let json = serde_json::to_vec(self)?;
        stream.write_all(&json)
    }
}
