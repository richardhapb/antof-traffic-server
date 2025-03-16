use std::io::Write;
use std::io;

use memcache::{CommandError, FromMemcacheValue, MemcacheError, ToMemcacheValue};
use crate::models::alerts::{AlertType, Alert, AlertsGroup};

type MemcacheValue<T> = Result<T, MemcacheError>;

pub enum Flags {
    Bytes = 0,
}

// AlertType implementation

impl<'a, W: Write> ToMemcacheValue<W> for &'a AlertType {
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
            _ => Err(MemcacheError::CommandError(CommandError::InvalidArguments))
        }
    }
}

// Alert implementation

impl<'a, W: Write> ToMemcacheValue<W> for &'a Alert {
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
        serde_json::from_slice(&value).map_err(|_| MemcacheError::CommandError(CommandError::InvalidArguments))
    }
}

// AlertsGroup implementation

impl <'a, W: Write> ToMemcacheValue<W> for &'a AlertsGroup {

    fn get_flags(&self) -> u32 {
        Flags::Bytes as u32
    }

    fn get_length(&self) -> usize {
        serde_json::to_vec(self)
            .map_or(0, |v| v.len())
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

