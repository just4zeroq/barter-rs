use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Candle`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events for a specific [`CandleInterval`].
///
/// ### Notes
/// Unlike the unit-like Barter [`SubscriptionKind`]s (eg/ [`PublicTrades`]),
/// [`Candles`] carries the [`CandleInterval`] it subscribes to. This is required because
/// the kline stream name (eg/ Binance `@kline_1m`) is interval specific.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize,
)]
pub struct Candles {
    pub interval: CandleInterval,
}

impl Candles {
    /// Construct a new [`Self`] that yields [`Candle`] events for the provided
    /// [`CandleInterval`].
    pub fn new(interval: CandleInterval) -> Self {
        Self { interval }
    }
}

impl SubscriptionKind for Candles {
    type Event = Candle;

    fn as_str(&self) -> &'static str {
        "candles"
    }
}

impl Display for Candles {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "candles|{}", self.interval)
    }
}

/// Normalised Barter OHLCV candle time interval.
///
/// ### Notes
/// Represented as a closed set of the common cross-exchange intervals. Each variant maps to
/// an exchange specific stream interval via the exchange adapter (eg/ Binance `@kline_1m`).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub enum CandleInterval {
    Second1,
    #[default]
    Minute1,
    Minute3,
    Minute5,
    Minute15,
    Minute30,
    Hour1,
    Hour2,
    Hour4,
    Hour6,
    Hour8,
    Hour12,
    Day1,
    Day3,
    Week1,
    Month1,
}

impl CandleInterval {
    /// Return the canonical lowercase string representation of [`Self`]
    /// (eg/ `CandleInterval::Minute1` => "1m").
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Second1 => "1s",
            Self::Minute1 => "1m",
            Self::Minute3 => "3m",
            Self::Minute5 => "5m",
            Self::Minute15 => "15m",
            Self::Minute30 => "30m",
            Self::Hour1 => "1h",
            Self::Hour2 => "2h",
            Self::Hour4 => "4h",
            Self::Hour6 => "6h",
            Self::Hour8 => "8h",
            Self::Hour12 => "12h",
            Self::Day1 => "1d",
            Self::Day3 => "3d",
            Self::Week1 => "1w",
            Self::Month1 => "1M",
        }
    }
}

impl Display for CandleInterval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl AsRef<str> for CandleInterval {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Normalised Barter OHLCV [`Candle`] model.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Candle {
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
}
