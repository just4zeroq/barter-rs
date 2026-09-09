use super::SubscriptionKind;
use barter_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Ticker`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// ### Notes
/// A [`Ticker`] describes the latest 24h rolling market statistics for an instrument. It is
/// subscribable per-symbol (eg/ Binance `<symbol>@miniTicker`) or whole-market
/// (eg/ Binance `!miniTicker@arr`).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct Tickers;

impl SubscriptionKind for Tickers {
    type Event = Ticker;

    fn as_str(&self) -> &'static str {
        "tickers"
    }
}

impl std::fmt::Display for Tickers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Ticker`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events for *every* instrument on an exchange.
///
/// ### Notes
/// A whole-market subscription consumes a single stream that broadcasts a [`Ticker`] for every
/// symbol (eg/ Binance `!miniTicker@arr`), rather than subscribing per-symbol.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default,
)]
pub struct AllTickers;

impl SubscriptionKind for AllTickers {
    type Event = Ticker;

    fn as_str(&self) -> &'static str {
        "all_tickers"
    }
}

impl std::fmt::Display for AllTickers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`Ticker`] model describing the latest 24h rolling market statistics for
/// an instrument.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Ticker {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub last: f64,
    pub volume_base: f64,
    pub volume_quote: f64,
}
