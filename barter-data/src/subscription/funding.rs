use super::SubscriptionKind;
use barter_macro::{DeSubKind, SerSubKind};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields
/// [`FundingRate`] [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// ### Notes
/// Funding rates are only defined for perpetual instruments. Subscribable per-symbol
/// (eg/ Binance futures `<symbol>@markPrice`) or whole-market
/// (eg/ Binance futures `!markPrice@arr`).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct FundingRates;

impl SubscriptionKind for FundingRates {
    type Event = FundingRate;

    fn as_str(&self) -> &'static str {
        "funding_rates"
    }
}

impl std::fmt::Display for FundingRates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields
/// [`FundingRate`] [`MarketEvent<T>`](crate::event::MarketEvent) events for *every* perpetual
/// instrument on an exchange.
///
/// ### Notes
/// A whole-market subscription consumes a single stream that broadcasts a [`FundingRate`] for
/// every perpetual symbol (eg/ Binance futures `!markPrice@arr`), rather than subscribing
/// per-symbol.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default,
)]
pub struct AllFundingRates;

impl SubscriptionKind for AllFundingRates {
    type Event = FundingRate;

    fn as_str(&self) -> &'static str {
        "all_funding_rates"
    }
}

impl std::fmt::Display for AllFundingRates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`FundingRate`] model describing the current funding state of a perpetual
/// instrument, including the associated mark and index prices.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct FundingRate {
    pub mark_price: f64,
    pub index_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: DateTime<Utc>,
}
