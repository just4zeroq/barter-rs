use super::SubscriptionKind;
use barter_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`AggTrade`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// ### Notes
/// An [`AggTrade`] is the exchange aggregation of individual [`PublicTrade`](super::trade::PublicTrade)s
/// executed at the same price. Subscribable per-symbol (eg/ Binance `<symbol>@aggTrade`).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct AggTrades;

impl SubscriptionKind for AggTrades {
    type Event = AggTrade;

    fn as_str(&self) -> &'static str {
        "agg_trades"
    }
}

impl std::fmt::Display for AggTrades {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`AggTrade`] model describing an aggregated trade.
///
/// ### Notes
/// The associated [`MarketEvent::time_exchange`](crate::event::MarketEvent) is the aggregated
/// trade time, hence it is not stored here.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct AggTrade {
    pub aggregate_trade_id: u64,
    pub price: f64,
    pub quantity: f64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub buyer_is_maker: bool,
}
