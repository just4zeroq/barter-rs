use super::channel::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::agg_trade::AggTrade,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// [`Binance`](super::Binance) real-time aggregated trade message (spot & futures).
///
/// ### Raw Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams>
/// ```json
/// {
///     "e":"aggTrade",
///     "E":123456789,
///     "s":"BTCUSDT",
///     "a":12345,
///     "p":"0.001",
///     "q":"100",
///     "f":100,
///     "l":105,
///     "T":123456785,
///     "m":true,
///     "M":true
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceAggTrade {
    #[serde(alias = "s", deserialize_with = "de_agg_trade_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(
        alias = "T",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    #[serde(alias = "a")]
    pub aggregate_trade_id: u64,
    #[serde(
        alias = "p",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub price: f64,
    #[serde(
        alias = "q",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub quantity: f64,
    #[serde(alias = "f")]
    pub first_trade_id: u64,
    #[serde(alias = "l")]
    pub last_trade_id: u64,
    #[serde(alias = "m")]
    pub buyer_is_maker: bool,
}

impl Identifier<Option<SubscriptionId>> for BinanceAggTrade {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceAggTrade)>
    for MarketIter<InstrumentKey, AggTrade>
{
    fn from(
        (exchange_id, instrument, agg_trade): (ExchangeId, InstrumentKey, BinanceAggTrade),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: agg_trade.time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: AggTrade {
                aggregate_trade_id: agg_trade.aggregate_trade_id,
                price: agg_trade.price,
                quantity: agg_trade.quantity,
                first_trade_id: agg_trade.first_trade_id,
                last_trade_id: agg_trade.last_trade_id,
                buyer_is_maker: agg_trade.buyer_is_maker,
            },
        })])
    }
}

/// Deserialize a [`BinanceAggTrade`] "s" (eg/ "BTCUSDT") as the associated [`SubscriptionId`].
///
/// eg/ "@aggTrade|BTCUSDT"
pub fn de_agg_trade_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::AGG_TRADE, market)).id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_binance_agg_trade() {
        let input = r#"
            {
                "e":"aggTrade",
                "E":123456789,
                "s":"BTCUSDT",
                "a":12345,
                "p":"0.001",
                "q":"100",
                "f":100,
                "l":105,
                "T":123456785,
                "m":true,
                "M":true
            }
            "#;

        assert_eq!(
            serde_json::from_str::<BinanceAggTrade>(input).unwrap(),
            BinanceAggTrade {
                subscription_id: SubscriptionId::from("@aggTrade|BTCUSDT"),
                time: datetime_utc_from_epoch_duration(Duration::from_millis(123456785)),
                aggregate_trade_id: 12345,
                price: 0.001,
                quantity: 100.0,
                first_trade_id: 100,
                last_trade_id: 105,
                buyer_is_maker: true,
            }
        );
    }
}
