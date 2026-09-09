use super::channel::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::funding::FundingRate,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// [`BinanceFuturesUsd`](super::futures::BinanceFuturesUsd) real-time mark price / funding rate message.
///
/// ### Notes
/// The whole-market `!markPrice@arr` stream broadcasts an array of these messages.
///
/// ### Raw Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream>
/// ```json
/// {
///     "e":"markPriceUpdate",
///     "E":1562305380000,
///     "s":"BTCUSDT",
///     "p":"11794.15000000",
///     "i":"11785.00000000",
///     "P":"11784.00000000",
///     "r":"0.00038167",
///     "T":1562306400000
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceFundingRate {
    #[serde(alias = "s", deserialize_with = "de_funding_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    #[serde(
        alias = "p",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub mark_price: f64,
    #[serde(
        alias = "i",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub index_price: f64,
    #[serde(
        alias = "r",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub funding_rate: f64,
    #[serde(
        alias = "T",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub next_funding_time: DateTime<Utc>,
}

impl Identifier<Option<SubscriptionId>> for BinanceFundingRate {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceFundingRate)>
    for MarketIter<InstrumentKey, FundingRate>
{
    fn from(
        (exchange_id, instrument, funding): (ExchangeId, InstrumentKey, BinanceFundingRate),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: funding.time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: FundingRate {
                mark_price: funding.mark_price,
                index_price: funding.index_price,
                funding_rate: funding.funding_rate,
                next_funding_time: funding.next_funding_time,
            },
        })])
    }
}

/// Deserialize a [`BinanceFundingRate`] "s" (eg/ "BTCUSDT") as the associated [`SubscriptionId`].
///
/// eg/ "@markPrice|BTCUSDT"
pub fn de_funding_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::MARK_PRICE, market)).id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_binance_funding_rate() {
        let input = r#"
            {
                "e":"markPriceUpdate",
                "E":1562305380000,
                "s":"BTCUSDT",
                "p":"11794.15000000",
                "i":"11785.00000000",
                "P":"11784.00000000",
                "r":"0.00038167",
                "T":1562306400000
            }
            "#;

        assert_eq!(
            serde_json::from_str::<BinanceFundingRate>(input).unwrap(),
            BinanceFundingRate {
                subscription_id: SubscriptionId::from("@markPrice|BTCUSDT"),
                time: datetime_utc_from_epoch_duration(Duration::from_millis(1562305380000)),
                mark_price: 11794.15000000,
                index_price: 11785.00000000,
                funding_rate: 0.00038167,
                next_funding_time: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1562306400000
                )),
            }
        );
    }
}
