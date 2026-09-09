use super::channel::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::ticker::Ticker,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// [`Binance`](super::Binance) real-time 24hr mini ticker message (spot & futures).
///
/// ### Notes
/// The whole-market `!miniTicker@arr` stream broadcasts an array of these messages.
///
/// ### Raw Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-mini-ticker-stream>
/// ```json
/// {
///     "e":"24hrMiniTicker",
///     "E":123456789,
///     "s":"BTCUSDT",
///     "c":"0.0025",
///     "o":"0.0010",
///     "h":"0.0025",
///     "l":"0.0010",
///     "v":"10000",
///     "q":"18"
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceTicker {
    #[serde(alias = "s", deserialize_with = "de_ticker_subscription_id")]
    pub subscription_id: SubscriptionId,
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
    #[serde(
        alias = "o",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub open: f64,
    #[serde(
        alias = "h",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub high: f64,
    #[serde(
        alias = "l",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub low: f64,
    #[serde(
        alias = "c",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub last: f64,
    #[serde(
        alias = "v",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub volume_base: f64,
    #[serde(
        alias = "q",
        deserialize_with = "barter_integration::serde::de::de_str"
    )]
    pub volume_quote: f64,
}

impl Identifier<Option<SubscriptionId>> for BinanceTicker {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceTicker)>
    for MarketIter<InstrumentKey, Ticker>
{
    fn from(
        (exchange_id, instrument, ticker): (ExchangeId, InstrumentKey, BinanceTicker),
    ) -> Self {
        Self(vec![Ok(MarketEvent {
            time_exchange: ticker.time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Ticker {
                open: ticker.open,
                high: ticker.high,
                low: ticker.low,
                last: ticker.last,
                volume_base: ticker.volume_base,
                volume_quote: ticker.volume_quote,
            },
        })])
    }
}

/// Deserialize a [`BinanceTicker`] "s" (eg/ "BTCUSDT") as the associated [`SubscriptionId`].
///
/// eg/ "@miniTicker|BTCUSDT"
pub fn de_ticker_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BinanceChannel::MINI_TICKER, market)).id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_binance_ticker() {
        let input = r#"
            {
                "e":"24hrMiniTicker",
                "E":123456789,
                "s":"BTCUSDT",
                "c":"0.0025",
                "o":"0.0010",
                "h":"0.0025",
                "l":"0.0010",
                "v":"10000",
                "q":"18"
            }
            "#;

        assert_eq!(
            serde_json::from_str::<BinanceTicker>(input).unwrap(),
            BinanceTicker {
                subscription_id: SubscriptionId::from("@miniTicker|BTCUSDT"),
                time: datetime_utc_from_epoch_duration(Duration::from_millis(123456789)),
                open: 0.0010,
                high: 0.0025,
                low: 0.0010,
                last: 0.0025,
                volume_base: 10000.0,
                volume_quote: 18.0,
            }
        );
    }
}
