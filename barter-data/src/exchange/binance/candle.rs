use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// [`Binance`](super::Binance) real-time kline / candlestick message (spot & futures).
///
/// ### Notes
/// - The subscribed interval is only present within the nested `"k"."i"` field, so the
///   associated [`SubscriptionId`] (eg/ `@kline_1m|BTCUSDT`) is reconstructed after
///   deserialisation via [`Identifier`].
/// - Only *closed* candles (`"k"."x" == true`) are emitted; streaming partials are filtered
///   out in the [`MarketIter`] [`From`] implementation.
///
/// ### Raw Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-streams>
/// ```json
/// {
///     "e": "kline",
///     "E": 123456789,
///     "s": "BTCUSDT",
///     "k": {
///         "t": 123400000,
///         "T": 123460000,
///         "s": "BTCUSDT",
///         "i": "1m",
///         "f": 100,
///         "L": 200,
///         "o": "0.0010",
///         "c": "0.0020",
///         "h": "0.0025",
///         "l": "0.0015",
///         "v": "1000",
///         "n": 100,
///         "x": false,
///         "q": "1.0000"
///     }
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceCandle {
    #[serde(alias = "s")]
    pub symbol: SmolStr,
    #[serde(
        alias = "E",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub event_time: DateTime<Utc>,
    #[serde(alias = "k")]
    pub candle: BinanceCandleKline,
}

impl Identifier<Option<SubscriptionId>> for BinanceCandle {
    fn id(&self) -> Option<SubscriptionId> {
        Some(SubscriptionId::from(format!(
            "@kline_{}|{}",
            self.candle.interval, self.symbol
        )))
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceCandle)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, candle): (ExchangeId, InstrumentKey, BinanceCandle),
    ) -> Self {
        // Filter out non-closed (streaming partial) candles
        if !candle.candle.is_closed {
            return Self(vec![]);
        }

        Self(vec![Ok(MarketEvent {
            time_exchange: candle.event_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                close_time: candle.candle.close_time,
                open: candle.candle.open,
                high: candle.candle.high,
                low: candle.candle.low,
                close: candle.candle.close,
                volume: candle.candle.volume,
                trade_count: candle.candle.trade_count,
            },
        })])
    }
}

/// Binance kline `"k"` sub-object.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceCandleKline {
    #[serde(
        alias = "t",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub open_time: DateTime<Utc>,
    #[serde(
        alias = "T",
        deserialize_with = "barter_integration::serde::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub close_time: DateTime<Utc>,
    #[serde(alias = "i")]
    pub interval: SmolStr,
    #[serde(alias = "o", deserialize_with = "barter_integration::serde::de::de_str")]
    pub open: f64,
    #[serde(alias = "c", deserialize_with = "barter_integration::serde::de::de_str")]
    pub close: f64,
    #[serde(alias = "h", deserialize_with = "barter_integration::serde::de::de_str")]
    pub high: f64,
    #[serde(alias = "l", deserialize_with = "barter_integration::serde::de::de_str")]
    pub low: f64,
    #[serde(alias = "v", deserialize_with = "barter_integration::serde::de::de_str")]
    pub volume: f64,
    #[serde(alias = "n")]
    pub trade_count: u64,
    #[serde(alias = "x")]
    pub is_closed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_de_binance_candle_open() {
        // Non-closed candle should deserialise but be filtered when converted to MarketIter
        let input = r#"
            {
                "e": "kline",
                "E": 123456789,
                "s": "BTCUSDT",
                "k": {
                    "t": 123400000,
                    "T": 123460000,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "f": 100,
                    "L": 200,
                    "o": "0.0010",
                    "c": "0.0020",
                    "h": "0.0025",
                    "l": "0.0015",
                    "v": "1000",
                    "n": 100,
                    "x": false,
                    "q": "1.0000"
                }
            }
            "#;

        let candle = serde_json::from_str::<BinanceCandle>(input).unwrap();
        assert_eq!(
            candle.id(),
            Some(SubscriptionId::from("@kline_1m|BTCUSDT"))
        );
        assert!(candle.candle.close_time > candle.candle.open_time);
        assert!(!candle.candle.is_closed);
    }

    #[test]
    fn test_de_binance_candle_closed_id_and_fields() {
        let input = r#"
            {
                "e": "kline",
                "E": 123456789,
                "s": "BTCUSDT",
                "k": {
                    "t": 123400000,
                    "T": 123460000,
                    "s": "BTCUSDT",
                    "i": "1d",
                    "f": 100,
                    "L": 200,
                    "o": "0.0010",
                    "c": "0.0020",
                    "h": "0.0025",
                    "l": "0.0015",
                    "v": "1000",
                    "n": 100,
                    "x": true,
                    "q": "1.0000"
                }
            }
            "#;

        let candle = serde_json::from_str::<BinanceCandle>(input).unwrap();
        assert_eq!(candle.id(), Some(SubscriptionId::from("@kline_1d|BTCUSDT")));
        assert_eq!(
            candle.candle.close_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(123460000))
        );
        assert_eq!(candle.candle.open, 0.0010);
        assert_eq!(candle.candle.close, 0.0020);
        assert_eq!(candle.candle.high, 0.0025);
        assert_eq!(candle.candle.low, 0.0015);
        assert_eq!(candle.candle.volume, 1000.0);
        assert_eq!(candle.candle.trade_count, 100);
        assert!(candle.candle.is_closed);
    }
}
