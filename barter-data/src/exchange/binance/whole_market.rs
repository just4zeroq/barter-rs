use super::{
    Binance, BinanceWsStream, channel::BinanceChannel, funding::BinanceFundingRate,
    futures::{BinanceFuturesUsd, BinanceServerFuturesUsd},
    market::BinanceMarket,
    spot::{BinanceServerSpot, BinanceSpot},
    ticker::BinanceTicker,
};
use crate::{
    Identifier, NoInitialSnapshots,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::{ExchangeServer, StreamSelector},
    instrument::InstrumentData,
    subscription::{
        Map, Subscription, SubscriptionKind, funding::AllFundingRates, ticker::AllTickers,
    },
    transformer::ExchangeTransformer,
};
use async_trait::async_trait;
use barter_instrument::{
    asset::name::AssetNameInternal,
    exchange::ExchangeId,
    instrument::{
        market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
        name::InstrumentNameInternal,
    },
};
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::Deserialize;
use smol_str::{SmolStr, format_smolstr};
use std::{fmt, marker::PhantomData};
use tokio::sync::mpsc::UnboundedSender;

/// Whole-market [`Subscription`] instrument used to subscribe to a single stream that broadcasts
/// events for *every* symbol (eg/ Binance `!miniTicker@arr`).
///
/// ### Notes
/// - The wrapped [`MarketDataInstrument`] is only used to communicate the exchange
///   [`MarketDataInstrumentKind`] to [`Validator`](barter_integration::Validator) logic; whole-market
///   events are emitted with a per-symbol [`InstrumentNameInternal`] resolved directly from each
///   streamed event (eg/ `binance_spot-btcusdt`), hence no exchange universe is fetched.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct WholeMarketInstrument {
    representative: MarketDataInstrument,
    key: InstrumentNameInternal,
}

impl WholeMarketInstrument {
    /// Construct a new whole-market [`Self`] for the provided [`MarketDataInstrumentKind`]
    /// (eg/ [`Spot`](MarketDataInstrumentKind::Spot) for [`BinanceSpot`],
    /// [`Perpetual`](MarketDataInstrumentKind::Perpetual) for [`BinanceFuturesUsd`]).
    ///
    /// ### Notes
    /// - The `base` / `quote` are arbitrary placeholders: whole-market [`Subscription`]s have no
    ///   single symbol, so only the `kind` is used for [`Validator`](barter_integration::Validator)
    ///   logic and whole-market stream routing.
    pub fn new<S>(base: S, quote: S, kind: MarketDataInstrumentKind) -> Self
    where
        S: Into<AssetNameInternal>,
    {
        let representative = MarketDataInstrument::new(base, quote, kind);

        Self {
            key: InstrumentNameInternal::new(format_smolstr!(
                "whole_market_{}",
                representative.kind
            )),
            representative,
        }
    }
}

impl InstrumentData for WholeMarketInstrument {
    type Key = InstrumentNameInternal;

    /// ### Notes
    /// Whole-market [`Subscription`]s have no single instrument, hence this `Key` only ever keys
    /// the (unused) `instrument_map` built during subscription. Consumed events are keyed by the
    /// per-symbol [`InstrumentNameInternal`] resolved in the associated [`ExchangeTransformer`].
    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn kind(&self) -> &MarketDataInstrumentKind {
        &self.representative.kind
    }
}

impl fmt::Display for WholeMarketInstrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

/// Whole-market subscriptions have no single symbol, hence an empty [`BinanceMarket`] is combined
/// with a whole-market channel (eg/ `!miniTicker@arr`) by [`Connector::requests`]
/// (super::super::Connector) to construct the stream name.
impl<Server, Kind> Identifier<BinanceMarket>
    for Subscription<Binance<Server>, WholeMarketInstrument, Kind>
{
    fn id(&self) -> BinanceMarket {
        BinanceMarket(SmolStr::new(""))
    }
}

impl<Server> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, WholeMarketInstrument, AllTickers>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ALL_MINI_TICKER
    }
}

impl Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsd, WholeMarketInstrument, AllFundingRates>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ALL_MARK_PRICE
    }
}

/// [`StreamSelector`] for [`BinanceSpot`] whole-market [`AllTickers`] streams.
impl StreamSelector<WholeMarketInstrument, AllTickers> for BinanceSpot {
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        BinanceWholeMarketTransformer<BinanceServerSpot, AllTickers, BinanceTicker>,
    >;
}

/// [`StreamSelector`] for [`BinanceFuturesUsd`] whole-market [`AllTickers`] streams.
impl StreamSelector<WholeMarketInstrument, AllTickers> for BinanceFuturesUsd {
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        BinanceWholeMarketTransformer<BinanceServerFuturesUsd, AllTickers, BinanceTicker>,
    >;
}

/// [`StreamSelector`] for [`BinanceFuturesUsd`] whole-market [`AllFundingRates`] streams.
impl StreamSelector<WholeMarketInstrument, AllFundingRates> for BinanceFuturesUsd {
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        BinanceWholeMarketTransformer<BinanceServerFuturesUsd, AllFundingRates, BinanceFundingRate>,
    >;
}

/// Generic stateless [`ExchangeTransformer`] for [`Binance`] whole-market array streams (eg/
/// `!miniTicker@arr`, `!markPrice@arr`), translating each streamed array into one normalised
/// [`MarketEvent`] per symbol.
///
/// ### Notes
/// This is the whole-market counterpart of
/// [`StatelessTransformer`](crate::transformer::stateless::StatelessTransformer): rather than
/// resolving each event against the `instrument_map` built at subscription time, the per-symbol
/// `InstrumentKey` is derived from each event's own [`SubscriptionId`] (eg/ `@miniTicker|BTCUSDT`),
/// combined with the [`ExchangeId`] into an [`InstrumentNameInternal`] (eg/
/// `binance_spot-btcusdt`).
///
/// Because no exchange universe is fetched, initialisation performs no IO and newly listed symbols
/// are streamed without requiring a reconnection.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct BinanceWholeMarketTransformer<Server, Kind, Input> {
    phantom: PhantomData<(Server, Kind, Input)>,
}

#[async_trait]
impl<Server, Kind, Input> ExchangeTransformer<Binance<Server>, InstrumentNameInternal, Kind>
    for BinanceWholeMarketTransformer<Server, Kind, Input>
where
    Server: ExchangeServer + Send,
    Kind: SubscriptionKind + Send,
    Input: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>,
    MarketIter<InstrumentNameInternal, Kind::Event>:
        From<(ExchangeId, InstrumentNameInternal, Input)>,
{
    async fn init(
        _instrument_map: Map<InstrumentNameInternal>,
        _initial_snapshots: &[MarketEvent<InstrumentNameInternal, Kind::Event>],
        _ws_sink_tx: UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self {
            phantom: PhantomData,
        })
    }
}

impl<Server, Kind, Input> Transformer for BinanceWholeMarketTransformer<Server, Kind, Input>
where
    Server: ExchangeServer,
    Kind: SubscriptionKind,
    Input: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>,
    MarketIter<InstrumentNameInternal, Kind::Event>:
        From<(ExchangeId, InstrumentNameInternal, Input)>,
{
    type Error = DataError;
    type Input = Vec<Input>;
    type Output = MarketEvent<InstrumentNameInternal, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let mut events = Vec::with_capacity(input.len());

        for element in input {
            // Determine the symbol associated with this whole-market element, which is carried by
            // the element's own SubscriptionId (eg/ "@miniTicker|BTCUSDT" -> "BTCUSDT")
            let Some(subscription_id) = element.id() else {
                continue;
            };

            let Some(symbol) = market_from_subscription_id(subscription_id.as_ref()) else {
                continue;
            };

            let instrument = InstrumentNameInternal::new_from_exchange(Server::ID, symbol);

            events.extend(
                MarketIter::<InstrumentNameInternal, Kind::Event>::from((
                    Server::ID,
                    instrument,
                    element,
                ))
                .0,
            );
        }

        events
    }
}

/// Determines whether a whole-market [`BinanceTicker`] / [`BinanceFundingRate`] subscription id
/// (eg/ `@miniTicker|BTCUSDT`) can be resolved to a symbol, returning the market component (eg/
/// `BTCUSDT`) if so.
fn market_from_subscription_id(subscription_id: &str) -> Option<&str> {
    subscription_id.split_once('|').map(|(_, market)| market)
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::exchange::ExchangeId;
    use barter_integration::serde::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_market_from_subscription_id() {
        assert_eq!(
            market_from_subscription_id("@miniTicker|BTCUSDT"),
            Some("BTCUSDT")
        );
        assert_eq!(
            market_from_subscription_id("@markPrice|ETHUSDT"),
            Some("ETHUSDT")
        );
        assert_eq!(market_from_subscription_id("no_delimiter"), None);
    }

    #[test]
    fn test_whole_market_instrument_key_from_exchange_symbol() {
        // Whole-market events are keyed by "{exchange}-{symbol}", lowercased
        assert_eq!(
            InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceSpot, "BTCUSDT").as_ref(),
            "binance_spot-btcusdt"
        );
        assert_eq!(
            InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceFuturesUsd, "BTCUSDT")
                .as_ref(),
            "binance_futures_usd-btcusdt"
        );

        // Spot & perpetual BTCUSDT are distinct keys, since ExchangeId encodes the market type
        assert_ne!(
            InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceSpot, "BTCUSDT"),
            InstrumentNameInternal::new_from_exchange(ExchangeId::BinanceFuturesUsd, "BTCUSDT"),
        );

        // Delivery contracts are no longer filtered out, and retain their exchange symbol
        assert_eq!(
            InstrumentNameInternal::new_from_exchange(
                ExchangeId::BinanceFuturesUsd,
                "BTCUSDT_250627"
            )
            .as_ref(),
            "binance_futures_usd-btcusdt_250627"
        );
    }

    #[tokio::test]
    async fn test_whole_market_ticker_transformer_keys_events_per_symbol() {
        // A `!miniTicker@arr` payload covering two symbols, one of which is a delivery contract
        // that the removed `/exchangeInfo` universe would previously have filtered out
        let input = serde_json::from_str::<Vec<BinanceTicker>>(
            r#"[
                {"e":"24hrMiniTicker","E":123456789,"s":"BTCUSDT","c":"1","o":"2","h":"3","l":"4","v":"5","q":"6"},
                {"e":"24hrMiniTicker","E":123456789,"s":"BTCUSDT_250627","c":"1","o":"2","h":"3","l":"4","v":"5","q":"6"}
            ]"#,
        )
        .unwrap();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut transformer = <BinanceWholeMarketTransformer<
            BinanceServerFuturesUsd,
            AllTickers,
            BinanceTicker,
        > as ExchangeTransformer<
            Binance<BinanceServerFuturesUsd>,
            InstrumentNameInternal,
            AllTickers,
        >>::init(Map(Default::default()), &[], tx)
        .await
        .unwrap();

        let output = transformer
            .transform(input)
            .into_iter()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(output.len(), 2);
        assert_eq!(
            output[0].instrument.as_ref(),
            "binance_futures_usd-btcusdt"
        );
        assert_eq!(
            output[1].instrument.as_ref(),
            "binance_futures_usd-btcusdt_250627"
        );
        assert_eq!(output[0].exchange, ExchangeId::BinanceFuturesUsd);
    }

    #[tokio::test]
    async fn test_whole_market_funding_rate_transformer_keys_events_per_symbol() {
        // The same generic transformer, instantiated for the `!markPrice@arr` array stream
        let input = serde_json::from_str::<Vec<BinanceFundingRate>>(
            r#"[
                {"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"1","i":"2","P":"3","r":"0.0001","T":1562306400000},
                {"e":"markPriceUpdate","E":1562305380000,"s":"ETHUSDT","p":"1","i":"2","P":"3","r":"0.0002","T":1562306400000}
            ]"#,
        )
        .unwrap();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut transformer = <BinanceWholeMarketTransformer<
            BinanceServerFuturesUsd,
            AllFundingRates,
            BinanceFundingRate,
        > as ExchangeTransformer<
            Binance<BinanceServerFuturesUsd>,
            InstrumentNameInternal,
            AllFundingRates,
        >>::init(Map(Default::default()), &[], tx)
        .await
        .unwrap();

        let output = transformer
            .transform(input)
            .into_iter()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(output.len(), 2);
        assert_eq!(output[0].instrument.as_ref(), "binance_futures_usd-btcusdt");
        assert_eq!(output[1].instrument.as_ref(), "binance_futures_usd-ethusdt");
        assert_eq!(output[0].kind.funding_rate, 0.0001);
    }

    #[test]
    fn test_de_binance_whole_market_tickers_array() {
        // Each `!miniTicker@arr` message is a JSON array of mini ticker elements
        let input = r#"[
            {
                "e": "24hrMiniTicker",
                "E": 123456789,
                "s": "BTCUSDT",
                "c": "0.0025",
                "o": "0.0010",
                "h": "0.0025",
                "l": "0.0010",
                "v": "10000",
                "q": "18"
            },
            {
                "e": "24hrMiniTicker",
                "E": 123456789,
                "s": "ETHUSDT",
                "c": "0.1000",
                "o": "0.0900",
                "h": "0.1100",
                "l": "0.0800",
                "v": "5000",
                "q": "12"
            }
        ]"#;

        let tickers = serde_json::from_str::<Vec<BinanceTicker>>(input).unwrap();
        assert_eq!(tickers.len(), 2);
        assert_eq!(
            tickers[0].subscription_id.as_ref(),
            "@miniTicker|BTCUSDT"
        );
        assert_eq!(
            tickers[1].subscription_id.as_ref(),
            "@miniTicker|ETHUSDT"
        );
        assert_eq!(
            tickers[0].time,
            datetime_utc_from_epoch_duration(Duration::from_millis(123456789))
        );
    }

    #[test]
    fn test_de_binance_whole_market_funding_array() {
        // Each `!markPrice@arr` message is a JSON array of mark price / funding rate elements
        let input = r#"[
            {
                "e": "markPriceUpdate",
                "E": 1562305380000,
                "s": "BTCUSDT",
                "p": "11794.15000000",
                "i": "11785.00000000",
                "P": "11784.00000000",
                "r": "0.00038167",
                "T": 1562306400000
            },
            {
                "e": "markPriceUpdate",
                "E": 1562305380000,
                "s": "ETHUSDT",
                "p": "400.00000000",
                "i": "399.00000000",
                "P": "399.50000000",
                "r": "0.00010000",
                "T": 1562306400000
            }
        ]"#;

        let funding_rates = serde_json::from_str::<Vec<BinanceFundingRate>>(input).unwrap();
        assert_eq!(funding_rates.len(), 2);
        assert_eq!(
            funding_rates[0].subscription_id.as_ref(),
            "@markPrice|BTCUSDT"
        );
        assert_eq!(
            funding_rates[1].subscription_id.as_ref(),
            "@markPrice|ETHUSDT"
        );
        assert_eq!(funding_rates[0].funding_rate, 0.00038167);
    }
}
