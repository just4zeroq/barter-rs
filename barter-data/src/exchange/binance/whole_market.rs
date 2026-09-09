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
        Map, Subscription,
        funding::{AllFundingRates, FundingRate},
        ticker::{AllTickers, Ticker},
    },
    transformer::ExchangeTransformer,
};
use async_trait::async_trait;
use barter_instrument::{
    asset::name::AssetNameInternal,
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use barter_integration::{
    Transformer,
    protocol::websocket::WsMessage,
};
use serde::Deserialize;
use smol_str::SmolStr;
use std::{collections::HashMap, fmt, marker::PhantomData};
use tokio::sync::mpsc::UnboundedSender;

/// [`BinanceSpot`] HTTP exchange information url, used to resolve every streaming symbol into a
/// normalised [`MarketDataInstrument`].
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#exchange-information>
const HTTP_EXCHANGE_INFO_BINANCE_SPOT: &str = "https://api.binance.com/api/v3/exchangeInfo";

/// [`BinanceFuturesUsd`] HTTP exchange information url, used to resolve every streaming symbol into
/// a normalised [`MarketDataInstrument`].
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#exchange-information>
const HTTP_EXCHANGE_INFO_BINANCE_FUTURES_USD: &str =
    "https://fapi.binance.com/fapi/v1/exchangeInfo";

/// Whole-market [`Subscription`] instrument used to subscribe to a single stream that broadcasts
/// events for *every* symbol (eg/ Binance `!miniTicker@arr`).
///
/// ### Notes
/// - The wrapped [`MarketDataInstrument`] is only used to communicate the exchange
///   [`MarketDataInstrumentKind`] to [`Validator`](barter_integration::Validator) logic; whole-market
///   events are emitted with a real per-symbol [`MarketDataInstrument`] resolved from the exchange
///   universe (eg/ fetched via `GET /exchangeInfo`).
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct WholeMarketInstrument {
    representative: MarketDataInstrument,
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
        Self {
            representative: MarketDataInstrument::new(base, quote, kind),
        }
    }
}

impl InstrumentData for WholeMarketInstrument {
    type Key = MarketDataInstrument;

    fn key(&self) -> &Self::Key {
        &self.representative
    }

    fn kind(&self) -> &MarketDataInstrumentKind {
        &self.representative.kind
    }
}

impl fmt::Display for WholeMarketInstrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "whole_market_{}", self.representative.kind)
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
    type Stream = BinanceWsStream<BinanceWholeMarketTickerTransformer<BinanceServerSpot>>;
}

/// [`StreamSelector`] for [`BinanceFuturesUsd`] whole-market [`AllTickers`] streams.
impl StreamSelector<WholeMarketInstrument, AllTickers> for BinanceFuturesUsd {
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        BinanceWsStream<BinanceWholeMarketTickerTransformer<BinanceServerFuturesUsd>>;
}

/// [`StreamSelector`] for [`BinanceFuturesUsd`] whole-market [`AllFundingRates`] streams.
impl StreamSelector<WholeMarketInstrument, AllFundingRates> for BinanceFuturesUsd {
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BinanceWsStream<
        BinanceWholeMarketFundingRateTransformer<BinanceServerFuturesUsd>,
    >;
}

/// [`Binance`] whole-market [`AllTickers`] [`ExchangeTransformer`] that translates the
/// `!miniTicker@arr` array stream into one normalised [`Ticker`] [`MarketEvent`] per symbol.
///
/// ### Notes
/// The per-symbol [`MarketDataInstrument`] universe is resolved on initialisation from the
/// associated exchange `GET /exchangeInfo` endpoint.
#[derive(Debug)]
pub struct BinanceWholeMarketTickerTransformer<Server> {
    universe: HashMap<String, MarketDataInstrument>,
    _phantom: PhantomData<Server>,
}

#[async_trait]
impl<Server> ExchangeTransformer<Binance<Server>, MarketDataInstrument, AllTickers>
    for BinanceWholeMarketTickerTransformer<Server>
where
    Server: ExchangeServer,
{
    async fn init(
        _instrument_map: Map<MarketDataInstrument>,
        _initial_snapshots: &[MarketEvent<MarketDataInstrument, Ticker>],
        _ws_sink_tx: UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        let universe = fetch_binance_exchange_universe::<Server>().await?;

        Ok(Self {
            universe,
            _phantom: PhantomData,
        })
    }
}

impl<Server> Transformer for BinanceWholeMarketTickerTransformer<Server>
where
    Server: ExchangeServer,
{
    type Error = DataError;
    type Input = Vec<BinanceTicker>;
    type Output = MarketEvent<MarketDataInstrument, Ticker>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let mut events = Vec::with_capacity(input.len());

        for ticker in input {
            // Determine the symbol associated with this whole-market element (eg/ "@miniTicker|BTCUSDT")
            let Some(symbol) = market_from_subscription_id(ticker.subscription_id.as_ref()) else {
                continue;
            };

            if let Some(instrument) = self.universe.get(symbol) {
                events.extend(
                    MarketIter::<MarketDataInstrument, Ticker>::from((
                        Server::ID,
                        instrument.clone(),
                        ticker,
                    ))
                    .0,
                );
            }
        }

        events
    }
}

/// [`Binance`] whole-market [`AllFundingRates`] [`ExchangeTransformer`] that translates the
/// `!markPrice@arr` array stream into one normalised [`FundingRate`] [`MarketEvent`] per symbol.
///
/// ### Notes
/// The per-symbol [`MarketDataInstrument`] universe is resolved on initialisation from the
/// associated exchange `GET /exchangeInfo` endpoint.
#[derive(Debug)]
pub struct BinanceWholeMarketFundingRateTransformer<Server> {
    universe: HashMap<String, MarketDataInstrument>,
    _phantom: PhantomData<Server>,
}

#[async_trait]
impl<Server> ExchangeTransformer<Binance<Server>, MarketDataInstrument, AllFundingRates>
    for BinanceWholeMarketFundingRateTransformer<Server>
where
    Server: ExchangeServer,
{
    async fn init(
        _instrument_map: Map<MarketDataInstrument>,
        _initial_snapshots: &[MarketEvent<MarketDataInstrument, FundingRate>],
        _ws_sink_tx: UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        let universe = fetch_binance_exchange_universe::<Server>().await?;

        Ok(Self {
            universe,
            _phantom: PhantomData,
        })
    }
}

impl<Server> Transformer for BinanceWholeMarketFundingRateTransformer<Server>
where
    Server: ExchangeServer,
{
    type Error = DataError;
    type Input = Vec<BinanceFundingRate>;
    type Output = MarketEvent<MarketDataInstrument, FundingRate>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let mut events = Vec::with_capacity(input.len());

        for funding in input {
            // Determine the symbol associated with this whole-market element (eg/ "@markPrice|BTCUSDT")
            let Some(symbol) = market_from_subscription_id(funding.subscription_id.as_ref()) else {
                continue;
            };

            if let Some(instrument) = self.universe.get(symbol) {
                events.extend(
                    MarketIter::<MarketDataInstrument, FundingRate>::from((
                        Server::ID,
                        instrument.clone(),
                        funding,
                    ))
                    .0,
                );
            }
        }

        events
    }
}

/// Fetches the exchange universe from `GET /exchangeInfo`, mapping every streamed symbol into a
/// normalised [`MarketDataInstrument`].
///
/// ### Notes
/// - [`BinanceSpot`] maps every listed symbol to a [`Spot`](MarketDataInstrumentKind::Spot)
///   [`MarketDataInstrument`].
/// - [`BinanceFuturesUsd`] only maps perpetual contracts (`contractType == "PERPETUAL"`) to a
///   [`Perpetual`](MarketDataInstrumentKind::Perpetual) [`MarketDataInstrument`], dropping
///   quarterlies/deliverables.
async fn fetch_binance_exchange_universe<Server>() -> Result<HashMap<String, MarketDataInstrument>, DataError>
where
    Server: ExchangeServer,
{
    let (url, kind_filter) = match Server::ID {
        ExchangeId::BinanceSpot => (
            HTTP_EXCHANGE_INFO_BINANCE_SPOT,
            spot_filter as fn(&BinanceExchangeInfoSymbol) -> Option<MarketDataInstrumentKind>,
        ),
        ExchangeId::BinanceFuturesUsd => (
            HTTP_EXCHANGE_INFO_BINANCE_FUTURES_USD,
            perpetual_filter as fn(&BinanceExchangeInfoSymbol) -> Option<MarketDataInstrumentKind>,
        ),
        exchange => {
            return Err(DataError::Socket(format!(
                "unsupported whole-market exchange: {exchange}"
            )))
        }
    };

    let info = reqwest::get(url)
        .await
        .map_err(|error| DataError::Http(error.to_string()))?
        .json::<BinanceExchangeInfo>()
        .await
        .map_err(|error| DataError::Http(error.to_string()))?;

    let mut universe = HashMap::with_capacity(info.symbols.len());

    for symbol in info.symbols {
        if let Some(kind) = kind_filter(&symbol) {
            universe.insert(
                symbol.symbol.clone(),
                MarketDataInstrument::new(symbol.base_asset, symbol.quote_asset, kind),
            );
        }
    }

    Ok(universe)
}

/// Determines whether a whole-market [`BinanceTicker`] / [`BinanceFundingRate`] subscription id
/// (eg/ `@miniTicker|BTCUSDT`) can be resolved to a symbol, returning the market component (eg/
/// `BTCUSDT`) if so.
fn market_from_subscription_id(subscription_id: &str) -> Option<&str> {
    subscription_id.split_once('|').map(|(_, market)| market)
}

/// Whole-market instrument kind filter for [`BinanceSpot`]: every listed symbol is a spot market.
fn spot_filter(_: &BinanceExchangeInfoSymbol) -> Option<MarketDataInstrumentKind> {
    Some(MarketDataInstrumentKind::Spot)
}

/// Whole-market instrument kind filter for [`BinanceFuturesUsd`]: only perpetual contracts are
/// mapped (dropping quarterly & delivery contracts).
fn perpetual_filter(symbol: &BinanceExchangeInfoSymbol) -> Option<MarketDataInstrumentKind> {
    match symbol.contract_type.as_deref() {
        Some("PERPETUAL") => Some(MarketDataInstrumentKind::Perpetual),
        _ => None,
    }
}

/// Minimal `GET /exchangeInfo` response representation.
#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceExchangeInfoSymbol>,
}

/// A single symbol entry in the `GET /exchangeInfo` response.
#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoSymbol {
    symbol: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "contractType", default)]
    contract_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_de_binance_exchange_info() {
        // Spot
        let input = r#"{
            "timezone": "UTC",
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT"
                }
            ]
        }"#;

        let info = serde_json::from_str::<BinanceExchangeInfo>(input).unwrap();
        assert_eq!(info.symbols[0].symbol, "BTCUSDT");
        assert_eq!(info.symbols[0].base_asset, "BTC");
        assert_eq!(info.symbols[0].quote_asset, "USDT");
        assert_eq!(info.symbols[0].contract_type, None);
        assert!(matches!(
            spot_filter(&info.symbols[0]),
            Some(MarketDataInstrumentKind::Spot)
        ));

        // Futures perpetual
        let input = r#"{
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "pair": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT"
                },
                {
                    "symbol": "BTCUSDT_250627",
                    "pair": "BTCUSDT_250627",
                    "contractType": "CURRENT_QUARTER",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT"
                }
            ]
        }"#;

        let info = serde_json::from_str::<BinanceExchangeInfo>(input).unwrap();
        assert!(matches!(
            perpetual_filter(&info.symbols[0]),
            Some(MarketDataInstrumentKind::Perpetual)
        ));
        assert!(matches!(perpetual_filter(&info.symbols[1]), None));
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
