use super::{Binance, futures::BinanceFuturesUsd};
use crate::{
    Identifier,
    subscription::{
        Subscription,
        agg_trade::AggTrades,
        book::{OrderBooksL1, OrderBooksL2},
        candle::{CandleInterval, Candles},
        funding::FundingRates,
        liquidation::Liquidations,
        ticker::Tickers,
        trade::PublicTrades,
    },
};
use serde::Serialize;
use std::borrow::Cow;

/// Type that defines how to translate a Barter [`Subscription`] into a [`Binance`]
/// channel to be subscribed to.
///
/// ### Notes
/// [`Cow`] is used so channel names can be either `'static` (eg/ [`Self::TRADES`]) or owned at
/// runtime for parameterised streams (eg/ [`BinanceChannel::kline`] building `@kline_1m`).
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BinanceChannel(pub Cow<'static, str>);

impl BinanceChannel {
    /// [`Binance`] real-time trades channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#trade-streams>
    ///
    /// Note:
    /// For [`BinanceFuturesUsd`] this real-time
    /// stream is undocumented.
    ///
    /// See discord: <https://discord.com/channels/910237311332151317/923160222711812126/975712874582388757>
    pub const TRADES: Self = Self(Cow::Borrowed("@trade"));

    /// [`Binance`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs:<https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams>
    /// See docs:<https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams>
    pub const ORDER_BOOK_L1: Self = Self(Cow::Borrowed("@bookTicker"));

    /// [`Binance`] OrderBook Level2 channel name (100ms delta updates).
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams>
    pub const ORDER_BOOK_L2: Self = Self(Cow::Borrowed("@depth@100ms"));

    /// [`BinanceFuturesUsd`] liquidation orders channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams>
    pub const LIQUIDATIONS: Self = Self(Cow::Borrowed("@forceOrder"));

    /// [`Binance`] real-time aggregate trade channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams>
    pub const AGG_TRADE: Self = Self(Cow::Borrowed("@aggTrade"));

    /// [`Binance`] real-time 24hr mini ticker channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-mini-ticker-stream>
    pub const MINI_TICKER: Self = Self(Cow::Borrowed("@miniTicker"));

    /// [`Binance`] real-time 24hr mini ticker whole-market channel name.
    ///
    /// Subscribes to the `!miniTicker@arr` stream which broadcasts an array of 24hr mini tickers
    /// for *every* symbol.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#all-market-mini-tickers-stream>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#all-market-mini-tickers-stream>
    pub const ALL_MINI_TICKER: Self = Self(Cow::Borrowed("!miniTicker@arr"));

    /// [`BinanceFuturesUsd`] mark price / funding rate channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream>
    pub const MARK_PRICE: Self = Self(Cow::Borrowed("@markPrice"));

    /// [`BinanceFuturesUsd`] mark price / funding rate whole-market channel name.
    ///
    /// Subscribes to the `!markPrice@arr` stream which broadcasts an array of mark prices for
    /// *every* perpetual symbol.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream-for-all-market>
    pub const ALL_MARK_PRICE: Self = Self(Cow::Borrowed("!markPrice@arr"));

    /// Construct a new [`Self`] for the provided kline [`CandleInterval`] (eg/ `@kline_1m`).
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-streams>
    pub fn kline(interval: CandleInterval) -> Self {
        Self(Cow::Owned(format!("@kline_{}", interval.as_str())))
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::TRADES
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ORDER_BOOK_L1
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL2>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ORDER_BOOK_L2
    }
}

impl<Instrument> Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsd, Instrument, Liquidations>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::LIQUIDATIONS
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, AggTrades>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::AGG_TRADE
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, Tickers>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::MINI_TICKER
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, Candles>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::kline(self.kind.interval)
    }
}

impl<Instrument> Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsd, Instrument, FundingRates>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::MARK_PRICE
    }
}

impl AsRef<str> for BinanceChannel {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}
