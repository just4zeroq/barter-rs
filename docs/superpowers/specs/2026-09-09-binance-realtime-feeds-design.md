# Design: Binance Realtime Feeds Capability (barter-data S1+S2)

Date: 2026-09-09
Status: Draft for review

## 1. Context & Goal

Build a multi-exchange unified market-data service, extending the `barter-data` /
`barter-instrument` crates with **capability**: the library exposes a set of
normalised realtime feeds and REST metadata fetchers; the application layer decides
which symbols/feeds/streams to actually use.

First slice: **Binance** — both `BinanceSpot` (wss://stream.binance.com:9443/ws) and
`BinanceFuturesUsd` (wss://fstream.binance.com/ws). Funding-rate data is futures-only
and realtime-only (no historical backfill in this slice).

### Goals (what the capability set must cover)

Realtime market data kinds, exposed per-symbol **and** whole-market where the source
allows:

1. **Ticker** (24h mini-ticker) — whole-market via Binance `!miniTicker@arr`; per-symbol
   via `<symbol>@miniTicker`.
2. **OrderBookL1** (top-of-book) — whole-market via `!bookTicker`; per-symbol already
   exists (`<symbol>@bookTicker`).
3. **Candles / K-line** — per-symbol only (`<symbol>@kline_<interval>`; Binance has no
   whole-market kline stream). The `Candles` subscription kind must gain an interval.
4. **FundingRate** (mark/index/funding) — whole-market via futures `!markPrice@arr`;
   per-symbol via `<symbol>@markPrice`. Spot has no funding.
5. **AggTrades** — per-symbol only (`<symbol>@aggTrade`).
6. Existing kinds kept unchanged: `PublicTrades`, `OrderBooksL2` (already implemented,
   spot + futures).

Metadata (REST) is **deferred to a separate spec (S3)**: symbol/trading-pair info and
token/asset info via `exchangeInfo`. The S1/S2 whole-market transformer will however
depend on an `exchangeInfo`-shaped symbol→instrument table (see §4.4); a minimal fetch
is in scope for the transformer's own init.

### Non-goals (this slice)

- Application/service layer (subscription fanout service, persistence, public API) — S4, external.
- Historical REST backfill (klines history, funding history) — S3 or later.
- Other exchanges — later slices, but framework additions are designed to be reusable.
- Aggregated depth semantics beyond the existing L2 diff+snapshot pattern.

## 2. Current Architecture (constraints we extend)

Facts verified in code (2026-09-09):

- `Subscription<Exchange, Instrument, Kind>` has exactly 3 fields: `exchange`,
  `instrument`, `kind`. `Instrument: InstrumentData` (verbosity: base/quote/instrument
  kind; unique id = market-exchange name). `Kind: SubscriptionKind` with associated
  `type Event`.
- `SubKind` is a plain unit-only enum: `PublicTrades, OrderBooksL1, OrderBooksL2,
  OrderBooksL3, Liquidations, Candles`. No params possible through `SubKind`.
- `Candles` (subscription/candle.rs) is a unit struct with no interval — this is why no
  exchange implements klines. `Candle` event model exists (close_time, o/h/l/c, volume,
  trade_count); `DataKind::Candle` exists.
- `DataKind` enum (event.rs): Trade / OrderBookL1 / OrderBook / Candle / Liquidation.
  `MarketEvent<InstrumentKey, DataKind>` carries time_exchange, time_received, exchange,
  instrument, kind. `MultiStreamBuilder` / `DynamicStreams::select_all` merge typed
  streams into `MarketEvent<DataKind>`.
- `Transformer::transform(&mut self, Input) -> Vec<Result<MarketEvent, DataError>>`
  (`OutputIter`). A single ws message **can yield many events** (`MarketIter`).
  `StatelessTransformer` maps each input by its `SubscriptionId` (stream name) to **one**
  instrument via `Map<InstrumentKey>`; used when every message identifies one symbol.
- Whole-market arrays (many symbols in one message) therefore cannot reuse
  `StatelessTransformer`; they need a dedicated transformer holding a reverse
  symbol→instrument table.
- `ExchangeTransformer::init(instrument_map, initial_snapshots, ws_sink_tx)` runs once
  per stream and may do HTTP work — precedent: `BinanceOrderBooksL2SnapshotFetcher`
  fetches REST `/depth` during init; a whole-market transformer can fetch REST metadata
  the same way.
- `Identifier<Channel>`/`Identifier<Market>` for a `Subscription` translate a
  subscription into the channel name and the exchange market string. **`Market` is
  derived from the subscription's instrument, it is not the instrument.**
  `Connector::requests(exchange_subs)` builds the SUBSCRIBE payload from
  `{market_lowercase}{channel}` per subscription.
- `Subscription.instrument` may be one of: `MarketDataInstrument` (verbose, holds
  base/quote/kind), `Keyed<InstrumentKey, MarketDataInstrument>`, or
  `MarketInstrumentData<InstrumentKey>`. The stream's event key type ==
  `Instrument::Key`, fixed per stream.
- barter-instrument models instruments by *construction* from known base/quote; there is
  **no reverse parser** `"BTCUSDT" -> base/quote`. Ambiguous symbol strings therefore
  require an authoritative universe to resolve.

## 3. Selected approach

### 3.1 Whole-market (Option A) — a distinct whole-market subscription

Whole-market is a **first-class subscription form**, not a pile of per-symbol subs.
It composes with per-symbol feeds because event keys stay canonical
(`MarketDataInstrument`) everywhere.

Representation (two parts, kept distinct):

1. **Instrument layer** — a dedicated whole-market instrument whose
   `InstrumentData::Key = MarketDataInstrument` (so streams, `Map`, and `DataKind`
   combiners all use the same key type as per-symbol feeds). Because
   `InstrumentData::key()` must return a concrete `&MarketDataInstrument`, the
   whole-market instrument carries a **stable synthetic placeholder** value. This
   placeholder is **never emitted**: the whole-market transformer re-keys every event
   to the real per-symbol instrument from the universe (§4.4). Invariant guarded by a
   unit test.

2. **Wire layer** — the whole-market instrument returns an **empty** `BinanceMarket` (no
   single symbol), combined with distinct whole-market channel constants
   (`BinanceChannel::ALL_MINI_TICKER` = `!miniTicker@arr`,
   `BinanceChannel::ALL_MARK_PRICE` = `!markPrice@arr`). `Connector::requests()`
   concatenation is unchanged and yields the correct whole-market stream name
   (`"" + "!miniTicker@arr"`), while per-symbol streams are untouched.

   **Coherence note:** distinct whole-market behaviour is expressed with a dedicated
   `WholeMarketInstrument` type on the *instrument* axis and dedicated `AllTickers` /
   `AllFundingRates` kinds on the *kind* axis — never by widening the existing generic
   channel/market `Identifier` impls (those are fixed per kind / bound to the three
   existing instrument containers).

Consequences:
- Per-symbol path is untouched (all existing `Identifier`/transformer/request code keeps
  working).
- A whole-market subscription is one `Subscription` → one stream; the stream yields
  one `MarketEvent<MarketDataInstrument, Kind::Event>` per symbol per message.

### 3.2 K-line interval is a Kind parameter

`Candles` becomes a parameterised kind so the app chooses intervals:

```rust
pub struct Candles { pub interval: CandleInterval }
```

`CandleInterval` is a normalised enum (Second1, Minute1, Minute3, Minute5, Minute15,
Minute30, Hour1, Hour2, Hour4, Hour6, Hour12, Day1, Week1, Month1 — capability set;
apps use a subset). `Display`/`as_str` of the kind includes the interval so stream ids
and the SUBSCRIBE channel string (`@kline_<interval>`) stay unambiguous.

Dynamic `SubKind::Candles` (unit) cannot carry an interval; the parameterised path uses
a typed `Subscription<..., Candles>`. Limitation recorded in §6.

## 4. Design

### 4.1 New / changed subscription kinds and normalised events (barter-data)

Normalised-event rule: fields are the **cross-exchange minimal set** needed by an app
(unified service); Binance-specific extras are consumed inside the adapter and not
exposed unless they generalise.

1. **Ticker** — `subscription/ticker.rs`
   ```rust
   pub struct Ticker;                      // SubscriptionKind, Event = Ticker
   pub struct Ticker {
       pub open: f64, pub high: f64, pub low: f64,
       pub last: f64, pub volume_base: f64, pub volume_quote: f64,
   }
   ```
   Whole-market (`!miniTicker@arr`) and per-symbol (`<symbol>@miniTicker`) produce the
   same event.

2. **Candles** — modify `subscription/candle.rs` (see §3.2). Event `Candle` unchanged
   in this slice (note: `Candle` has no open_time; add only if a consumer needs it —
   out of scope now).

3. **FundingRate** — `subscription/funding.rs` (futures only)
   ```rust
   pub struct FundingRate;                 // Event = FundingRate
   pub struct FundingRate {
       pub mark_price: f64,
       pub index_price: f64,
       pub funding_rate: f64,
       pub next_funding_time: DateTime<Utc>,
   }
   ```
   Realtime only. Whole-market `!markPrice@arr` and per-symbol `<symbol>@markPrice`.

4. **AggTrades** — `subscription/agg_trade.rs`
   ```rust
   pub struct AggTrades;                   // Event = AggTrade
   pub struct AggTrade {
       pub aggregate_trade_id: u64,
       pub price: f64,
       pub quantity: f64,
       pub first_trade_id: u64,
       pub last_trade_id: u64,
       pub buyer_is_maker: bool,
   }
   ```
   Time already lives on `MarketEvent.time_exchange`. Per-symbol only.

5. **DataKind / SubKind / event wiring** — add `Ticker`, `FundingRate`, `AggTrade`
   variants to `DataKind` (matching `From`, `kind_name`, accessors `as_ticker`,
   `as_funding_rate`, `as_agg_trade`); add the unit-like kinds to `SubKind`
   (Ticker/FundingRate/AggTrades yes; Candles carries params — see §3.2).

### 4.2 Binance connector additions

- `channel.rs`: add channel constants for whole-market (`!miniTicker@arr`,
  `!bookTicker`, `!markPrice@arr`) and per-symbol (`<symbol>@miniTicker`,
  `<symbol>@markPrice`, `<symbol>@aggTrade`, `<symbol>@kline_<interval>`). Channel
  choice is kind-driven; the `!` vs `<symbol>@` distinction is market-driven in
  `requests()`.
- `market.rs`: `BinanceMarket` becomes `{ Symbol(SmolStr), All }`; `AsRef<str>` and
  all existing `Identifier<BinanceMarket>` impls return `Symbol` (unchanged behaviour);
  new impl for the whole-market instrument returns `All`.
- `Identifier<BinanceChannel>` impls per new kind. For candles the impl reads
  `self.kind.interval` to build `@kline_1m` etc.
- `Connector::requests()`: emit `!`-prefixed names when market is `All`, `<symbol>`
  (lowercase) otherwise. `expected_responses()` unchanged (one SUBSCRIBE ack).
- **Stateless per-symbol transformers** (mirror `trade.rs`/`book/l1.rs`) for:
  `Ticker` (Symbol), `Candles`, `FundingRate` (Symbol), `AggTrades`.
- **Whole-market transformers** (dedicated, mirror spot/futures `l2.rs` shape) for:
  `Ticker` (All), `OrderBookL1` (All — `!bookTicker`), `FundingRate` (All). Each holds
  a `symbol → MarketDataInstrument` table resolved during `init` (§4.4) and returns one
  event per array element via `OutputIter`.

### 4.3 Server variants

| Exchange | Server | new whole-market channels |
|---|---|---|
| BinanceSpot    | stream.binance.com:9443 | `!miniTicker@arr`, `!bookTicker` |
| BinanceFuturesUsd | fstream.binance.com  | `!miniTicker@arr`, `!bookTicker`, `!markPrice@arr` |

### 4.4 Whole-market symbol resolution (the key mechanism)

A whole-market message contains per-symbol payloads (`"s":"BTCUSDT"`, ...). Each event
must carry the canonical `MarketDataInstrument` key so feeds compose. Because barter has
no ambiguous-symbol parser, the transformer builds a **symbol→instrument** table during
`init` by fetching the authoritative universe:

- Fetch `GET /api/v3/exchangeInfo` (spot) or `/fapi/v1/exchangeInfo` (futures) once per
  whole-market stream init — same pattern as the L2 REST snapshot fetcher.
- Parse each entry (symbol, baseAsset, quoteAsset, contractType/perpetual flags, base &
  quote precision, filters) into a `MarketDataInstrument`; index by exchange symbol
  string (uppercase, as Binance emits it).
- `transform` iterates the array payload: look up symbol → instrument → emit one
  `MarketEvent<MarketDataInstrument, Kind::Event>` per element. Unknown symbols are
  dropped (single `DataError`-typed entry, mirroring the `Map::find` error path), never
  a stream panic. The synthetic placeholder (§3.1) is never a lookup target and is
  never emitted.
- Multi-event `transform` is supported natively: `OutputIter` is a
  `Vec<Result<MarketEvent, DataError>>`.

This makes S1/S2 self-contained (no app plumbing) and establishes the shared
`exchangeInfo` consumer that S3 later generalises.

### 4.5 Error handling

- Unknown symbol in a whole-market payload → `DataError` entry, stream continues.
- `exchangeInfo` fetch failure at transformer init → stream init error (same as L2
  snapshot fetch failure today).
- Binance subscribe ack failure → existing `WebSocketSubValidator` path.

### 4.6 Testing

Follow the existing pattern of `#[cfg(test)]` serde parse tests with raw payload
fixtures (see `book/l1.rs`, `subscription.rs`, spot/futures `l2.rs`):

- Parse+normalise fixtures per Binance raw payload: `!miniTicker@arr` element,
  `<symbol>@miniTicker`, `!bookTicker`/bookTicker, `@aggTrade`, `@kline_<interval>`,
  `!markPrice@arr`/`@markPrice`.
- Whole-market fanout test: one array message → N events, each carrying the correct
  symbol's instrument key; placeholder never emitted.
- `requests()` test: `All` market → `!...` stream names; `Symbol` → `<symbol>@...`.

## 5. Deliverables (this slice)

1. `subscription/{ticker,funding,agg_trade}.rs` new kinds+events; `candle.rs` gains
   interval; whole-market instrument; `SubKind`/`DataKind`/`event.rs` wired.
2. `exchange/binance/{ticker,funding,agg_trade}.rs` + wire types; `channel.rs`,
   `market.rs`, `mod.rs`, `spot/mod.rs`, `futures/mod.rs` updated.
3. Whole-market transformer(s) with `exchangeInfo`-based symbol→instrument resolution.
4. Unit tests per §4.6.

## 6. Known limitations / decisions to confirm

- **Candle open_time**: `Candle` lacks `open_time`. Deferred — revisit if a consumer
  needs it.
- **Dynamic `SubKind::Candles`**: cannot carry interval; klines require the typed
  `Candles { interval }` kind. Dynamic-builder candle subscriptions are out of scope.
- **Whole-market instrument placeholder**: `InstrumentData::key()` needs a concrete
  `MarketDataInstrument`; a stable synthetic placeholder is stored and never emitted.
  Flagged for a focused spike during implementation planning to confirm the cleanest
  shape (candidate: reserved pseudo pair, excluded by transformer invariant + test).
- **L2 whole-market**: Binance depth has no whole-market array stream; L2 stays
  per-symbol (existing implementation). No new L2 work in this slice.
- **Symbol resolution cost**: one `exchangeInfo` fetch per whole-market stream init.
  Acceptable; cache/memoise when S3 generalises the fetcher.
- **S3 / S4** (metadata spec, service app) are separate follow-on slices.

## 7. Open questions for reviewer

1. Confirm the normalised event field sets in §4.1 (esp. Ticker without 24h open_time /
   bid-ask; AggTrade without base/quote assets).
2. Whole-market L1: subscribe `!bookTicker` as its own whole-market subscription, or
   fold top-of-book into the Ticker whole-market subscription? (Recommend: separate —
   bookTicker pushes per-symbol changes faster than the 1s mini-ticker array.)
3. `CandleInterval` location: `barter-data::subscription::candle` vs
   `barter-instrument`. (Recommend: barter-data, it is a feed parameter not an
   instrument property.)
4. Should `Ticker` carry per-symbol bid/ask from a paired `bookTicker`? (Recommend: no
   in this slice — separate L1 subscription; a service can join them.)
