# Binance Spot market-stream implementation references

This integration consumes only public Spot `@trade` streams and never requests API keys or account permissions.

## Official stream reference

Source: [Binance Spot API — WebSocket Streams](https://developers.binance.com/en/docs/products/spot/web-sockets/market-data-streams)

Implementation details verified against the official documentation on 2026-10-12:

- WebSocket base endpoints include `wss://stream.binance.com:9443` and `wss://stream.binance.com:443`.
- Combined subscriptions use `/stream?streams=<stream1>/<stream2>`. Stream symbols are lowercase; this client subscribes to each configured pair's raw `<symbol>@trade` stream.
- A `trade` event provides exchange event time (`E`), symbol (`s`), trade ID (`t`), price (`p`), quantity (`q`), trade time (`T`) and buyer-is-maker (`m`). Values are parsed and validated before use.
- Binance sends a server ping every 20 seconds; the client must pong promptly and can be disconnected if no pong is received within one minute. `websocket-client` handles control-frame pong responses. This client reconnects with bounded exponential backoff after disconnects and after the 24-hour connection lifetime.
- Current documented limits include five incoming messages per second on a connection, at most 1,024 subscribed streams on one connection and at most 300 connection attempts per five minutes per IP. The current dashboard limits its watchlist to 20 pairs and uses one combined connection.
- The official market-data-only endpoint is `wss://data-stream.binance.vision`; it does not provide user data streams. The dashboard does not request user streams.

## Terms reference

The official [Spot Exchange Terms of Use](https://github.com/binance/binance-spot-api-docs/blob/master/PROD-TERMS-OF-USE.md) direct users to Binance's [Product Terms of Use](https://www.binance.com/en/terms). This project provides links to those sources but does not interpret legal terms; operators should review the current terms applicable to their location and use.

## Project-specific safety boundary

The bundled XGBoost model was trained and evaluated on engineered, labeled historical benchmark rows. Live trade messages do not supply the same validated feature schema. Therefore the live path uses only documented market fields, transparent five-second aggregation and explicit price/volume rules; it does not apply the saved XGBoost model or call a market warning a confirmed manipulation event.
