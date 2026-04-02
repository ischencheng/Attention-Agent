#!/usr/bin/env python3
"""Finnhub WebSocket ingestor — real-time stock trade prices -> source_raw."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from typing import Any

import websockets
from prometheus_client import Counter, Gauge, start_http_server

from base import create_producer, ensure_topic, iso_now, make_envelope, produce_with_flush

LOGGER = logging.getLogger("finnhub-ingestor")

TRADES_RECEIVED = Counter("finnhub_trades_received_total", "Total trades received")
PUBLISH_SUCCESS = Counter("finnhub_publish_success_total", "Successful publishes")
PUBLISH_FAILED = Counter("finnhub_publish_failed_total", "Failed publishes")
LAST_TRADE_TIME = Gauge("finnhub_last_trade_unixtime", "Unix time of last trade")

DEFAULT_SYMBOLS = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,AMD,INTC,AVGO"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Finnhub WebSocket -> Kafka source_raw")
    p.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    p.add_argument("--topic", default="source_raw")
    p.add_argument("--api-key", default=os.getenv("FINNHUB_API_KEY", ""))
    p.add_argument("--symbols", default=os.getenv("FINNHUB_SYMBOLS", DEFAULT_SYMBOLS))
    p.add_argument("--metrics-port", type=int, default=8021)
    p.add_argument("--reconnect-max-backoff-seconds", type=float, default=30.0)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


class FinnhubIngestor:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        self.ws_url = f"wss://ws.finnhub.io?token={args.api_key}"
        self.running = True
        self.reconnect_backoff = 1.0
        self.producer = create_producer(args.bootstrap)

    def _trade_to_envelope(self, trade: dict[str, Any]) -> bytes | None:
        symbol = trade.get("s")
        price = trade.get("p")
        volume = trade.get("v")
        ts_ms = trade.get("t")  # unix ms
        if not symbol or price is None:
            return None

        timestamp = iso_now()
        if ts_ms:
            from datetime import datetime, timezone
            timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3] + "Z"

        return make_envelope(
            source="finnhub",
            source_id=f"finnhub-trade-{symbol}-{ts_ms or 0}",
            content_type="price_tick",
            timestamp=timestamp,
            data={
                "ticker": symbol,
                "price": float(price),
                "volume": int(volume) if volume else 0,
                "conditions": trade.get("c", []),
            },
            raw=trade,
        )

    async def run_forever(self) -> None:
        while self.running:
            try:
                LOGGER.info("connecting finnhub ws symbols=%s", self.symbols)
                async with websockets.connect(
                    self.ws_url, ping_interval=30, ping_timeout=60, close_timeout=10
                ) as ws:
                    # Subscribe to symbols
                    for sym in self.symbols:
                        await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
                        LOGGER.info("subscribed symbol=%s", sym)

                    self.reconnect_backoff = 1.0
                    await self._consume(ws)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                LOGGER.warning("finnhub error=%s reconnect_in=%.1fs", ex, self.reconnect_backoff)
                await asyncio.sleep(self.reconnect_backoff)
                self.reconnect_backoff = min(
                    self.reconnect_backoff * 2, self.args.reconnect_max_backoff_seconds
                )

    async def _consume(self, ws: Any) -> None:
        async for message in ws:
            if not self.running:
                break

            try:
                msg = json.loads(message)
            except json.JSONDecodeError:
                continue

            if msg.get("type") != "trade":
                continue

            trades = msg.get("data", [])
            for trade in trades:
                TRADES_RECEIVED.inc()
                LAST_TRADE_TIME.set(asyncio.get_event_loop().time())

                envelope = self._trade_to_envelope(trade)
                if envelope is None:
                    continue

                try:
                    produce_with_flush(
                        self.producer, self.args.topic, trade.get("s", "unknown"), envelope
                    )
                    PUBLISH_SUCCESS.inc()
                except Exception as ex:
                    PUBLISH_FAILED.inc()
                    LOGGER.error("publish failed error=%s", ex)

    def stop(self) -> None:
        self.running = False
        self.producer.flush(10)


async def async_main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not args.api_key:
        LOGGER.error("FINNHUB_API_KEY not set, exiting")
        return

    start_http_server(args.metrics_port)
    ensure_topic(args.bootstrap, args.topic)

    ingestor = FinnhubIngestor(args)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        ingestor.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _handle_signal())

    LOGGER.info("finnhub-ingestor started topic=%s metrics=0.0.0.0:%s/metrics", args.topic, args.metrics_port)
    run_task = asyncio.create_task(ingestor.run_forever())
    stop_task = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
