import json
import unittest

from backend.live.market_stream import LiveMarketManager, normalize_symbols


class MarketStreamTests(unittest.TestCase):
    def setUp(self):
        self.manager = LiveMarketManager(base_url="wss://example.invalid")
        self.manager._symbols = ["BTCUSDT", "ETHUSDT"]

    @staticmethod
    def trade(symbol, price, qty, ts, trade_id=1, buyer_is_maker=False):
        return json.dumps({
            "stream": f"{symbol.lower()}@trade",
            "data": {"e": "trade", "E": ts + 1, "s": symbol, "t": trade_id,
                     "p": str(price), "q": str(qty), "T": ts, "m": buyer_is_maker, "M": True},
        })

    def test_trade_message_updates_live_only_counters_and_price(self):
        self.assertTrue(self.manager.handle_message(self.trade("BTCUSDT", 100.5, 0.25, 1_800_000_000_000)))
        snapshot = self.manager.snapshot()
        self.assertEqual(snapshot["processed_events"], 1)
        self.assertEqual(snapshot["latest_prices"]["BTCUSDT"]["price"], 100.5)
        self.assertEqual(snapshot["latest_prices"]["BTCUSDT"]["quantity"], 0.25)
        self.assertEqual(snapshot["latest_prices"]["BTCUSDT"]["quote_quantity"], 25.125)
        self.assertTrue(snapshot["last_exchange_event_at"].endswith("Z"))

    def test_two_real_trades_in_same_bucket_aggregate_without_fabricated_rows(self):
        t0 = 1_800_000_000_000
        self.manager.handle_message(self.trade("BTCUSDT", 100, 1, t0, 1, False))
        self.manager.handle_message(self.trade("BTCUSDT", 102, 2, t0 + 1_000, 2, True))
        self.manager.handle_message(self.trade("BTCUSDT", 101, 1, t0 + 5_000, 3, False))
        snapshot = self.manager.snapshot()
        bars = snapshot["bars"]["BTCUSDT"]
        self.assertEqual(len(bars), 1)  # second bucket is still open, not closed/fabricated
        bar = bars[0]
        self.assertEqual((bar["open"], bar["high"], bar["low"], bar["close"]), (100, 102, 100, 102))
        self.assertEqual(bar["base_volume"], 3)
        self.assertEqual(bar["trade_count"], 2)
        self.assertEqual(bar["taker_buy_base_volume"], 1)
        self.assertTrue(snapshot["current_bars"]["BTCUSDT"])

    def test_rapid_price_rule_alert_is_explainable_and_not_an_xgboost_score(self):
        t0 = 1_800_000_000_000
        self.manager.handle_message(self.trade("BTCUSDT", 100, 1, t0, 1))
        self.manager.handle_message(self.trade("BTCUSDT", 101.2, 1, t0 + 5_000, 2))
        self.manager.handle_message(self.trade("BTCUSDT", 101.1, 1, t0 + 10_000, 3))
        alerts = self.manager.snapshot()["alerts"]
        self.assertEqual(len(alerts), 1)
        self.assertIn("rapid_price_change", alerts[0]["signals"])
        self.assertEqual(alerts[0]["detection_method"], "rule_based_five_second_market_bar")
        self.assertEqual(alerts[0]["model_prediction"], "unavailable_live_schema")
        self.assertIn("not evidence of manipulation", alerts[0]["explanation"])

    def test_unsubscribed_and_malformed_payloads_are_not_counted_as_market_trades(self):
        t0 = 1_800_000_000_000
        self.assertFalse(self.manager.handle_message(self.trade("XRPUSDT", 0.5, 3, t0)))
        malformed = json.dumps({"e": "trade", "s": "BTCUSDT", "p": "NaN", "q": "0", "T": t0, "E": t0})
        self.assertFalse(self.manager.handle_message(malformed))
        state = self.manager.status()
        self.assertEqual(state["processed_events"], 0)
        self.assertGreaterEqual(state["invalid_messages"], 2)

    def test_symbol_validation_deduplicates_and_caps_pairs(self):
        self.assertEqual(normalize_symbols("btcusdt, ETHUSDT, btcusdt"), ["BTCUSDT", "ETHUSDT"])
        with self.assertRaises(ValueError):
            normalize_symbols("BTC/USDT")
        with self.assertRaises(ValueError):
            normalize_symbols([f"COIN{i}USDT" for i in range(21)])

    def test_volume_spike_rule_requires_twenty_prior_observed_bars(self):
        t0 = 1_800_000_000_000
        # Twenty normal 5-second buckets; each bucket is closed only when a real trade
        # arrives in the following bucket. A 3x bar is tested after that warm-up.
        for i in range(21):
            qty = 1 if i < 20 else 4
            self.manager.handle_message(self.trade("BTCUSDT", 100, qty, t0 + i * 5_000, i + 1))
        # One more observed trade closes the 4x bucket.
        self.manager.handle_message(self.trade("BTCUSDT", 100, 1, t0 + 21 * 5_000, 22))
        alerts = self.manager.snapshot()["alerts"]
        self.assertTrue(any("volume_spike" in alert["signals"] for alert in alerts))
        volume_alert = next(alert for alert in alerts if "volume_spike" in alert["signals"])
        self.assertGreaterEqual(volume_alert["volume_ratio_vs_median"], 3.0)


if __name__ == "__main__":
    unittest.main()
