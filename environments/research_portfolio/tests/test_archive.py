from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from environments.research_portfolio.archive import _session_calendar, build_archive, digest, load_archive, validate_market
from environments.research_portfolio.collector import source_record
from environments.research_portfolio.scenario import timestamp


def inputs():
    start, end = "2026-10-22T12:00:00Z", "2026-10-23T20:00:00Z"
    raw = {"title": "Issuer update", "content_html": "Capital spending increased."}
    row = source_record({"id": 1, "created_at": start, "raw": raw,
                         "payload": {"source": "Issuer", "source_id": "https://issuer.example/update",
                                     "timestamp": "2026-10-22T10:00:00Z"}})
    sources = {"schema_version": 1, "collected_at": "2026-10-24T12:00:00Z", "records": [row],
               "selection": {"start_at": start, "end_at": end, "received_row_count": 1}}
    bars = [{"asset_id": "issuer", "open_at": f"2026-10-{day}T13:30:00Z",
             "close_at": f"2026-10-{day}T20:00:00Z", "open": 100, "close": close}
            for day, close in [(22, 101), (23, 95)]]
    raw_prices = {"chart": {"result": [{"meta": {"symbol": "ISS", "exchangeTimezoneName": "America/New_York"},
                                       "timestamp": [int(timestamp(bar["open_at"]).timestamp()) for bar in bars],
                                       "indicators": {"quote": [{"open": [100, 100], "close": [101, 95]}]}}]}}
    market = {"schema_version": 1, "price_basis": "raw", "price_vintage": "historical_reconstruction",
              "collected_at": "2026-10-24T12:00:00Z",
              "assets": [{"asset_id": "issuer", "symbol": "ISS", "name": "Issuer",
                          "available_at": start, "terms": ["capital spending"]}],
              "bars": bars, "corporate_actions": [], "receipts": [{"asset_id": "issuer",
              "provider": "Yahoo Finance chart API", "request_url": "https://query1.finance.yahoo.com/v8/finance/chart/ISS",
              "fetched_at": "2026-10-24T11:00:00Z", "raw_response": raw_prices,
              "raw_response_sha256": digest(raw_prices), "bars_sha256": digest(bars),
              "corporate_actions_sha256": digest([]), "calendar": "XNYS", "calendar_version": "4.13.2"}]}
    identity = {"model_id": "fixture/model", "revision": "a" * 40,
                "released_at": "2025-04-29T00:00:00Z", "release_evidence_url": "https://models.example/release",
                "revision_evidence_url": "https://models.example/commit/" + "a" * 40,
                "knowledge_cutoff": None, "temporal_basis": "checkpoint_published_before_episode",
                "artifact_sha256": {"model.safetensors": "b" * 64}}
    policies = {"schema_version": 1, "roles": {role: deepcopy(identity) for role in ("fast", "mid", "strong")}}
    spec = {"episode_id": "fixture-historical", "start_at": start, "end_at": end,
            "scope": ["capital spending"], "portfolio": {"initial_cash": 100000, "fee_bps": 10,
                                                         "slippage_bps": 5, "max_weight": 0.5}}
    return spec, sources, market, policies


class ArchiveTests(unittest.TestCase):
    def test_archive_round_trip_preserves_dates_and_explicit_vintage_limits(self):
        with tempfile.TemporaryDirectory() as temporary:
            expected = build_archive(*inputs(), output=temporary)
            actual = load_archive(temporary)
            self.assertEqual(actual, expected)
            integrity = actual["contract"]["replay_integrity"]
            self.assertTrue(integrity["benchmark_eligible"])
            self.assertFalse(integrity["strict_point_in_time_eligible"])
            self.assertFalse(integrity["model_cutoff_verified"])
            self.assertFalse(integrity["model_files_verified"])
            source = actual["manifest"]["sources"][0]
            self.assertNotEqual(source["published_at"], source["available_at"])
            self.assertNotIn("raw_response", json.dumps(actual["manifest"]))

    def test_each_role_must_precede_episode_with_full_immutable_identity(self):
        for role in ("fast", "mid", "strong"):
            with self.subTest(role=role):
                spec, sources, market, policies = inputs()
                policies["roles"][role]["released_at"] = spec["start_at"]
                with self.assertRaisesRegex(ValueError, "released strictly before"):
                    build_archive(spec, sources, market, policies)
        spec, sources, market, policies = inputs()
        policies["roles"]["mid"]["revision"] = "main"
        with self.assertRaisesRegex(ValueError, "immutable"):
            build_archive(spec, sources, market, policies)

    def test_missing_role_or_model_files_does_not_pass_admission(self):
        spec, sources, market, policies = inputs()
        del policies["roles"]["fast"]
        with self.assertRaisesRegex(ValueError, "fast, mid and strong"):
            build_archive(spec, sources, market, policies)
        spec, sources, market, policies = inputs()
        policies["roles"]["strong"]["artifact_sha256"] = {}
        with self.assertRaisesRegex(ValueError, "artifact hashes"):
            build_archive(spec, sources, market, policies)

    def test_known_cutoff_requires_evidence_and_cannot_be_after_episode(self):
        spec, sources, market, policies = inputs()
        identity = policies["roles"]["fast"]
        identity["knowledge_cutoff"] = "2026-10-23T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "training cutoff"):
            build_archive(spec, sources, market, policies)
        identity["knowledge_cutoff"] = "2025-01-01T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "knowledge_cutoff_evidence"):
            build_archive(spec, sources, market, policies)

    def test_old_publication_cannot_replace_actual_receipt(self):
        spec, sources, market, policies = inputs()
        sources["records"][0]["available_at"] = sources["records"][0]["published_at"]
        with self.assertRaisesRegex(ValueError, "actual database receipt"):
            build_archive(spec, sources, market, policies)

    def test_source_and_price_mutations_fail_hash_checks(self):
        for target in ("raw", "body", "quote", "bar"):
            with self.subTest(target=target):
                spec, sources, market, policies = inputs()
                if target == "raw":
                    sources["records"][0]["raw"]["title"] = "Edited"
                elif target == "body":
                    sources["records"][0]["body"] = "Edited"
                elif target == "quote":
                    market["receipts"][0]["raw_response"]["chart"]["result"][0]["indicators"]["quote"][0]["close"][0] = 200
                else:
                    market["bars"][0]["open"] = 200
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    build_archive(spec, sources, market, policies)

    def test_market_incomplete_terminal_cannot_be_admitted(self):
        spec, sources, market, policies = inputs()
        market["bars"].pop()
        market["receipts"][0]["bars_sha256"] = digest(market["bars"])
        with self.assertRaisesRegex(ValueError, "terminal"):
            build_archive(spec, sources, market, policies)

    def test_saved_manifest_and_admission_tampering_are_rejected(self):
        for filename in ("episode.json", "archive-contract.json"):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as temporary:
                build_archive(*inputs(), output=temporary)
                path = Path(temporary) / filename
                value = json.loads(path.read_text())
                if filename == "episode.json":
                    value["sources"][0]["body"] = "Retrospective rewrite"
                else:
                    value["replay_integrity"]["strict_point_in_time_eligible"] = True
                path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    load_archive(temporary)

    def test_selected_policy_must_equal_frozen_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec, sources, market, policies = inputs()
            build_archive(spec, sources, market, policies, temporary)
            policies["roles"]["strong"]["revision"] = "c" * 40
            with self.assertRaisesRegex(ValueError, "selected policy"):
                load_archive(temporary, policies)

    def test_arrival_sort_is_stable_and_cutoffs_do_not_use_publication(self):
        spec, sources, market, policies = inputs()
        first = sources["records"][0]
        second = source_record({"id": 2, "created_at": "2026-10-22T14:00:00Z",
                                "raw": first["raw"], "payload": first["payload"]})
        sources["records"].insert(0, second)
        sources["selection"]["received_row_count"] = 2
        result = build_archive(spec, sources, market, policies)
        self.assertEqual([row["id"] for row in result["manifest"]["sources"]], [first["id"], second["id"]])
        self.assertIn("2026-10-22T14:00:00+00:00", result["manifest"]["timeline"])
        self.assertNotIn("2026-10-22T10:00:00+00:00", result["manifest"]["timeline"])

    def test_partial_or_short_source_export_is_not_admitted(self):
        spec, sources, market, policies = inputs()
        sources["rejected_records"] = [{"raw_id": 2, "reason": "missing publication"}]
        with self.assertRaisesRegex(ValueError, "partial source"):
            build_archive(spec, sources, market, policies)
        del sources["rejected_records"]
        sources["selection"]["end_at"] = "2026-10-22T20:00:00Z"
        with self.assertRaisesRegex(ValueError, "entire episode"):
            build_archive(spec, sources, market, policies)

    def test_rehashed_body_and_publication_must_match_original_projection(self):
        for field in ("body", "published_at"):
            spec, sources, market, policies = inputs()
            row = sources["records"][0]
            row[field] = "Fabricated evidence" if field == "body" else "2026-10-22T09:00:00+00:00"
            row["body_sha256"] = digest(row["body"])
            with self.assertRaisesRegex(ValueError, "captured original projection"):
                build_archive(spec, sources, market, policies)

    def test_rehashed_prices_and_removed_actions_do_not_bypass_original_checks(self):
        spec, sources, market, policies = inputs()
        market["bars"][0]["close"] = 150
        market["receipts"][0]["bars_sha256"] = digest(market["bars"])
        with self.assertRaisesRegex(ValueError, "original provider projection"):
            build_archive(spec, sources, market, policies)
        spec, sources, market, policies = inputs()
        receipt = market["receipts"][0]
        receipt["raw_response"]["chart"]["result"][0]["events"] = {
            "splits": {"event": {"date": int(timestamp(market["bars"][0]["open_at"]).timestamp()), "numerator": 2, "denominator": 1}}}
        receipt["raw_response_sha256"] = digest(receipt["raw_response"])
        with self.assertRaisesRegex(ValueError, "cannot be silently removed"):
            build_archive(spec, sources, market, policies)

    def test_rehashed_session_times_cannot_expose_a_close_before_the_market_opens(self):
        changes = ({"open_at": "2026-10-22T12:30:00Z"}, {"close_at": "2026-10-22T14:00:00Z"},
                   {"open_at": "2026-10-22T12:30:00Z", "close_at": "2026-10-22T13:00:00Z"})
        for changed in changes:
            with self.subTest(changed=changed):
                spec, sources, market, policies = inputs()
                market["bars"][0].update(changed)
                market["receipts"][0]["bars_sha256"] = digest(market["bars"])
                with self.assertRaisesRegex(ValueError, "session calendar"):
                    build_archive(spec, sources, market, policies)

    def test_session_calendar_honors_dst_and_early_closes(self):
        sessions = [("2025-10-31", "13:30:00", "20:00:00"),
                    ("2025-11-03", "14:30:00", "21:00:00"),
                    ("2025-11-28", "14:30:00", "18:00:00")]
        for day, opened, closed in sessions:
            with self.subTest(session=day):
                market = inputs()[2]
                bar = {**market["bars"][0], "open_at": day + "T" + opened + "Z",
                       "close_at": day + "T" + closed + "Z"}
                market["bars"] = [bar]
                receipt = market["receipts"][0]
                original = receipt["raw_response"]["chart"]["result"][0]
                original["timestamp"] = [int(timestamp(bar["open_at"]).timestamp())]
                original["indicators"]["quote"] = [{"open": [bar["open"]], "close": [bar["close"]]}]
                receipt["bars_sha256"] = digest(market["bars"])
                receipt["raw_response_sha256"] = digest(receipt["raw_response"])
                self.assertEqual(validate_market(market), market)
                # The same raw daily prices do not authorize a fixed UTC close
                # copied across the DST boundary or onto the half-day session.
                bar["close_at"] = day + ("T21:00:00Z" if closed != "21:00:00" else "T20:00:00Z")
                receipt["bars_sha256"] = digest(market["bars"])
                with self.assertRaisesRegex(ValueError, "session calendar"):
                    validate_market(market)

    def test_missing_or_changed_calendar_identity_is_rejected(self):
        for key, value in (("calendar", "24/7"), ("calendar_version", "unknown"), ("calendar_version", None)):
            market = inputs()[2]
            market["receipts"][0][key] = value
            with self.subTest(field=key, value=value), self.assertRaisesRegex(ValueError, "calendar"):
                validate_market(market)

    def test_missing_calendar_dependency_fails_closed_for_archive_admission(self):
        _session_calendar.cache_clear()
        self.addCleanup(_session_calendar.cache_clear)
        with patch.dict("sys.modules", {"exchange_calendars": None}):
            with self.assertRaisesRegex(ValueError, "pinned archive extra"):
                validate_market(inputs()[2])


if __name__ == "__main__":
    unittest.main()
