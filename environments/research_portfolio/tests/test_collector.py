from copy import deepcopy
from datetime import datetime, timezone
import unittest

from environments.research_portfolio.collector import export_rows, market_bundle


def raw_row():
    raw = {"title": "Fed release", "content_html": "An <b>official</b> update &amp; context."}
    return {"id": 5, "raw": raw, "created_at": datetime(2026, 10, 22, 14, tzinfo=timezone.utc),
            "payload": {"source": "Federal Reserve releases", "source_id": "https://fed.example/release",
                        "timestamp": "2026-10-22T12:00:00Z", "raw": raw}}


class CollectorTests(unittest.TestCase):
    def test_export_preserves_real_receipt_and_full_raw_with_readable_projection(self):
        row = raw_row()
        result = export_rows([row], "2026-10-22T00:00:00Z", "2026-10-23T00:00:00Z",
                             collected_at="2026-10-24T00:00:00Z")
        source = result["records"][0]
        self.assertEqual(source["available_at"], row["created_at"].isoformat())
        self.assertEqual(source["raw"], row["raw"])
        self.assertEqual(source["body"], "An  official  update & context.")
        self.assertEqual(result["selection"]["received_row_count"], 1)

    def test_unknown_publication_is_rejected_instead_of_using_collection_date(self):
        row = raw_row()
        row["payload"]["timestamp"] = None
        result = export_rows([row], "2026-10-22T00:00:00Z", "2026-10-23T00:00:00Z")
        self.assertEqual(result["records"], [])
        self.assertEqual(result["rejected_records"][0]["raw_id"], 5)

    def test_export_does_not_accept_out_of_range_originals(self):
        with self.assertRaisesRegex(ValueError, "outside"):
            export_rows([raw_row()], "2026-10-23T00:00:00Z", "2026-10-24T00:00:00Z")

    def test_market_import_refuses_missing_sessions_and_dividend_payment_guessing(self):
        asset = {"asset_id": "issuer", "symbol": "ISS"}
        snapshot = {"symbol": "ISS", "missing_sessions": ["2026-10-22"]}
        with self.assertRaisesRegex(ValueError, "not imputed"):
            market_bundle([asset], [snapshot], calendar_for=lambda *args: None)
        snapshot.update(missing_sessions=[], corporate_actions=[{"type": "dividend", "session": "2026-10-22", "amount": 1}])
        with self.assertRaisesRegex(ValueError, "corporate-action"):
            market_bundle([asset], [snapshot], calendar_for=lambda *args: None)

    def test_market_symbol_mismatch_cannot_relabel_price_tape(self):
        with self.assertRaisesRegex(ValueError, "symbol mismatch"):
            market_bundle([{"asset_id": "issuer", "symbol": "ISS"}], [{"symbol": "OTHER"}],
                          calendar_for=lambda *args: None)


if __name__ == "__main__":
    unittest.main()
