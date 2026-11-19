from collections import Counter
import json
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "100")]
framework = types.ModuleType("brain_framework")
framework.get_service_config = lambda: {}
sys.modules["brain_framework"] = framework
history = types.ModuleType("news_history")
history.history_for = lambda *args, **kwargs: None
history.invalidate = lambda *args, **kwargs: None
sys.modules["news_history"] = history
import model as gdelt


class GdeltPayloadProjectionTests(unittest.TestCase):
    def test_query_uses_type_specific_projection_instead_of_full_payload(self):
        sql = gdelt.RAW_PAYLOAD_PROJECTION_SQL
        self.assertIn("JSON_OBJECT", sql)
        for field in (
            "EventRootCode", "GoldsteinScale", "QuadClass", "AvgTone",
            "Confidence", "MentionDocTone", "V2Tone", "Themes", "V2Themes",
        ):
            self.assertIn(field, sql)
        self.assertNotIn("full_text", sql.lower())
        self.assertNotIn("source_url", sql.lower())

    def test_projection_preserves_news_enricher_features_for_all_streams(self):
        fixtures = [
            ("event", {
                "EventRootCode": "14", "GoldsteinScale": -7.5,
                "QuadClass": "4", "AvgTone": "-2.5", "unread_blob": "x" * 10000,
            }),
            ("mention", {
                "Confidence": "75", "MentionDocTone": "3.2",
                "unread_blob": "x" * 10000,
            }),
            ("article_metadata", {
                "V2Tone": "2.0,1.5,3.0,2.5", "Themes": "ECON_INFLATION;WB_442",
                "V2Themes": "ECON_INFLATION,1;WB_442,1", "unread_blob": "x" * 10000,
            }),
        ]
        fields = {
            "event": ("EventRootCode", "GoldsteinScale", "QuadClass", "AvgTone"),
            "mention": ("Confidence", "MentionDocTone"),
            "article_metadata": ("V2Tone", "Themes", "V2Themes"),
        }
        for record_type, raw in fixtures:
            base = {
                "record_key": "stable-key", "provider": "gdelt",
                "record_type": record_type, "publication_id": "publication",
                "event_id": "event-id", "available_at": "2025-02-06T15:30:00Z",
                "loaded_at": "2025-02-06T15:31:00Z", "themes": [],
                "v2_themes": [], "v2_tone": None,
            }
            original = {**base, "raw_payload": json.dumps(raw)}
            projected = {**base, "raw_payload": json.dumps({k: raw[k] for k in fields[record_type]})}
            original_stats, projected_stats = Counter(), Counter()
            original_result = gdelt._observation(original, gdelt._settings(), original_stats)
            projected_result = gdelt._observation(projected, gdelt._settings(), projected_stats)
            self.assertEqual(projected_result, original_result, record_type)
            self.assertEqual(projected_stats, original_stats, record_type)


if __name__ == "__main__":
    unittest.main()
