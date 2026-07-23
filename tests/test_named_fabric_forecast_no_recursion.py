import unittest
from unittest.mock import patch

from jobs.feishu import generate_fabric_forecast_named_colors as named


class NamedFabricForecastRecursionTests(unittest.TestCase):
    def test_main_uses_captured_original_main(self):
        sentinel = [{"ok": True}]

        with patch.object(named, "_ORIGINAL_MAIN", return_value=sentinel) as original_main:
            with patch.object(
                named.base,
                "main",
                side_effect=AssertionError("patched module main must not be called"),
            ):
                result = named.main(resolver="resolver")

        self.assertEqual(result, sentinel)
        original_main.assert_called_once_with("resolver")


if __name__ == "__main__":
    unittest.main()
