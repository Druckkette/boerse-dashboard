import pandas as pd

from boerse_regeln_kap_2_2_bis_2_5 import count_distribution_days


def test_distribution_day_drops_after_six_percent_recovery():
    dates = pd.date_range("2026-01-01", periods=58, freq="B")
    closes = [100.0 + i * 0.05 for i in range(55)] + [100.0, 105.9, 106.0]
    volumes = [1_000_000.0] * 55 + [1_500_000.0, 900_000.0, 900_000.0]
    frame = pd.DataFrame(
        {
            "open": closes,
            "high": [c + 1.0 for c in closes],
            "low": [c - 1.0 for c in closes],
            "close": closes,
            "volume": volumes,
        },
        index=dates,
    )

    counts = count_distribution_days(frame)

    assert counts.iloc[-3] == 1
    assert counts.iloc[-2] == 1
    assert counts.iloc[-1] == 0


def test_distribution_day_still_expires_after_rolling_window():
    dates = pd.date_range("2026-01-01", periods=82, freq="B")
    closes = [100.0 + i * 0.05 for i in range(55)] + [100.0] + [101.0] * 26
    volumes = [1_000_000.0] * 55 + [1_500_000.0] + [900_000.0] * 26
    frame = pd.DataFrame(
        {
            "open": closes,
            "high": [c + 1.0 for c in closes],
            "low": [c - 1.0 for c in closes],
            "close": closes,
            "volume": volumes,
        },
        index=dates,
    )

    counts = count_distribution_days(frame)

    assert counts.iloc[55] == 1
    assert counts.iloc[79] == 1
    assert counts.iloc[80] == 0
