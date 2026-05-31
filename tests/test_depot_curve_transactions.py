import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app


def test_transaction_curve_uses_intraday_event_order_for_same_day_roundtrip(monkeypatch):
    dates = pd.DatetimeIndex(pd.date_range("2026-05-13", "2026-05-14", freq="B"))

    monkeypatch.setattr(app, "_fetch_close_history", lambda *args, **kwargs: pd.Series([100.0, 100.0], index=dates))
    monkeypatch.setattr(
        app,
        "_bulk_close_history_map",
        lambda *args, **kwargs: {"TEST": pd.Series([10.0, 10.0], index=dates)},
    )

    tx_df = pd.DataFrame(
        [
            {
                "date": "2026-05-13",
                "datetime": "2026-05-13T11:00:00Z",
                "event_ts": "2026-05-13T11:00:00Z",
                "type": "SELL",
                "asset_class": "STOCK",
                "symbol": "US0000000001",
                "shares": -10,
                "shares_num": -10,
                "amount_num": 100.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
            {
                "date": "2026-05-13",
                "datetime": "2026-05-13T10:00:00Z",
                "event_ts": "2026-05-13T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "symbol": "US0000000001",
                "shares": 10,
                "shares_num": 10,
                "amount_num": -100.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
        ]
    )
    cash = pd.Series([100.0, 100.0], index=dates)

    curve = app._build_curve_from_transactions(
        tx_df,
        isin_to_ticker={"US0000000001": "TEST"},
        start_date="2026-05-13",
        end_date="2026-05-14",
        cash_series=cash,
    )

    assert not curve.empty
    assert curve["positions_value"].tolist() == [0.0, 0.0]
    assert curve["depot_value"].tolist() == [100.0, 100.0]
