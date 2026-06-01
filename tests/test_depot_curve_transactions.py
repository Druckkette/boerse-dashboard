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


def test_suggest_yahoo_ticker_uses_local_german_isin_mapping():
    assert app._suggest_yahoo_ticker("DE0007030009", "Rheinmetall", "STOCK") == "RHM-DE"


def test_suggest_yahoo_ticker_uses_local_non_german_isin_mapping():
    assert app._suggest_yahoo_ticker("US20717M1036", "Confluent", "STOCK") == "CFLT"
    assert app._suggest_yahoo_ticker("US4878361082", "Kellanova", "STOCK") == "K"
    assert app._suggest_yahoo_ticker("KYG7397A1067", "Razer", "STOCK") == "1337-HK"
    assert app._suggest_yahoo_ticker("US8740391003", "TSMC (ADR)", "STOCK") == "TSM"


def test_suggest_yahoo_ticker_rejects_non_us_listing_for_us_isin(monkeypatch):
    def fake_search(query):
        if query == "US0000000041":
            return [{"symbol": "TEST.MX", "name": "Test Corp", "exchange": "MEX", "type": "EQUITY"}]
        return [{"symbol": "TST", "name": "Test Corp", "exchange": "NYQ", "type": "EQUITY"}]

    monkeypatch.setattr(app, "search_symbol_candidates", fake_search)

    assert app._suggest_yahoo_ticker("US0000000041", "Test Corp", "STOCK") == "TST"


def test_suggest_yahoo_ticker_prefers_german_fallback_and_ignores_raw_isin(monkeypatch):
    def fake_search(query):
        if query == "DE000TEST001":
            return [{"symbol": "DE000TEST001", "name": "", "exchange": "", "type": "MANUAL"}]
        return [
            {"symbol": "TEST", "name": "Test AG ADR", "exchange": "NYQ", "type": "EQUITY"},
            {"symbol": "TST.DE", "name": "Test AG", "exchange": "GER", "type": "EQUITY"},
        ]

    monkeypatch.setattr(app, "search_symbol_candidates", fake_search)

    assert app._suggest_yahoo_ticker("DE000TEST001", "Test AG", "STOCK") == "TST-DE"


def test_symbol_variants_try_yahoo_dotted_exchange_suffixes():
    assert "RHM.DE" in app._symbol_variants("RHM-DE")
    assert "AGI.TO" in app._symbol_variants("AGI-TO")
    assert "1211.HK" in app._symbol_variants("1211-HK")


def test_symbol_variants_do_not_add_fx_or_index_search_candidates(monkeypatch):
    monkeypatch.setattr(
        app,
        "_search_yahoo_symbol_candidates",
        lambda symbol: {"candidates": ["KRW=X", "^GSPC", "K"]},
    )

    variants = app._symbol_variants("K")

    assert "KRW=X" not in variants
    assert "^GSPC" not in variants


def test_transaction_curve_converts_yahoo_usd_close_to_eur(monkeypatch):
    dates = pd.DatetimeIndex(pd.date_range("2026-01-02", "2026-01-05", freq="B"))

    def fake_fetch(symbol, *args, **kwargs):
        if symbol == "^GSPC":
            return pd.Series([100.0, 100.0], index=dates)
        if symbol == "EURUSD=X":
            return pd.Series([2.0, 2.0], index=dates)
        return pd.Series(dtype=float)

    monkeypatch.setattr(app, "_fetch_close_history", fake_fetch)
    monkeypatch.setattr(app, "_ticker_market_currency", lambda ticker: "USD")
    monkeypatch.setattr(
        app,
        "_bulk_close_history_map",
        lambda *args, **kwargs: {"TEST": pd.Series([10.0, 12.0], index=dates)},
    )

    tx_df = pd.DataFrame(
        [
            {
                "date": "2026-01-02",
                "event_ts": "2026-01-02T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "symbol": "US0000000002",
                "shares": 10,
                "shares_num": 10,
                "price": 5.0,
                "price_num": 5.0,
                "amount_num": -50.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
        ]
    )

    curve = app._build_curve_from_transactions(
        tx_df,
        isin_to_ticker={"US0000000002": "TEST"},
        start_date="2026-01-02",
        end_date="2026-01-05",
        cash_series=pd.Series([0.0, 0.0], index=dates),
    )

    assert curve["positions_value"].tolist() == [50.0, 60.0]


def test_transaction_curve_uses_trade_price_fallback_when_yahoo_history_missing(monkeypatch):
    dates = pd.DatetimeIndex(pd.date_range("2026-02-02", "2026-02-03", freq="B"))

    monkeypatch.setattr(
        app,
        "_fetch_close_history",
        lambda symbol, *args, **kwargs: pd.Series([100.0, 100.0], index=dates) if symbol == "^GSPC" else pd.Series(dtype=float),
    )
    monkeypatch.setattr(app, "_bulk_close_history_map", lambda *args, **kwargs: {})

    tx_df = pd.DataFrame(
        [
            {
                "date": "2026-02-02",
                "event_ts": "2026-02-02T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "symbol": "US0000000003",
                "shares": 10,
                "shares_num": 10,
                "price": 12.0,
                "price_num": 12.0,
                "amount_num": -120.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
        ]
    )

    curve = app._build_curve_from_transactions(
        tx_df,
        isin_to_ticker={"US0000000003": "MISSING"},
        start_date="2026-02-02",
        end_date="2026-02-03",
        cash_series=pd.Series([0.0, 0.0], index=dates),
    )

    assert curve["positions_value"].tolist() == [120.0, 120.0]
    assert curve.attrs["price_fallback_isins"] == ["US0000000003"]
    assert curve.attrs["open_price_fallback_isins"] == ["US0000000003"]
    assert curve.attrs["unresolved_isins"] == []


def test_transaction_curve_values_derivatives_from_trade_prices_without_ticker(monkeypatch):
    dates = pd.DatetimeIndex(pd.date_range("2026-03-02", "2026-03-03", freq="B"))

    monkeypatch.setattr(
        app,
        "_fetch_close_history",
        lambda symbol, *args, **kwargs: pd.Series([100.0, 100.0], index=dates) if symbol == "^GSPC" else pd.Series(dtype=float),
    )
    monkeypatch.setattr(app, "_bulk_close_history_map", lambda *args, **kwargs: {})

    tx_df = pd.DataFrame(
        [
            {
                "date": "2026-03-02",
                "event_ts": "2026-03-02T10:00:00Z",
                "type": "BUY",
                "asset_class": "DERIVATIVE",
                "symbol": "DE000DERIV01",
                "shares": 5,
                "shares_num": 5,
                "price": 20.0,
                "price_num": 20.0,
                "amount_num": -100.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
        ]
    )

    curve = app._build_curve_from_transactions(
        tx_df,
        isin_to_ticker={},
        start_date="2026-03-02",
        end_date="2026-03-03",
        cash_series=pd.Series([0.0, 0.0], index=dates),
    )

    assert curve["positions_value"].tolist() == [100.0, 100.0]
    assert curve.attrs["price_fallback_isins"] == ["DE000DERIV01"]
    assert curve.attrs["open_price_fallback_isins"] == ["DE000DERIV01"]
    assert curve.attrs["unresolved_isins"] == []


def test_transaction_curve_reports_only_open_price_fallback_isins(monkeypatch):
    dates = pd.DatetimeIndex(pd.date_range("2026-03-02", "2026-03-04", freq="B"))

    monkeypatch.setattr(
        app,
        "_fetch_close_history",
        lambda symbol, *args, **kwargs: pd.Series([100.0, 100.0, 100.0], index=dates) if symbol == "^GSPC" else pd.Series(dtype=float),
    )
    monkeypatch.setattr(app, "_bulk_close_history_map", lambda *args, **kwargs: {})

    tx_df = pd.DataFrame(
        [
            {
                "date": "2026-03-02",
                "event_ts": "2026-03-02T10:00:00Z",
                "type": "BUY",
                "asset_class": "DERIVATIVE",
                "symbol": "DE000CLOSED1",
                "shares": 5,
                "shares_num": 5,
                "price": 10.0,
                "price_num": 10.0,
                "amount_num": -50.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
            {
                "date": "2026-03-03",
                "event_ts": "2026-03-03T10:00:00Z",
                "type": "SELL",
                "asset_class": "DERIVATIVE",
                "symbol": "DE000CLOSED1",
                "shares": -5,
                "shares_num": -5,
                "price": 11.0,
                "price_num": 11.0,
                "amount_num": 55.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
            {
                "date": "2026-03-03",
                "event_ts": "2026-03-03T11:00:00Z",
                "type": "BUY",
                "asset_class": "DERIVATIVE",
                "symbol": "DE000OPEN01",
                "shares": 3,
                "shares_num": 3,
                "price": 20.0,
                "price_num": 20.0,
                "amount_num": -60.0,
                "fee_num": 0.0,
                "tax_num": 0.0,
            },
        ]
    )

    curve = app._build_curve_from_transactions(
        tx_df,
        isin_to_ticker={},
        start_date="2026-03-02",
        end_date="2026-03-04",
        cash_series=pd.Series([0.0, 0.0, 0.0], index=dates),
    )

    assert curve.attrs["price_fallback_isins"] == ["DE000CLOSED1", "DE000OPEN01"]
    assert curve.attrs["open_price_fallback_isins"] == ["DE000OPEN01"]


def test_depot_curve_csv_import_merges_only_new_and_changed_transactions():
    existing = pd.DataFrame(
        [
            {
                "date": "2026-04-22",
                "datetime": "2026-04-22T10:00:00Z",
                "event_ts": "2026-04-22T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "name": "Old Holding",
                "symbol": "US0000000101",
                "shares": 1,
                "shares_num": 1,
                "price": 10.0,
                "price_num": 10.0,
                "amount": -10.0,
                "amount_num": -10.0,
                "fee": 0.0,
                "fee_num": 0.0,
                "tax": 0.0,
                "tax_num": 0.0,
                "transaction_id": "tx-old-kept",
            },
            {
                "date": "2026-04-23",
                "datetime": "2026-04-23T10:00:00Z",
                "event_ts": "2026-04-23T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "name": "Changed Holding",
                "symbol": "US0000000102",
                "shares": 2,
                "shares_num": 2,
                "price": 20.0,
                "price_num": 20.0,
                "amount": -40.0,
                "amount_num": -40.0,
                "fee": 0.0,
                "fee_num": 0.0,
                "tax": 0.0,
                "tax_num": 0.0,
                "transaction_id": "tx-updated",
            },
        ]
    )
    uploaded = pd.DataFrame(
        [
            {
                "date": "2026-04-23",
                "datetime": "2026-04-23T10:00:00Z",
                "event_ts": "2026-04-23T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "name": "Changed Holding",
                "symbol": "US0000000102",
                "shares": 2,
                "shares_num": 2,
                "price": 21.0,
                "price_num": 21.0,
                "amount": -42.0,
                "amount_num": -42.0,
                "fee": 0.0,
                "fee_num": 0.0,
                "tax": 0.0,
                "tax_num": 0.0,
                "transaction_id": "tx-updated",
            },
            {
                "date": "2026-04-24",
                "datetime": "2026-04-24T10:00:00Z",
                "event_ts": "2026-04-24T10:00:00Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "name": "New Holding",
                "symbol": "US0000000103",
                "shares": 3,
                "shares_num": 3,
                "price": 30.0,
                "price_num": 30.0,
                "amount": -90.0,
                "amount_num": -90.0,
                "fee": 0.0,
                "fee_num": 0.0,
                "tax": 0.0,
                "tax_num": 0.0,
                "transaction_id": "tx-new",
            },
        ]
    )

    merged, summary = app._merge_depot_curve_csv_import(existing, uploaded)

    assert summary == {
        "added": 1,
        "updated": 1,
        "unchanged": 0,
        "kept_missing": 1,
        "total_before": 2,
        "total_uploaded": 2,
        "total_after": 3,
    }
    by_id = merged.set_index("transaction_id")
    assert by_id.loc["tx-old-kept", "price_num"] == 10.0
    assert by_id.loc["tx-updated", "price_num"] == 21.0
    assert by_id.loc["tx-new", "price_num"] == 30.0


def test_depot_curve_csv_import_state_roundtrips_records_and_overrides():
    tx_df = pd.DataFrame(
        [
            {
                "date": "2026-05-04",
                "datetime": "2026-05-04T09:30:00.123456Z",
                "event_ts": "2026-05-04T09:30:00.123456Z",
                "type": "BUY",
                "asset_class": "STOCK",
                "name": "Persisted Holding",
                "symbol": "US0000000201",
                "shares": 4,
                "shares_num": 4,
                "price": 25.0,
                "price_num": 25.0,
                "amount": -100.0,
                "amount_num": -100.0,
                "fee": 0.0,
                "fee_num": 0.0,
                "tax": 0.0,
                "tax_num": 0.0,
                "transaction_id": "tx-persisted",
            }
        ]
    )

    state = app._build_depot_curve_csv_import_state(
        tx_df,
        filename="Transaktionsexport.csv",
        summary={"added": 1, "total_uploaded": 1, "total_after": 1},
        isin_overrides={"US0000000201": " test "},
    )
    normalized = app._normalize_depot_curve_csv_import_state(state)
    restored = app._depot_curve_csv_records_to_frame(normalized["records"])

    assert normalized["filename"] == "Transaktionsexport.csv"
    assert normalized["row_count"] == 1
    assert normalized["last_import_summary"]["added"] == 1
    assert normalized["isin_overrides"] == {"US0000000201": "TEST"}
    assert restored.iloc[0]["transaction_id"] == "tx-persisted"
    assert restored.iloc[0]["price_num"] == 25.0
    _, summary = app._merge_depot_curve_csv_import(restored, tx_df)
    assert summary["updated"] == 0
    assert summary["unchanged"] == 1
