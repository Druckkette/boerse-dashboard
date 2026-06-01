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
