"""Reusable Streamlit dataframe column configurations."""

from __future__ import annotations

import streamlit as st


def performance_column_config() -> dict:
    return {
        "Ticker": st.column_config.TextColumn("Ticker", width="small"),
        "P&L %": st.column_config.NumberColumn("P&L %", format="%.2f%%"),
        "Risikobeitrag": st.column_config.ProgressColumn("Risikobeitrag", format="%.2f", min_value=0.0, max_value=1.0),
    }


def flow_column_config() -> dict:
    return {
        "Datum": st.column_config.DateColumn("Datum", format="DD.MM.YYYY"),
        "Typ": st.column_config.TextColumn("Typ", width="small"),
        "Betrag": st.column_config.NumberColumn("Betrag", format="%.2f €"),
        "Notiz": st.column_config.TextColumn("Notiz", width="large"),
    }


def rating_overview_column_config() -> dict:
    return {
        "Ticker": st.column_config.TextColumn("Ticker", width="small"),
        "RS-Rating": st.column_config.NumberColumn("RS-Rating", format="%d"),
        "Momentum": st.column_config.NumberColumn("Momentum", format="%.1f"),
        "Volatilität": st.column_config.NumberColumn("Volatilität", format="%.2f"),
    }
