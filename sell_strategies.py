"""Modulare Verkaufsstrategien nach *Börse ohne Bauchgefühl*.

Jede Strategie liefert eine Liste von Signal-Dicts.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import pandas as pd


@dataclass
class Position:
    ticker: str
    einstiegspreis: float
    einstiegsdatum: Any
    stueckzahl: float
    pivot: float | None = None
    tief_tag_1: float | None = None
    tief_tag_0: float | None = None
    peak: float | None = None
    realisierte_tranchen: list[float] | None = None


def _signal(name, tranche_pct, trigger_typ, aktiv, marke, ref, grund):
    return {
        "name": name,
        "tranche_pct": int(tranche_pct),
        "trigger_typ": trigger_typ,
        "aktuell_aktiv": bool(aktiv),
        "naechste_marke": marke,
        "buch_verweis": ref,
        "begruendung": grund,
    }


def sma(series: pd.Series, periode: int) -> pd.Series:
    return series.rolling(periode, min_periods=periode).mean()


def ema(series: pd.Series, periode: int) -> pd.Series:
    return series.ewm(span=periode, adjust=False).mean()


def letzter_schlusskurs(daten: pd.DataFrame) -> float:
    return float(daten["close"].iloc[-1])


def pnl_pct(position: Position, daten: pd.DataFrame) -> float:
    return (letzter_schlusskurs(daten) / position.einstiegspreis - 1) * 100


def drawdown_vom_peak(position: Position, daten: pd.DataFrame) -> float:
    peak = float(position.peak or daten["high"].max())
    return max(0.0, (peak - letzter_schlusskurs(daten)) / peak * 100)


def tage_unter_ma(daten: pd.DataFrame, ma: pd.Series) -> int:
    cnt = 0
    for c, m in zip(daten["close"].iloc[::-1], ma.iloc[::-1]):
        if pd.isna(m) or c >= m:
            break
        cnt += 1
    return cnt


def vol_verhaeltnis(daten: pd.DataFrame) -> float:
    if len(daten) < 50:
        return 1.0
    return float(daten["volume"].iloc[-1] / daten["volume"].tail(50).mean())


def atr(daten: pd.DataFrame, periode: int = 14) -> float:
    h, l, c = daten["high"], daten["low"], daten["close"]
    prev = c.shift(1)
    tr = pd.concat([(h - l), (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    atr_abs = tr.rolling(periode, min_periods=periode).mean().iloc[-1]
    return float(atr_abs / c.iloc[-1] * 100) if c.iloc[-1] else 0.0


def strategie_notbremse_verlust(position: Position, daten: pd.DataFrame, markt: str):
    pnl = pnl_pct(position, daten)
    schwelle = -4 if markt == "Bärisch" else -5 if markt == "Unsicher" else -7
    if pnl <= schwelle:
        return [_signal("Notbremse nach Verlusthöhe", 100, "intraday", True, None, "Kap. 6.1", f"Verlustgrenze {schwelle}% erreicht")]
    return [_signal("Notbremse-Marke", 0, "info", False, position.einstiegspreis * (1 + schwelle / 100), "Kap. 6.1", "Info-Marke")]


def strategie_drei_stufen_nach_kauf(position: Position, daten: pd.DataFrame):
    s, pnl = letzter_schlusskurs(daten), pnl_pct(position, daten)
    out = []
    if position.tief_tag_1 and s < position.tief_tag_1:
        out.append(_signal("Schluss unter Tief Ausbruchstag", 33, "schluss", True, position.tief_tag_0, "Kap. 5.3 / 6.4", "Ausbruch hält nicht"))
    if position.tief_tag_0 and s < position.tief_tag_0:
        out.append(_signal("Schluss unter Tief Vortag", 33, "schluss", True, position.einstiegspreis * 0.93, "Kap. 5.3 / 6.4", "Ausbruch gescheitert"))
    if pnl <= -7:
        out.append(_signal("7%-Notbremse", 100, "intraday", True, None, "Kap. 5.3 / 6.1 / 6.4", "Maximalverlust"))
    return out


def strategie_gewinn_in_stufen(position: Position, daten: pd.DataFrame, markt: str):
    pnl = pnl_pct(position, daten)
    already = sum(position.realisierte_tranchen or [])
    nd, low, high = (10, 10, 15) if markt == "Bärisch" else (15, 20, 35)
    out = []
    if pnl >= nd and already == 0:
        out.append(_signal("Gewinn-Nachdenkschwelle erreicht", 0, "info", True, position.einstiegspreis * (1 + low / 100), "Kap. 6.2", "Teilverkauf planen"))
    if pnl >= low and already < 50:
        out.append(_signal("Pflicht-Teilverkauf Gewinnzone", 33 if pnl < high else 50, "schluss", True, position.einstiegspreis, "Kap. 6.2", "Gewinnzone erreicht"))
    return out


def strategie_21ma_bruch(position: Position, daten: pd.DataFrame, variante: str = "gestaffelt"):
    if pnl_pct(position, daten) <= 0:
        return []
    ma21 = sma(daten["close"], 21)
    m, s, t = float(ma21.iloc[-1]), letzter_schlusskurs(daten), tage_unter_ma(daten, ma21)
    out = []
    if variante == "gestaffelt":
        if t == 1: out.append(_signal("Erster Schluss unter 21-MA", 25, "schluss", True, m, "Kap. 6.2", "Stufe 1"))
        if t == 2 and s < float(daten["close"].iloc[-2]): out.append(_signal("Zweiter Tag unter 21-MA (tiefer)", 25, "schluss", True, m, "Kap. 6.2", "Stufe 2"))
        if t >= 3: out.append(_signal("Dritter Tag unter 21-MA", 25, "schluss", True, position.einstiegspreis, "Kap. 6.2", "Stufe 3"))
    elif variante == "aggressiv":
        bruch = (m - s) / m * 100 if m else 0
        if s < m and bruch >= 2 and vol_verhaeltnis(daten) >= 1.2: out.append(_signal("Deutlicher 21-MA-Bruch mit Volumen", 33, "schluss", True, float(sma(daten['close'],50).iloc[-1]), "Kap. 6.2", "Klarer Bruch"))
    else:
        if t >= 3: out.append(_signal("21-MA seit 3 Tagen gebrochen", 33, "schluss", True, float(daten["low"].tail(10).min()), "Kap. 6.2", "Bruch bestätigt"))
    return out


def strategie_drawdown_vom_peak(position: Position, daten: pd.DataFrame):
    if pnl_pct(position, daten) <= 0: return []
    dd, s = drawdown_vom_peak(position, daten), letzter_schlusskurs(daten)
    ma21 = float(sma(daten["close"], 21).iloc[-1])
    if 8 <= dd < 12: return [_signal("Drawdown 8% vom Peak", 25, "schluss", True, (position.peak or 0) * 0.88, "Kap. 6.2", "erste Sicherung")]
    if 12 <= dd < 15: return [_signal("Drawdown 12-15% vom Peak", 33, "schluss", True, (position.peak or 0) * 0.85, "Kap. 6.2", "deutliche Reduktion")]
    if dd >= 15 and s < ma21: return [_signal("Drawdown >15% + Trendbruch", 100, "schluss", True, None, "Kap. 6.2", "Komplettausstieg")]
    if dd >= 15: return [_signal("Drawdown >15%", 50, "schluss", True, ma21, "Kap. 6.2", "reduzieren")]
    return []


def verkaufs_empfehlung_gesamt(position: Position, daten: pd.DataFrame, wochen_daten: pd.DataFrame, daten_spy: pd.DataFrame | None, wochen_daten_spy: pd.DataFrame | None, markt: str, industrie: str, aktive_strategien: list[str], strategie_optionen: dict | None = None):
    optionen = strategie_optionen or {}
    registry = {
        "notbremse_verlust": lambda: strategie_notbremse_verlust(position, daten, markt),
        "drei_stufen_nach_kauf": lambda: strategie_drei_stufen_nach_kauf(position, daten),
        "gewinn_in_stufen": lambda: strategie_gewinn_in_stufen(position, daten, markt),
        "ma21_bruch": lambda: strategie_21ma_bruch(position, daten, optionen.get("ma21_variante", "gestaffelt")),
        "drawdown_vom_peak": lambda: strategie_drawdown_vom_peak(position, daten),
    }
    all_signals = []
    for key in aktive_strategien:
        fn = registry.get(key)
        if fn: all_signals.extend(fn())
    killer = [s for s in all_signals if s["aktuell_aktiv"] and s["tranche_pct"] == 100]
    if killer:
        ges = 100
        grund = killer[0]["name"]
    else:
        active = [s for s in all_signals if s["aktuell_aktiv"] and s["tranche_pct"] > 0]
        summe = sum(s["tranche_pct"] for s in active)
        if len(active) >= 4 and summe < 75: summe = 75
        stufen = [0, 25, 33, 50, 66, 75, 100]
        ges = max(v for v in stufen if v <= min(summe, 100))
        if markt == "Bärisch" and 0 < ges < 100:
            ges = next((v for v in stufen if v > ges), 100)
        grund = ", ".join(s["name"] for s in active[:3]) if active else "keine aktiven Signale"
    schon = sum(position.realisierte_tranchen or [])
    return {"gesamt_tranche": ges, "bereits_realisiert": schon, "jetzt_zu_verkaufen": max(0, ges - schon), "haupt_grund": grund, "alle_signale": all_signals}
