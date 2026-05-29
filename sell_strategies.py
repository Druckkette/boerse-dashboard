from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import pandas as pd


# Themen-Klassifikation jeder Hub-Strategie. Wird in `verkaufs_empfehlung_gesamt`
# verwendet, um pro Thema nur das stärkste aktive Signal in die Tranche-Summe einzubringen.
# Damit wird verhindert, dass mehrere Strategien dieselbe Ursache mehrfach zählen
# (z. B. Drawdown + 21-EMA-Bruch + MA-Abstand bei demselben Pullback).
STRATEGY_THEMES: dict[str, str] = {
    "notbremse_verlust": "verlust_notbremse",
    "einfache_verluststufen": "verlust_notbremse",
    "rueckkehr_pivot": "pivot_fail",
    "misslungener_ausbruch_5stufen": "pivot_fail",
    "ma21_bruch": "trendbruch",
    "ma_bruch_defensiv": "trendbruch",
    "ma_basierte_sequenz": "trendbruch",
    "drawdown_vom_peak": "drawdown",
    "gewinn_in_stufen": "gewinnmitnahme",
    "einfach_halbe_position": "gewinnmitnahme",
    "atr_basiert": "gewinnmitnahme",
    "ma_abstand": "ueberdehnung",
    "groesster_anstieg_volumen": "klimax",
    "split_anstieg": "klimax",
    "erschoepfungsluecke": "klimax",
    "downside_reversal": "umkehr",
    "groesster_einbruch": "umkehr",
    "verlusttage_haeufung": "distribution",
    "stau_tage": "distribution",
    "drei_verlustwochen": "distribution",
    "rs_linie": "rs_schwaeche",
}


STRATEGIE_INFO: dict[str, str] = {
    "notbremse_verlust": "Strategie 2 (Kap. 6.1): Marktabhängige Notbremse nach Verlusthöhe, die immer parallel zu allen anderen Regeln aktiv ist. Sobald die positionsbezogene P&L die Schwelle erreicht oder unterschreitet, wird ein Intraday-Vollausstieg (100%) ausgelöst. Standard-Schwellen: Bärisch 4%, Unsicher 5%, Bullisch 7%. Zusätzlich wird unterhalb der Schwelle eine konkrete Notbremse-Marke als kritischer Kurs angezeigt.",
    "gewinn_in_stufen": "Strategie 3 (Kap. 6.2): Gewinnmitnahme in Stufen mit Nachdenkschwelle und Pflicht-Teilverkauf. Standard: Bullisch/Unsicher 15% Hinweis, dann 20–35% Teilverkauf (33% bis 50% Tranche). Bärisch: 10% Hinweis, dann 10–15% Teilverkauf. Alle Schwellen sind im Setup konfigurierbar.",
    "ma21_bruch": "Strategie 4 (Kap. 6.2): Bruch der 21-EMA in drei Risikoprofilen. Aggressiv: schneller Teilverkauf bei klarem Bruch + Volumenbestätigung (33%, +50% bei -7% Tagesverlust Tag 2). Gestaffelt: stufenweises Vorgehen über 3 Tage unter 21-EMA (jeweils 25%). Geduldig: erst nach 3 bestätigten Tagen unter 21-EMA aktiv (33%). Nur im Gewinnfall aktiv.",
    "drawdown_vom_peak": "Strategie 5 (Kap. 6.2): Drawdown vom Peak mit 3 Eskalationsstufen. Stufe 1 (Standard 8%): erstes Sicherungssignal 25%. Stufe 2 (12%): deutliche Reduktion 33%. Stufe 3 (15%): harte Reduktion (50%); bei zusätzlichem Trendbruch unter 21-EMA Vollausstieg (100%).",
    "ma_abstand": "Strategie 6 (Kap. 6.2): Teilverkäufe bei Überdehnung über gleitende Durchschnitte. Nur im Gewinnfall aktiv. Vier Stufen: 10-MA (Standard 10% → 25% Tranche), 21-EMA (15% → 33%), 50-MA (25% → 33%), 200-MA (70% → 50%). 200-MA Klimaxzone ab 100% löst Vollausstieg aus.",
    "verlusttage_haeufung": "Strategie 7 (Kap. 6.2): Erkennt Distribution über eine Häufung schwacher Tage. Signal 1: drei tiefere Schlusskurse in Folge mit erhöhter 3-Tage-Volumenquote gegen den Referenz-Lookback. Signal 2: im Up/Down-Fenster überwiegen Abwärtstage gegenüber Aufwärtstagen deutlich (Mindestdifferenz). Beide Trigger erzeugen standardmäßig eine 25%-Tranche mit Stop-/Marker-Logik auf lokale Tiefs.",
    "groesster_anstieg_volumen": "Strategie 9 (Kap. 6.2): Klimax-/Spätphasen-Signal bei größtem Tagesanstieg mit extremem Volumen. Aktiviert ab P&L > 15%. Bei höchstem Volumen 33%, sonst 20% Vorwarnung.",
    "split_anstieg": "Strategie 10 (Kap. 6.2): Warnt vor möglichem Gipfel, wenn die Aktie innerhalb der ersten 1-2 Wochen nach Aktiensplit stark steigt. Trigger nur, wenn ein Split-Datum bekannt ist. Ab +25% seit Split wird ein aktives Signal erzeugt (33% Tranche), ab +50% erhöht auf 50%. Referenz-/Stoppmarke ist der Schlusskurs am Split-Tag.",
    "erschoepfungsluecke": "Strategie 11 (Kap. 6.2): Gap-up nach langem Aufwärtstrend mit hohem Volumen und großer Distanz zur Basis. Identifiziert ein potenzielles Spätphasen-/Klimax-Muster. Standard-Tranche 33%.",
    "downside_reversal": "Strategie 12 (Kap. 6.2): Downside Reversal für Gewinnerpositionen. Variante 1 (stark): neues 30-Tage-Hoch, Schluss im unteren Tagesdrittel und Volumenquote ≥ 1.2 erzeugt 33%-Signal. Variante 2 (mittel): weite Umkehrkerze (Tagesspanne ≥ 1.5× 10-Tage-Schnitt), Schluss im unteren Drittel und Volumenquote ≥ 1.2 erzeugt 20%-Signal. Variante 3 (Warnstufe): weite Kerze mit Schluss unter Spannenmitte erzeugt 15%-Signal.",
    "stau_tage": "Strategie 13 (Kap. 6.2): Sucht in einem Fenster (Standard 10 Sessions) nach Stau-Tagen mit kaum Fortschritt (|Tagesveränderung| < 1%) bei überdurchschnittlichem Volumen (≥1.3× gegen 50-Tage-Schnitt). Ab mindestens 2 Stau-Tagen entsteht ein aktives Verkaufssignal. Die Tranche ist kontextabhängig: nahe Hoch (Drawdown < 5%) defensiver mit 33%, sonst 20%.",
    "rueckkehr_pivot": "Strategie 14 (Kap. 6.3): Rückkehr zum Ausbruchspunkt. Sicherheitslinie 1 ist ein Schlusskurs unter Tief Tag 1 (33%; bei Volumenquote ≥1.5 auf 50% erhöht). Sicherheitslinie 2 ist ein Schlusskurs unter Tief Tag 0 (weitere 33%). Bleibt die Aktie 10 Handelstage in Folge unter dem Pivot, folgt ein 50%-Signal wegen ausbleibender Rückeroberung des Ausbruchspunkt. Alternativ Notbremse bei max. Verlust (Standard -7%) als Intraday-Vollausstieg.",
    "ma_bruch_defensiv": "Strategie 15 (Kap. 6.3): Defensiver Exit-Prozess bei Trendbruch. Klarer 50-MA-Bruch (mind. max(2%, ATR%) unter MA und Volumenanstieg ≥1.3) triggert 50%, sonst nach 3 Schlusskursen unter 50-MA 33%. Nach 8 Wochen unter der 10-Wochen-Linie folgt ein Vollsignal (100%). Unter 200-MA werden 75% reduziert bzw. 100% bei hohem Volumen; dreht die 200-MA zusätzlich nach unten, wird ein bestätigendes Info-Signal ausgegeben.",
    "drei_verlustwochen": "Strategie 16 (Kap. 6.3): Triggert bei drei Verlustwochen in Folge mit jeweils tieferem Wochenschluss als in der Vorwoche und gleichzeitig steigendem Wochenvolumen. Vollsignal (100%) nur, wenn alle drei Wochen klare Abwärtswochen sind (Close < Open). Vorwarnstufe (33%) falls nur die Sequenz aus fallenden Wochenschlüssen + steigendem Volumen erfüllt ist.",
    "groesster_einbruch": "Strategie 17 (Kap. 6.3): Reagiert auf den größten Einbruch seit Einstieg nach bereits gelaufener Position. Tagesregel: wenn der heutige Verlust der größte seit Einstieg ist und über einer Mindestschwelle liegt, wird defensiv reduziert (33%) oder bei deutlich erhöhtem Volumen stärker (50%). Wochenregel: wenn die aktuelle Verlustwoche die größte seit Einstieg ist und gleichzeitig das Wochenvolumen überdurchschnittlich hoch ist, folgt eine starke Reduktion (66%).",
    "rs_linie": "Strategie 18 (Kap. 6.4): 3-Stufen-Strategie auf Basis relativer Stärke gegen Benchmark (z. B. SPY) mit adaptivem Zeithorizont. Unter Schwelle 1 Tagesebene (21/50-MA), zwischen Schwelle 1 und 2 Wochenebene (10/25-MA), ab Schwelle 2 Monatsebene (12/24-MA). Stufe 1 (20%): RS bricht schnellen MA. Stufe 2 (30%): RS bleibt 3 Perioden darunter. Stufe 3 (50%): RS bricht langsamen MA.",
    "ma_basierte_sequenz": "Strategie 19 (Kap. 6.4): Geschlossene MA-Verkaufssequenz von Gewinnzone bis klarem 50-MA-Bruch. Punkt 1: Teilverkauf in Gewinnzone 20-25% (33%). Punkt 2: Überdehnung über 10-MA (20%). Punkt 3: Bruch 10-MA (20% normal / 25% pendelnd). Punkt 4: Bruch 21-EMA (25%). Punkt 5: klarer 50-MA-Bruch = Vollausstieg (100%).",
    "einfach_halbe_position": "Strategie 20 (Kap. 6.4): Einfache 50/50-Logik. Erste Hälfte sichern bei festgelegtem Gewinn (Standard 20%), zweite Hälfte über Break-Even/erneute Stärke managen. Erneut 20% Gewinn nach erster Hälfte → weitere Tranche.",
    "misslungener_ausbruch_5stufen": "Strategie 21 (Kap. 6.4): Fehl-Ausbruch detailliert in 5 Stufen inkl. Intraday-/Gap-Logik. Behandelt Gap-down-Sonderfälle, Intraday-/Schlussbrüche unter Tief Tag 1 und Tag 0, sowie 7%-Notbremse als Intraday-Exit. Zusätzlich: zweite Rückkehr zum Pivot nach Erholung.",
    "einfache_verluststufen": "Strategie 22 (Kap. 6.4): Minimalregel mit gestaffelten Verluststufen. Standard: -3% (33%), -5% (33%), -7% (Komplettverkauf intraday).",
    "atr_basiert": "Strategie 23 (Kap. 6.4): Adaptive Verkaufsregel auf Basis der typischen Schwankungsbreite (ATR) der Aktie. Eignet sich besonders für volatile Aktien. Regeln: Teilverkauf (33%) ab ATR-Ziel-Gewinn, Vollausstieg bei Schluss ≤ Einstieg minus 1.5 ATR, sowie Überdehnungs-Teilverkauf über 21-EMA ab x ATR (Basis) bzw. y ATR (stark, 50%).",
}


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
    return {"name": name, "tranche_pct": int(tranche_pct), "trigger_typ": trigger_typ, "aktuell_aktiv": bool(aktiv), "naechste_marke": marke, "buch_verweis": ref, "begruendung": grund}


def sma(series: pd.Series, periode: int) -> pd.Series: return series.rolling(periode, min_periods=periode).mean()
def ema(series: pd.Series, periode: int) -> pd.Series: return series.ewm(span=periode, adjust=False).mean()
def letzter_schlusskurs(daten: pd.DataFrame) -> float: return float(daten["close"].iloc[-1])
def tagestief(daten: pd.DataFrame) -> float: return float(daten["low"].iloc[-1])
def pnl_pct(position: Position, daten: pd.DataFrame) -> float: return (letzter_schlusskurs(daten)/position.einstiegspreis - 1) * 100
def drawdown_vom_peak(position: Position, daten: pd.DataFrame) -> float:
    if position.peak:
        peak = float(position.peak)
    else:
        # Fallback: Peak seit Einstieg, nicht das All-Time-High der gesamten Historie.
        daten_seit = _seit_einstieg(daten, position) if position.einstiegsdatum else daten
        basis = daten_seit if daten_seit is not None and len(daten_seit) > 0 else daten
        peak = float(basis["high"].max())
    return max(0.0, (peak - letzter_schlusskurs(daten)) / peak * 100) if peak else 0.0

def vol_verhaeltnis(daten: pd.DataFrame) -> float:
    if len(daten) < 50: return 1.0
    return float(daten["volume"].iloc[-1] / daten["volume"].tail(50).mean())

def tage_unter_ma(daten: pd.DataFrame, ma: pd.Series) -> int:
    c = 0
    for x, m in zip(daten["close"].iloc[::-1], ma.iloc[::-1]):
        if pd.isna(m) or x >= m: break
        c += 1
    return c

def atr(daten: pd.DataFrame, periode: int = 14) -> float:
    h,l,c = daten["high"], daten["low"], daten["close"]
    tr = pd.concat([(h-l), (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(periode, min_periods=periode).mean().iloc[-1]
    cl = c.iloc[-1]
    if a is None or pd.isna(a) or cl is None or pd.isna(cl) or cl == 0:
        return 0.0
    return float(a / cl * 100)

def distribution_tage(daten: pd.DataFrame, n: int = 25) -> int:
    d = daten.tail(n).copy(); v50 = daten["volume"].rolling(50, min_periods=10).mean()
    return int(((d["close"].diff() < 0) & (d["volume"] >= 1.2*v50.loc[d.index])).sum())

def up_down_volume_ratio(daten: pd.DataFrame, n: int = 50):
    """Liefert das Verhältnis Up-Volumen / Down-Volumen über die letzten ``n`` Tage.

    Konvention (konsistent mit ``sell_decision_metrics``): liefert ``None``, wenn
    keine Down-Tage existieren oder zu wenig Daten vorliegen — so können Aufrufer
    den Wert klar als „nicht verfügbar" behandeln statt einen Pseudo-Maximalwert
    (z. B. 999) zu interpretieren.
    """
    if daten is None or len(daten) == 0:
        return None
    d = daten.tail(n)
    up = float(d.loc[d["close"] > d["open"], "volume"].sum())
    dn = float(d.loc[d["close"] < d["open"], "volume"].sum())
    if dn <= 0:
        return None
    return up / dn

def _none_if_nan(x): return None if pd.isna(x) else float(x)
def _linear_marke(points, idx):
    if not points or len(points) < 2: return None
    y0, y1 = float(points[0][1]), float(points[-1][1])
    n = max(1, len(points)-1)
    pos = max(0, min(int(idx), n))
    return y0 + (y1-y0) * (pos/n)


def _seit_einstieg(daten: pd.DataFrame, position: Position) -> pd.DataFrame:
    """Schneidet ``daten`` auf den Zeitraum ab Einstiegsdatum der Position.

    Wird von Strategien genutzt, die explizit „seit Einstieg" rechnen sollen
    (z. B. größter Einbruch). Indikator-basierte Strategien (MAs, ATR) erhalten
    weiterhin die vollständige Historie, damit gleitende Durchschnitte
    berechenbar sind.
    """
    if daten is None or len(daten) == 0 or position.einstiegsdatum is None:
        return daten
    try:
        idx = pd.to_datetime(daten.index)
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        einstieg = pd.Timestamp(position.einstiegsdatum)
        if getattr(einstieg, "tzinfo", None) is not None:
            einstieg = einstieg.tz_localize(None)
        gefiltert = daten.loc[idx >= einstieg]
        return gefiltert if len(gefiltert) > 0 else daten
    except Exception:
        return daten

# Strategien 2-23

def strategie_notbremse_verlust(
    position: Position,
    daten: pd.DataFrame,
    markt: str,
    schwelle_baerisch_pct: float = 4.0,
    schwelle_unsicher_pct: float = 5.0,
    schwelle_bullisch_pct: float = 7.0,
):
    pnl = pnl_pct(position, daten)
    b = -abs(float(schwelle_baerisch_pct))
    u = -abs(float(schwelle_unsicher_pct))
    bull = -abs(float(schwelle_bullisch_pct))
    s = b if markt=="Bärisch" else u if markt=="Unsicher" else bull
    if pnl <= s:
        return [_signal("Notbremse nach Verlusthöhe",100,"intraday",True,None,"Kap. 6.1",f"Verlustgrenze {s:g}% erreicht (Markt: {markt})")]
    kritischer_kurs = position.einstiegspreis*(1+s/100)
    return [_signal("Notbremse-Marke",0,"info",False,kritischer_kurs,"Kap. 6.1",f"Komplettausstieg bei Schluss/Intraday unter {kritischer_kurs:.2f} (Markt: {markt}, Schwelle: {s:g}%)")]

def strategie_gewinn_in_stufen(
    position: Position,
    daten: pd.DataFrame,
    markt: str,
    nachdenken_schwelle_bull_pct: float = 15.0,
    teilverkauf_schwelle_unten_bull_pct: float = 20.0,
    teilverkauf_schwelle_oben_bull_pct: float = 35.0,
    nachdenken_schwelle_bear_pct: float = 10.0,
    teilverkauf_schwelle_unten_bear_pct: float = 10.0,
    teilverkauf_schwelle_oben_bear_pct: float = 15.0,
):
    pnl = pnl_pct(position, daten); r = sum(position.realisierte_tranchen or []); out=[]
    if markt=="Bärisch":
        nd = float(nachdenken_schwelle_bear_pct)
        lo = float(max(teilverkauf_schwelle_unten_bear_pct, nd))
        hi = float(max(teilverkauf_schwelle_oben_bear_pct, lo))
    else:
        nd = float(nachdenken_schwelle_bull_pct)
        lo = float(max(teilverkauf_schwelle_unten_bull_pct, nd))
        hi = float(max(teilverkauf_schwelle_oben_bull_pct, lo))
    if pnl >= nd and r==0: out.append(_signal("Gewinn-Nachdenkschwelle erreicht",0,"info",True,position.einstiegspreis*(1+lo/100),"Kap. 6.2 Bulkowski-Schwelle","Über erster Nachdenkschwelle — Teilverkauf planen"))
    if pnl >= lo and r < 50: out.append(_signal("Pflicht-Teilverkauf Gewinnzone",33 if pnl<hi else 50,"schluss",True,position.einstiegspreis,"Kap. 6.2 Bulkowski 20-35%",f"Gewinnzone {lo:.1f}-{hi:.1f}% — Teilverkauf"))
    return out

def strategie_21ma_bruch(position: Position, daten: pd.DataFrame, variante: str = "gestaffelt"):
    """Strategie 4 (Kap. 6.2): Bruch der 21-EMA in drei Risikoprofilen.

    Setup:
    - variante="aggressiv": schneller Teilverkauf bei klarem Bruch + Volumenbestätigung.
    - variante="gestaffelt": stufenweises Vorgehen über 3 Tage unter 21-EMA.
    - variante="geduldig": erst nach 3 bestätigten Tagen unter 21-EMA aktiv.

    Regel: Nur im Gewinnfall aktiv (pnl > 0). Im Verlustfall greift Strategie 1.
    """
    if pnl_pct(position,daten) <= 0: return []
    ma21=ema(daten["close"],21); m=float(ma21.iloc[-1]); s=letzter_schlusskurs(daten); t=tage_unter_ma(daten,ma21); out=[]
    if variante=="aggressiv":
        br=(m-s)/m*100 if m else 0
        if s<m and br>=2 and vol_verhaeltnis(daten)>=1.2: out.append(_signal("Deutlicher 21-EMA-Bruch mit Volumen",33,"schluss",True,_none_if_nan(sma(daten['close'],50).iloc[-1]),"Kap. 6.2","Klarer Bruch"))
        if t==2 and len(daten)>=2 and (daten["close"].iloc[-1]/daten["close"].iloc[-2]-1)*100<=-7: out.append(_signal("21-EMA Bruch + 7% Tagesverlust",50,"intraday",True,None,"Kap. 6.2","Beschleunigte Schwäche"))
    elif variante=="geduldig":
        if t>=3: out.append(_signal("21-EMA seit 3 Tagen gebrochen",33,"schluss",True,float(daten["low"].tail(10).min()),"Kap. 6.2","Bruch bestätigt"))
    else:
        if t==1: out.append(_signal("Erster Schluss unter 21-EMA",25,"schluss",True,m,"Kap. 6.2","Stufe 1"))
        if t==2 and s<float(daten["close"].iloc[-2]): out.append(_signal("Zweiter Tag unter 21-EMA (tiefer)",25,"schluss",True,m,"Kap. 6.2","Stufe 2"))
        if t>=3: out.append(_signal("Dritter Tag unter 21-EMA",25,"schluss",True,position.einstiegspreis,"Kap. 6.2","Stufe 3"))
    return out

def strategie_drawdown_vom_peak(
    position: Position,
    daten: pd.DataFrame,
    drawdown_stufe1_min_pct: float = 8.0,
    drawdown_stufe2_min_pct: float = 12.0,
    drawdown_stufe3_min_pct: float = 15.0,
    tranche_stufe1_pct: float = 25.0,
    tranche_stufe2_pct: float = 33.0,
    tranche_stufe3_ohne_trendbruch_pct: float = 50.0,
    tranche_stufe3_mit_trendbruch_pct: float = 100.0,
):
    """Strategie 5 (Kap. 6.2): Drawdown vom Peak mit 3 Eskalationsstufen.

    Setup/Anpassung:
    - Stufe 1: erstes Sicherungssignal bei moderatem Rücksetzer vom Peak.
    - Stufe 2: deutliche Reduktion bei fortgeschrittenem Drawdown.
    - Stufe 3: harte Reduktion; bei zusätzlichem Trendbruch (Schluss unter 21-EMA)
      vollständiger Ausstieg.
    """
    if pnl_pct(position,daten)<=0: return []
    d1=max(0.0,float(drawdown_stufe1_min_pct)); d2=max(d1,float(drawdown_stufe2_min_pct)); d3=max(d2,float(drawdown_stufe3_min_pct))
    dd=drawdown_vom_peak(position,daten); ma21=_none_if_nan(ema(daten["close"],21).iloc[-1]); s=letzter_schlusskurs(daten); peak=float(position.peak or daten["high"].max()); out=[]
    if d1<=dd<d2: out.append(_signal("Drawdown 8% vom Peak",tranche_stufe1_pct,"schluss",True,peak*(1-d2/100),"Kap. 6.2 Drawdown vom Peak","Erster Schwellwert — Stopps enger ziehen, Teil sichern"))
    if d2<=dd<d3: out.append(_signal("Drawdown 12-15% vom Peak",tranche_stufe2_pct,"schluss",True,peak*(1-d3/100),"Kap. 6.2","Gewinnsicherung Pflicht — Stoppmarken: Tief schwächste Kerze oder 21-EMA"))
    if dd>=d3: out.append(_signal("Drawdown >15% + Trendbruch" if (ma21 and s<ma21) else "Drawdown >15%",tranche_stufe3_mit_trendbruch_pct if (ma21 and s<ma21) else tranche_stufe3_ohne_trendbruch_pct,"schluss",True,None if (ma21 and s<ma21) else ma21,"Kap. 6.2",">15% Rückgang + Bruch 21-EMA — Komplettausstieg" if (ma21 and s<ma21) else "Position deutlich reduzieren"))
    return out

def strategie_ma_abstand(
    position,
    daten,
    schwelle_ma10_pct: float = 10.0,
    schwelle_ma21_pct: float = 15.0,
    schwelle_ma50_pct: float = 25.0,
    schwelle_ma200_pct: float = 70.0,
    klimax_ma200_vollausstieg_pct: float = 100.0,
    tranche_ma10_pct: float = 25.0,
    tranche_ma21_pct: float = 33.0,
    tranche_ma50_pct: float = 33.0,
    tranche_ma200_basis_pct: float = 50.0,
):
    """Strategie 6 (Kap. 6.2): Teilverkäufe bei Überdehnung über gleitende Durchschnitte."""
    if pnl_pct(position,daten)<=0:return []

    s=letzter_schlusskurs(daten); out=[]
    ma10=_none_if_nan(sma(daten["close"],10).iloc[-1]); ma21=_none_if_nan(ema(daten["close"],21).iloc[-1]); ma50=_none_if_nan(sma(daten["close"],50).iloc[-1]); ma200=_none_if_nan(sma(daten["close"],200).iloc[-1])

    if ma10:
        abstand_10=(s-ma10)/ma10*100
        if abstand_10>=float(schwelle_ma10_pct):
            out.append(_signal("10% über 10-MA",tranche_ma10_pct,"schluss",True,ma10,"Kap. 6.2 MA-Abstand","Überhitzt zur 10-MA — Stoppmarke Tagestief Kerze mit großer Spanne + hohem Volumen"))
    if ma21:
        abstand_21=(s-ma21)/ma21*100
        if abstand_21>=float(schwelle_ma21_pct):
            out.append(_signal("15% über 21-EMA",tranche_ma21_pct,"schluss",True,ma21,"Kap. 6.2 MA-Abstand","Erste klare Überdehnung"))
    if ma50:
        abstand_50=(s-ma50)/ma50*100
        if abstand_50>=float(schwelle_ma50_pct):
            out.append(_signal("25% über 50-MA",tranche_ma50_pct,"schluss",True,ma50,"Kap. 6.2 MA-Abstand","Spätphasen-Signal"))
    if ma200:
        abstand_200=(s-ma200)/ma200*100
        if abstand_200>=float(schwelle_ma200_pct):
            tranche=100 if abstand_200>=float(klimax_ma200_vollausstieg_pct) else float(tranche_ma200_basis_pct)
            out.append(_signal(f"{abstand_200:.1f}% über 200-MA (Klimaxzone)",tranche,"schluss",True,ma50,"Kap. 6.2 MA-Abstand","Klimaxzone — historisch häufigste Top-Region"))
    return out

def strategie_verlusttage_haeufung(
    position,
    daten,
    min_tiefere_schlusskurse_in_folge: int = 3,
    volumen_lookback_tage: int = 50,
    volumen_ratio_min: float = 1.1,
    tief_marker_lookback_tage: int = 5,
    updown_fenster_tage: int = 15,
    updown_diff_min: int = 3,
    tranche_pct: float = 25.0,
):
    if pnl_pct(position,daten)<=0 or len(daten)<max(3, int(updown_fenster_tage)):return []
    out=[]; l3=daten.tail(3); seq=1
    for i in [1,2]:
        if l3["close"].iloc[i] < l3["close"].iloc[i-1]: seq += 1
        else: seq = 1
    if len(daten) >= int(volumen_lookback_tage):
        avg_vol = float(daten["volume"].tail(int(volumen_lookback_tage)).mean())
        vol_ratio = (float(l3["volume"].mean()) / avg_vol) if avg_vol else 0.0
    else:
        vol_ratio = 1.0
    if seq>=int(min_tiefere_schlusskurse_in_folge) and vol_ratio>=float(volumen_ratio_min):
        out.append(_signal("3 tiefere Schlusskurse mit Volumen",tranche_pct,"schluss",True,float(daten["low"].tail(int(tief_marker_lookback_tage)).min()),"Kap. 6.2 Häufung Verlusttage","Rebound nach 2,5 Tagen ausgeblieben"))
    fen=daten.tail(int(updown_fenster_tage)); up=(fen["close"]>fen["open"]).sum(); dn=(fen["close"]<fen["open"]).sum()
    if dn>up and (dn-up)>=int(updown_diff_min): out.append(_signal("Abwärtstage überwiegen im 15-Tage-Fenster",tranche_pct,"schluss",True,float(fen["low"].min()),"Kap. 6.2 Häufung Verlusttage","Persönlichkeit der Aktie kippt — Reduktion in Gegenbewegung"))
    return out

def strategie_groesster_anstieg_volumen(position,daten):
    if pnl_pct(position,daten)<=15 or len(daten)<3:return []
    # „Seit Kauf" — Vergleich des größten Tagesanstiegs/Volumens nur ab Einstiegsdatum.
    daten_seit = _seit_einstieg(daten, position)
    if len(daten_seit)<3:return []
    ch=daten_seit["close"].pct_change(fill_method=None)*100; t=ch.iloc[-1]
    mx=float(ch.iloc[1:-1].max()) if len(ch) > 2 else float("-inf")
    prev_vol_max = float(daten_seit["volume"].iloc[:-1].max()) if len(daten_seit) > 1 else float("inf")
    v = float(daten_seit["volume"].iloc[-1]) >= prev_vol_max
    if t>=mx and v:return [_signal("Größter Anstieg + höchstes Volumen",33,"schluss",True,float(daten_seit["low"].iloc[-1]),"Kap. 6.2","Klimax-Muster")]
    if t>=mx:return [_signal("Größter Tagesanstieg",20,"schluss",True,float(daten_seit["low"].iloc[-1]),"Kap. 6.2","Vorwarnung")]
    return []

def strategie_split_anstieg(position,daten,split_datum=None,signal_schwelle_pct: float = 25.0,starke_tranche_ab_pct: float = 50.0):
    if not split_datum: return []
    split_dt=pd.Timestamp(split_datum).tz_localize(None)
    d=daten.copy(); d.index=pd.to_datetime(d.index).tz_localize(None)
    if split_dt not in d.index: return []
    if (d.index[-1]-split_dt).days > 14: return []
    kurs=float(d.loc[split_dt,"close"]); s=letzter_schlusskurs(d); an=(s/kurs-1)*100 if kurs else 0
    signal_schwelle = float(signal_schwelle_pct)
    starke_schwelle = float(max(starke_tranche_ab_pct, signal_schwelle))
    if an>=signal_schwelle:
        tranche = 50 if an>=starke_schwelle else 33
        return [_signal(f"Anstieg nach Split: {an:.1f}%",tranche,"schluss",True,kurs,"Kap. 6.2 Split-Anstieg","25-50% Anstieg innerhalb 1-2 Wochen nach Split — typische Gipfel-Vorwarnung")]
    return []
def strategie_erschoepfungsluecke(
    position,
    daten,
    min_pnl_pct: float = 15.0,
    gap_up_min_pct: float = 3.0,
    volumen_lookback_tage: int = 50,
    volumen_ratio_min: float = 1.5,
    min_distanz_zur_basis_pct: float = 30.0,
    tranche_pct: float = 33.0,
):
    """Strategie 11 (Kap. 6.2): Erschöpfungslücke nach langem Aufwärtstrend.

    Die Strategie identifiziert ein potenzielles Spätphasen-/Klimax-Muster:
    Ein deutliches Gap-up zur Eröffnung, erhöhtes Volumen und große Distanz
    zur Ausgangsbasis (Pivot). In dieser Kombination wird ein Teilverkauf
    ausgelöst und der Rest eng über das aktuelle Tagestief abgesichert.

    Parameter:
    - min_pnl_pct: Mindestgewinn der Position, ab dem das Muster relevant wird.
    - gap_up_min_pct: Mindestgröße der Aufwärtslücke gegenüber dem Vortagesschluss.
    - volumen_lookback_tage: Basisfenster zur Berechnung des Durchschnittsvolumens.
    - volumen_ratio_min: Mindestverhältnis Tagesvolumen / Durchschnittsvolumen.
    - min_distanz_zur_basis_pct: Mindestabstand des Schlusskurses zum Pivot.
    - tranche_pct: Standardgröße der Gewinnmitnahme in Prozent.
    """
    if not position.pivot or len(daten) < max(2, int(volumen_lookback_tage) + 1):
        return []

    pnl = pnl_pct(position, daten)
    if pnl <= float(min_pnl_pct):
        return []

    heute = daten.iloc[-1]
    gestern = daten.iloc[-2]

    gap_up_pct = (heute["open"] - gestern["close"]) / gestern["close"] * 100 if gestern["close"] else 0.0
    avg_vol = float(daten["volume"].tail(int(volumen_lookback_tage)).mean())
    vol_ratio = (float(heute["volume"]) / avg_vol) if avg_vol else 0.0
    distanz_zur_basis_pct = (float(heute["close"]) - float(position.pivot)) / float(position.pivot) * 100

    if gap_up_pct >= float(gap_up_min_pct) and vol_ratio >= float(volumen_ratio_min) and distanz_zur_basis_pct >= float(min_distanz_zur_basis_pct):
        begruendung = (
            "Gap-up nach langem Lauf mit hohem Volumen — letzte Kaufwelle "
            f"(Gap {gap_up_pct:.1f}%, Volumenfaktor {vol_ratio:.2f}, Distanz zur Basis {distanz_zur_basis_pct:.1f}%)."
        )
        return [_signal("Erschöpfungslücke", tranche_pct, "schluss", True, float(heute["low"]), "Kap. 6.2 Erschöpfungslücke", begruendung)]

    return []
def strategie_downside_reversal(
    position,
    daten,
    kerzenweite_lookback_tage: int = 10,
    volumen_lookback_tage: int = 50,
    neues_hoch_lookback_tage: int = 30,
    weite_kerze_faktor: float = 1.5,
    volumen_ratio_min: float = 1.2,
    schluss_unteres_drittel_faktor: float = 3.0,
    tranche_neues_hoch_pct: float = 33.0,
    tranche_weite_umkehr_pct: float = 20.0,
    tranche_warnstufe_pct: float = 15.0,
):
    if pnl_pct(position,daten)<=0 or len(daten)<max(12, int(kerzenweite_lookback_tage)):return []
    h=daten.iloc[-1]; span=h["high"]-h["low"]; avg=float((daten["high"].tail(int(kerzenweite_lookback_tage))-daten["low"].tail(int(kerzenweite_lookback_tage))).mean())
    if len(daten) >= int(volumen_lookback_tage):
        vr = float(daten["volume"].iloc[-1] / daten["volume"].tail(int(volumen_lookback_tage)).mean())
    else:
        vr = vol_verhaeltnis(daten)
    if span<=0:return []
    new_high=h["high"]>=float(daten["high"].tail(int(neues_hoch_lookback_tage)).iloc[:-1].max()) if len(daten)>=int(neues_hoch_lookback_tage)+1 else False
    low_third=h["close"]<=h["low"]+span/max(float(schluss_unteres_drittel_faktor), 1.01)
    wide_candle=span>=float(weite_kerze_faktor)*avg
    if new_high and low_third and vr>=float(volumen_ratio_min):return [_signal("Downside Reversal an neuem Hoch",tranche_neues_hoch_pct,"schluss",True,float(h["high"]),"Kap. 6.2 Downside Reversal","Neues Hoch und Schluss nahe Tagestief mit erhöhtem Volumen")]
    if wide_candle and low_third and vr>=float(volumen_ratio_min):return [_signal("Weite Umkehrkerze",tranche_weite_umkehr_pct,"schluss",True,float(h["high"]),"Kap. 6.2","Weite Umkehr — wachsam bleiben, erste Tranche")]
    if wide_candle and h["close"]<h["low"]+span/2:return [_signal("Schluss unter Spannenmitte",tranche_warnstufe_pct,"schluss",True,float(h["high"]),"Kap. 6.2","Warnstufe — Beobachtung intensivieren")]
    return []

def strategie_stau_tage(
    position,
    daten,
    fenster_tage: int = 10,
    volumen_lookback_tage: int = 50,
    max_tagesveraenderung_pct: float = 1.0,
    min_vol_ratio: float = 1.3,
    min_stau_tage: int = 2,
    nahe_hoch_drawdown_max_pct: float = 5.0,
    tranche_nahe_hoch_pct: float = 33.0,
    tranche_standard_pct: float = 20.0,
):
    if pnl_pct(position,daten)<=0 or len(daten)<int(fenster_tage):return []
    fen=daten.tail(int(fenster_tage)); avgv=float(daten["volume"].tail(int(volumen_lookback_tage)).mean()) if len(daten)>=int(volumen_lookback_tage) else float(daten["volume"].mean()); st=[]
    for _,r in fen.iterrows():
        ch=(r["close"]-r["open"])/r["open"]*100 if r["open"] else 0
        if abs(ch)<float(max_tagesveraenderung_pct) and (r["volume"]/avgv if avgv else 0)>=float(min_vol_ratio): st.append(r)
    if len(st)>=int(min_stau_tage):
        dd=drawdown_vom_peak(position,daten); tr=float(tranche_nahe_hoch_pct) if dd<float(nahe_hoch_drawdown_max_pct) else float(tranche_standard_pct)
        return [_signal(f"{len(st)} Stau-Tage in {int(fenster_tage)} Sessions",tr,"schluss",True,float(min(x["low"] for x in st)),"Kap. 6.2 Stau-Tage","Verdeckte Distribution — Stopp auf Tief des Stau-Tags")]
    return []

def strategie_rueckkehr_pivot(
    position: Position,
    daten: pd.DataFrame,
    tranche_stufe1_pct: float = 33.0,
    tranche_stufe1_volumen_pct: float = 50.0,
    volumen_schwelle: float = 1.5,
    tranche_stufe2_pct: float = 33.0,
    pivot_tage_schwelle: int = 10,
    tranche_pivot_pct: float = 50.0,
    notbremse_verlust_pct: float = 7.0,
):
    """Strategie 14 (Kap. 6.3): Rückkehr zum Ausbruchspunkt + Notbremse.

    Vereint die ursprüngliche Strategie 1 (Drei-Stufen-Regel direkt nach Kauf)
    und Strategie 14 (Rückkehr zum Ausbruchspunkt). Sicherheitslinien greifen
    bei Schluss unter Tief Tag 1 / Tag 0, ergänzt um die Zeitkomponente
    (X Handelstage in Folge unter Pivot) und die -7%-Notbremse als Intraday-Exit.
    """
    s = letzter_schlusskurs(daten)
    pnl = pnl_pct(position, daten)
    vr = vol_verhaeltnis(daten)
    out: list[dict] = []
    stufe1 = float(tranche_stufe1_pct)
    stufe1_vol = float(tranche_stufe1_volumen_pct)
    vol_schwelle = float(volumen_schwelle)
    stufe2 = float(tranche_stufe2_pct)
    pivot_tage = max(1, int(pivot_tage_schwelle))
    pivot_tranche = float(tranche_pivot_pct)
    notbremse = abs(float(notbremse_verlust_pct))
    if position.tief_tag_1 and s < position.tief_tag_1:
        begr = "Mit erhöhtem Volumen" if vr >= vol_schwelle else "Erste Sicherheitslinie verletzt"
        out.append(_signal("Schluss unter Tief Ausbruchstag", stufe1_vol if vr >= vol_schwelle else stufe1, "schluss", True, position.tief_tag_0, "Kap. 6.3 Rückkehr zum Ausbruchspunkt", begr))
    if position.tief_tag_0 and s < position.tief_tag_0:
        out.append(_signal("Schluss unter Tief Vortag", stufe2, "schluss", True, position.einstiegspreis * (1 - notbremse / 100), "Kap. 6.3 Rückkehr zum Ausbruchspunkt", "Zweite Sicherheitslinie verletzt"))
    if position.pivot:
        under = (daten["close"] < position.pivot).iloc[::-1]
        c = 0
        for b in under:
            if not b:
                break
            c += 1
        if s < position.pivot and c >= pivot_tage:
            out.append(_signal(f"{c} Tage unter Pivot", pivot_tranche, "schluss", True, position.einstiegspreis * (1 - notbremse / 100), "Kap. 6.3 Rückkehr zum Ausbruchspunkt", "Rückkehr über Ausbruchspunkt nicht gelungen"))
    if pnl <= -notbremse:
        out.append(_signal(f"{notbremse:g}%-Notbremse", 100, "intraday", True, None, "Kap. 6.1 / 6.3 Notbremse", "Maximaler Verlust erreicht — Rest sofort verkaufen"))
    return out

def strategie_ma_bruch_defensiv(position,daten,wochen_daten):
    out=[]; s=letzter_schlusskurs(daten); ma50=_none_if_nan(sma(daten["close"],50).iloc[-1]); ma200_series=sma(daten["close"],200); ma200=_none_if_nan(ma200_series.iloc[-1]); atrp=atr(daten,14); vr=vol_verhaeltnis(daten)
    if ma50 and s<ma50:
        dist=(ma50-s)/ma50*100
        if dist>=max(2,atrp) and vr>=1.3: out.append(_signal("Klarer 50-MA-Bruch mit Volumen",50,"schluss",True,ma200,"Kap. 6.3 50-MA","Deutlich unter 50-MA bei erhöhtem Volumen — schrittweise reduzieren"))
        elif tage_unter_ma(daten,sma(daten["close"],50))>=3: out.append(_signal("3 Tage unter 50-MA ohne Rückeroberung",33,"schluss",True,ma200,"Kap. 6.3 50-MA","Drei-Tage-Frist verstrichen"))
    if len(wochen_daten)>=10 and tage_unter_ma(wochen_daten, sma(wochen_daten["close"],10))>=8: out.append(_signal("8+ Wochen unter 10-Wochen-Linie",100,"schluss",True,None,"Kap. 6.3 10-Wochen-Linie","Acht oder mehr Wochen ohne Rückeroberung — klares Schwächesignal"))
    # 200-MA-Bruch: Volumen-Schwelle bewusst weicher (1.3) — der 50-MA-Bruch nutzt
    # bereits 1.3, und in volatilen Phasen wäre 1.5 zu restriktiv, um das
    # 100%-Killer-Signal noch auszulösen.
    if ma200 and s<ma200: out.append(_signal("200-MA-Bruch" + (" mit hohem Volumen" if vr>=1.3 else " ohne Volumen"),100 if vr>=1.3 else 75,"schluss",True,None,"Kap. 6.3 200-MA","Langfristiger Aufwärtszyklus beendet — Kapitalerhalt" if vr>=1.3 else "200-MA gebrochen — auf sehr kleinen Rest reduzieren"))
    ma200_valid = ma200_series.dropna()
    if len(ma200_valid) >= 20:
        ma200_richtung = float(ma200_valid.iloc[-1] - ma200_valid.iloc[-20])
        if ma200_richtung < 0 and ma200 and s < ma200:
            out.append(_signal("200-MA dreht abwärts",0,"info",True,None,"Kap. 6.3 200-MA","Trendwechsel bestätigt"))
    return out

def strategie_drei_verlustwochen(position,wochen_daten):
    if len(wochen_daten)<3:return []
    l=wochen_daten.tail(3)
    three=(l["close"].iloc[0]>l["close"].iloc[1]>l["close"].iloc[2])
    vol=(l["volume"].iloc[1]>l["volume"].iloc[0]) and (l["volume"].iloc[2]>l["volume"].iloc[1])
    red=bool((l["close"]<l["open"]).all())
    if three and vol and red:return [_signal("3 Verlustwochen + steigendes Volumen",100,"schluss",True,None,"Kap. 6.3","Verteilungsmuster")]
    if three and vol:return [_signal("Vorbereitung Drei-Wochen-Regel",33,"schluss",True,None,"Kap. 6.3","Muster im Aufbau")]
    return []

def strategie_groesster_einbruch(
    position,
    daten,
    wochen_daten,
    min_pnl_pct: float = 10.0,
    min_tagesverlust_pct: float = 3.0,
    tagesvol_ratio_schwelle: float = 1.5,
    wochenvol_ratio_schwelle: float = 1.3,
):
    if pnl_pct(position,daten)<=float(min_pnl_pct):return []
    # „Seit Einstieg" — Vergleich der Tages-/Wochenverluste nur ab Kaufdatum.
    daten_seit = _seit_einstieg(daten, position)
    wochen_seit = _seit_einstieg(wochen_daten, position) if wochen_daten is not None else None
    if len(daten_seit)<4:return []
    out=[]; d=(daten_seit["close"].shift(1)-daten_seit["close"])/daten_seit["close"].shift(1)*100; h=float(d.iloc[-1]); mx=float(d.iloc[1:-1].max()) if len(d)>3 else 0; vr=vol_verhaeltnis(daten)
    if h>=mx and h>float(min_tagesverlust_pct):
        if vr>=float(tagesvol_ratio_schwelle):
            out.append(_signal("Größter Tagesverlust + hohes Volumen",50,"schluss",True,tagestief(daten_seit),"Kap. 6.3 Größter Einbruch","Spätphasen-Warnsignal mit Volumen"))
        else:
            out.append(_signal("Größter Tagesverlust seit Beginn",33,"schluss",True,tagestief(daten_seit),"Kap. 6.3","Defensive Reduktion"))
    if wochen_seit is not None and len(wochen_seit)>=12:
        w=(wochen_seit["close"].shift(1)-wochen_seit["close"])/wochen_seit["close"].shift(1)*100; cur=float(w.iloc[-1]); mxw=float(w.iloc[1:-1].max()) if len(w)>3 else 0
        # Volumen relativ zur gesamten Historie (analog vol_verhaeltnis im Tagespfad),
        # damit die Volumen-Bewertung nicht von der Halteperiode abhängt.
        wochen_vol_basis = wochen_daten if wochen_daten is not None and len(wochen_daten) >= 12 else wochen_seit
        wr=float(wochen_vol_basis["volume"].iloc[-1]/wochen_vol_basis["volume"].tail(12).mean())
        if cur>=mxw and wr>=float(wochenvol_ratio_schwelle): out.append(_signal("Größte Verlustwoche seit Beginn",66,"schluss",True,None,"Kap. 6.3","Wahrscheinliches Rally-Ende"))
    return out

def _rs_linie_context(
    position,
    daten,
    daten_spy,
    wochen_daten,
    wochen_daten_spy,
    pnl_tag_zu_woche=20.0,
    pnl_woche_zu_monat=80.0,
):
    if daten_spy is None or len(daten_spy) == 0:
        return {"error": "Keine Benchmark-Daten (SPY) verfügbar — RS-Linie kann nicht berechnet werden."}
    pnl = pnl_pct(position, daten)
    schwelle_tag_woche = float(pnl_tag_zu_woche)
    schwelle_woche_monat = float(max(pnl_woche_zu_monat, schwelle_tag_woche))
    if pnl < schwelle_tag_woche:
        intended_zeitebene = "tag"
        intended_reason = f"P&L {pnl:.1f}% liegt unter der Tag→Woche-Schwelle {schwelle_tag_woche:g}%"
    elif pnl < schwelle_woche_monat:
        intended_zeitebene = "woche"
        intended_reason = f"P&L {pnl:.1f}% liegt zwischen Tag→Woche {schwelle_tag_woche:g}% und Woche→Monat {schwelle_woche_monat:g}%"
    else:
        intended_zeitebene = "monat"
        intended_reason = f"P&L {pnl:.1f}% liegt ab der Woche→Monat-Schwelle {schwelle_woche_monat:g}%"
    basis_details = {
        "tag": "Tages-Schlusskurse aus Aktie und Benchmark",
        "woche": "W-FRI-Wochen-Schlusskurse aus Aktie und Benchmark",
        "monat": "Monats-Schlusskurse (ME) aus Aktie und Benchmark",
    }

    def _monthly_frame(frame: pd.DataFrame | None) -> pd.DataFrame | None:
        if frame is None or len(frame) == 0:
            return None
        if not isinstance(frame.index, pd.DatetimeIndex):
            return None
        return frame.resample("ME").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna(subset=["close"])

    def _timeframe_candidates() -> list[tuple[str, pd.DataFrame | None, pd.DataFrame | None, int, int]]:
        daily = ("tag", daten, daten_spy, 21, 50)
        weekly = ("woche", wochen_daten, wochen_daten_spy, 10, 25)
        monthly = ("monat", _monthly_frame(daten), _monthly_frame(daten_spy), 12, 24)
        if intended_zeitebene == "tag":
            return [daily]
        if intended_zeitebene == "woche":
            return [weekly, daily]
        # Bei hohen Gewinnen ist die Monatsebene fachlich bevorzugt. Wenn aber
        # noch keine 25 Monats-RS-Punkte vorliegen (z. B. 17/25), soll die
        # Strategie nicht komplett ausfallen, sondern auf die nächstschnellere
        # Ebene mit ausreichend Daten zurückfallen.
        return [monthly, weekly, daily]

    fehlversuche: list[str] = []
    fallback_from: str | None = None
    selected = None
    for zeitebene, basis, basis_spy, sp, lp in _timeframe_candidates():
        required = max(lp + 1, 4)
        if basis is None or basis_spy is None or len(basis) == 0 or len(basis_spy) == 0:
            fehlversuche.append(f"{zeitebene}: keine ausreichenden Kurs-/Benchmark-Daten ({basis_details[zeitebene]})")
            continue
        joined = pd.concat([basis["close"].rename("a"), basis_spy["close"].rename("b")], axis=1, join="inner").dropna()
        if joined.empty:
            fehlversuche.append(f"{zeitebene}: keine überlappenden Kursdaten ({basis_details[zeitebene]})")
            continue
        rs = (joined["a"] / joined["b"]).dropna()
        if len(rs) < required:
            fehlversuche.append(f"{zeitebene}: {len(rs)}/{required} RS-Datenpunkte für {lp}-MA ({basis_details[zeitebene]})")
            continue
        selected = (zeitebene, sp, lp, rs)
        fallback_from = intended_zeitebene if zeitebene != intended_zeitebene else None
        break

    if selected is None:
        detail = "; ".join(fehlversuche) if fehlversuche else "keine auswertbaren RS-Daten"
        return {
            "error": f"Zu wenige RS-Datenpunkte für die RS-Linie ({detail}). Bevorzugte Zeitebene: {intended_zeitebene} — {intended_reason}.",
            "pnl": pnl,
            "zeitebene": intended_zeitebene,
            "intended_zeitebene": intended_zeitebene,
            "intended_reason": intended_reason,
        }

    zeitebene, sp, lp, rs = selected
    required_points = max(lp + 1, 4)
    rsf = sma(rs, sp)
    rsl = sma(rs, lp)
    latest_rs = _none_if_nan(rs.iloc[-1])
    latest_fast = _none_if_nan(rsf.iloc[-1])
    latest_slow = _none_if_nan(rsl.iloc[-1])
    cnt = 0
    for a, b in zip(rs.iloc[::-1], rsf.iloc[::-1]):
        if pd.isna(b) or a >= b:
            break
        cnt += 1
    return {
        "pnl": pnl,
        "zeitebene": zeitebene,
        "sp": sp,
        "lp": lp,
        "rs": rs,
        "rsf": rsf,
        "rsl": rsl,
        "latest_rs": latest_rs,
        "latest_fast": latest_fast,
        "latest_slow": latest_slow,
        "days_under_fast": cnt,
        "fallback_from": fallback_from,
        "intended_zeitebene": intended_zeitebene,
        "intended_reason": intended_reason,
        "basis_detail": basis_details[zeitebene],
        "actual_points": len(rs),
        "required_points": required_points,
        "failed_attempts": fehlversuche,
    }

def _rs_zeitraum_label(zeitebene: str, count: int | None = None) -> str:
    labels = {
        "tag": ("Tag", "Tage"),
        "woche": ("Woche", "Wochen"),
        "monat": ("Monat", "Monate"),
    }
    singular, plural = labels.get(zeitebene, (zeitebene, zeitebene))
    if count is None:
        return singular
    return singular if count == 1 else plural


def _rs_ma_label(perioden: int, zeitebene: str) -> str:
    labels = {
        "tag": "Tage",
        "woche": "Wochen",
        "monat": "Monats",
    }
    return f"{perioden}-{labels.get(zeitebene, zeitebene)}-MA"


def _rs_datencheck_text(ctx: dict[str, Any], zeitebene: str) -> str:
    actual = ctx.get("actual_points")
    required = ctx.get("required_points")
    basis = ctx.get("basis_detail")
    fallback_from = ctx.get("fallback_from")
    failed_attempts = ctx.get("failed_attempts") or []
    basis_labels = {"tag": "Tagesbasis", "woche": "Wochenbasis", "monat": "Monatsbasis"}
    parts = [f"Datencheck: {actual}/{required} RS-Punkte auf {basis_labels.get(zeitebene, zeitebene)} vorhanden — ausreichend für schnellen und langsamen Durchschnitt"]
    if basis:
        parts.append(f"Basis: {basis}")
    if fallback_from and failed_attempts:
        parts.append(f"Fallback von {fallback_from} auf {zeitebene}, weil die bevorzugte Ebene nicht genug Daten hatte ({failed_attempts[0]})")
    return "; ".join(parts) + "."


def _rs_langsamer_ma_text(latest_rs: float | None, latest_slow: float | None, slow_ma: str) -> str:
    if latest_rs is None or latest_slow is None:
        return f"Langsamer Durchschnitt: {slow_ma} kann noch nicht zuverlässig beurteilt werden."
    lage = "auch unter" if latest_rs < latest_slow else "noch nicht unter"
    folge = "Damit ist zusätzlich RS-Stufe 3 aktiv." if latest_rs < latest_slow else "Deshalb ist nur die schnelle RS-Schwäche aktiv, nicht die langsame Stufe 3."
    return f"Langsamer Durchschnitt: RS liegt {lage} dem {slow_ma} (RS {latest_rs:.4f} vs. {slow_ma} {latest_slow:.4f}). {folge}"

def _rs_ma_label(perioden: int, zeitebene: str) -> str:
    labels = {
        "tag": "Tage",
        "woche": "Wochen",
        "monat": "Monats",
    }
    return f"{perioden}-{labels.get(zeitebene, zeitebene)}-MA"

def strategie_rs_linie(
    position,
    daten,
    daten_spy,
    wochen_daten,
    wochen_daten_spy,
    pnl_tag_zu_woche=20.0,
    pnl_woche_zu_monat=80.0,
):
    ctx = _rs_linie_context(position, daten, daten_spy, wochen_daten, wochen_daten_spy, pnl_tag_zu_woche, pnl_woche_zu_monat)
    if ctx.get("error"):
        return []
    rs = ctx["rs"]; rsf = ctx["rsf"]; rsl = ctx["rsl"]
    sp = ctx["sp"]; lp = ctx["lp"]; zeitebene = ctx["zeitebene"]
    fast_ma = _rs_ma_label(sp, zeitebene)
    slow_ma = _rs_ma_label(lp, zeitebene)
    latest_rs = ctx.get("latest_rs")
    latest_slow = ctx.get("latest_slow")
    datencheck = _rs_datencheck_text(ctx, zeitebene)
    slow_status = _rs_langsamer_ma_text(latest_rs, latest_slow, slow_ma)
    out=[]
    if len(rs)>=2 and rs.iloc[-1] < rsf.iloc[-1] and rs.iloc[-2] >= rsf.iloc[-2]: out.append(_signal(f"RS-Linie bricht {fast_ma}",20,"schluss",True,None,"Kap. 6.4 RS-Stufe 1",f"Erstes Warnsignal — die relative Stärke fällt auf {zeitebene}-Basis unter den schnellen Durchschnitt ({fast_ma}). {slow_status} {datencheck}"))
    if ctx["days_under_fast"]>=3: out.append(_signal(f"RS-Linie seit 3 {_rs_zeitraum_label(zeitebene, 3)} unter {fast_ma}",30,"schluss",True,_none_if_nan(rsl.iloc[-1]),"Kap. 6.4 RS-Stufe 2",f"Bestätigte relative Schwäche — die Aktie läuft seit 3 {_rs_zeitraum_label(zeitebene, 3)} schwächer als die Benchmark und bleibt unter dem schnellen RS-Durchschnitt; zweite Tranche prüfen. {slow_status} {datencheck}"))
    if rs.iloc[-1] < rsl.iloc[-1]: out.append(_signal(f"RS-Linie bricht {slow_ma}",50,"schluss",True,None,"Kap. 6.4 RS-Stufe 3",f"Endgültiges RS-Schwächesignal — die relative Stärke liegt unter dem langsamen Durchschnitt ({slow_ma}); Restverkauf prüfen. {datencheck}"))
    return out

def strategie_ma_basierte_sequenz(
    position,
    daten,
    gewinnzone_min_pct=20.0,
    gewinnzone_max_pct=25.0,
    gewinnzone_tranche_pct=33.0,
    ueber_ma10_pct=10.0,
    ueber_ma10_tranche_pct=20.0,
    unter_ma10_mindestgewinn_pct=5.0,
    unter_ma10_tranche_pct=20.0,
    pendel_tranche_pct=25.0,
    pendel_lookback_tage=5,
    pendel_wechsel_min=3,
    unter_ma21_mindestgewinn_pct=5.0,
    unter_ma21_tranche_pct=25.0,
    klarer_ma50_bruch_pct=2.0,
):
    pnl=pnl_pct(position,daten); s=letzter_schlusskurs(daten); r=sum(position.realisierte_tranchen or []); out=[]
    ma10=_none_if_nan(sma(daten["close"],10).iloc[-1]); ma21=_none_if_nan(ema(daten["close"],21).iloc[-1]); ma50=_none_if_nan(sma(daten["close"],50).iloc[-1])
    if gewinnzone_min_pct<=pnl<=gewinnzone_max_pct and r<gewinnzone_tranche_pct:
        out.append(_signal("Gewinnzone 20-25%",gewinnzone_tranche_pct,"schluss",True,ma10,"Kap. 6.4 Sequenz Punkt 1","Objektive Gewinnzone — ersten Teil sichern"))
    if ma10 and (s-ma10)/ma10*100>=ueber_ma10_pct and r<50:
        out.append(_signal("10% über 10-MA (überhitzt)",ueber_ma10_tranche_pct,"schluss",True,ma10,"Kap. 6.4 Sequenz Punkt 2","Anstieg überhitzt — Tranche in Stärke"))
    if ma10 and s<ma10 and pnl>unter_ma10_mindestgewinn_pct:
        pendelt=False
        n=max(int(pendel_lookback_tage),2)
        w=max(int(pendel_wechsel_min),1)
        if len(daten)>=n and len(sma(daten["close"],10))>=n:
            last_close=daten["close"].tail(n).reset_index(drop=True)
            last_ma10=sma(daten["close"],10).tail(n).reset_index(drop=True)
            wechsel=0
            for i in range(1,n):
                if pd.notna(last_ma10.iloc[i]) and pd.notna(last_ma10.iloc[i-1]):
                    now_below=last_close.iloc[i] < last_ma10.iloc[i]
                    prev_below=last_close.iloc[i-1] < last_ma10.iloc[i-1]
                    if now_below != prev_below:
                        wechsel += 1
            pendelt = wechsel >= w
        tranche=pendel_tranche_pct if pendelt else unter_ma10_tranche_pct
        out.append(_signal("Schluss unter 10-MA (pendelnd)" if pendelt else "Schluss unter 10-MA",tranche,"schluss",True,ma21,"Kap. 6.4 Sequenz Punkt 3","Pendel um 10-MA — Warnzeichen" if pendelt else "Kurzfristige Unterstützung verloren"))
    if ma21 and s<ma21 and pnl>unter_ma21_mindestgewinn_pct:
        out.append(_signal("Schluss unter 21-EMA",unter_ma21_tranche_pct,"schluss",True,ma50,"Kap. 6.4 Sequenz Punkt 4","Mittelfristige Linie verloren"))
    if ma50 and s<ma50 and (ma50-s)/ma50*100>=klarer_ma50_bruch_pct:
        out.append(_signal("Klarer 50-MA-Bruch",100,"schluss",True,None,"Kap. 6.4 Sequenz Punkt 5","Mittelfristiger Trend gebrochen — alle Reste schließen"))
    return out

def strategie_einfach_halbe_position(position,daten,erste_haelfte_gewinn_pct=20.0):
    pnl=pnl_pct(position,daten); r=sum(position.realisierte_tranchen or []); out=[]; h=r>=50
    gw=max(float(erste_haelfte_gewinn_pct),0.0)
    if pnl>=gw and not h: out.append(_signal(f"Erste Hälfte bei {gw:g}%+",50,"schluss",True,position.einstiegspreis,"Kap. 6.4","Hälfte sichern"))
    if h and pnl>=20: out.append(_signal("Erneut 20% — weitere Tranche",50,"schluss",True,position.einstiegspreis,"Kap. 6.4","Zweite Gewinnmitnahme"))
    if h and -1<=pnl<=1: out.append(_signal("Break-Even-Stopp greift",100,"intraday",True,None,"Kap. 6.4","Rest auf BE"))
    return out

def strategie_misslungener_ausbruch_5stufen(position,daten):
    out=[]; h=daten.iloc[-1]; pnl=pnl_pct(position,daten); nb=position.einstiegspreis*0.93
    g=daten.iloc[-2] if len(daten)>=2 else h
    gap_down_unter_tag1 = bool(position.tief_tag_1) and h["open"] < position.tief_tag_1 and h["open"] < g["close"]
    gap_down_unter_tag0 = bool(position.tief_tag_0) and h["open"] < position.tief_tag_0
    if h["open"]<=nb:return [_signal("Gap-down durch 7%-Grenze",100,"intraday",True,None,"Kap. 6.4 Sonderfall Gap-down","Sofort schließen — nicht auf Schluss warten")]
    if gap_down_unter_tag1 or gap_down_unter_tag0:
        out.append(_signal("Gap-down unter Ausbruchsmarken",20,"intraday",True,position.tief_tag_1 if gap_down_unter_tag1 else position.tief_tag_0,"Kap. 6.4 Sonderfall Gap-down","Eröffnung bereits unter kritischer Marke"))
    tag0_unter_notbremse = bool(position.tief_tag_0) and position.tief_tag_0 <= nb
    if position.tief_tag_1 and h["low"]<position.tief_tag_1: out.append(_signal("Intraday unter Tief Tag 1",20,"intraday",True,position.tief_tag_1,"Kap. 6.4","Stufe 1a"))
    if position.tief_tag_1 and h["close"]<position.tief_tag_1: out.append(_signal("Schluss unter Tief Tag 1",20,"schluss",True,position.tief_tag_0,"Kap. 6.4","Stufe 1b"))
    if (not tag0_unter_notbremse) and position.tief_tag_0 and h["low"]<position.tief_tag_0: out.append(_signal("Intraday unter Tief Tag 0",20,"intraday",True,position.tief_tag_0,"Kap. 6.4","Stufe 2a"))
    if (not tag0_unter_notbremse) and position.tief_tag_0 and h["close"]<position.tief_tag_0: out.append(_signal("Schluss unter Tief Tag 0",20,"schluss",True,nb,"Kap. 6.4","Stufe 2b"))
    if pnl<=-7: out.append(_signal("7%-Notbremse (intraday)",100,"intraday",True,None,"Kap. 6.4","Stufe 3"))
    if (position.realisierte_tranchen or []) and position.pivot:
        d = daten[daten.index >= pd.Timestamp(position.einstiegsdatum)] if "index" in dir(daten) else daten
        closes = d["close"] if "close" in d else pd.Series(dtype=float)
        if len(closes):
            war_unter_pivot = float(closes.min()) <= float(position.pivot)
            erholt = float(closes.tail(20).max()) >= float(position.pivot) * 1.03
            jetzt_wieder = float(h["close"]) <= float(position.pivot)
            if war_unter_pivot and erholt and jetzt_wieder:
                out.append(_signal("Zweite Rückkehr zum Pivot",33,"schluss",True,nb,"Kap. 6.4 Ergänzung","Doppelter Rücklauf — klares Schwächesignal"))
    return out

def strategie_einfache_verluststufen(position,daten,verlust_stufe_1=3.0,verlust_stufe_2=5.0,verlust_stufe_3=7.0):
    pnl=pnl_pct(position,daten)
    s1=abs(float(verlust_stufe_1)); s2=max(abs(float(verlust_stufe_2)),s1); s3=max(abs(float(verlust_stufe_3)),s2)
    # Höchste Stufe zuerst prüfen, damit auch bei gleichen Schwellen (s1==s2==s3) das
    # stärkste passende Signal feuert, statt durch das offene Intervall „leerzulaufen".
    if pnl<=-s3:return [_signal(f"Verlust ≥ {s3:g}%",100,"intraday",True,None,"Kap. 6.4","Rest sofort schließen")]
    if pnl<=-s2:return [_signal(f"Verlust ≥ {s2:g}%",33,"intraday",True,position.einstiegspreis*(1-s3/100),"Kap. 6.4","Zweite Tranche")]
    if pnl<=-s1:return [_signal(f"Verlust ≥ {s1:g}%",33,"intraday",True,position.einstiegspreis*(1-s2/100),"Kap. 6.4","Erste Tranche")]
    return []

def strategie_atr_basiert(position,daten,ziel_atr_multiplikator=3,ueberdehnung_atr_start=3,ueberdehnung_atr_stark=4):
    p=pnl_pct(position,daten); s=letzter_schlusskurs(daten); a=atr(daten,14)
    if a is None or pd.isna(a) or a == 0: return []
    out=[]; ga=p/a
    if ga>=ziel_atr_multiplikator: out.append(_signal(f"{ga:.1f} ATR Gewinn erreicht",33,"schluss",True,None,"Kap. 6.4",f"Ziel {ziel_atr_multiplikator} ATR erreicht — Tranche in Stärke"))
    stop=position.einstiegspreis*(1-1.5*a/100)
    if s<=stop: out.append(_signal("Stopp bei -1.5 ATR",100,"intraday",True,None,"Kap. 6.4","Volatilitätsangepasste Stoppmarke gerissen"))
    e=_none_if_nan(ema(daten["close"],21).iloc[-1])
    if e:
        ab=(s-e)/e*100/a
        if ab>=ueberdehnung_atr_start: out.append(_signal(f"{ab:.1f} ATR über 21-EMA",50 if ab>=ueberdehnung_atr_stark else 33,"schluss",True,e,"Kap. 6.4","Überdehnt — volatilitätsbereinigt überhitzt"))
    return out

def berechne_watch_signale(position,daten):
    w=[]; pnl=pnl_pct(position,daten); ma21=ema(daten["close"],21); t=tage_unter_ma(daten,ma21); dd=drawdown_vom_peak(position,daten)
    if pnl>0 and t in [1,2]: w.append({"name":f"{t} Tage unter 21-EMA","buch_verweis":"Kap. 6.2"})
    if pnl>0 and 5<=dd<8: w.append({"name":f"Drawdown {dd:.1f}% vom Peak","buch_verweis":"Kap. 6.2"})
    udv = up_down_volume_ratio(daten,50)
    if udv is not None and udv < 1.0: w.append({"name":"Up/Down-Volume < 1.0","buch_verweis":"Kap. 5.3"})
    d=distribution_tage(daten,25)
    if d>=4: w.append({"name":f"{d} Distribution-Tage in 25 Sessions","buch_verweis":"Kap. 5.3"})
    return w


def diagnose_strategie_kein_signal(
    strategie_key: str,
    position: Position,
    daten: pd.DataFrame,
    wochen_daten: pd.DataFrame | None = None,
    daten_spy: pd.DataFrame | None = None,
    wochen_daten_spy: pd.DataFrame | None = None,
    markt: str = "Unsicher",
    strategie_optionen: dict | None = None,
) -> str:
    """Erklärt, warum eine Strategie aktuell kein aktives Signal liefert.

    Wird vom UI aufgerufen, wenn `verkaufs_empfehlung_gesamt` für diese
    Strategie eine leere Signalliste zurückgegeben hat. Liefert je nach
    Strategie-Konfiguration einen konkreten Grund (z. B. „Position im
    Verlust", „21-EMA noch nicht gebrochen", „Variante geduldig braucht
    mindestens 3 Tage unter MA"), so dass im Hub nicht nur eine generische
    Fallback-Meldung steht.
    """
    o = strategie_optionen or {}
    if daten is None or len(daten) == 0:
        return "Keine Kursdaten verfügbar."
    pnl = pnl_pct(position, daten)
    s = letzter_schlusskurs(daten)

    if strategie_key == "notbremse_verlust":
        b = abs(float(o.get("notbremse_verlust_schwelle_baerisch_pct", 4.0)))
        u = abs(float(o.get("notbremse_verlust_schwelle_unsicher_pct", 5.0)))
        bull = abs(float(o.get("notbremse_verlust_schwelle_bullisch_pct", 7.0)))
        sch = b if markt == "Bärisch" else u if markt == "Unsicher" else bull
        return (f"P&L {pnl:.1f}% liegt über der Notbremse-Schwelle (-{sch:g}%, Markt {markt}). "
                "Die Strategie zeigt normalerweise eine Info-Marke; falls hier nichts erscheint, liefert sie aktuell trotzdem kein Signal.")

    if strategie_key == "gewinn_in_stufen":
        if markt == "Bärisch":
            nd = float(o.get("gewinn_nachdenken_schwelle_bear_pct", 10.0))
            lo = float(max(o.get("gewinn_teilverkauf_unten_bear_pct", 10.0), nd))
        else:
            nd = float(o.get("gewinn_nachdenken_schwelle_bull_pct", 15.0))
            lo = float(max(o.get("gewinn_teilverkauf_unten_bull_pct", 20.0), nd))
        r = sum(position.realisierte_tranchen or [])
        if pnl < nd:
            return f"Aktueller Gewinn {pnl:.1f}% liegt unter der Nachdenkschwelle ({nd:g}%, Markt {markt}). Warten auf erste Gewinnzone."
        if pnl < lo and r > 0:
            return f"Nachdenkschwelle erreicht ({pnl:.1f}%), Pflicht-Teilverkauf erst ab {lo:g}%. Bereits {r:.0f}% realisiert — Hinweissignal ausgeblendet."
        if r >= 50:
            return f"Bereits {r:.0f}% realisiert — Pflicht-Teilverkauf abgedeckt."
        return f"Gewinn {pnl:.1f}% — derzeit zwischen den Stufen, kein Trigger aktiv."

    if strategie_key == "ma21_bruch":
        if pnl <= 0:
            return f"Position im Verlust ({pnl:.1f}%). Strategie ist nur im Gewinnfall aktiv (im Verlust greift Strategie 1 / rueckkehr_pivot)."
        variante = str(o.get("ma21_variante", "gestaffelt"))
        ma21 = ema(daten["close"], 21)
        if pd.isna(ma21.iloc[-1]):
            return "21-EMA konnte nicht berechnet werden (zu wenige Daten)."
        m = float(ma21.iloc[-1]); t = tage_unter_ma(daten, ma21)
        if t == 0:
            # Auch bei „kein Bruch" die aktive Variante nennen, damit User die
            # konfigurierte Aggressivität (z. B. ‚geduldig‘) in der Diagnose sehen.
            return f"Variante '{variante}': Kurs ({s:.2f}) liegt über der 21-EMA ({m:.2f}) — noch kein Bruch."
        if variante == "geduldig":
            return f"Variante 'geduldig': erst nach 3 Tagen unter 21-EMA aktiv, aktuell {t} Tag(e) unter MA."
        if variante == "aggressiv":
            br = (m - s) / m * 100 if m else 0
            vr = vol_verhaeltnis(daten)
            return f"Variante 'aggressiv': Bruch nur {br:.1f}% (≥2% nötig) oder Volumenfaktor {vr:.2f} (≥1.2 nötig); Tage unter MA: {t}."
        # gestaffelt
        if t == 2 and len(daten) >= 2 and not (s < float(daten["close"].iloc[-2])):
            return f"Variante 'gestaffelt': Tag 2 unter 21-EMA, aber Schluss ({s:.2f}) nicht tiefer als Vortag ({float(daten['close'].iloc[-2]):.2f})."
        return f"Variante 'gestaffelt': {t} Tag(e) unter 21-EMA, Stufe-Bedingung aktuell nicht erfüllt."

    if strategie_key == "drawdown_vom_peak":
        if pnl <= 0:
            return f"Position im Verlust ({pnl:.1f}%). Strategie greift nur im Gewinnfall."
        d1 = float(o.get("drawdown_stufe1_min_pct", 8.0))
        dd = drawdown_vom_peak(position, daten)
        return f"Drawdown vom Peak nur {dd:.1f}% — Stufe 1 startet erst ab {d1:g}%."

    if strategie_key == "ma_abstand":
        if pnl <= 0:
            return f"Position im Verlust ({pnl:.1f}%). Strategie greift nur im Gewinnfall."
        ma10 = _none_if_nan(sma(daten["close"], 10).iloc[-1])
        ma21 = _none_if_nan(ema(daten["close"], 21).iloc[-1])
        ma50 = _none_if_nan(sma(daten["close"], 50).iloc[-1])
        ma200 = _none_if_nan(sma(daten["close"], 200).iloc[-1])
        teile = []
        if ma10: teile.append(f"10-MA {((s-ma10)/ma10*100):+.1f}% (Schwelle {float(o.get('ma_abstand_schwelle_ma10_pct', 10.0)):g}%)")
        if ma21: teile.append(f"21-EMA {((s-ma21)/ma21*100):+.1f}% (Schwelle {float(o.get('ma_abstand_schwelle_ma21_pct', 15.0)):g}%)")
        if ma50: teile.append(f"50-MA {((s-ma50)/ma50*100):+.1f}% (Schwelle {float(o.get('ma_abstand_schwelle_ma50_pct', 25.0)):g}%)")
        if ma200: teile.append(f"200-MA {((s-ma200)/ma200*100):+.1f}% (Schwelle {float(o.get('ma_abstand_schwelle_ma200_pct', 70.0)):g}%)")
        return "Keine MA-Überdehnung erreicht: " + "; ".join(teile) if teile else "MA-Abstände nicht berechenbar (zu wenige Daten)."

    if strategie_key == "verlusttage_haeufung":
        if pnl <= 0:
            return f"Position im Verlust ({pnl:.1f}%). Strategie greift nur im Gewinnfall."
        fenster = int(o.get("verlusttage_updown_fenster_tage", 15))
        if len(daten) < max(3, fenster):
            return f"Zu wenige Daten ({len(daten)}) für Up/Down-Fenster von {fenster} Tagen."
        l3 = daten.tail(3); seq = 1
        for i in [1, 2]:
            if l3["close"].iloc[i] < l3["close"].iloc[i-1]: seq += 1
            else: seq = 1
        min_seq = int(o.get("verlusttage_min_tiefere_schlusskurse_in_folge", 3))
        fen = daten.tail(fenster); up = int((fen["close"] > fen["open"]).sum()); dn = int((fen["close"] < fen["open"]).sum())
        return (f"Nur {seq} tiefere Schlusskurse in Folge (≥{min_seq} nötig); "
                f"im {fenster}-Tage-Fenster {up} Auf- vs. {dn} Abwärtstage.")

    if strategie_key == "groesster_anstieg_volumen":
        if pnl <= 15:
            return f"Aktueller Gewinn {pnl:.1f}% liegt unter dem Mindestgewinn 15%."
        if len(daten) < 3:
            return f"Zu wenige Daten ({len(daten)}) für Klimax-Vergleich."
        daten_seit = _seit_einstieg(daten, position)
        if len(daten_seit) < 3:
            return f"Zu wenige Daten seit Einstieg ({len(daten_seit)}) für Klimax-Vergleich (mind. 3 Sessions nötig)."
        ch = daten_seit["close"].pct_change(fill_method=None) * 100
        vergleich = ch.iloc[1:-1] if len(ch) > 2 else ch.iloc[1:]
        ref = float(vergleich.max()) if len(vergleich) > 0 else float("nan")
        return f"Heutige Tagesveränderung {float(ch.iloc[-1]):+.1f}% ist nicht der größte Anstieg seit Einstieg ({ref:+.1f}%)."

    if strategie_key == "split_anstieg":
        split_datum = o.get("split_datum")
        if not split_datum:
            return "Kein Split-Datum gesetzt (kein automatischer Yahoo-Treffer und keine manuelle Eingabe)."
        try:
            split_dt = pd.Timestamp(split_datum).tz_localize(None)
        except Exception:
            return f"Split-Datum konnte nicht interpretiert werden: {split_datum!r}."
        d = daten.copy(); d.index = pd.to_datetime(d.index).tz_localize(None)
        if split_dt not in d.index:
            return f"Split-Datum {split_dt.date().isoformat()} liegt nicht im Datensatz."
        age = (d.index[-1] - split_dt).days
        if age > 14:
            return f"Split ist {age} Tage alt — Fenster (max. 14 Tage) überschritten."
        kurs = float(d.loc[split_dt, "close"]); an = (s/kurs - 1)*100 if kurs else 0
        schwelle = float(o.get("split_signal_schwelle_pct", 25.0))
        return f"Anstieg seit Split {an:+.1f}% liegt unter Signal-Schwelle {schwelle:g}%."

    if strategie_key == "erschoepfungsluecke":
        if not position.pivot:
            return "Kein Pivot in der Position hinterlegt — Distanz zur Basis nicht berechenbar."
        if pnl <= 15:
            return f"Gewinn {pnl:.1f}% liegt unter Mindestgewinn 15%."
        if len(daten) < 2:
            return "Zu wenige Daten für Gap-Berechnung."
        heute = daten.iloc[-1]; gestern = daten.iloc[-2]
        gap = (heute["open"] - gestern["close"]) / gestern["close"] * 100 if gestern["close"] else 0.0
        avg = float(daten["volume"].tail(50).mean()) if len(daten) >= 50 else float(daten["volume"].mean())
        vr = (float(heute["volume"]) / avg) if avg else 0.0
        dist = (float(heute["close"]) - float(position.pivot)) / float(position.pivot) * 100
        return f"Gap {gap:+.1f}% (≥3% nötig), Volumenfaktor {vr:.2f} (≥1.5 nötig), Distanz zur Basis {dist:+.1f}% (≥30% nötig)."

    if strategie_key == "downside_reversal":
        if pnl <= 0:
            return f"Position im Verlust ({pnl:.1f}%). Strategie greift nur im Gewinnfall."
        if len(daten) < 12:
            return f"Zu wenige Daten ({len(daten)}) — mindestens 12 Tage nötig."
        h = daten.iloc[-1]; span = h["high"] - h["low"]
        if span <= 0:
            return "Tagesspanne ist 0 (Kerze ohne Range)."
        return "Keine Umkehrkerze: weder neues 30-Tage-Hoch mit Schluss im unteren Drittel noch weite Reversal-Kerze nach Volumen-/Spannenkriterien."

    if strategie_key == "stau_tage":
        if pnl <= 0:
            return f"Position im Verlust ({pnl:.1f}%). Strategie greift nur im Gewinnfall."
        fenster = int(o.get("stau_fenster_tage", 10))
        if len(daten) < fenster:
            return f"Zu wenige Daten ({len(daten)}) für Stau-Fenster {fenster}."
        return f"Weniger als {int(o.get('stau_min_tage', 2))} Stau-Tage im {fenster}-Tage-Fenster (|Tagesveränderung|<{float(o.get('stau_max_tagesveraenderung_pct', 1.0)):g}% + Volumen ≥{float(o.get('stau_min_vol_ratio', 1.3)):.2f}× Schnitt)."

    if strategie_key == "rueckkehr_pivot":
        # Diese Strategie liefert idR immer mindestens ein Info-Signal, wenn relevante Werte gesetzt sind.
        tt1 = _none_if_nan(position.tief_tag_1)
        tt0 = _none_if_nan(position.tief_tag_0)
        pvt = _none_if_nan(position.pivot)
        if not (tt0 or tt1 or pvt):
            return "Keine Sicherheitslinien hinterlegt (Tief Tag 0/1, Pivot fehlen in der Position)."
        nb = abs(float(o.get("rueckkehr_notbremse_verlust_pct", 7.0)))
        teile = []
        if tt1: teile.append(f"Tief Tag 1 {float(tt1):.2f}")
        if tt0: teile.append(f"Tief Tag 0 {float(tt0):.2f}")
        if pvt: teile.append(f"Pivot {float(pvt):.2f}")
        return f"Schlusskurs {s:.2f} verletzt keine Sicherheitslinie ({', '.join(teile)}); Notbremse erst bei {pnl:.1f}% ≤ -{nb:g}%."

    if strategie_key == "ma_bruch_defensiv":
        ma50 = _none_if_nan(sma(daten["close"], 50).iloc[-1])
        ma200 = _none_if_nan(sma(daten["close"], 200).iloc[-1])
        teile = []
        if ma50: teile.append(f"50-MA {ma50:.2f} (Kurs {s:.2f})")
        if ma200: teile.append(f"200-MA {ma200:.2f}")
        if wochen_daten is not None and len(wochen_daten) >= 10:
            w10 = sma(wochen_daten["close"], 10)
            wt = tage_unter_ma(wochen_daten, w10)
            teile.append(f"{wt} Wochen unter 10-Wochen-Linie (≥8 für Vollsignal)")
        return "Kein 50-MA-/200-MA-/10-Wochen-Bruch: " + "; ".join(teile) if teile else "MAs nicht berechenbar."

    if strategie_key == "drei_verlustwochen":
        if wochen_daten is None or len(wochen_daten) < 3:
            return f"Zu wenige Wochenkerzen ({0 if wochen_daten is None else len(wochen_daten)}) — mindestens 3 nötig."
        l = wochen_daten.tail(3)
        three = (l["close"].iloc[0] > l["close"].iloc[1] > l["close"].iloc[2])
        vol = (l["volume"].iloc[0] < l["volume"].iloc[1] < l["volume"].iloc[2])
        red = bool((l["close"] < l["open"]).all())
        return f"Bedingungen letzte 3 Wochen: fallende Schlüsse={three}, steigendes Volumen={vol}, alle Wochen rot={red}."

    if strategie_key == "groesster_einbruch":
        min_pnl = float(o.get("groesster_einbruch_min_pnl_pct", 10.0))
        if pnl <= min_pnl:
            return f"Aktueller Gewinn {pnl:.1f}% liegt unter Mindestgewinn {min_pnl:g}% für diese Strategie."
        daten_seit = _seit_einstieg(daten, position)
        if len(daten_seit) < 4:
            return f"Zu wenige Daten seit Einstieg ({len(daten_seit)}) für Größter-Einbruch-Vergleich (mind. 4 Sessions nötig)."
        d = (daten_seit["close"].shift(1) - daten_seit["close"]) / daten_seit["close"].shift(1) * 100
        h = float(d.iloc[-1]); mx = float(d.iloc[1:-1].max()) if len(d) > 3 else 0.0
        min_verlust = float(o.get("groesster_einbruch_min_tagesverlust_pct", 3.0))
        # Heute war ein Gewinntag — kein Verlust zum Vergleichen.
        if h <= 0:
            return f"Heute ist ein Gewinntag (+{-h:.2f}%) — die Strategie reagiert nur auf Tagesverluste; bisher größter Verlust seit Einstieg {mx:+.2f}%."
        if h <= min_verlust:
            return f"Heutiger Tagesverlust {h:.2f}% liegt unter der Mindestschwelle ({min_verlust:g}%); bisher größter Verlust seit Einstieg {mx:.2f}%."
        return f"Heutiger Tagesverlust {h:.2f}% ist nicht der größte seit Einstieg (bisher max. {mx:.2f}%)."

    if strategie_key == "rs_linie":
        ctx = _rs_linie_context(
            position,
            daten,
            daten_spy,
            wochen_daten,
            wochen_daten_spy,
            o.get("rs_pnl_tag_zu_woche", 20.0),
            o.get("rs_pnl_woche_zu_monat", 80.0),
        )
        if ctx.get("error"):
            return ctx["error"]
        rs_wert = ctx.get("latest_rs")
        fast = ctx.get("latest_fast")
        slow = ctx.get("latest_slow")
        sp = ctx.get("sp")
        lp = ctx.get("lp")
        zeitebene = ctx.get("zeitebene")
        tage_unter_fast = int(ctx.get("days_under_fast") or 0)
        fallback_from = ctx.get("fallback_from")
        intended_reason = ctx.get("intended_reason")
        basis_detail = ctx.get("basis_detail")
        actual_points = ctx.get("actual_points")
        required_points = ctx.get("required_points")
        fallback_text = f"; Fallback von {fallback_from} auf {zeitebene} wegen zu kurzer RS-Historie" if fallback_from else ""
        fast_lage = "über" if rs_wert is not None and fast is not None and rs_wert >= fast else "unter"
        slow_lage = "über" if rs_wert is not None and slow is not None and rs_wert >= slow else "unter"
        return (
            f"RS-Linie ({zeitebene}) liegt aktuell {fast_lage} dem {sp}-MA und {slow_lage} dem {lp}-MA "
            f"(RS {rs_wert:.4f}, {sp}-MA {fast:.4f}, {lp}-MA {slow:.4f}; "
            f"{tage_unter_fast} Perioden in Folge unter schnellem MA; "
            f"Basis: {basis_detail} mit {actual_points}/{required_points} Datenpunkten; "
            f"Zeitebene wegen {intended_reason}{fallback_text})."
        )

    if strategie_key == "ma_basierte_sequenz":
        ma10 = _none_if_nan(sma(daten["close"], 10).iloc[-1])
        ma21 = _none_if_nan(ema(daten["close"], 21).iloc[-1])
        ma50 = _none_if_nan(sma(daten["close"], 50).iloc[-1])
        teile = [f"Gewinn {pnl:.1f}%"]
        if ma10: teile.append(f"10-MA {ma10:.2f} ({'über' if s >= ma10 else 'unter'})")
        if ma21: teile.append(f"21-EMA {ma21:.2f} ({'über' if s >= ma21 else 'unter'})")
        if ma50: teile.append(f"50-MA {ma50:.2f} ({'über' if s >= ma50 else 'unter'})")
        return "Kein Sequenzpunkt aktiv — " + "; ".join(teile) + "."

    if strategie_key == "einfach_halbe_position":
        gw = float(o.get("erste_haelfte_gewinn_pct", 20.0))
        r = sum(position.realisierte_tranchen or [])
        if pnl < gw and r < 50:
            return f"Aktueller Gewinn {pnl:.1f}% liegt unter Schwelle {gw:g}% (erste Hälfte sichern)."
        if r >= 50 and pnl < 20 and not (-1 <= pnl <= 1):
            return f"Erste Hälfte bereits realisiert ({r:.0f}%); zweite Tranche erst ab +20% Gewinn oder BE-Stopp."
        return f"Status passt aktuell zu keinem Trigger (Gewinn {pnl:.1f}%, realisiert {r:.0f}%)."

    if strategie_key == "misslungener_ausbruch_5stufen":
        if not (position.tief_tag_0 or position.tief_tag_1):
            return "Keine Tiefs Tag 0/Tag 1 in der Position hinterlegt — Stufen können nicht ausgewertet werden."
        return f"Weder Gap-down noch Intraday-/Schluss-Bruch der Tiefs Tag 0/1 noch -7%-Notbremse aktiv (Schluss {s:.2f}, P&L {pnl:.1f}%)."

    if strategie_key == "einfache_verluststufen":
        s1 = abs(float(o.get("verlust_stufe_1", 3.0)))
        if pnl > -s1:
            return f"P&L {pnl:.1f}% liegt über der ersten Verluststufe (-{s1:g}%) — keine Stufe aktiv."
        return f"P&L {pnl:.1f}% — Stufe-Logik aktuell zwischen den definierten Schwellen."

    if strategie_key == "atr_basiert":
        a = atr(daten, 14)
        if not a:
            return "ATR konnte nicht berechnet werden (zu wenige Daten)."
        ga = pnl / a if a else 0
        ziel = float(o.get("ziel_atr_multiplikator", 3.0))
        start = float(o.get("ueberdehnung_atr_start", 3.0))
        e = _none_if_nan(ema(daten["close"], 21).iloc[-1])
        ext = ((s - e) / e * 100 / a) if e and a else 0.0
        return (f"ATR-Gewinn {ga:.2f} ATR (Ziel {ziel:g} ATR nicht erreicht); "
                f"Abstand zu 21-EMA {ext:.2f} ATR (Überdehnung ab {start:g} ATR); kein -1.5 ATR-Stopp verletzt.")

    return "Keine Signale dieser Strategie aktiv — keine spezifische Diagnose hinterlegt."


def verkaufs_empfehlung_gesamt(position: Position, daten: pd.DataFrame, wochen_daten: pd.DataFrame, daten_spy: pd.DataFrame | None, wochen_daten_spy: pd.DataFrame | None, markt: str, industrie: str, aktive_strategien: list[str], strategie_optionen: dict | None = None):
    o = strategie_optionen or {}
    r = {
        "notbremse_verlust": lambda: strategie_notbremse_verlust(
            position,daten,markt,
            o.get("notbremse_verlust_schwelle_baerisch_pct",4.0),
            o.get("notbremse_verlust_schwelle_unsicher_pct",5.0),
            o.get("notbremse_verlust_schwelle_bullisch_pct",7.0),
        ),
        "gewinn_in_stufen": lambda: strategie_gewinn_in_stufen(
            position,daten,markt,
            o.get("gewinn_nachdenken_schwelle_bull_pct",15.0),
            o.get("gewinn_teilverkauf_unten_bull_pct",20.0),
            o.get("gewinn_teilverkauf_oben_bull_pct",35.0),
            o.get("gewinn_nachdenken_schwelle_bear_pct",10.0),
            o.get("gewinn_teilverkauf_unten_bear_pct",10.0),
            o.get("gewinn_teilverkauf_oben_bear_pct",15.0),
        ),
        "ma21_bruch": lambda: strategie_21ma_bruch(position,daten,o.get("ma21_variante","gestaffelt")),
        "drawdown_vom_peak": lambda: strategie_drawdown_vom_peak(
            position,daten,
            o.get("drawdown_stufe1_min_pct",8.0),
            o.get("drawdown_stufe2_min_pct",12.0),
            o.get("drawdown_stufe3_min_pct",15.0),
            o.get("drawdown_tranche_stufe1_pct",25.0),
            o.get("drawdown_tranche_stufe2_pct",33.0),
            o.get("drawdown_tranche_stufe3_ohne_trendbruch_pct",50.0),
            o.get("drawdown_tranche_stufe3_mit_trendbruch_pct",100.0),
        ),
        "ma_abstand": lambda: strategie_ma_abstand(position,daten,
            o.get("ma_abstand_schwelle_ma10_pct",10.0),
            o.get("ma_abstand_schwelle_ma21_pct",15.0),
            o.get("ma_abstand_schwelle_ma50_pct",25.0),
            o.get("ma_abstand_schwelle_ma200_pct",70.0),
            o.get("ma_abstand_klimax_ma200_vollausstieg_pct",100.0),
            o.get("ma_abstand_tranche_ma10_pct",25.0),
            o.get("ma_abstand_tranche_ma21_pct",33.0),
            o.get("ma_abstand_tranche_ma50_pct",33.0),
            o.get("ma_abstand_tranche_ma200_basis_pct",50.0),
        ),
        "verlusttage_haeufung": lambda: strategie_verlusttage_haeufung(
            position,daten,
            o.get("verlusttage_min_tiefere_schlusskurse_in_folge", 3),
            o.get("verlusttage_volumen_lookback_tage", 50),
            o.get("verlusttage_volumen_ratio_min", 1.1),
            o.get("verlusttage_tief_marker_lookback_tage", 5),
            o.get("verlusttage_updown_fenster_tage", 15),
            o.get("verlusttage_updown_diff_min", 3),
            o.get("verlusttage_tranche_pct", 25.0),
        ),
        "groesster_anstieg_volumen": lambda: strategie_groesster_anstieg_volumen(position,daten),
        "split_anstieg": lambda: strategie_split_anstieg(
            position,daten,
            o.get("split_datum"),
            o.get("split_signal_schwelle_pct", 25.0),
            o.get("split_starke_schwelle_pct", 50.0),
        ),
        "erschoepfungsluecke": lambda: strategie_erschoepfungsluecke(position,daten),
        "downside_reversal": lambda: strategie_downside_reversal(
            position,daten,
            o.get("downside_kerzenweite_lookback_tage", 10),
            o.get("downside_volumen_lookback_tage", 50),
            o.get("downside_neues_hoch_lookback_tage", 30),
            o.get("downside_weite_kerze_faktor", 1.5),
            o.get("downside_volumen_ratio_min", 1.2),
            o.get("downside_schluss_unteres_drittel_faktor", 3.0),
            o.get("downside_tranche_neues_hoch_pct", 33.0),
            o.get("downside_tranche_weite_umkehr_pct", 20.0),
            o.get("downside_tranche_warnstufe_pct", 15.0),
        ),
        "stau_tage": lambda: strategie_stau_tage(
            position,daten,
            o.get("stau_fenster_tage",10),
            o.get("stau_volumen_lookback_tage",50),
            o.get("stau_max_tagesveraenderung_pct",1.0),
            o.get("stau_min_vol_ratio",1.3),
            o.get("stau_min_tage",2),
            o.get("stau_nahe_hoch_drawdown_max_pct",5.0),
            o.get("stau_tranche_nahe_hoch_pct",33.0),
            o.get("stau_tranche_standard_pct",20.0),
        ),
        "rueckkehr_pivot": lambda: strategie_rueckkehr_pivot(
            position,daten,
            o.get("rueckkehr_tranche_stufe1_pct", 33.0),
            o.get("rueckkehr_tranche_stufe1_volumen_pct", 50.0),
            o.get("rueckkehr_volumen_schwelle", 1.5),
            o.get("rueckkehr_tranche_stufe2_pct", 33.0),
            o.get("rueckkehr_pivot_tage_schwelle", 10),
            o.get("rueckkehr_tranche_pivot_pct", 50.0),
            o.get("rueckkehr_notbremse_verlust_pct", 7.0),
        ),
        "ma_bruch_defensiv": lambda: strategie_ma_bruch_defensiv(position,daten,wochen_daten),
        "drei_verlustwochen": lambda: strategie_drei_verlustwochen(position,wochen_daten),
        "groesster_einbruch": lambda: strategie_groesster_einbruch(
            position,daten,wochen_daten,
            o.get("groesster_einbruch_min_pnl_pct",10.0),
            o.get("groesster_einbruch_min_tagesverlust_pct",3.0),
            o.get("groesster_einbruch_tagesvol_ratio_schwelle",1.5),
            o.get("groesster_einbruch_wochenvol_ratio_schwelle",1.3),
        ),
        "rs_linie": lambda: strategie_rs_linie(position,daten,daten_spy,wochen_daten,wochen_daten_spy,o.get("rs_pnl_tag_zu_woche",20.0),o.get("rs_pnl_woche_zu_monat",80.0)),
        "ma_basierte_sequenz": lambda: strategie_ma_basierte_sequenz(
            position,daten,
            o.get("ma_seq_gewinnzone_min_pct",20.0),
            o.get("ma_seq_gewinnzone_max_pct",25.0),
            o.get("ma_seq_gewinnzone_tranche_pct",33.0),
            o.get("ma_seq_ueber_ma10_pct",10.0),
            o.get("ma_seq_ueber_ma10_tranche_pct",20.0),
            o.get("ma_seq_unter_ma10_mindestgewinn_pct",5.0),
            o.get("ma_seq_unter_ma10_tranche_pct",20.0),
            o.get("ma_seq_pendel_tranche_pct",25.0),
            o.get("ma_seq_pendel_lookback_tage",5),
            o.get("ma_seq_pendel_wechsel_min",3),
            o.get("ma_seq_unter_ma21_mindestgewinn_pct",5.0),
            o.get("ma_seq_unter_ma21_tranche_pct",25.0),
            o.get("ma_seq_klarer_ma50_bruch_pct",2.0),
        ),
        "einfach_halbe_position": lambda: strategie_einfach_halbe_position(position,daten,o.get("erste_haelfte_gewinn_pct",20.0)),
        "misslungener_ausbruch_5stufen": lambda: strategie_misslungener_ausbruch_5stufen(position,daten),
        "einfache_verluststufen": lambda: strategie_einfache_verluststufen(position,daten,o.get("verlust_stufe_1",3.0),o.get("verlust_stufe_2",5.0),o.get("verlust_stufe_3",7.0)),
        "atr_basiert": lambda: strategie_atr_basiert(position,daten,o.get("ziel_atr_multiplikator",3),o.get("ueberdehnung_atr_start",3),o.get("ueberdehnung_atr_stark",4)),
    }
    all_signals=[]
    for k in aktive_strategien:
        if k in r:
            for sig in r[k]():
                if isinstance(sig, dict):
                    sig.setdefault("strategy_key", k)
                all_signals.append(sig)
    # Tag every signal with its theme for downstream consumers and deduplication.
    for sig in all_signals:
        if isinstance(sig, dict):
            sig.setdefault("thema", STRATEGY_THEMES.get(str(sig.get("strategy_key", "")), "sonstige"))

    killer=[s for s in all_signals if s["aktuell_aktiv"] and s["tranche_pct"]==100]
    if killer:
        ges, grund = 100, killer[0]["name"]
        themen_in_summe: list[str] = []
    else:
        act=[s for s in all_signals if s["aktuell_aktiv"] and s["tranche_pct"]>0]
        # Themen-Deduplizierung: pro Thema nur das Signal mit dem höchsten Tranche-Beitrag
        # in die Summe einbringen. Verhindert, dass mehrere Strategien dieselbe Ursache
        # (z. B. Pullback unter 21-EMA) zur Summe verstärken.
        best_per_theme: dict[str, dict] = {}
        for s in act:
            thema = str(s.get("thema") or "sonstige")
            current = best_per_theme.get(thema)
            if current is None or int(s["tranche_pct"]) > int(current["tranche_pct"]):
                best_per_theme[thema] = s
        dominanten = list(best_per_theme.values())
        themen_in_summe = list(best_per_theme.keys())
        summe=sum(s["tranche_pct"] for s in dominanten)
        # Breiten-Boost: ab 4 unterschiedlichen Themen mindestens 75% Ziel.
        if len(dominanten)>=4 and summe<75: summe=75
        st=[0,25,33,50,66,75,100]
        ges=max(v for v in st if v<=min(summe,100))
        if markt=="Bärisch" and 0<ges<100: ges=next((v for v in st if v>ges),100)
        grund=", ".join(s["name"] for s in dominanten[:3]) if dominanten else "keine aktiven Signale"
    schon=sum(position.realisierte_tranchen or [])
    return {
        "gesamt_tranche":ges,
        "bereits_realisiert":schon,
        "jetzt_zu_verkaufen":max(0,ges-schon),
        "haupt_grund":grund,
        "alle_signale":all_signals,
        "themen_in_summe":themen_in_summe,
    }
