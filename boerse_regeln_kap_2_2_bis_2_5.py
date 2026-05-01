"""
Codierbare Regeln aus dem Buch "Börse ohne Bauchgefühl" von A. M. Groos
Kapitel 2.2 bis einschließlich 2.5

Zweck:
    Dieses Modul fasst die im Buch beschriebenen Regeln in deterministischen
    Python-Funktionen zusammen, sodass eine eigene Implementierung schrittweise
    gegen diese Referenz geprüft werden kann.

Konventionen für Eingabedaten:
    Die meisten Funktionen erwarten einen pandas DataFrame mit Tagesdaten
    eines Index oder ETFs und mindestens den Spalten:
        - "open", "high", "low", "close", "volume"
    Der Index des DataFrames ist ein DatetimeIndex, sortiert aufsteigend.
    Alle Funktionen lesen die Daten nur, sie verändern den Eingabe-DataFrame
    nicht.

Hinweise zu Schwellenwerten:
    Wo das Buch Bandbreiten nennt (zum Beispiel "5 bis 6%"), werden die
    Bandgrenzen als Default-Parameter abgebildet. Werte können beim Aufruf
    überschrieben werden, das Default-Verhalten entspricht der Buchvorgabe.

Die Regeln sind nach Buchabschnitt gruppiert. Jede Regel verweist im
Docstring auf den zugehörigen Unterabschnitt.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence

import numpy as np
import pandas as pd


# =====================================================================
# Hilfsfunktionen (keine eigenständigen Buch-Regeln)
# =====================================================================

def sma(series: pd.Series, window: int) -> pd.Series:
    """Einfacher gleitender Durchschnitt über window Tage."""
    return series.rolling(window=window, min_periods=window).mean()


def ema(series: pd.Series, window: int) -> pd.Series:
    """Exponentiell gewichteter gleitender Durchschnitt über window Tage."""
    return series.ewm(span=window, adjust=False, min_periods=window).mean()


def average_true_range(df: pd.DataFrame, window: int = 21) -> pd.Series:
    """
    Average True Range nach Wilder-Definition, gemittelt über window Tage.

    Buchbezug Abschnitt 2.2, "Abstand des Index zu gleitenden Durchschnitten":
        Die ATR basiert auf der Spanne zwischen Tageshoch und Tagestief und
        berücksichtigt zusätzlich Kurslücken zum Vortag. Aus den letzten 21
        Handelstagen wird ein Durchschnitt gebildet.
    """
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=window, min_periods=window).mean()


# =====================================================================
# Abschnitt 2.2 – Marktkorrekturen erkennen
# =====================================================================

# ---------------------------------------------------------------------
# 2.2.A – Definition einer Korrektur
# ---------------------------------------------------------------------

class CorrectionCategory(str, Enum):
    """Kategorisierung des Drawdowns gemäß Abschnitt 2.2."""
    NORMAL_PULLBACK = "normaler_pullback"   # weniger als 10%
    MILD = "milde_korrektur"                # 10% bis 12%
    MODERATE = "moderate_korrektur"         # 13% bis 16%
    DEEP = "tiefe_korrektur"                # ab 17%
    BEAR_MARKET = "baerenmarkt"             # ab 20%


def classify_correction(close: pd.Series) -> pd.DataFrame:
    """
    Berechnet den aktuellen Drawdown vom Allzeithoch (rolling max ab Reihenbeginn)
    und ordnet ihn den Kategorien aus Abschnitt 2.2 zu.

    Regeln aus dem Buch:
        - Korrektur ab etwa 10% Rückgang vom letzten Hoch
        - Bärenmarkt ab 20% Rückgang vom Hoch
        - Milde Korrektur: 10 bis 12%
        - Moderate Korrektur: 13 bis 16%
        - Tiefe Korrektur: ab 17%
        - Aufmerksamkeit ab 8 bis 10% Rückgang

    Rückgabe:
        DataFrame mit Spalten "drawdown_pct" (negativ, in Prozent) und
        "category" (CorrectionCategory).
    """
    running_max = close.cummax()
    drawdown_pct = (close / running_max - 1.0) * 100.0

    def categorize(dd: float) -> CorrectionCategory:
        # dd ist negativ oder 0
        if dd > -10.0:
            return CorrectionCategory.NORMAL_PULLBACK
        if dd > -13.0:
            return CorrectionCategory.MILD
        if dd > -17.0:
            return CorrectionCategory.MODERATE
        if dd > -20.0:
            return CorrectionCategory.DEEP
        return CorrectionCategory.BEAR_MARKET

    category = drawdown_pct.apply(categorize)
    return pd.DataFrame({"drawdown_pct": drawdown_pct, "category": category})


def is_attention_zone(close: pd.Series, threshold_pct: float = 8.0) -> pd.Series:
    """
    Liefert eine boolesche Reihe: True ab einem Rückgang von mindestens
    threshold_pct vom letzten Hoch. Default 8% gemäß Buchregel
    "Spätestens ab einem Rückgang von etwa 8 bis 10% solltest du sehr
    aufmerksam werden."
    """
    running_max = close.cummax()
    drawdown_pct = (close / running_max - 1.0) * 100.0
    return drawdown_pct <= -abs(threshold_pct)


# ---------------------------------------------------------------------
# 2.2.B – Frühwarnzeichen
# ---------------------------------------------------------------------

def intraday_reversal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prüft auf Intraday-Umkehrungen gemäß Abschnitt 2.2.

    Buchregel:
        - Bullisches Muster: schwacher Start, Schluss deutlich über Eröffnung
        - Bärisches Muster: starker Start, Schluss deutlich unter Eröffnung

    Konkret operationalisiert:
        - bullish_reversal: open < prev_close und close > open
        - bearish_reversal: open > prev_close und close < open

    Rückgabe:
        DataFrame mit Spalten "bullish_reversal" und "bearish_reversal" (bool).
    """
    prev_close = df["close"].shift(1)
    bullish = (df["open"] < prev_close) & (df["close"] > df["open"])
    bearish = (df["open"] > prev_close) & (df["close"] < df["open"])
    return pd.DataFrame({"bullish_reversal": bullish, "bearish_reversal": bearish})


def closing_range(df: pd.DataFrame) -> pd.Series:
    """
    Closing Range: Position des Schlusskurses innerhalb der Tagesspanne,
    als Wert zwischen 0 (Schluss am Tagestief) und 1 (Schluss am Tageshoch).

    Buchbezug Abschnitt 2.2 "Abschlüsse am Tagestief":
        Bei Schluss wiederholt nahe dem Tagestief (low closing range)
        liegt anhaltender Verkaufsdruck vor.
    """
    span = df["high"] - df["low"]
    # Bei span == 0 (theoretisch) wird der Wert auf 0.5 gesetzt
    cr = (df["close"] - df["low"]) / span.replace(0, np.nan)
    return cr.fillna(0.5)


def low_closing_range_streak(
    df: pd.DataFrame,
    threshold: float = 0.3,
    min_streak: int = 3,
) -> pd.Series:
    """
    Markiert Tage, an denen der Schlusskurs an mindestens min_streak
    aufeinanderfolgenden Handelstagen im unteren threshold-Bereich der
    Tagesspanne liegt.

    Default: untere 30%, mindestens 3 Tage in Folge.
    """
    cr = closing_range(df)
    is_low = cr <= threshold
    streak = is_low.rolling(window=min_streak, min_periods=min_streak).sum()
    return streak >= min_streak


def distance_to_ma_in_atr(
    df: pd.DataFrame,
    ma_window: int = 21,
    atr_window: int = 21,
) -> pd.Series:
    """
    Abstand zwischen Schlusskurs und gleitendem Durchschnitt, ausgedrückt in
    ATR-Einheiten. Positive Werte bedeuten Schluss über dem Durchschnitt.

    Buchbezug Abschnitt 2.2:
        Du drückst den Abstand zur 21-Tage-Linie in ATR-Einheiten aus.
        Ab etwa drei ATR ist der Index kurzfristig oft überdehnt.
    """
    ma = sma(df["close"], ma_window)
    atr = average_true_range(df, atr_window)
    return (df["close"] - ma) / atr


def is_overextended_atr(
    df: pd.DataFrame,
    threshold_atr: float = 3.0,
    ma_window: int = 21,
    atr_window: int = 21,
) -> pd.Series:
    """
    True, wenn der Schluss mehr als threshold_atr Tagesspannen über
    der 21-Tage-Linie liegt. Default 3 ATR (Buchregel).
    """
    return distance_to_ma_in_atr(df, ma_window, atr_window) >= threshold_atr


def pct_distance_to_50dma(close: pd.Series) -> pd.Series:
    """Prozentualer Abstand des Schlusskurses zur 50-Tage-Linie."""
    ma50 = sma(close, 50)
    return (close / ma50 - 1.0) * 100.0


def is_overextended_pct_sp500(close: pd.Series, threshold_pct: float = 5.0) -> pd.Series:
    """
    Buchregel S&P 500:
        Pullback-Risiko steigt häufig deutlich, wenn der Index 5 bis 6%
        über der 50-Tage-Linie notiert.
    Default: 5% (untere Grenze des Buchbereichs).
    """
    return pct_distance_to_50dma(close) >= threshold_pct


def is_overextended_pct_nasdaq(close: pd.Series, threshold_pct: float = 7.0) -> pd.Series:
    """
    Buchregel Nasdaq:
        Pullback-Risiko steigt häufig deutlich, wenn der Index 7 bis 8%
        über der 50-Tage-Linie notiert.
    Default: 7% (untere Grenze des Buchbereichs).
    """
    return pct_distance_to_50dma(close) >= threshold_pct


# ---------------------------------------------------------------------
# 2.2.C – Bruch wichtiger gleitender Durchschnitte
# ---------------------------------------------------------------------

def below_ma(close: pd.Series, window: int) -> pd.Series:
    """True, wenn der Schluss unter dem gleitenden Durchschnitt notiert."""
    return close < sma(close, window)


def trend_warning_levels(close: pd.Series) -> pd.DataFrame:
    """
    Buchregel Abschnitt 2.2:
        - Schluss unter 21-Tage-Linie: erste Trendwarnung
        - Schluss unter 50-Tage-Linie: deutliche Trendwarnung
        - Schluss unter 200-Tage-Linie: langfristiger Trend gilt als gebrochen,
          Aktienquote deutlich reduzieren oder komplett aussteigen.
    """
    return pd.DataFrame(
        {
            "below_21": below_ma(close, 21),
            "below_50": below_ma(close, 50),
            "below_200": below_ma(close, 200),
        }
    )


# ---------------------------------------------------------------------
# 2.2.D – Distribution Days und Stau-Tage
# ---------------------------------------------------------------------

def is_distribution_day(
    df: pd.DataFrame,
    min_loss_pct: float = 0.2,
    volume_lookback: int = 50,
) -> pd.Series:
    """
    Distributionstag gemäß Abschnitt 2.2:
        - Tagesschluss negativ
        - Volumen höher als am Vortag ODER über dem 50-Tage-Volumendurchschnitt

    Parameter:
        min_loss_pct: Mindestverlust in Prozent, damit ein leichter Minus-
            Schluss nicht als Distribution gilt. Buch: ein "negativer Schluss"
            mit "ungewöhnlich viel Aktivität". Default 0.2% als pragmatischer
            Filter für Rauschen.
        volume_lookback: Fensterlänge für den Volumendurchschnitt. Default 50.
    """
    pct_change = df["close"].pct_change() * 100.0
    vol_prev = df["volume"].shift(1)
    vol_avg = df["volume"].rolling(window=volume_lookback, min_periods=volume_lookback).mean().shift(1)

    higher_volume = (df["volume"] > vol_prev) | (df["volume"] > vol_avg)
    negative_close = pct_change <= -abs(min_loss_pct)
    return negative_close & higher_volume


def count_distribution_days(
    df: pd.DataFrame,
    window_days: int = 25,
    **kwargs,
) -> pd.Series:
    """
    Zählt Distributionstage innerhalb eines rollierenden Fensters.

    Buchregel Abschnitt 2.2:
        Häufung mehrerer Distributionstage innerhalb weniger Wochen ist
        ein ernstes Warnsignal. Ein gängiges Fenster sind etwa 4 bis 5
        Handelswochen, hier als 25 Tage parametrisiert.
    """
    dd = is_distribution_day(df, **kwargs)
    return dd.rolling(window=window_days, min_periods=1).sum()


def is_stalling_day(
    df: pd.DataFrame,
    max_gain_pct: float = 0.5,
    min_volume_ratio: float = 0.95,
) -> pd.Series:
    """
    Stau-Tag gemäß Abschnitt 2.2:
        - Index schließt im Plus oder nur leicht im Minus
        - Tagesgewinn sehr gering (Default unter 0,5%)
        - Volumen mindestens 95% des Vortagsvolumens

    Default-Parameter entsprechen dem Wortlaut des Buchs.
    """
    pct_change = df["close"].pct_change() * 100.0
    vol_prev = df["volume"].shift(1)
    small_change = pct_change.abs() < max_gain_pct
    high_volume = df["volume"] >= min_volume_ratio * vol_prev
    return small_change & high_volume


# ---------------------------------------------------------------------
# 2.2.E – Drei-Wellen-Top
# ---------------------------------------------------------------------

def detect_three_push_top(
    df: pd.DataFrame,
    pivot_window: int = 10,
    min_pushes: int = 3,
) -> pd.Series:
    """
    Heuristische Erkennung eines Drei-Schübe-Tops.

    Buchbezug Abschnitt 2.2 "Drei Wellen aufwärts":
        Drei aufeinanderfolgende Schübe mit jeweils neuen Hochs, kombiniert
        mit nachlassendem Volumen zum zweiten Peak und hohem Volumen auf der
        Abwärtsseite des dritten Peaks.

    Diese Funktion liefert für jeden Tag, ob er der dritte Peak einer solchen
    Sequenz aus aufsteigenden Pivot-Hochs ist. Die genaue Volumeninterpretation
    bleibt der individuellen Implementierung überlassen, hier wird nur das
    Kursmuster geprüft.

    Parameter:
        pivot_window: halbe Fensterbreite, in der ein lokales Hoch das
            absolute Maximum sein muss.
        min_pushes: Anzahl aufsteigender Pivot-Hochs in Folge.
    """
    high = df["high"]
    is_pivot_high = (
        (high == high.rolling(pivot_window * 2 + 1, center=True).max())
        & high.notna()
    )

    result = pd.Series(False, index=df.index)
    pivots = high[is_pivot_high]
    if len(pivots) < min_pushes:
        return result

    # Schiebefenster über aufsteigende Pivot-Hochs
    pivot_values = pivots.values
    pivot_index = pivots.index
    for i in range(min_pushes - 1, len(pivot_values)):
        window_vals = pivot_values[i - min_pushes + 1 : i + 1]
        if all(window_vals[k] < window_vals[k + 1] for k in range(len(window_vals) - 1)):
            result.loc[pivot_index[i]] = True
    return result


# ---------------------------------------------------------------------
# 2.2.F – VIX und VXX
# ---------------------------------------------------------------------

def vix_panic_signal(vix_close: pd.Series, sma_window: int = 10, threshold_pct: float = 20.0) -> pd.Series:
    """
    Buchregel Abschnitt 2.2 "VIX – der Angst-Index":
        Notiert der VIX mehr als etwa 20% über seiner 10-Tage-Linie, spricht
        das häufig für eine panikartige Übertreibung nach unten im Aktienmarkt.

    Parameter:
        vix_close: Tagesschluss des VIX-Index.
    """
    ma = sma(vix_close, sma_window)
    return vix_close >= ma * (1.0 + threshold_pct / 100.0)


def vxx_risk_off(vxx_close: pd.Series, ema_window: int = 21) -> pd.Series:
    """
    Buchregel Abschnitt 2.2 "VXX – die Alternative zum VIX":
        Liegt der VXX über einer steigenden 21-Tage-Linie, nimmt die kurz-
        fristige Volatilität zu und das Risiko sollte gedrosselt werden.
    """
    ema_line = ema(vxx_close, ema_window)
    rising = ema_line.diff() > 0
    return (vxx_close > ema_line) & rising


def vxx_risk_on(vxx_close: pd.Series, ema_window: int = 21) -> pd.Series:
    """
    Buchregel Abschnitt 2.2:
        Liegt der VXX unter einer fallenden 21-Tage-Linie, entspannt sich
        die Volatilität, Risk-on-Strategien sind dann meist von Vorteil.
    """
    ema_line = ema(vxx_close, ema_window)
    falling = ema_line.diff() < 0
    return (vxx_close < ema_line) & falling


# ---------------------------------------------------------------------
# 2.2.G – Margin Debt und Sentiment
# ---------------------------------------------------------------------

def margin_debt_warning(margin_growth_pct_yoy: pd.Series, threshold_pct: float = 55.0) -> pd.Series:
    """
    Buchregel Abschnitt 2.2 "Kreditfinanzierte Aktienkäufe":
        Historisch galt ein Anstieg der Margin Debt von über etwa 55% als
        Warnsignal.

    Eingabe:
        margin_growth_pct_yoy: Jahreswachstum der Margin Debt in Prozent.
    """
    return margin_growth_pct_yoy >= threshold_pct


# ---------------------------------------------------------------------
# 2.2.H – Erste Abwärtswelle und Wellenstruktur
# ---------------------------------------------------------------------

def steep_first_leg_signal(
    df: pd.DataFrame,
    window_days: int = 20,
    big_loss_pct: float = -3.0,
    loss_to_gain_ratio: float = 3.0,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.2 "Die erste Abwärtswelle":
        - Mehr Verlust- als Gewinntage, etwa 3 bis 4 zu 1
        - Mindestens ein Tag mit Verlust größer 3%
        - Dieser Tag bricht zentrale Marken (50- oder 200-Tage-Linie)

    Diese Funktion prüft die ersten beiden Bedingungen über ein rollierendes
    Fenster und gibt True am Ende des Fensters zurück, wenn beide erfüllt sind.
    Die dritte Bedingung wird mit `trend_warning_levels` separat geprüft.
    """
    pct = df["close"].pct_change() * 100.0
    losses = (pct < 0).astype(int).rolling(window_days).sum()
    gains = (pct > 0).astype(int).rolling(window_days).sum()
    has_big_loss = (pct <= big_loss_pct).rolling(window_days).max().astype(bool)
    return (losses >= loss_to_gain_ratio * gains.replace(0, np.nan)) & has_big_loss


# ---------------------------------------------------------------------
# 2.2.I – Scheiternde Rallyes
# ---------------------------------------------------------------------

def is_low_volume_rally(
    df: pd.DataFrame,
    rally_days: int = 5,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.2 "Abnehmendes Volumen bei steigenden Kursen":
        Steigt der Index an Tag drei, vier oder fünf einer Rally weiter,
        während das Handelsvolumen von Tag zu Tag zurückgeht, ist das ein
        Warnsignal.

    Operationalisiert: Schluss höher als vor rally_days Tagen UND Volumen
    hat in den letzten rally_days Tagen monoton abgenommen.
    """
    higher_close = df["close"] > df["close"].shift(rally_days)
    vol_decreasing = df["volume"].diff().rolling(rally_days).max() < 0
    return higher_close & vol_decreasing


def recovery_quote(prior_high: float, first_low: float, current: float) -> float:
    """
    Buchregel Abschnitt 2.2 "Geringe Erholungsquote vom ersten Absturz":
        Eine gesunde Erholung holt mindestens die Hälfte der ersten
        Abwärtsbewegung wieder auf.

    Rückgabe:
        Erholungsquote als Bruchteil zwischen 0 und 1 (oder darüber). Bei
        einer Quote unter 0,5 spricht das Buch von Schwäche.
    """
    if prior_high <= first_low:
        return float("nan")
    return (current - first_low) / (prior_high - first_low)


def weak_rally_failed(prior_high: float, first_low: float, current: float) -> bool:
    """True, wenn die Erholungsquote unter 50% liegt (Buchregel)."""
    quote = recovery_quote(prior_high, first_low, current)
    return quote < 0.5 if not np.isnan(quote) else False


# =====================================================================
# Abschnitt 2.3 – Vom Bären zum Bullen: Trendwende-Ampel
# =====================================================================

class TrendwendePhase(str, Enum):
    """Phasen der Trendwende-Ampel aus Abschnitt 2.3."""
    ROT = "rot"      # Abwarten und Boden beobachten
    GELB = "gelb"    # Startschuss erfolgt, kontrollierter Wiedereinstieg
    GRUEN = "gruen"  # Frühe Bestätigung


@dataclass
class Ankertag:
    """
    Ankertag gemäß Abschnitt 2.3 Rot-Phase.

    Attribute:
        date: Datum des Ankertags.
        bodenmarke: Tiefstes Tief am Ankertag oder am Vortag, je nachdem
            welcher Wert tiefer liegt.
    """
    date: pd.Timestamp
    bodenmarke: float


def has_substantial_correction(
    df: pd.DataFrame,
    min_drawdown_pct: float = 10.0,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.3 Rot-Phase, erste Bedingung:
        Ein echtes Trendwendesignal ist nur nach einer substanziellen
        Korrektur relevant. In der Praxis: Index notiert signifikant unter
        seinen längerfristigen Durchschnitten.

    Operationalisiert: Drawdown vom letzten Hoch mindestens min_drawdown_pct
    UND Schluss unter 200-Tage-Linie.
    """
    dd = (df["close"] / df["close"].cummax() - 1.0) * 100.0
    return (dd <= -abs(min_drawdown_pct)) & below_ma(df["close"], 200)


def has_stopped_falling(
    df: pd.DataFrame,
    lookback_days: int = 5,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.3 Rot-Phase, zweite Bedingung:
        Der Index hört auf, neue Tiefs zu markieren.

    Operationalisiert: Tiefstes Tief der letzten lookback_days Tage liegt
    nicht über dem Tief der Vorperiode. True, wenn das aktuelle Tief NICHT
    das tiefste der erweiterten Vorperiode ist (also kein neues Tief markiert
    wurde).
    """
    recent_low = df["low"].rolling(window=lookback_days, min_periods=lookback_days).min()
    earlier_low = df["low"].rolling(window=lookback_days * 2, min_periods=lookback_days * 2).min()
    return recent_low > earlier_low


def detect_ankertag(df: pd.DataFrame) -> pd.Series:
    """
    Buchregel Abschnitt 2.3 Rot-Phase, dritte Bedingung:
        Ankertag = erster Tag nach Stabilisierung, an dem
        - der Index im Plus schließt ODER
        - der Schluss in der oberen Hälfte der Tagesspanne liegt.

    Liefert eine boolesche Reihe.
    """
    closed_up = df["close"] > df["close"].shift(1)
    upper_half = closing_range(df) >= 0.5
    return closed_up | upper_half


def bodenmarke_for_ankertag(df: pd.DataFrame, ankertag_idx: int) -> float:
    """
    Buchregel Abschnitt 2.3:
        Bodenmarke = tieferes Tief von Ankertag oder unmittelbarem Vortag.
    """
    if ankertag_idx < 1:
        return df["low"].iloc[ankertag_idx]
    return min(df["low"].iloc[ankertag_idx], df["low"].iloc[ankertag_idx - 1])


def bodenmarke_held(df: pd.DataFrame, ankertag_idx: int, current_idx: int) -> bool:
    """
    Buchregel Abschnitt 2.3 Rot-Phase:
        Solange der Index die Bodenmarke nicht intraday unterschreitet,
        bleibt die Beobachtung aktiv.

    Rückgabe:
        True, wenn die Bodenmarke vom Tag nach Ankertag bis current_idx
        durchgehend nicht intraday gerissen wurde.
    """
    if current_idx < ankertag_idx:
        return False
    boden = bodenmarke_for_ankertag(df, ankertag_idx)
    relevant_lows = df["low"].iloc[ankertag_idx + 1 : current_idx + 1]
    if relevant_lows.empty:
        return True
    return relevant_lows.min() >= boden


def is_startschuss(
    df: pd.DataFrame,
    ankertag_idx: int,
    current_idx: int,
    min_days_after_ankertag: int = 4,
    min_gain_pct: float = 1.0,
    require_volume_increase: bool = True,
) -> bool:
    """
    Buchregel Abschnitt 2.3 Gelb-Phase – Startschuss:
        - Mindestens 4 vollständige Handelstage seit dem Ankertag (also
          frühestens am 5. Tag nach Ankertag).
        - Tagesgewinn mindestens 1% gegenüber Vortagesschluss (in
          hochvolatilen Phasen ggf. höhere Schwelle).
        - Handelsvolumen über dem des Vortags.
        - Bodenmarke an diesem Tag intraday nicht gerissen.

    Bonuspunkt (nicht Voraussetzung): Schluss über der 21-Tage-Linie.
    """
    if current_idx - ankertag_idx <= min_days_after_ankertag:
        return False
    if not bodenmarke_held(df, ankertag_idx, current_idx):
        return False

    today_close = df["close"].iloc[current_idx]
    prev_close = df["close"].iloc[current_idx - 1]
    pct = (today_close / prev_close - 1.0) * 100.0
    if pct < min_gain_pct:
        return False

    if require_volume_increase:
        if df["volume"].iloc[current_idx] <= df["volume"].iloc[current_idx - 1]:
            return False

    return True


def startschuss_bonus_above_21dma(df: pd.DataFrame, current_idx: int) -> bool:
    """Bonuspunkt aus Abschnitt 2.3: Schluss über der 21-Tage-Linie."""
    ma21 = sma(df["close"], 21).iloc[current_idx]
    return df["close"].iloc[current_idx] > ma21


def startschuss_holds(
    df: pd.DataFrame,
    startschuss_idx: int,
    current_idx: int,
) -> bool:
    """
    Buchregel Abschnitt 2.3 Grün-Phase, Validierungskriterium:
        Solange der Index in den Tagen nach dem Startschuss nicht auf
        Schlusskursbasis unter das Tief des Startschuss-Tages fällt, bleibt
        das Signal intakt. Fällt er darunter, geht die Ampel zurück auf Rot.
    """
    if current_idx < startschuss_idx:
        return False
    anchor_low = df["low"].iloc[startschuss_idx]
    closes = df["close"].iloc[startschuss_idx + 1 : current_idx + 1]
    if closes.empty:
        return True
    return closes.min() >= anchor_low


# ---------------------------------------------------------------------
# 2.3 – Risikomanagement-Parameter beim Startschuss
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class StartschussRiskRules:
    """
    Risikoregeln am Startschuss aus Abschnitt 2.3 Gelb-Phase.

    Werte direkt aus dem Buch (Defaults):
        min_capital_pct: 10
        max_capital_pct: 30
        min_positions: 1
        max_positions: 3
        stop_loss_min_pct: 5
        stop_loss_max_pct: 7
        max_risk_per_position_pct: 0.5
    """
    min_capital_pct: float = 10.0
    max_capital_pct: float = 30.0
    min_positions: int = 1
    max_positions: int = 3
    stop_loss_min_pct: float = 5.0
    stop_loss_max_pct: float = 7.0
    max_risk_per_position_pct: float = 0.5


def validate_startschuss_position(
    capital_pct: float,
    n_positions: int,
    stop_loss_pct: float,
    risk_per_position_pct: float,
    rules: StartschussRiskRules = StartschussRiskRules(),
) -> dict:
    """
    Prüft, ob eine geplante Startschuss-Position den Buchregeln entspricht.

    Rückgabe:
        Dict mit Schlüssel "ok" (bool) und einer Liste "violations".
    """
    violations = []
    if not (rules.min_capital_pct <= capital_pct <= rules.max_capital_pct):
        violations.append(
            f"Kapitaleinsatz {capital_pct}% außerhalb {rules.min_capital_pct}-{rules.max_capital_pct}%"
        )
    if not (rules.min_positions <= n_positions <= rules.max_positions):
        violations.append(
            f"Positionsanzahl {n_positions} außerhalb {rules.min_positions}-{rules.max_positions}"
        )
    if not (rules.stop_loss_min_pct <= stop_loss_pct <= rules.stop_loss_max_pct):
        violations.append(
            f"Stop {stop_loss_pct}% außerhalb {rules.stop_loss_min_pct}-{rules.stop_loss_max_pct}%"
        )
    if risk_per_position_pct > rules.max_risk_per_position_pct:
        violations.append(
            f"Risiko pro Position {risk_per_position_pct}% > {rules.max_risk_per_position_pct}%"
        )
    return {"ok": not violations, "violations": violations}


# =====================================================================
# Abschnitt 2.4 – Aufwärtstrend
# =====================================================================

class UptrendStage(str, Enum):
    """Stufen des Aufwärtstrend-Aufbaus aus Abschnitt 2.4."""
    NONE = "kein_aufwaertstrend"
    ABOVE_200 = "ueber_200"            # Voraussetzung erfüllt
    ABOVE_21 = "ueber_21"              # Erste Markterholung (mind. 3 Tage)
    ABOVE_50 = "ueber_50"              # Nächste Stufe (mind. 3 Tage)
    GOLDEN_CROSS_21_OVER_50 = "21_ueber_50"  # 21-Tage kreuzt 50-Tage von unten
    FULL_STACK = "voller_aufwaertstrend"     # 21 > 50 > 200, alle aufsteigend


def closes_above_ma_for_n_days(close: pd.Series, ma_window: int, n_days: int = 3) -> pd.Series:
    """
    Buchregel Abschnitt 2.4:
        Der Index sollte mindestens drei Tage über einer Linie schließen,
        ohne sie erneut zu berühren.

    Operationalisiert: An n_days aufeinanderfolgenden Tagen liegen sowohl
    Schluss als auch Tagestief über dem gleitenden Durchschnitt.
    """
    ma = sma(close, ma_window)
    above = close > ma
    return above.rolling(window=n_days, min_periods=n_days).sum() == n_days


def low_above_ma_for_n_days(df: pd.DataFrame, ma_window: int, n_days: int = 3) -> pd.Series:
    """
    Strengere Variante: Tagestief liegt an n_days aufeinanderfolgenden Tagen
    über dem gleitenden Durchschnitt (entspricht "ohne sie erneut zu berühren").
    """
    ma = sma(df["close"], ma_window)
    above = df["low"] > ma
    return above.rolling(window=n_days, min_periods=n_days).sum() == n_days


def ma_crossover(close: pd.Series, fast: int, slow: int, hold_days: int = 3) -> pd.Series:
    """
    Buchregel Abschnitt 2.4:
        Die 21-Tage-Linie kreuzt die 50-Tage-Linie von unten nach oben und
        diese Konstellation bleibt mindestens drei Tage bestehen.
    """
    fast_ma = sma(close, fast)
    slow_ma = sma(close, slow)
    above = fast_ma > slow_ma
    return above.rolling(window=hold_days, min_periods=hold_days).sum() == hold_days


def ma_full_stack(close: pd.Series) -> pd.Series:
    """
    Buchregel Abschnitt 2.4:
        Finales Zeichen eines intakten Aufwärtstrends:
        21-Tage-Linie über 50-Tage-Linie, beide über 200-Tage-Linie.
    """
    ma21 = sma(close, 21)
    ma50 = sma(close, 50)
    ma200 = sma(close, 200)
    return (ma21 > ma50) & (ma50 > ma200)


def classify_uptrend_stage(df: pd.DataFrame) -> pd.Series:
    """
    Liefert die jeweils höchste erreichte Stufe nach Abschnitt 2.4.
    Stufen können in der Praxis in unterschiedlicher Reihenfolge auftreten,
    deshalb wird hier konservativ ausgewertet.
    """
    close = df["close"]
    ma200 = sma(close, 200)
    above_200 = close > ma200
    full = ma_full_stack(close)
    cross = ma_crossover(close, 21, 50, hold_days=3)
    above_50 = closes_above_ma_for_n_days(close, 50, 3)
    above_21 = closes_above_ma_for_n_days(close, 21, 3)

    stage = pd.Series(UptrendStage.NONE, index=df.index, dtype=object)
    stage[above_200] = UptrendStage.ABOVE_200
    stage[above_200 & above_21] = UptrendStage.ABOVE_21
    stage[above_200 & above_50] = UptrendStage.ABOVE_50
    stage[above_200 & cross] = UptrendStage.GOLDEN_CROSS_21_OVER_50
    stage[full] = UptrendStage.FULL_STACK
    return stage


# =====================================================================
# Abschnitt 2.5 – Marktbreite
# =====================================================================

# ---------------------------------------------------------------------
# 2.5.A – Vergleich Equal-Weight vs. Cap-Weight
# ---------------------------------------------------------------------

def equal_weight_warning(
    cap_weight_close: pd.Series,
    equal_weight_close: pd.Series,
    ma_window: int = 50,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5 "Gleichgewichtete ETFs":
        Warnsignal, wenn der Equal-Weight-ETF (z.B. RSP) unter wichtige
        gleitende Durchschnitte wie die 50-Tage-Linie fällt, während der
        kapitalgewichtete ETF (z.B. SPY) weiter steigt.
    """
    rsp_below_ma = equal_weight_close < sma(equal_weight_close, ma_window)
    spy_rising = cap_weight_close > sma(cap_weight_close, ma_window)
    return rsp_below_ma & spy_rising


# ---------------------------------------------------------------------
# 2.5.B – Marktbreitenindikator über 52-Wochen-Hoch
# ---------------------------------------------------------------------

class BreadthMode(str, Enum):
    """Marktbreite-Modi aus Abschnitt 2.5."""
    RUECKENWIND = "rueckenwind"   # Abstand bis 4% unter 52W-Hoch
    WACHSAM = "wachsam"           # 4 bis 8% unter 52W-Hoch
    SCHUTZ = "schutz"             # mehr als 8% unter 52W-Hoch


def distance_from_52w_high(equal_weight_close: pd.Series, weeks: int = 52) -> pd.Series:
    """
    Abstand zum 52-Wochen-Hoch in Prozent (negativ oder 0).
    52 Wochen werden als 252 Handelstage angenähert.
    """
    days = weeks * 5
    high_52w = equal_weight_close.rolling(window=days, min_periods=days).max()
    return (equal_weight_close / high_52w - 1.0) * 100.0


def breadth_mode_raw(
    equal_weight_close: pd.Series,
    weeks: int = 52,
    rueckenwind_max_pct: float = 4.0,
    wachsam_max_pct: float = 8.0,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5 "Marktbreitenindikator auf Basis von gleich-
    gewichteten ETFs":
        - Bis 4% unter 52W-Hoch: Modus Rückenwind
        - Mehr als 4% bis 8%: Modus Wachsam
        - Mehr als 8%: Modus Schutz

    Diese Variante liefert den tagesgenauen Modus ohne Stabilitätsfilter.
    """
    dist = distance_from_52w_high(equal_weight_close, weeks)
    out = pd.Series(BreadthMode.RUECKENWIND, index=equal_weight_close.index, dtype=object)
    out[dist < -rueckenwind_max_pct] = BreadthMode.WACHSAM
    out[dist < -wachsam_max_pct] = BreadthMode.SCHUTZ
    return out


def breadth_mode_confirmed(
    equal_weight_close: pd.Series,
    confirm_days: int = 3,
    weeks: int = 52,
    rueckenwind_max_pct: float = 4.0,
    wachsam_max_pct: float = 8.0,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5 (Stabilitätsregel):
        Ein Modus wird erst aktiv, wenn er an drei aufeinanderfolgenden
        Handelstagen bestätigt wird.

    Diese Funktion gibt den jeweils zuletzt bestätigten Modus zurück.
    Vor dem ersten bestätigten Modus enthält die Reihe NaN.
    """
    raw = breadth_mode_raw(equal_weight_close, weeks, rueckenwind_max_pct, wachsam_max_pct)
    confirmed = pd.Series([None] * len(raw), index=raw.index, dtype=object)
    last_mode = None
    streak = 0
    candidate = None
    for i, mode in enumerate(raw):
        if mode == candidate:
            streak += 1
        else:
            candidate = mode
            streak = 1
        if streak >= confirm_days:
            last_mode = candidate
        confirmed.iloc[i] = last_mode
    return confirmed


# ---------------------------------------------------------------------
# 2.5.C – Advance-Decline
# ---------------------------------------------------------------------

def ad_line(advancers: pd.Series, decliners: pd.Series) -> pd.Series:
    """
    Buchregel Abschnitt 2.5:
        A/D-Linie = kumulative Summe der Tagesdifferenz aus Advancers minus
        Decliners.
    """
    return (advancers - decliners).cumsum()


def ad_ratio(advancers: pd.Series, decliners: pd.Series) -> pd.Series:
    """
    Buchregel Abschnitt 2.5:
        A/D-Ratio = Advancers durch Decliners. Werte über 1 sind tendenziell
        bullisch, unter 1 tendenziell bärisch.
    """
    return advancers / decliners.replace(0, np.nan)


def ad_divergence_top(
    index_close: pd.Series,
    ad_line_values: pd.Series,
    lookback_days: int = 20,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5:
        Bärische Divergenz: Index markiert neue Hochs, A/D-Linie fällt.

    Operationalisiert: Index auf neuem Höchststand des lookback_days-Fensters,
    A/D-Linie aber unter ihrem Maximum dieses Fensters.
    """
    index_high = index_close.rolling(window=lookback_days, min_periods=lookback_days).max()
    ad_high = ad_line_values.rolling(window=lookback_days, min_periods=lookback_days).max()
    new_index_high = index_close >= index_high
    ad_below = ad_line_values < ad_high
    return new_index_high & ad_below


# ---------------------------------------------------------------------
# 2.5.D – Up/Down Volume
# ---------------------------------------------------------------------

def updown_volume_ratio(up_volume: pd.Series, down_volume: pd.Series) -> pd.Series:
    """
    Buchregel Abschnitt 2.5 "Up-/Down-Volume":
        Up-/Down-Volume-Ratio = Up-Volume durch Down-Volume.
        Werte über 1 sind konstruktiv, Werte unter 1 deuten auf
        angeschlagene Marktbreite hin.
    """
    return up_volume / down_volume.replace(0, np.nan)


def updown_volume_smoothed(
    up_volume: pd.Series,
    down_volume: pd.Series,
    weeks: int = 10,
    trading_days_per_week: int = 5,
) -> pd.Series:
    """
    Geglättetes Up/Down-Volume-Verhältnis über einen 10-Wochen-Durchschnitt
    (Buchempfehlung Abschnitt 2.5).
    """
    window = weeks * trading_days_per_week
    up_smooth = up_volume.rolling(window=window, min_periods=window).mean()
    down_smooth = down_volume.rolling(window=window, min_periods=window).mean()
    return up_smooth / down_smooth.replace(0, np.nan)


# ---------------------------------------------------------------------
# 2.5.E – McClellan Oscillator
# ---------------------------------------------------------------------

def mcclellan_oscillator(
    advancers: pd.Series,
    decliners: pd.Series,
    fast: int = 19,
    slow: int = 39,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5:
        Net Advances = Advancers minus Decliners.
        McClellan Oscillator = 19-Tage-EMA(Net Advances) minus 39-Tage-EMA.
    """
    net_adv = advancers - decliners
    return ema(net_adv, fast) - ema(net_adv, slow)


def mcclellan_state(mco: pd.Series) -> pd.Series:
    """
    Klassifiziert den McClellan Oscillator nach Buch-Schwellen:
        - überkauft:    +70 bis +100 oder höher
        - überverkauft: -70 bis -100 oder tiefer
        - bullish:      über 0
        - bearish:      unter 0
    """
    state = pd.Series("neutral", index=mco.index, dtype=object)
    state[mco > 0] = "bullish"
    state[mco < 0] = "bearish"
    state[mco >= 70] = "ueberkauft"
    state[mco <= -70] = "ueberverkauft"
    return state


# ---------------------------------------------------------------------
# 2.5.F – Breakaway Momentum (Deemer)
# ---------------------------------------------------------------------

def deemer_ratio(
    advancers: pd.Series,
    decliners: pd.Series,
    window: int = 10,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5 "Breitenschub als Startsignal":
        Deemer-Ratio = Summe der Advancers über 10 Tage geteilt durch
        Summe der Decliners über 10 Tage.
    """
    adv_sum = advancers.rolling(window=window, min_periods=window).sum()
    dec_sum = decliners.rolling(window=window, min_periods=window).sum()
    return adv_sum / dec_sum.replace(0, np.nan)


def is_breakaway_momentum(
    advancers: pd.Series,
    decliners: pd.Series,
    threshold: float = 1.97,
    window: int = 10,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5:
        Ein gültiger Breakaway-Momentum-Thrust liegt vor, wenn die
        Deemer-Ratio größer als 1,97 ist.
    """
    return deemer_ratio(advancers, decliners, window) > threshold


# ---------------------------------------------------------------------
# 2.5.G – Neue Hochs und Tiefs
# ---------------------------------------------------------------------

class NhNtZone(str, Enum):
    """Zonen der NH/NT-Ratio aus Tabelle 7 (Abschnitt 2.5)."""
    OVERHEATED = "ueberhitzt"        # > 3.0
    SOLID = "soliden_aufwaertstrend" # 1.5 bis 3.0
    UNCERTAIN = "unentschieden"      # ungefähr 1.0
    BEARISH_TONE = "baerischer_unterton"  # 0.5 bis 1.0
    BROAD_SELLOFF = "breiter_abverkauf"   # unter 0.5
    CAPITULATION = "kapitulationszone"    # unter 0.2


def nh_nt_ratio(new_highs: pd.Series, new_lows: pd.Series) -> pd.Series:
    """NH/NT-Ratio = Anzahl neuer 52-Wochen-Hochs durch neue 52-Wochen-Tiefs."""
    return new_highs / new_lows.replace(0, np.nan)


def classify_nh_nt(ratio: pd.Series) -> pd.Series:
    """
    Buchregel Tabelle 7 (Abschnitt 2.5):
        > 3.0: überhitzt
        1.5 bis 3.0: solider Aufwärtstrend
        ungefähr 1.0: unentschieden (hier 0.9 bis 1.1)
        0.5 bis 1.0: bärischer Unterton
        unter 0.5: breiter Abverkauf
        unter 0.2: Kapitulationszone
    """
    out = pd.Series(NhNtZone.UNCERTAIN, index=ratio.index, dtype=object)
    out[ratio > 3.0] = NhNtZone.OVERHEATED
    out[(ratio >= 1.5) & (ratio <= 3.0)] = NhNtZone.SOLID
    out[(ratio >= 0.9) & (ratio < 1.5)] = NhNtZone.UNCERTAIN
    out[(ratio >= 0.5) & (ratio < 0.9)] = NhNtZone.BEARISH_TONE
    out[ratio < 0.5] = NhNtZone.BROAD_SELLOFF
    out[ratio < 0.2] = NhNtZone.CAPITULATION
    return out


# ---------------------------------------------------------------------
# 2.5.H – Anteil der Aktien über gleitenden Durchschnitten
# ---------------------------------------------------------------------

def pct_above_ma_warning(
    pct_above_50dma: pd.Series,
    overheated_threshold: float = 70.0,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5 "Anteil der Aktien über ihren gleitenden
    Durchschnitten":
        Wenn der Anteil zuvor über 70% lag und anschließend wieder unter
        70% fällt, ist das ein Hinweis auf nachlassendes Momentum und einen
        möglichen Trendwechsel.
    """
    was_overheated = pct_above_50dma.shift(1) > overheated_threshold
    fell_below = pct_above_50dma <= overheated_threshold
    return was_overheated & fell_below


def pct_above_ma_divergence_warning(
    index_close: pd.Series,
    pct_above_50dma: pd.Series,
    threshold: float = 70.0,
    lookback_days: int = 60,
) -> pd.Series:
    """
    Buchregel Abschnitt 2.5:
        Kritisch ist es, wenn der Index neue Hochs erreicht, der Anteil der
        Aktien über ihrer 50-Tage-Linie aber nicht mehr klar in den Bereich
        über 70% zurückkehrt.

    Operationalisiert: Index auf neuem Höchststand des lookback_days-Fensters,
    aber pct_above_50dma liegt unter threshold.
    """
    index_high = index_close.rolling(window=lookback_days, min_periods=lookback_days).max()
    new_high = index_close >= index_high
    return new_high & (pct_above_50dma < threshold)


# =====================================================================
# Schwellenwert-Übersicht
# =====================================================================

# Diese Konstanten sind die expliziten Buchschwellen. Sie sind hier zentral
# gesammelt, damit eine eigene Implementierung sie direkt referenzieren und
# vergleichen kann.

THRESHOLDS = {
    # Abschnitt 2.2
    "korrektur_grenze_pct": -10.0,
    "baerenmarkt_grenze_pct": -20.0,
    "milde_korrektur_max_pct": -12.0,
    "moderate_korrektur_max_pct": -16.0,
    "tiefe_korrektur_min_pct": -17.0,
    "aufmerksamkeitszone_pct": -8.0,
    "atr_overextended": 3.0,
    "atr_normaler_puffer_min": 0.5,
    "atr_normaler_puffer_max": 3.0,
    "sp500_overextended_pct_to_50dma": 5.0,
    "nasdaq_overextended_pct_to_50dma": 7.0,
    "stalling_max_pct_change": 0.5,
    "stalling_min_volume_ratio": 0.95,
    "vix_panic_pct_above_10dma": 20.0,
    "margin_debt_warning_yoy_pct": 55.0,
    "rally_recovery_min_quote": 0.5,
    "first_leg_big_loss_pct": -3.0,
    "first_leg_loss_to_gain_ratio": 3.0,

    # Abschnitt 2.3
    "ankertag_min_days_to_startschuss": 4,
    "startschuss_min_gain_pct": 1.0,
    "startschuss_min_capital_pct": 10.0,
    "startschuss_max_capital_pct": 30.0,
    "startschuss_min_positions": 1,
    "startschuss_max_positions": 3,
    "startschuss_stop_min_pct": 5.0,
    "startschuss_stop_max_pct": 7.0,
    "startschuss_max_risk_per_position_pct": 0.5,

    # Abschnitt 2.4
    "uptrend_min_days_above_ma": 3,

    # Abschnitt 2.5
    "breadth_rueckenwind_max_pct_below_52w": 4.0,
    "breadth_wachsam_max_pct_below_52w": 8.0,
    "breadth_confirm_days": 3,
    "deemer_breakaway_threshold": 1.97,
    "deemer_temporary_dip_threshold": 1.84,
    "mcclellan_overbought": 70.0,
    "mcclellan_overbought_extreme": 100.0,
    "mcclellan_oversold": -70.0,
    "mcclellan_oversold_extreme": -100.0,
    "mcclellan_fast_ema": 19,
    "mcclellan_slow_ema": 39,
    "nh_nt_overheated": 3.0,
    "nh_nt_solid_min": 1.5,
    "nh_nt_solid_max": 3.0,
    "nh_nt_bearish_min": 0.5,
    "nh_nt_bearish_max": 1.0,
    "nh_nt_capitulation": 0.2,
    "pct_above_50dma_overheated": 70.0,
}


__all__ = [
    # Hilfsfunktionen
    "sma", "ema", "average_true_range",
    # 2.2 Korrekturen
    "CorrectionCategory", "classify_correction", "is_attention_zone",
    "intraday_reversal", "closing_range", "low_closing_range_streak",
    "distance_to_ma_in_atr", "is_overextended_atr",
    "pct_distance_to_50dma", "is_overextended_pct_sp500", "is_overextended_pct_nasdaq",
    "below_ma", "trend_warning_levels",
    "is_distribution_day", "count_distribution_days", "is_stalling_day",
    "detect_three_push_top",
    "vix_panic_signal", "vxx_risk_off", "vxx_risk_on",
    "margin_debt_warning",
    "steep_first_leg_signal",
    "is_low_volume_rally", "recovery_quote", "weak_rally_failed",
    # 2.3 Trendwende
    "TrendwendePhase", "Ankertag",
    "has_substantial_correction", "has_stopped_falling", "detect_ankertag",
    "bodenmarke_for_ankertag", "bodenmarke_held",
    "is_startschuss", "startschuss_bonus_above_21dma", "startschuss_holds",
    "StartschussRiskRules", "validate_startschuss_position",
    # 2.4 Aufwärtstrend
    "UptrendStage", "closes_above_ma_for_n_days", "low_above_ma_for_n_days",
    "ma_crossover", "ma_full_stack", "classify_uptrend_stage",
    # 2.5 Marktbreite
    "equal_weight_warning",
    "BreadthMode", "distance_from_52w_high", "breadth_mode_raw", "breadth_mode_confirmed",
    "ad_line", "ad_ratio", "ad_divergence_top",
    "updown_volume_ratio", "updown_volume_smoothed",
    "mcclellan_oscillator", "mcclellan_state",
    "deemer_ratio", "is_breakaway_momentum",
    "NhNtZone", "nh_nt_ratio", "classify_nh_nt",
    "pct_above_ma_warning", "pct_above_ma_divergence_warning",
    # Schwellen
    "THRESHOLDS",
]
