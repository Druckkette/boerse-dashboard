"""
Börse ohne Bauchgefühl — Modul 1: Markt-Dashboard & Trendwende-Ampel
Basierend auf dem Handelssystem von Aljoscha Groos

Streamlit App mit echten Marktdaten via yfinance.
"""

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Börse ohne Bauchgefühl",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ──────────────────────────────────────────────────────
# CUSTOM CSS
# ──────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&display=swap');

    .stApp {
        background-color: #0a0e17;
        color: #e2e8f0;
        font-family: 'JetBrains Mono', monospace;
    }
    .main .block-container { padding-top: 1.5rem; max-width: 1200px; }

    /* Headers */
    h1, h2, h3 { font-family: 'JetBrains Mono', monospace !important; }
    h1 { font-size: 1.6rem !important; font-weight: 800 !important;
         background: linear-gradient(135deg, #06b6d4, #3b82f6);
         -webkit-background-clip: text; -webkit-text-fill-color: transparent; }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: #111827; border: 1px solid #1e293b; border-radius: 10px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.75rem !important;
        text-transform: uppercase; letter-spacing: 0.08em; }
    [data-testid="stMetricValue"] { color: #e2e8f0 !important; font-size: 1.4rem !important;
        font-weight: 700 !important; }
    [data-testid="stMetricDelta"] { font-size: 0.8rem !important; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; background: transparent; }
    .stTabs [data-baseweb="tab"] {
        background: #111827; border: 1px solid #1e293b; border-radius: 8px;
        color: #94a3b8; padding: 8px 16px; font-size: 0.8rem;
    }
    .stTabs [aria-selected="true"] {
        background: #06b6d420; border-color: #06b6d4; color: #06b6d4;
    }

    /* Expander */
    .streamlit-expanderHeader { background: #111827 !important; color: #94a3b8 !important;
        font-size: 0.85rem !important; border: 1px solid #1e293b !important; border-radius: 8px; }
    .streamlit-expanderContent { background: #111827 !important; border: 1px solid #1e293b !important; }

    /* Radio buttons horizontal */
    .stRadio > div { flex-direction: row; gap: 8px; flex-wrap: wrap; }
    .stRadio label { background: #111827; border: 1px solid #1e293b; border-radius: 6px;
        padding: 6px 14px; font-size: 0.8rem; color: #94a3b8; }

    /* Ampel box */
    .ampel-box {
        border-radius: 12px; padding: 16px 20px;
        display: flex; align-items: center; gap: 16px;
    }
    .ampel-dot {
        width: 48px; height: 48px; border-radius: 50%; flex-shrink: 0;
    }

    /* Checklist */
    .check-item {
        display: flex; align-items: flex-start; gap: 10px;
        padding: 8px 0; border-bottom: 1px solid #1e293b;
    }
    .check-icon {
        width: 22px; height: 22px; border-radius: 50%; flex-shrink: 0;
        display: flex; align-items: center; justify-content: center;
        font-size: 12px; font-weight: 700;
    }
    .check-ok { background: #22c55e20; border: 1.5px solid #22c55e50; color: #22c55e; }
    .check-fail { background: #ef444420; border: 1.5px solid #ef444450; color: #ef4444; }

    /* Card */
    .info-card {
        background: #111827; border: 1px solid #1e293b; border-radius: 12px;
        padding: 16px; margin-bottom: 12px;
    }
    .card-label {
        font-size: 0.7rem; color: #64748b; text-transform: uppercase;
        letter-spacing: 0.08em; margin-bottom: 8px;
    }

    /* Breadth bar */
    .breadth-track {
        height: 10px; border-radius: 5px; background: #1e293b;
        position: relative; overflow: hidden; margin: 8px 0;
    }
    .breadth-fill {
        position: absolute; left: 0; top: 0; bottom: 0; border-radius: 5px;
        background: linear-gradient(90deg, #22c55e, #f59e0b, #ef4444);
        transition: width 0.5s;
    }

    /* Plotly chart background */
    .js-plotly-plot .plotly .main-svg { background: #111827 !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────
# DATA LOADING (cached for 15 minutes)
# ──────────────────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)
def load_market_data(lookback_days=400):
    """Load OHLCV data for all required tickers."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)

    tickers = {
        "S&P 500": "^GSPC",
        "Nasdaq Composite": "^IXIC",
        "Russell 2000": "^RUT",
        "RSP (Equal-Weight S&P)": "RSP",
        "QQEW (Equal-Weight Nasdaq)": "QQEW",
        "VIX": "^VIX",
        "VXX": "VIXY",  # VXX was delisted, VIXY is the successor
    }

    data = {}
    for name, symbol in tickers.items():
        try:
            df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
            if df is not None and len(df) > 0:
                df = df.copy()
                # Flatten MultiIndex columns if present
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df.index = pd.to_datetime(df.index)
                df = df.sort_index()
                data[name] = df
        except Exception as e:
            st.warning(f"Konnte {name} ({symbol}) nicht laden: {e}")

    return data


# ──────────────────────────────────────────────────────
# TECHNICAL ANALYSIS
# ──────────────────────────────────────────────────────
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def calc_sma(series, period):
    return series.rolling(window=period).mean()


def calc_atr(df, period=21):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def add_indicators(df):
    """Add all technical indicators to a dataframe."""
    df = df.copy()
    df["EMA21"] = calc_ema(df["Close"], 21)
    df["SMA50"] = calc_sma(df["Close"], 50)
    df["SMA200"] = calc_sma(df["Close"], 200)
    df["ATR21"] = calc_atr(df, 21)
    df["ATR_pct"] = df["ATR21"] / df["Close"] * 100
    df["Vol_SMA50"] = calc_sma(df["Volume"], 50)
    df["Pct_Change"] = df["Close"].pct_change() * 100

    # Closing Range (0 = low, 1 = high)
    rng = df["High"] - df["Low"]
    df["Closing_Range"] = np.where(rng > 0, (df["Close"] - df["Low"]) / rng, 0.5)

    # Distance to 21-EMA in ATR units
    df["Dist_21EMA"] = (df["Close"] - df["EMA21"]) / df["ATR21"]

    # Distance to 50-SMA in percent
    df["Dist_50SMA_pct"] = (df["Close"] - df["SMA50"]) / df["SMA50"] * 100

    # Distance from 52-week high
    df["High_52w"] = df["High"].rolling(window=252, min_periods=1).max()
    df["Dist_52w_pct"] = (df["Close"] - df["High_52w"]) / df["High_52w"] * 100

    # MA Order: 21 > 50 > 200
    df["MA_Order"] = (df["EMA21"] > df["SMA50"]) & (df["SMA50"] > df["SMA200"])

    return df


# ──────────────────────────────────────────────────────
# DISTRIBUTION & STALL DAY DETECTION
# ──────────────────────────────────────────────────────
def detect_distribution_days(df):
    """Detect distribution and stall days."""
    df = df.copy()
    prev_close = df["Close"].shift(1)
    prev_vol = df["Volume"].shift(1)

    is_down = df["Close"] < prev_close
    high_vol = (df["Volume"] > prev_vol) | (df["Volume"] > df["Vol_SMA50"])
    df["Is_Distribution"] = is_down & high_vol

    # Stall days: <0.5% gain, volume >= 95% prev, close in lower half
    pct = df["Pct_Change"]
    stall_cond = (
        (~is_down) &
        (pct < 0.5) &
        (df["Volume"] >= prev_vol * 0.95) &
        (df["Closing_Range"] < 0.5)
    )
    df["Is_Stall"] = stall_cond

    # Rolling 25-day distribution count
    df["Dist_Count_25"] = df["Is_Distribution"].rolling(window=25, min_periods=1).sum().astype(int)

    return df


# ──────────────────────────────────────────────────────
# TRENDWENDE-AMPEL
# ──────────────────────────────────────────────────────
def compute_ampel(df):
    """Compute the Trendwende-Ampel phase for each day."""
    df = df.copy()

    phase = "neutral"
    anchor_idx = None
    floor_mark = None
    startschuss_idx = None
    startschuss_low = None

    phases = []
    anchor_dates = []
    floor_marks = []
    startschuss_lows = []

    closes = df["Close"].values
    highs = df["High"].values
    lows = df["Low"].values
    volumes = df["Volume"].values
    pct_changes = df["Pct_Change"].values
    closing_ranges = df["Closing_Range"].values
    dist_counts = df["Dist_Count_25"].values
    sma50s = df["SMA50"].values
    ema21s = df["EMA21"].values

    for i in range(len(df)):
        # Drawdown from recent 60-day high
        lookback_start = max(0, i - 60)
        recent_high = np.nanmax(highs[lookback_start:i + 1])
        drawdown = (closes[i] - recent_high) / recent_high * 100 if recent_high > 0 else 0

        under_sma50 = not np.isnan(sma50s[i]) and closes[i] < sma50s[i]
        has_distribution = dist_counts[i] >= 4

        # ── Phase transitions ──
        if phase in ("neutral", "reset"):
            if drawdown < -8 or (under_sma50 and has_distribution):
                phase = "rot"
                anchor_idx = None
                floor_mark = None
                startschuss_idx = None
                startschuss_low = None

        if phase == "rot":
            # Look for Ankertag
            if anchor_idx is None and i > 0:
                is_positive = pct_changes[i] > 0 if not np.isnan(pct_changes[i]) else False
                upper_half = closing_ranges[i] >= 0.5

                if is_positive or upper_half:
                    anchor_idx = i
                    prev_low = lows[i - 1] if i > 0 else lows[i]
                    floor_mark = min(prev_low, lows[i])

            # Check if floor breached
            if anchor_idx is not None and floor_mark is not None:
                if lows[i] < floor_mark and i != anchor_idx:
                    anchor_idx = None
                    floor_mark = None

            # Check for Startschuss
            if (anchor_idx is not None and floor_mark is not None
                    and i >= anchor_idx + 5 and i > 0):
                vol_up = volumes[i] > volumes[i - 1]
                gain_ok = not np.isnan(pct_changes[i]) and pct_changes[i] >= 1.0
                floor_held = lows[i] >= floor_mark

                if gain_ok and vol_up and floor_held:
                    phase = "gelb"
                    startschuss_idx = i
                    startschuss_low = lows[i]

        if phase == "gelb":
            if startschuss_low is not None and closes[i] < startschuss_low:
                phase = "rot"
                anchor_idx = None
                floor_mark = None
                startschuss_idx = None
                startschuss_low = None
            elif startschuss_idx is not None and i > startschuss_idx + 2:
                phase = "gruen"

        if phase == "gruen":
            if startschuss_low is not None and closes[i] < startschuss_low:
                phase = "rot"
                anchor_idx = None
                floor_mark = None
                startschuss_idx = None
                startschuss_low = None

        phases.append(phase)
        anchor_dates.append(df.index[anchor_idx].strftime("%Y-%m-%d") if anchor_idx is not None else None)
        floor_marks.append(round(floor_mark, 2) if floor_mark is not None else None)
        startschuss_lows.append(round(startschuss_low, 2) if startschuss_low is not None else None)

    df["Ampel_Phase"] = phases
    df["Anchor_Date"] = anchor_dates
    df["Floor_Mark"] = floor_marks
    df["Startschuss_Low"] = startschuss_lows

    return df


# ──────────────────────────────────────────────────────
# BREADTH MODE
# ──────────────────────────────────────────────────────
def compute_breadth_mode(df_ew):
    """Compute Rückenwind / Wachsam / Schutz from equal-weight ETF."""
    df_ew = df_ew.copy()
    df_ew["High_52w"] = df_ew["High"].rolling(window=252, min_periods=1).max()
    df_ew["Dist_52w_pct"] = (df_ew["Close"] - df_ew["High_52w"]) / df_ew["High_52w"] * 100

    def mode_from_dist(d):
        if d < -8:
            return "schutz"
        elif d < -4:
            return "wachsam"
        return "rueckenwind"

    df_ew["Breadth_Mode"] = df_ew["Dist_52w_pct"].apply(mode_from_dist)
    return df_ew


# ──────────────────────────────────────────────────────
# VIX ANALYSIS
# ──────────────────────────────────────────────────────
def analyze_vix(df_vix):
    """Analyze VIX with 10-day SMA for panic detection."""
    df_vix = df_vix.copy()
    df_vix["SMA10"] = calc_sma(df_vix["Close"], 10)
    df_vix["Pct_Above_SMA10"] = (df_vix["Close"] - df_vix["SMA10"]) / df_vix["SMA10"] * 100
    df_vix["Is_Panic"] = df_vix["Pct_Above_SMA10"] > 20
    return df_vix


# ──────────────────────────────────────────────────────
# HELPER: Render Ampel
# ──────────────────────────────────────────────────────
def render_ampel(phase):
    colors = {"rot": "#ef4444", "gelb": "#f59e0b", "gruen": "#22c55e", "neutral": "#64748b"}
    labels = {
        "rot": ("ROT — Abwarten", "Substanzielle Korrektur. Nicht kaufen. Ankertag beobachten."),
        "gelb": ("GELB — Startschuss", "Einstiegssignal! Erste Position(en) eröffnen (10–30% Kapital)."),
        "gruen": ("GRÜN — Bestätigung", "Startschuss hält. Aufwärtstrend-Steuerung aktiv."),
        "neutral": ("NEUTRAL", "Keine substanzielle Korrektur erkannt. Normale Marktbeobachtung."),
    }
    c = colors.get(phase, colors["neutral"])
    lbl, desc = labels.get(phase, labels["neutral"])

    st.markdown(f"""
    <div class="ampel-box" style="background:{c}15; border:1px solid {c}40;">
        <div class="ampel-dot" style="background:{c}; box-shadow: 0 0 24px {c}80, 0 0 48px {c}40;"></div>
        <div>
            <div style="font-size:1.1rem; font-weight:700; color:{c}; letter-spacing:0.05em;">{lbl}</div>
            <div style="font-size:0.8rem; color:#94a3b8; margin-top:2px;">{desc}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_check(label, ok, detail=""):
    icon_class = "check-ok" if ok else "check-fail"
    icon = "✓" if ok else "✗"
    st.markdown(f"""
    <div class="check-item">
        <div class="check-icon {icon_class}">{icon}</div>
        <div style="flex:1;">
            <div style="font-size:0.85rem; color:#e2e8f0;">{label}</div>
            <div style="font-size:0.7rem; color:#64748b; margin-top:1px;">{detail}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_breadth(mode, dist_pct):
    colors = {"rueckenwind": "#22c55e", "wachsam": "#f59e0b", "schutz": "#ef4444"}
    labels = {
        "rueckenwind": ("Rückenwind", "≤4% vom 52W-Hoch. Breite Stärke, Ausbrüche haben Anschluss."),
        "wachsam": ("Wachsam", "4–8% vom 52W-Hoch. Gemischte Breite, strenger auswählen."),
        "schutz": ("Schutz", ">8% vom 52W-Hoch. Kapitalschutz priorisieren."),
    }
    c = colors.get(mode, "#64748b")
    lbl, desc = labels.get(mode, ("—", ""))
    fill_pct = min(100, abs(dist_pct) / 12 * 100)

    st.markdown(f"""
    <div class="info-card" style="background:{c}12; border-color:{c}30;">
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">
            <div style="width:12px; height:12px; border-radius:50%; background:{c};
                        box-shadow: 0 0 8px {c}60;"></div>
            <div>
                <span style="font-size:1rem; font-weight:700; color:{c};">Modus: {lbl}</span>
                <span style="font-size:0.75rem; color:#94a3b8; margin-left:8px;">{dist_pct:.1f}% vom 52W-Hoch</span>
            </div>
        </div>
        <div style="font-size:0.75rem; color:#94a3b8; margin-bottom:8px;">{desc}</div>
        <div class="breadth-track"><div class="breadth-fill" style="width:{fill_pct}%;"></div></div>
        <div style="display:flex; justify-content:space-between; font-size:0.65rem; color:#64748b;">
            <span style="color:#22c55e;">Rückenwind</span>
            <span style="color:#f59e0b;">Wachsam</span>
            <span style="color:#ef4444;">Schutz</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────
# CHART HELPERS (using Streamlit native charts via plotly)
# ──────────────────────────────────────────────────────
def plot_price_chart(df, show_days=90):
    """Plot price with MAs using plotly for dark theme."""
    import plotly.graph_objects as go

    df_vis = df.tail(show_days).copy()

    fig = go.Figure()

    # Price line
    fig.add_trace(go.Scatter(
        x=df_vis.index, y=df_vis["Close"], name="Kurs",
        line=dict(color="#e2e8f0", width=2), hovertemplate="%{y:.2f}<extra>Kurs</extra>"
    ))

    # 21-EMA
    if "EMA21" in df_vis.columns:
        fig.add_trace(go.Scatter(
            x=df_vis.index, y=df_vis["EMA21"], name="21-EMA",
            line=dict(color="#06b6d4", width=1, dash="dot"),
            hovertemplate="%{y:.2f}<extra>21-EMA</extra>"
        ))

    # 50-SMA
    if "SMA50" in df_vis.columns:
        fig.add_trace(go.Scatter(
            x=df_vis.index, y=df_vis["SMA50"], name="50-SMA",
            line=dict(color="#f97316", width=1, dash="dot"),
            hovertemplate="%{y:.2f}<extra>50-SMA</extra>"
        ))

    # 200-SMA
    if "SMA200" in df_vis.columns:
        fig.add_trace(go.Scatter(
            x=df_vis.index, y=df_vis["SMA200"], name="200-SMA",
            line=dict(color="#a855f7", width=1, dash="dash"),
            hovertemplate="%{y:.2f}<extra>200-SMA</extra>"
        ))

    # Floor mark
    last_floor = df_vis["Floor_Mark"].dropna()
    if len(last_floor) > 0:
        fig.add_hline(y=last_floor.iloc[-1], line_dash="dash",
                      line_color="#ef4444", line_width=1,
                      annotation_text="Bodenmarke", annotation_font_color="#ef4444")

    # Distribution day markers
    dist_days = df_vis[df_vis["Is_Distribution"]]
    if len(dist_days) > 0:
        fig.add_trace(go.Scatter(
            x=dist_days.index, y=dist_days["Close"], name="Distribution",
            mode="markers", marker=dict(color="#ef4444", size=6, symbol="triangle-down"),
            hovertemplate="%{y:.2f}<extra>Distributionstag</extra>"
        ))

    # Stall day markers
    stall_days = df_vis[df_vis["Is_Stall"]]
    if len(stall_days) > 0:
        fig.add_trace(go.Scatter(
            x=stall_days.index, y=stall_days["Close"], name="Stau-Tag",
            mode="markers", marker=dict(color="#f59e0b", size=5, symbol="diamond"),
            hovertemplate="%{y:.2f}<extra>Stau-Tag</extra>"
        ))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0, r=0, t=30, b=0),
        height=360,
        legend=dict(orientation="h", yanchor="top", y=1.12, xanchor="left", x=0,
                    font=dict(size=10, color="#94a3b8")),
        xaxis=dict(gridcolor="#1e293b", showgrid=True, tickfont=dict(size=9, color="#64748b")),
        yaxis=dict(gridcolor="#1e293b", showgrid=True, tickfont=dict(size=9, color="#64748b")),
        hovermode="x unified",
    )

    return fig


def plot_volume_chart(df, show_days=90):
    """Volume bars colored by up/down day."""
    import plotly.graph_objects as go

    df_vis = df.tail(show_days).copy()
    colors = ["#22c55e" if c >= 0 else "#ef4444" for c in df_vis["Pct_Change"].fillna(0)]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_vis.index, y=df_vis["Volume"], name="Volumen",
        marker_color=colors, opacity=0.7,
        hovertemplate="%{y:,.0f}<extra>Volumen</extra>"
    ))

    # 50-day volume average
    if "Vol_SMA50" in df_vis.columns:
        fig.add_trace(go.Scatter(
            x=df_vis.index, y=df_vis["Vol_SMA50"], name="50T-Schnitt",
            line=dict(color="#64748b", width=1, dash="dot"),
        ))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0, r=0, t=10, b=0),
        height=120,
        showlegend=False,
        xaxis=dict(gridcolor="#1e293b", showgrid=False, tickfont=dict(size=9, color="#64748b")),
        yaxis=dict(gridcolor="#1e293b", showgrid=True, tickfont=dict(size=9, color="#64748b"),
                   tickformat=".2s"),
    )

    return fig


def plot_vix(df_vix, show_days=90):
    """Plot VIX with 10-day SMA."""
    import plotly.graph_objects as go

    df_vis = df_vix.tail(show_days).copy()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_vis.index, y=df_vis["Close"], name="VIX",
        line=dict(color="#ef4444", width=1.5),
    ))
    if "SMA10" in df_vis.columns:
        fig.add_trace(go.Scatter(
            x=df_vis.index, y=df_vis["SMA10"], name="10-SMA",
            line=dict(color="#3b82f6", width=1, dash="dot"),
        ))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0, r=0, t=10, b=0),
        height=160,
        legend=dict(orientation="h", yanchor="top", y=1.15, font=dict(size=10, color="#94a3b8")),
        xaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")),
        yaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")),
    )

    return fig


# ──────────────────────────────────────────────────────
# MAIN APP
# ──────────────────────────────────────────────────────
def main():
    st.title("BÖRSE OHNE BAUCHGEFÜHL")
    st.caption("Modul 1 — Markt-Dashboard & Trendwende-Ampel")

    # Load data
    with st.spinner("Lade Marktdaten von Yahoo Finance..."):
        data = load_market_data()

    if not data:
        st.error("Keine Marktdaten verfügbar. Bitte später erneut versuchen.")
        return

    # Index selection
    index_options = ["S&P 500", "Nasdaq Composite", "Russell 2000"]
    available = [idx for idx in index_options if idx in data]

    if not available:
        st.error("Keine Index-Daten verfügbar.")
        return

    col_sel1, col_sel2 = st.columns([3, 1])
    with col_sel1:
        selected_index = st.radio("Index", available, horizontal=True, label_visibility="collapsed")
    with col_sel2:
        show_days = st.selectbox("Zeitraum", [60, 90, 130, 200], index=1,
                                 format_func=lambda x: f"{x} Tage")

    # Process selected index
    df = data[selected_index].copy()
    df = add_indicators(df)
    df = detect_distribution_days(df)
    df = compute_ampel(df)

    # Latest values
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    pct_change = latest["Pct_Change"]

    # ── AMPEL ──
    st.markdown("---")
    render_ampel(latest["Ampel_Phase"])

    # ── KEY METRICS ──
    st.markdown("")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(selected_index, f"{latest['Close']:,.2f}",
                  f"{pct_change:+.2f}%")
    with c2:
        dist_count = int(latest["Dist_Count_25"])
        st.metric("Dist.-Tage (25T)", dist_count,
                  "⚠️ Häufung" if dist_count >= 4 else "OK" if dist_count <= 2 else "Beobachten")
    with c3:
        dist_21 = latest["Dist_21EMA"]
        st.metric("Abstand 21-EMA", f"{dist_21:.1f} ATR" if not np.isnan(dist_21) else "—",
                  f"ATR: {latest['ATR_pct']:.2f}%" if not np.isnan(latest["ATR_pct"]) else "")
    with c4:
        high_52w = latest["High_52w"]
        drawdown = latest["Dist_52w_pct"]
        st.metric("Drawdown v. 52W-Hoch", f"{drawdown:.1f}%" if not np.isnan(drawdown) else "—")

    # ── PRICE CHART ──
    st.markdown('<div class="card-label">KURSVERLAUF MIT GLEITENDEN DURCHSCHNITTEN</div>',
                unsafe_allow_html=True)
    st.plotly_chart(plot_price_chart(df, show_days), use_container_width=True, config={"displayModeBar": False})

    # ── VOLUME CHART ──
    st.markdown('<div class="card-label">VOLUMEN (grün = Gewinntag, rot = Verlusttag)</div>',
                unsafe_allow_html=True)
    st.plotly_chart(plot_volume_chart(df, show_days), use_container_width=True, config={"displayModeBar": False})

    # ── TWO COLUMN SECTION ──
    col_left, col_right = st.columns(2)

    with col_left:
        # Aufwärtstrend-Prüfung
        st.markdown('<div class="info-card"><div class="card-label">AUFWÄRTSTREND-PRÜFUNG</div>',
                    unsafe_allow_html=True)

        above_200 = not np.isnan(latest["SMA200"]) and latest["Close"] > latest["SMA200"]
        above_50 = not np.isnan(latest["SMA50"]) and latest["Close"] > latest["SMA50"]
        above_21 = not np.isnan(latest["EMA21"]) and latest["Close"] > latest["EMA21"]
        ma_order = latest["MA_Order"] if not pd.isna(latest["MA_Order"]) else False
        dist_ok = dist_count <= 3

        render_check("Kurs über 200-SMA", above_200,
                     f"{latest['Close']:.0f} vs. {latest['SMA200']:.0f}" if not np.isnan(latest["SMA200"]) else "")
        render_check("Kurs über 50-SMA", above_50,
                     f"{latest['Close']:.0f} vs. {latest['SMA50']:.0f}" if not np.isnan(latest["SMA50"]) else "")
        render_check("Kurs über 21-EMA", above_21,
                     f"{latest['Close']:.0f} vs. {latest['EMA21']:.0f}" if not np.isnan(latest["EMA21"]) else "")
        render_check("21-EMA > 50-SMA > 200-SMA", ma_order, "Richtige Ordnung der Durchschnitte")
        render_check("Distributionstage ≤ 3", dist_ok, f"{dist_count} im 25-Tage-Fenster")

        st.markdown("</div>", unsafe_allow_html=True)

    with col_right:
        # Trendwende-Ampel Details
        st.markdown('<div class="info-card"><div class="card-label">TRENDWENDE-AMPEL DETAILS</div>',
                    unsafe_allow_html=True)

        anchor = latest["Anchor_Date"]
        floor = latest["Floor_Mark"]
        ss_low = latest["Startschuss_Low"]

        detail_data = {
            "Kennzahl": ["Aktuelle Phase", "Ankertag", "Bodenmarke", "Startschuss-Tief", "MA-Ordnung"],
            "Wert": [
                latest["Ampel_Phase"].upper(),
                anchor if anchor else "—",
                f"{floor:.2f}" if floor else "—",
                f"{ss_low:.2f}" if ss_low else "—",
                "Korrekt ✓" if ma_order else "Gestört ✗",
            ],
        }
        st.dataframe(
            pd.DataFrame(detail_data).set_index("Kennzahl"),
            use_container_width=True,
            height=220,
        )

        st.markdown("</div>", unsafe_allow_html=True)

    # ── MARKTBREITE ──
    st.markdown("---")
    st.markdown('<div class="card-label">MARKTBREITE-INDIKATOR</div>', unsafe_allow_html=True)

    # Use RSP for S&P breadth, QQEW for Nasdaq
    breadth_etf_name = "RSP (Equal-Weight S&P)" if "S&P" in selected_index else "QQEW (Equal-Weight Nasdaq)"
    breadth_alt = "QQEW (Equal-Weight Nasdaq)" if "S&P" in selected_index else "RSP (Equal-Weight S&P)"

    b_col1, b_col2 = st.columns(2)

    for col, etf_name in [(b_col1, breadth_etf_name), (b_col2, breadth_alt)]:
        with col:
            if etf_name in data:
                df_ew = compute_breadth_mode(data[etf_name].copy())
                latest_ew = df_ew.iloc[-1]
                render_breadth(latest_ew["Breadth_Mode"], latest_ew["Dist_52w_pct"])
                st.caption(etf_name)
            else:
                st.info(f"{etf_name} nicht verfügbar.")

    # ── VIX / VOLATILITÄT ──
    st.markdown("---")
    st.markdown('<div class="card-label">VOLATILITÄT & STIMMUNG</div>', unsafe_allow_html=True)

    v_col1, v_col2 = st.columns([2, 1])

    with v_col1:
        if "VIX" in data:
            df_vix = analyze_vix(data["VIX"].copy())
            st.plotly_chart(plot_vix(df_vix, show_days), use_container_width=True,
                          config={"displayModeBar": False})

    with v_col2:
        if "VIX" in data:
            df_vix = analyze_vix(data["VIX"].copy())
            vix_latest = df_vix.iloc[-1]
            vix_val = vix_latest["Close"]
            vix_sma = vix_latest["SMA10"]
            pct_above = vix_latest["Pct_Above_SMA10"]

            st.metric("VIX", f"{vix_val:.1f}")
            st.metric("10-Tage-SMA", f"{vix_sma:.1f}" if not np.isnan(vix_sma) else "—")

            if not np.isnan(pct_above):
                if pct_above > 20:
                    st.error(f"⚠️ VIX {pct_above:.0f}% über SMA → Paniksignal")
                elif vix_val > 20:
                    st.warning(f"VIX erhöht ({pct_above:+.0f}% vs SMA)")
                else:
                    st.success(f"VIX ruhig ({pct_above:+.0f}% vs SMA)")

        if "VXX" in data:
            df_vxx = data["VXX"].copy()
            df_vxx["EMA21"] = calc_ema(df_vxx["Close"], 21)
            vxx_latest = df_vxx.iloc[-1]
            vxx_above_ema = vxx_latest["Close"] > vxx_latest["EMA21"]
            ema_rising = df_vxx["EMA21"].iloc[-1] > df_vxx["EMA21"].iloc[-5] if len(df_vxx) > 5 else False

            if vxx_above_ema and ema_rising:
                st.warning("VXX über steigender 21-EMA → Risk-Off")
            elif not vxx_above_ema and not ema_rising:
                st.success("VXX unter fallender 21-EMA → Risk-On")
            else:
                st.info("VXX gemischt")

    # ── TÄGLICHE CHECKLISTE ──
    st.markdown("---")
    st.markdown('<div class="info-card"><div class="card-label">TÄGLICHE CHECKLISTE — MARKTLAGE</div>',
                unsafe_allow_html=True)

    drawdown_val = latest["Dist_52w_pct"] if not np.isnan(latest["Dist_52w_pct"]) else 0

    render_check(
        "Substanzielle Korrektur hinter sich?",
        drawdown_val < -8,
        f"Drawdown: {drawdown_val:.1f}%"
    )
    render_check(
        "Markt aufgehört neue Tiefs zu markieren?",
        latest["Ampel_Phase"] != "rot" or latest["Anchor_Date"] is not None,
        f"Ankertag: {latest['Anchor_Date']}" if latest["Anchor_Date"] else "Noch keine Stabilisierung"
    )
    render_check(
        "Startschuss erfolgt (Phase ≥ Gelb)?",
        latest["Ampel_Phase"] in ("gelb", "gruen"),
        f"Phase: {latest['Ampel_Phase'].upper()}"
    )

    # Breadth check
    breadth_ok = True
    if breadth_etf_name in data:
        df_ew = compute_breadth_mode(data[breadth_etf_name].copy())
        breadth_mode = df_ew.iloc[-1]["Breadth_Mode"]
        breadth_ok = breadth_mode != "schutz"
        render_check(
            "Marktbreite unterstützend?",
            breadth_ok,
            f"Modus: {breadth_mode.capitalize()}"
        )
    else:
        render_check("Marktbreite unterstützend?", True, "Keine Equal-Weight-Daten verfügbar")

    render_check(
        "Distributionstage unter Kontrolle (≤ 3)?",
        dist_count <= 3,
        f"{dist_count} Tage im 25-Tage-Fenster"
    )

    render_check(
        "VIX nicht in Panikzone?",
        not ("VIX" in data and analyze_vix(data["VIX"].copy()).iloc[-1].get("Is_Panic", False)),
        f"VIX: {data['VIX'].iloc[-1]['Close']:.1f}" if "VIX" in data else "Keine VIX-Daten"
    )

    st.markdown("</div>", unsafe_allow_html=True)

    # ── FOOTER ──
    st.markdown("---")
    st.caption(
        "Börse ohne Bauchgefühl · Markt-Dashboard v1.0 · "
        f"Daten: Yahoo Finance · Letztes Update: {latest.name.strftime('%d.%m.%Y')}"
    )
    st.caption("Basierend auf dem Handelssystem von Aljoscha Groos")


if __name__ == "__main__":
    main()
