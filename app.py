"""
Börse ohne Bauchgefühl — Modul 1: Markt-Dashboard & Trendwende-Ampel
Basierend auf dem Handelssystem von Aljoscha Groos
Streamlit App mit echten Marktdaten via yfinance.
"""

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="Börse ohne Bauchgefühl", page_icon="🚦", layout="wide", initial_sidebar_state="collapsed")

# ── CSS ──
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&display=swap');
.stApp { background-color:#0a0e17; color:#e2e8f0; font-family:'JetBrains Mono',monospace; }
.main .block-container { padding-top:1.5rem; max-width:1200px; }
h1,h2,h3 { font-family:'JetBrains Mono',monospace!important; }
h1 { font-size:1.6rem!important; font-weight:800!important;
     background:linear-gradient(135deg,#06b6d4,#3b82f6);
     -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
[data-testid="stMetric"] { background:#111827; border:1px solid #1e293b; border-radius:10px; padding:12px 16px; }
[data-testid="stMetricLabel"] { color:#64748b!important; font-size:0.75rem!important; text-transform:uppercase; letter-spacing:0.08em; }
[data-testid="stMetricValue"] { color:#e2e8f0!important; font-size:1.4rem!important; font-weight:700!important; }
.stTabs [data-baseweb="tab-list"] { gap:4px; background:transparent; }
.stTabs [data-baseweb="tab"] { background:#111827; border:1px solid #1e293b; border-radius:8px; color:#94a3b8; padding:8px 16px; font-size:0.8rem; }
.stTabs [aria-selected="true"] { background:#06b6d420; border-color:#06b6d4; color:#06b6d4; }
.ampel-box { border-radius:12px; padding:16px 20px; display:flex; align-items:center; gap:16px; }
.ampel-dot { width:48px; height:48px; border-radius:50%; flex-shrink:0; }
.check-item { display:flex; align-items:flex-start; gap:10px; padding:8px 0; border-bottom:1px solid #1e293b; }
.check-icon { width:22px; height:22px; border-radius:50%; flex-shrink:0; display:flex; align-items:center; justify-content:center; font-size:12px; font-weight:700; }
.check-ok { background:#22c55e20; border:1.5px solid #22c55e50; color:#22c55e; }
.check-fail { background:#ef444420; border:1.5px solid #ef444450; color:#ef4444; }
.info-card { background:#111827; border:1px solid #1e293b; border-radius:12px; padding:16px; margin-bottom:12px; }
.card-label { font-size:0.7rem; color:#64748b; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:8px; }
.breadth-track { height:10px; border-radius:5px; background:#1e293b; position:relative; overflow:hidden; margin:8px 0; }
.breadth-fill { position:absolute; left:0; top:0; bottom:0; border-radius:5px; background:linear-gradient(90deg,#22c55e,#f59e0b,#ef4444); transition:width 0.5s; }
</style>
""", unsafe_allow_html=True)

# ── DATA ──
def _safe_download(symbol, start, end):
    try:
        df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or len(df) == 0: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        for c in ["Open","High","Low","Close","Volume"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["Close"])
        return df
    except Exception as e:
        return None

@st.cache_data(ttl=900, show_spinner=False)
def load_market_data(lookback_days=400):
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    tickers = {"S&P 500":"^GSPC","Nasdaq Composite":"^IXIC","Russell 2000":"^RUT",
               "RSP (Equal-Weight S&P)":"RSP","QQEW (Equal-Weight Nasdaq)":"QQEW",
               "VIX":"^VIX","VXX":"VIXY"}
    data = {}
    for name, sym in tickers.items():
        df = _safe_download(sym, start, end)
        if df is not None and len(df) > 20: data[name] = df
    return data

# ── TECHNICALS ──
def calc_ema(s, p): return s.ewm(span=p, adjust=False).mean()
def calc_sma(s, p): return s.rolling(window=p, min_periods=p).mean()

def calc_atr(df, period=21):
    h, l, pc = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=period).mean()

def add_indicators(df):
    df = df.copy()
    df["EMA21"] = calc_ema(df["Close"], 21)
    df["SMA50"] = calc_sma(df["Close"], 50)
    df["SMA200"] = calc_sma(df["Close"], 200)
    df["ATR21"] = calc_atr(df, 21)
    df["ATR_pct"] = df["ATR21"] / df["Close"] * 100
    df["Vol_SMA50"] = calc_sma(df["Volume"], 50)
    df["Pct_Change"] = df["Close"].pct_change() * 100
    rng = df["High"] - df["Low"]
    df["Closing_Range"] = np.where(rng > 0, (df["Close"] - df["Low"]) / rng, 0.5)
    df["Dist_21EMA"] = (df["Close"] - df["EMA21"]) / df["ATR21"]
    df["High_52w"] = df["High"].rolling(window=252, min_periods=1).max()
    df["Dist_52w_pct"] = (df["Close"] - df["High_52w"]) / df["High_52w"] * 100
    df["MA_Order"] = (df["EMA21"] > df["SMA50"]) & (df["SMA50"] > df["SMA200"])
    return df

def detect_distribution_days(df):
    df = df.copy()
    pc = df["Close"].shift(1); pv = df["Volume"].shift(1)
    is_down = df["Close"] < pc
    high_vol = (df["Volume"] > pv) | (df["Volume"] > df["Vol_SMA50"])
    df["Is_Distribution"] = is_down & high_vol
    pct = df["Pct_Change"]
    df["Is_Stall"] = (~is_down) & (pct < 0.5) & (df["Volume"] >= pv * 0.95) & (df["Closing_Range"] < 0.5)
    df["Dist_Count_25"] = df["Is_Distribution"].rolling(window=25, min_periods=1).sum().astype(int)
    return df

# ── TRENDWENDE-AMPEL (FIXED) ──
def compute_ampel(df):
    """
    Ankertag = erster Tag mit POSITIVEM Schluss (Close > Prev Close)
               ODER ein Erholungstag bei dem Close > Open UND Closing Range >= 0.5.
               Ein Tag mit negativem Schluss, bei dem die Kerze zufällig in der
               oberen Hälfte liegt, zählt NICHT.
    Bodenmarke = min(Low Ankertag, Low Vortag). Darf intraday nicht unterschritten werden.
    Startschuss = frühestens Tag 5 nach Ankertag: >=1% Gewinn, Vol > Vortag, Boden gehalten.
    Grün = Schlusskurs bleibt über Tief des Startschuss-Tages.
    """
    df = df.copy()
    n = len(df)
    phase = "neutral"
    anchor_idx = None; floor_mark = None
    startschuss_idx = None; startschuss_low = None

    phases = ["neutral"] * n
    anchor_dates = [None] * n
    floor_marks = [None] * n
    startschuss_lows = [None] * n

    closes = df["Close"].values; opens = df["Open"].values
    highs = df["High"].values; lows = df["Low"].values
    volumes = df["Volume"].values; pct_ch = df["Pct_Change"].values
    cr = df["Closing_Range"].values; dc = df["Dist_Count_25"].values
    sma50 = df["SMA50"].values

    for i in range(1, n):
        pct_i = pct_ch[i] if not np.isnan(pct_ch[i]) else 0.0
        cr_i = cr[i] if not np.isnan(cr[i]) else 0.5

        # Drawdown from 60-day high
        lb = max(0, i - 60)
        rh = np.nanmax(highs[lb:i+1])
        dd = (closes[i] - rh) / rh * 100 if rh > 0 else 0

        under50 = not np.isnan(sma50[i]) and closes[i] < sma50[i]
        has_dist = dc[i] >= 4

        # Enter ROT
        if phase in ("neutral", "reset"):
            if dd < -8 or (under50 and has_dist):
                phase = "rot"; anchor_idx = None; floor_mark = None
                startschuss_idx = None; startschuss_low = None

        if phase == "rot":
            # Check floor breach (after anchor day)
            if anchor_idx is not None and i > anchor_idx:
                if lows[i] < floor_mark:
                    anchor_idx = None; floor_mark = None

            # Look for Ankertag
            if anchor_idx is None:
                positive_close = pct_i > 0.0
                # Recovery candle: close > open AND upper half of range
                recovery = closes[i] > opens[i] and cr_i >= 0.5
                if positive_close or recovery:
                    anchor_idx = i
                    floor_mark = min(lows[i], lows[i-1])

            # Check Startschuss (>= 5 days after anchor)
            if anchor_idx is not None and i >= anchor_idx + 5:
                if pct_i >= 1.0 and volumes[i] > volumes[i-1] and lows[i] >= floor_mark:
                    phase = "gelb"; startschuss_idx = i; startschuss_low = lows[i]

        if phase == "gelb":
            if startschuss_low is not None and closes[i] < startschuss_low:
                phase = "rot"; anchor_idx = None; floor_mark = None
                startschuss_idx = None; startschuss_low = None
            elif startschuss_idx is not None and i > startschuss_idx + 2:
                phase = "gruen"

        if phase == "gruen":
            if startschuss_low is not None and closes[i] < startschuss_low:
                phase = "rot"; anchor_idx = None; floor_mark = None
                startschuss_idx = None; startschuss_low = None

        phases[i] = phase
        if anchor_idx is not None: anchor_dates[i] = df.index[anchor_idx].strftime("%Y-%m-%d")
        if floor_mark is not None: floor_marks[i] = round(floor_mark, 2)
        if startschuss_low is not None: startschuss_lows[i] = round(startschuss_low, 2)

    df["Ampel_Phase"] = phases
    df["Anchor_Date"] = anchor_dates
    df["Floor_Mark"] = floor_marks
    df["Startschuss_Low"] = startschuss_lows
    return df

# ── BREADTH ──
def compute_breadth_mode(df_ew):
    df_ew = df_ew.copy()
    df_ew["High_52w"] = df_ew["High"].rolling(252, min_periods=1).max()
    df_ew["Dist_52w_pct"] = (df_ew["Close"] - df_ew["High_52w"]) / df_ew["High_52w"] * 100
    df_ew["Breadth_Mode"] = df_ew["Dist_52w_pct"].apply(lambda d: "schutz" if d < -8 else "wachsam" if d < -4 else "rueckenwind")
    return df_ew

def analyze_vix(df_vix):
    df_vix = df_vix.copy()
    df_vix["SMA10"] = calc_sma(df_vix["Close"], 10)
    df_vix["Pct_Above_SMA10"] = (df_vix["Close"] - df_vix["SMA10"]) / df_vix["SMA10"] * 100
    df_vix["Is_Panic"] = df_vix["Pct_Above_SMA10"] > 20
    return df_vix

# ── RENDER HELPERS ──
def render_ampel(phase):
    c = {"rot":"#ef4444","gelb":"#f59e0b","gruen":"#22c55e","neutral":"#64748b"}.get(phase,"#64748b")
    lbl, desc = {"rot":("ROT — Abwarten","Substanzielle Korrektur. Nicht kaufen. Ankertag beobachten."),
        "gelb":("GELB — Startschuss","Einstiegssignal! Erste Position(en) eröffnen (10–30% Kapital)."),
        "gruen":("GRÜN — Bestätigung","Startschuss hält. Aufwärtstrend-Steuerung aktiv."),
        "neutral":("NEUTRAL","Keine substanzielle Korrektur erkannt. Normale Marktbeobachtung.")}.get(phase,("NEUTRAL",""))
    st.markdown(f'<div class="ampel-box" style="background:{c}15;border:1px solid {c}40;"><div class="ampel-dot" style="background:{c};box-shadow:0 0 24px {c}80,0 0 48px {c}40;"></div><div><div style="font-size:1.1rem;font-weight:700;color:{c};letter-spacing:0.05em;">{lbl}</div><div style="font-size:0.8rem;color:#94a3b8;margin-top:2px;">{desc}</div></div></div>', unsafe_allow_html=True)

def render_check(label, ok, detail=""):
    cls = "check-ok" if ok else "check-fail"; icon = "✓" if ok else "✗"
    st.markdown(f'<div class="check-item"><div class="check-icon {cls}">{icon}</div><div style="flex:1;"><div style="font-size:0.85rem;color:#e2e8f0;">{label}</div><div style="font-size:0.7rem;color:#64748b;margin-top:1px;">{detail}</div></div></div>', unsafe_allow_html=True)

def render_breadth(mode, dist_pct):
    c = {"rueckenwind":"#22c55e","wachsam":"#f59e0b","schutz":"#ef4444"}.get(mode,"#64748b")
    lbl, desc = {"rueckenwind":("Rückenwind","≤4% vom 52W-Hoch. Breite Stärke."),"wachsam":("Wachsam","4–8% vom 52W-Hoch. Strenger auswählen."),"schutz":("Schutz",">8% vom 52W-Hoch. Kapitalschutz.")}.get(mode,("—",""))
    fp = min(100, abs(dist_pct)/12*100)
    st.markdown(f'<div class="info-card" style="background:{c}12;border-color:{c}30;"><div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;"><div style="width:12px;height:12px;border-radius:50%;background:{c};box-shadow:0 0 8px {c}60;"></div><span style="font-size:1rem;font-weight:700;color:{c};">Modus: {lbl}</span><span style="font-size:0.75rem;color:#94a3b8;">{dist_pct:.1f}% vom 52W-Hoch</span></div><div style="font-size:0.75rem;color:#94a3b8;margin-bottom:8px;">{desc}</div><div class="breadth-track"><div class="breadth-fill" style="width:{fp}%;"></div></div><div style="display:flex;justify-content:space-between;font-size:0.65rem;color:#64748b;"><span style="color:#22c55e;">Rückenwind</span><span style="color:#f59e0b;">Wachsam</span><span style="color:#ef4444;">Schutz</span></div></div>', unsafe_allow_html=True)

# ── CHARTS (FIXED: explicit list conversion for plotly) ──
def _x(idx):
    """DatetimeIndex → list of ISO strings for plotly."""
    return [d.strftime("%Y-%m-%d") for d in idx]

def _y(series):
    """Series → list replacing NaN with None so plotly draws gaps."""
    return [None if pd.isna(v) else round(float(v), 2) for v in series]

def plot_price_chart(df, show_days=90):
    dv = df.tail(show_days)
    x = _x(dv.index)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Close"]), name="Kurs", line=dict(color="#e2e8f0", width=2)))
    fig.add_trace(go.Scatter(x=x, y=_y(dv["EMA21"]), name="21-EMA", line=dict(color="#06b6d4", width=1, dash="dot")))
    fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA50"]), name="50-SMA", line=dict(color="#f97316", width=1, dash="dot")))
    fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA200"]), name="200-SMA", line=dict(color="#a855f7", width=1, dash="dash")))
    floors = dv["Floor_Mark"].dropna()
    if len(floors) > 0:
        fig.add_hline(y=float(floors.iloc[-1]), line_dash="dash", line_color="#ef4444", line_width=1, annotation_text="Bodenmarke", annotation_font_color="#ef4444")
    dist = dv[dv["Is_Distribution"]==True]
    if len(dist) > 0:
        fig.add_trace(go.Scatter(x=_x(dist.index), y=_y(dist["Close"]), name="Distributionstag", mode="markers", marker=dict(color="#ef4444", size=7, symbol="triangle-down")))
    stall = dv[dv["Is_Stall"]==True]
    if len(stall) > 0:
        fig.add_trace(go.Scatter(x=_x(stall.index), y=_y(stall["Close"]), name="Stau-Tag", mode="markers", marker=dict(color="#f59e0b", size=6, symbol="diamond")))
    fig.update_layout(template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0,r=0,t=30,b=0), height=380,
        legend=dict(orientation="h",yanchor="top",y=1.12,xanchor="left",x=0,font=dict(size=10,color="#94a3b8")),
        xaxis=dict(gridcolor="#1e293b",showgrid=True,tickfont=dict(size=9,color="#64748b")),
        yaxis=dict(gridcolor="#1e293b",showgrid=True,tickfont=dict(size=9,color="#64748b")),
        hovermode="x unified")
    return fig

def plot_volume_chart(df, show_days=90):
    dv = df.tail(show_days)
    x = _x(dv.index)
    pct = dv["Pct_Change"].fillna(0).values
    colors = ["#22c55e" if p >= 0 else "#ef4444" for p in pct]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=_y(dv["Volume"]), marker_color=colors, opacity=0.7))
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Vol_SMA50"]), name="50T-Schnitt", line=dict(color="#64748b", width=1, dash="dot")))
    fig.update_layout(template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0,r=0,t=10,b=0), height=140, showlegend=False,
        xaxis=dict(gridcolor="#1e293b",showgrid=False,tickfont=dict(size=9,color="#64748b")),
        yaxis=dict(gridcolor="#1e293b",showgrid=True,tickfont=dict(size=9,color="#64748b"),tickformat=".2s"))
    return fig

def plot_vix(df_vix, show_days=90):
    dv = df_vix.tail(show_days)
    x = _x(dv.index)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Close"]), name="VIX", line=dict(color="#ef4444", width=1.5)))
    if "SMA10" in dv.columns:
        fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA10"]), name="10-SMA", line=dict(color="#3b82f6", width=1, dash="dot")))
    fig.update_layout(template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0,r=0,t=10,b=0), height=170,
        legend=dict(orientation="h",yanchor="top",y=1.15,font=dict(size=10,color="#94a3b8")),
        xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),
        yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")))
    return fig

# ── MAIN ──
def main():
    st.title("BÖRSE OHNE BAUCHGEFÜHL")
    st.caption("Modul 1 — Markt-Dashboard & Trendwende-Ampel")

    with st.spinner("Lade Marktdaten von Yahoo Finance …"):
        data = load_market_data()
    if not data:
        st.error("Keine Marktdaten verfügbar. Bitte später erneut versuchen.")
        return

    available = [i for i in ["S&P 500","Nasdaq Composite","Russell 2000"] if i in data]
    if not available:
        st.error("Keine Index-Daten."); return

    c1, c2 = st.columns([3,1])
    with c1: selected = st.radio("Index", available, horizontal=True, label_visibility="collapsed")
    with c2: show_days = st.selectbox("Zeitraum", [60,90,130,200], index=1, format_func=lambda x: f"{x} Tage")

    df = add_indicators(data[selected].copy())
    df = detect_distribution_days(df)
    df = compute_ampel(df)
    L = df.iloc[-1]
    pct = L["Pct_Change"] if not np.isnan(L["Pct_Change"]) else 0.0

    st.markdown("---")
    render_ampel(L["Ampel_Phase"])
    st.markdown("")

    m1,m2,m3,m4 = st.columns(4)
    with m1: st.metric(selected, f"{L['Close']:,.2f}", f"{pct:+.2f}%")
    dc = int(L["Dist_Count_25"])
    with m2: st.metric("Dist.-Tage (25T)", dc, "⚠ Häufung" if dc >= 4 else "OK")
    d21 = L["Dist_21EMA"]
    with m3: st.metric("Abstand 21-EMA", f"{d21:.1f} ATR" if not np.isnan(d21) else "—", f"ATR: {L['ATR_pct']:.2f}%" if not np.isnan(L["ATR_pct"]) else "")
    dd = L["Dist_52w_pct"]
    with m4: st.metric("Drawdown", f"{dd:.1f}%" if not np.isnan(dd) else "—")

    st.markdown('<div class="card-label">KURSVERLAUF · GLEITENDE DURCHSCHNITTE · DISTRIBUTIONSTAGE</div>', unsafe_allow_html=True)
    st.plotly_chart(plot_price_chart(df, show_days), use_container_width=True, config={"displayModeBar": False})
    st.markdown('<div class="card-label">VOLUMEN (grün=Gewinntag · rot=Verlusttag · grau=50T-Schnitt)</div>', unsafe_allow_html=True)
    st.plotly_chart(plot_volume_chart(df, show_days), use_container_width=True, config={"displayModeBar": False})

    cl, cr_ = st.columns(2)
    with cl:
        st.markdown('<div class="info-card"><div class="card-label">AUFWÄRTSTREND-PRÜFUNG</div>', unsafe_allow_html=True)
        a200 = not np.isnan(L["SMA200"]) and L["Close"]>L["SMA200"]
        a50 = not np.isnan(L["SMA50"]) and L["Close"]>L["SMA50"]
        a21 = not np.isnan(L["EMA21"]) and L["Close"]>L["EMA21"]
        mao = bool(L["MA_Order"]) if not pd.isna(L["MA_Order"]) else False
        render_check("Kurs über 200-SMA", a200, f"{L['Close']:.0f} vs {L['SMA200']:.0f}" if not np.isnan(L["SMA200"]) else "")
        render_check("Kurs über 50-SMA", a50, f"{L['Close']:.0f} vs {L['SMA50']:.0f}" if not np.isnan(L["SMA50"]) else "")
        render_check("Kurs über 21-EMA", a21, f"{L['Close']:.0f} vs {L['EMA21']:.0f}" if not np.isnan(L["EMA21"]) else "")
        render_check("21>50>200 (richtige Ordnung)", mao, "Durchschnitte korrekt gestaffelt")
        render_check("Distributionstage ≤ 3", dc<=3, f"{dc} im 25T-Fenster")
        st.markdown("</div>", unsafe_allow_html=True)

    with cr_:
        st.markdown('<div class="info-card"><div class="card-label">TRENDWENDE-AMPEL DETAILS</div>', unsafe_allow_html=True)
        rows = {"Aktuelle Phase": L["Ampel_Phase"].upper(), "Ankertag": L["Anchor_Date"] or "—",
                "Bodenmarke": f"{L['Floor_Mark']:.2f}" if L["Floor_Mark"] else "—",
                "Startschuss-Tief": f"{L['Startschuss_Low']:.2f}" if L["Startschuss_Low"] else "—",
                "MA-Ordnung": "Korrekt ✓" if mao else "Gestört ✗"}
        st.dataframe(pd.DataFrame({"Kennzahl":rows.keys(),"Wert":rows.values()}).set_index("Kennzahl"), use_container_width=True, height=220)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div class="card-label">MARKTBREITE (Equal-Weight Abstand zum 52W-Hoch)</div>', unsafe_allow_html=True)
    ep = "RSP (Equal-Weight S&P)" if "S&P" in selected else "QQEW (Equal-Weight Nasdaq)"
    es = "QQEW (Equal-Weight Nasdaq)" if "S&P" in selected else "RSP (Equal-Weight S&P)"
    bc1, bc2 = st.columns(2)
    for col, etf in [(bc1,ep),(bc2,es)]:
        with col:
            if etf in data:
                dfe = compute_breadth_mode(data[etf].copy())
                le = dfe.iloc[-1]
                render_breadth(le["Breadth_Mode"], float(le["Dist_52w_pct"]))
                st.caption(etf)
            else: st.info(f"{etf} nicht verfügbar.")

    st.markdown("---")
    st.markdown('<div class="card-label">VOLATILITÄT & STIMMUNG</div>', unsafe_allow_html=True)
    vc1, vc2 = st.columns([2,1])
    with vc1:
        if "VIX" in data:
            dfv = analyze_vix(data["VIX"].copy())
            st.plotly_chart(plot_vix(dfv, show_days), use_container_width=True, config={"displayModeBar": False})
    with vc2:
        if "VIX" in data:
            dfv = analyze_vix(data["VIX"].copy())
            vl = dfv.iloc[-1]
            st.metric("VIX", f"{vl['Close']:.1f}")
            if not np.isnan(vl["SMA10"]):
                st.metric("10T-SMA", f"{vl['SMA10']:.1f}")
                pab = vl["Pct_Above_SMA10"]
                if not np.isnan(pab):
                    if pab > 20: st.error(f"⚠ VIX {pab:.0f}% über SMA → Panik")
                    elif vl["Close"] > 20: st.warning(f"VIX erhöht ({pab:+.0f}% vs SMA)")
                    else: st.success(f"VIX ruhig ({pab:+.0f}% vs SMA)")
        if "VXX" in data:
            dfx = data["VXX"].copy(); dfx["EMA21"] = calc_ema(dfx["Close"], 21)
            xl = dfx.iloc[-1]; above = xl["Close"] > xl["EMA21"]
            rising = dfx["EMA21"].iloc[-1] > dfx["EMA21"].iloc[-5] if len(dfx) > 5 else False
            if above and rising: st.warning("VIXY über steigender 21-EMA → Risk-Off")
            elif not above and not rising: st.success("VIXY unter fallender 21-EMA → Risk-On")
            else: st.info("VIXY gemischt")

    st.markdown("---")
    st.markdown('<div class="info-card"><div class="card-label">TÄGLICHE CHECKLISTE</div>', unsafe_allow_html=True)
    ddv = float(L["Dist_52w_pct"]) if not np.isnan(L["Dist_52w_pct"]) else 0
    render_check("Substanzielle Korrektur?", ddv < -8, f"Drawdown: {ddv:.1f}%")
    render_check("Stabilisierung erkannt?", L["Ampel_Phase"]!="rot" or L["Anchor_Date"] is not None, f"Ankertag: {L['Anchor_Date']}" if L["Anchor_Date"] else "Noch keine")
    render_check("Startschuss (Phase ≥ Gelb)?", L["Ampel_Phase"] in ("gelb","gruen"), f"Phase: {L['Ampel_Phase'].upper()}")
    if ep in data:
        dfe = compute_breadth_mode(data[ep].copy()); bm = dfe.iloc[-1]["Breadth_Mode"]
        render_check("Marktbreite unterstützend?", bm!="schutz", f"Modus: {bm.capitalize()}")
    render_check("Distributionstage ≤ 3?", dc<=3, f"{dc} im 25T-Fenster")
    if "VIX" in data:
        dfv = analyze_vix(data["VIX"].copy()); ip = bool(dfv.iloc[-1].get("Is_Panic", False))
        render_check("VIX nicht in Panik?", not ip, f"VIX: {data['VIX'].iloc[-1]['Close']:.1f}")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.caption(f"Börse ohne Bauchgefühl · v1.1 · Yahoo Finance · Stand: {L.name.strftime('%d.%m.%Y')}")
    st.caption("Basierend auf dem Handelssystem von Aljoscha Groos")

if __name__ == "__main__":
    main()
