"""
Börse ohne Bauchgefühl — Modul 1: Markt-Dashboard & Trendwende-Ampel v2.0
Basierend auf dem Handelssystem von Aljoscha Groos
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

# ═══════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&display=swap');
.stApp{background-color:#0a0e17;color:#e2e8f0;font-family:'JetBrains Mono',monospace}
.main .block-container{padding-top:1.5rem;max-width:1200px}
h1,h2,h3{font-family:'JetBrains Mono',monospace!important}
h1{font-size:1.6rem!important;font-weight:800!important;background:linear-gradient(135deg,#06b6d4,#3b82f6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
[data-testid="stMetric"]{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:12px 16px}
[data-testid="stMetricLabel"]{color:#64748b!important;font-size:.75rem!important;text-transform:uppercase;letter-spacing:.08em}
[data-testid="stMetricValue"]{color:#e2e8f0!important;font-size:1.4rem!important;font-weight:700!important}
.stTabs [data-baseweb="tab-list"]{gap:4px;background:transparent}
.stTabs [data-baseweb="tab"]{background:#111827;border:1px solid #1e293b;border-radius:8px;color:#94a3b8;padding:8px 16px;font-size:.8rem}
.stTabs [aria-selected="true"]{background:#06b6d420;border-color:#06b6d4;color:#06b6d4}
.ampel-box{border-radius:12px;padding:16px 20px;display:flex;align-items:center;gap:16px}
.ampel-dot{width:48px;height:48px;border-radius:50%;flex-shrink:0}
.check-item{display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid #1e293b}
.check-icon{width:22px;height:22px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700}
.check-ok{background:#22c55e20;border:1.5px solid #22c55e50;color:#22c55e}
.check-fail{background:#ef444420;border:1.5px solid #ef444450;color:#ef4444}
.check-warn{background:#f59e0b20;border:1.5px solid #f59e0b50;color:#f59e0b}
.info-card{background:#111827;border:1px solid #1e293b;border-radius:12px;padding:16px;margin-bottom:12px}
.card-label{font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}
.breadth-track{height:10px;border-radius:5px;background:#1e293b;position:relative;overflow:hidden;margin:8px 0}
.breadth-fill{position:absolute;left:0;top:0;bottom:0;border-radius:5px;background:linear-gradient(90deg,#22c55e,#f59e0b,#ef4444);transition:width .5s}
.warn-badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.7rem;font-weight:600;margin:2px}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════
def _dl(symbol, start, end):
    try:
        df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or len(df) == 0: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index); df = df.sort_index()
        for c in ["Open","High","Low","Close","Volume"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["Close"])
    except: return None

@st.cache_data(ttl=900, show_spinner=False)
def load_market_data(lookback_days=400):
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    tickers = {
        "S&P 500":"^GSPC","Nasdaq Composite":"^IXIC","Russell 2000":"^RUT",
        "RSP (Equal-Weight S&P)":"RSP","QQEW (Equal-Weight Nasdaq)":"QQEW",
        "VIX":"^VIX","VXX":"VIXY",
        # Sector ETFs for rotation detection
        "XLU (Utilities)":"XLU","XLP (Consumer Staples)":"XLP",
        "XLK (Technology)":"XLK","XLY (Consumer Discr.)":"XLY",
    }
    data = {}
    for name, sym in tickers.items():
        df = _dl(sym, start, end)
        if df is not None and len(df) > 20: data[name] = df
    return data

# ═══════════════════════════════════════════════════════
# TECHNICAL HELPERS
# ═══════════════════════════════════════════════════════
def _ema(s, p): return s.ewm(span=p, adjust=False).mean()
def _sma(s, p): return s.rolling(window=p, min_periods=p).mean()
def _atr(df, p=21):
    h, l, pc = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([h-l,(h-pc).abs(),(l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(window=p, min_periods=p).mean()

def _consec(series):
    out = np.zeros(len(series), dtype=int)
    for i in range(len(series)):
        if series.iloc[i]: out[i] = out[i-1]+1 if i>0 else 1
    return pd.Series(out, index=series.index)

# ═══════════════════════════════════════════════════════
# ADD ALL INDICATORS
# ═══════════════════════════════════════════════════════
def add_indicators(df):
    df = df.copy()
    df["EMA21"] = _ema(df["Close"],21); df["SMA50"] = _sma(df["Close"],50); df["SMA200"] = _sma(df["Close"],200)
    df["ATR21"] = _atr(df,21); df["ATR_pct"] = df["ATR21"]/df["Close"]*100
    df["Vol_SMA50"] = _sma(df["Volume"],50)
    df["Pct_Change"] = df["Close"].pct_change()*100
    rng = df["High"]-df["Low"]
    df["Closing_Range"] = np.where(rng>0,(df["Close"]-df["Low"])/rng,0.5)
    df["Dist_21EMA"] = (df["Close"]-df["EMA21"])/df["ATR21"]
    df["Dist_50SMA_pct"] = (df["Close"]-df["SMA50"])/df["SMA50"]*100
    df["High_52w"] = df["High"].rolling(252,min_periods=1).max()
    df["Dist_52w_pct"] = (df["Close"]-df["High_52w"])/df["High_52w"]*100
    df["MA_Order"] = (df["EMA21"]>df["SMA50"])&(df["SMA50"]>df["SMA200"])

    # Consecutive days Low above each MA
    df["Low_above_21"] = df["Low"]>df["EMA21"]; df["Low_above_50"] = df["Low"]>df["SMA50"]; df["Low_above_200"] = df["Low"]>df["SMA200"]
    df["Consec_Low_above_21"] = _consec(df["Low_above_21"])
    df["Consec_Low_above_50"] = _consec(df["Low_above_50"])
    df["Consec_Low_above_200"] = _consec(df["Low_above_200"])

    # MA held = Close above MA
    df["EMA21_held"] = df["Close"]>df["EMA21"]
    df["SMA50_held"] = df["Close"]>df["SMA50"]
    df["SMA200_held"] = df["Close"]>df["SMA200"]

    # ── NEW: Intraday reversal detection ──
    # Negative reversal: Open > prev Close (gap up or strong open) but Close < Open (sold off)
    df["Intraday_Reversal_Down"] = (df["Open"]>df["Close"].shift(1)) & (df["Close"]<df["Open"]) & (df["Closing_Range"]<0.35)
    # Positive reversal: Open < prev Close but Close > Open and upper half
    df["Intraday_Reversal_Up"] = (df["Open"]<df["Close"].shift(1)) & (df["Close"]>df["Open"]) & (df["Closing_Range"]>0.65)
    # Count neg reversals in last 10 days
    df["Neg_Reversals_10d"] = df["Intraday_Reversal_Down"].rolling(10,min_periods=1).sum().astype(int)

    # ── NEW: Closing Range clustering (how many of last 5 days closed in lower 25%) ──
    df["Low_CR"] = df["Closing_Range"]<0.25
    df["Low_CR_5d"] = df["Low_CR"].rolling(5,min_periods=1).sum().astype(int)

    # ── NEW: Declining volume on up days (rally failing signal) ──
    # Check last 5 up-days: is volume declining?
    is_up = df["Close"]>df["Close"].shift(1)
    up_vol = df["Volume"].where(is_up)
    df["Up_Vol_SMA5"] = up_vol.ffill().rolling(5,min_periods=2).mean()
    df["Up_Vol_Declining"] = df["Up_Vol_SMA5"]<df["Up_Vol_SMA5"].shift(5)

    return df

# ═══════════════════════════════════════════════════════
# DISTRIBUTION & STALL DAYS
# ═══════════════════════════════════════════════════════
def detect_distribution_days(df):
    df = df.copy()
    pc = df["Close"].shift(1); pv = df["Volume"].shift(1)
    is_down = df["Close"]<pc
    high_vol = (df["Volume"]>pv)|(df["Volume"]>df["Vol_SMA50"])
    df["Is_Distribution"] = is_down & high_vol
    pct = df["Pct_Change"]
    df["Is_Stall"] = (~is_down)&(pct<0.5)&(df["Volume"]>=pv*0.95)&(df["Closing_Range"]<0.5)
    df["Dist_Count_25"] = df["Is_Distribution"].rolling(25,min_periods=1).sum().astype(int)
    return df

# ═══════════════════════════════════════════════════════
# TRENDWENDE-AMPEL v3
# ═══════════════════════════════════════════════════════
def compute_ampel(df):
    df = df.copy(); n = len(df)
    phase = "neutral"
    anchor_idx = None; floor_mark = None
    startschuss_idx = None; startschuss_low = None; gruen_since = None

    phases=["neutral"]*n; anchor_dates=[None]*n; floor_marks=[None]*n; startschuss_lows=[None]*n
    c_=df["Close"].values; o_=df["Open"].values; h_=df["High"].values; l_=df["Low"].values
    v_=df["Volume"].values; pct_=df["Pct_Change"].values; cr_=df["Closing_Range"].values
    dc_=df["Dist_Count_25"].values; s50_=df["SMA50"].values; s200_=df["SMA200"].values; e21_=df["EMA21"].values

    def _clear():
        nonlocal anchor_idx,floor_mark,startschuss_idx,startschuss_low,gruen_since
        anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None

    def _corr(i):
        lb=max(0,i-60); rh=np.nanmax(h_[lb:i+1])
        dd=(c_[i]-rh)/rh*100 if rh>0 else 0
        u50=not np.isnan(s50_[i]) and c_[i]<s50_[i]
        return dd<-8 or (u50 and dc_[i]>=4)

    for i in range(1,n):
        pi = pct_[i] if not np.isnan(pct_[i]) else 0.0
        cri = cr_[i] if not np.isnan(cr_[i]) else 0.5

        if phase in ("neutral","aufwaertstrend"):
            if _corr(i): phase="rot"; _clear()
            elif phase=="aufwaertstrend":
                if not np.isnan(e21_[i]) and not np.isnan(s50_[i]) and e21_[i]<s50_[i]:
                    phase="rot"; _clear()

        elif phase=="rot":
            if anchor_idx is not None and i>anchor_idx:
                if l_[i]<floor_mark: anchor_idx=None; floor_mark=None
            if anchor_idx is None:
                pos_close = pi>0.0; recovery = c_[i]>o_[i] and cri>=0.5
                if pos_close or recovery:
                    anchor_idx=i; floor_mark=min(l_[i],l_[i-1])
            if anchor_idx is not None and i>=anchor_idx+5:
                if pi>=1.0 and v_[i]>v_[i-1] and l_[i]>=floor_mark:
                    phase="gelb"; startschuss_idx=i; startschuss_low=l_[i]

        elif phase=="gelb":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot"; _clear()
            elif startschuss_idx is not None and i>startschuss_idx+2: phase="gruen"; gruen_since=i

        elif phase=="gruen":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot"; _clear()
            else:
                a200=not np.isnan(s200_[i]) and c_[i]>s200_[i]
                eas=not np.isnan(e21_[i]) and not np.isnan(s50_[i]) and e21_[i]>s50_[i]
                dig=i-gruen_since if gruen_since else 0
                if a200 and eas and dig>=10: phase="aufwaertstrend"; _clear()

        phases[i]=phase
        if anchor_idx is not None: anchor_dates[i]=df.index[anchor_idx].strftime("%Y-%m-%d")
        if floor_mark is not None: floor_marks[i]=round(floor_mark,2)
        if startschuss_low is not None: startschuss_lows[i]=round(startschuss_low,2)

    df["Ampel_Phase"]=phases; df["Anchor_Date"]=anchor_dates
    df["Floor_Mark"]=floor_marks; df["Startschuss_Low"]=startschuss_lows
    return df

# ═══════════════════════════════════════════════════════
# BREADTH MODE with 3-day stability rule
# ═══════════════════════════════════════════════════════
def compute_breadth_mode(df_ew):
    df_ew = df_ew.copy()
    df_ew["High_52w"] = df_ew["High"].rolling(252,min_periods=1).max()
    df_ew["Dist_52w_pct"] = (df_ew["Close"]-df_ew["High_52w"])/df_ew["High_52w"]*100

    def _raw(d):
        if d<-8: return "schutz"
        if d<-4: return "wachsam"
        return "rueckenwind"

    df_ew["Raw_Mode"] = df_ew["Dist_52w_pct"].apply(_raw)

    # 3-day stability rule: mode only changes if new mode persists 3 days
    modes = df_ew["Raw_Mode"].values
    stable = [modes[0]]
    pending = None; pending_count = 0
    for i in range(1, len(modes)):
        if modes[i] != stable[-1]:
            if pending == modes[i]: pending_count += 1
            else: pending = modes[i]; pending_count = 1
            if pending_count >= 3: stable.append(pending); pending = None; pending_count = 0
            else: stable.append(stable[-1])
        else:
            stable.append(modes[i]); pending = None; pending_count = 0

    df_ew["Breadth_Mode"] = stable
    return df_ew

def analyze_vix(df_vix):
    df_vix = df_vix.copy()
    df_vix["SMA10"] = _sma(df_vix["Close"],10)
    df_vix["Pct_Above_SMA10"] = (df_vix["Close"]-df_vix["SMA10"])/df_vix["SMA10"]*100
    df_vix["Is_Panic"] = df_vix["Pct_Above_SMA10"]>20
    return df_vix

# ═══════════════════════════════════════════════════════
# INTERMARKET DIVERGENCE
# ═══════════════════════════════════════════════════════
def detect_intermarket_divergence(data):
    """Compare indices: if one makes new 20-day high while another doesn't."""
    results = []
    indices = ["S&P 500","Nasdaq Composite","Russell 2000"]
    available = {k:data[k] for k in indices if k in data}
    if len(available) < 2: return results

    for name, df in available.items():
        high_20 = df["High"].rolling(20,min_periods=10).max()
        at_high = df["Close"].iloc[-1] >= high_20.iloc[-2] * 0.998  # within 0.2% of 20d high
        results.append({"name": name, "at_20d_high": at_high,
                        "pct_from_20d_high": round((df["Close"].iloc[-1]/high_20.iloc[-1]-1)*100, 2)})
    return results

# ═══════════════════════════════════════════════════════
# SECTOR ROTATION DETECTION
# ═══════════════════════════════════════════════════════
def detect_sector_rotation(data):
    """Compare defensive (XLU, XLP) vs offensive (XLK, XLY) performance."""
    def _perf(name, days=10):
        if name not in data: return None
        df = data[name]
        if len(df) < days+1: return None
        return (df["Close"].iloc[-1] / df["Close"].iloc[-days-1] - 1) * 100

    defensive = [("XLU (Utilities)", _perf("XLU (Utilities)")),
                 ("XLP (Consumer Staples)", _perf("XLP (Consumer Staples)"))]
    offensive = [("XLK (Technology)", _perf("XLK (Technology)")),
                 ("XLY (Consumer Discr.)", _perf("XLY (Consumer Discr.)"))]

    def_perfs = [p for _,p in defensive if p is not None]
    off_perfs = [p for _,p in offensive if p is not None]

    if not def_perfs or not off_perfs: return None, None, None
    avg_def = np.mean(def_perfs); avg_off = np.mean(off_perfs)
    rotation = avg_def > avg_off  # True = defensive outperforming = warning
    return rotation, {"Defensiv":defensive,"Offensiv":offensive}, avg_def-avg_off

# ═══════════════════════════════════════════════════════
# FAILING RALLY DETECTION
# ═══════════════════════════════════════════════════════
def detect_failing_rally(df):
    """After a decline, check if recovery retraces < 50% of the drop."""
    # Find recent swing high (highest close in last 60 days from 20 days ago)
    if len(df) < 30: return None, None
    recent = df.tail(60)
    high_idx = recent["Close"].idxmax()
    high_val = recent.loc[high_idx,"Close"]
    # Find low after that high
    after_high = recent.loc[high_idx:]
    if len(after_high) < 5: return None, None
    low_idx = after_high["Close"].idxmin()
    low_val = after_high.loc[low_idx,"Close"]
    drop = high_val - low_val
    if drop / high_val < 0.03: return None, None  # less than 3% drop, not a correction

    # Current close as recovery
    current = df["Close"].iloc[-1]
    recovery = current - low_val
    recovery_pct = (recovery / drop * 100) if drop > 0 else 0
    return round(recovery_pct, 1), round(drop / high_val * 100, 1)

# ═══════════════════════════════════════════════════════
# RENDER HELPERS
# ═══════════════════════════════════════════════════════
def render_ampel(phase):
    c={"rot":"#ef4444","gelb":"#f59e0b","gruen":"#22c55e","aufwaertstrend":"#3b82f6","neutral":"#64748b"}.get(phase,"#64748b")
    lbl,desc={"rot":("ROT — Abwarten","Substanzielle Korrektur. Nicht kaufen. Ankertag beobachten."),
        "gelb":("GELB — Startschuss","Einstiegssignal! Erste Position(en) eröffnen (10–30% Kapital)."),
        "gruen":("GRÜN — Bestätigung","Startschuss hält. Frühe Bestätigungsphase."),
        "aufwaertstrend":("AUFWÄRTSTREND ↑","MA-Ordnung bestätigt. Offensiv handeln, Exponierung erhöhen."),
        "neutral":("NEUTRAL","Keine substanzielle Korrektur erkannt. Normale Marktbeobachtung.")}.get(phase,("NEUTRAL",""))
    st.markdown(f'<div class="ampel-box" style="background:{c}15;border:1px solid {c}40;"><div class="ampel-dot" style="background:{c};box-shadow:0 0 24px {c}80,0 0 48px {c}40;"></div><div><div style="font-size:1.1rem;font-weight:700;color:{c};letter-spacing:.05em;">{lbl}</div><div style="font-size:.8rem;color:#94a3b8;margin-top:2px;">{desc}</div></div></div>', unsafe_allow_html=True)

def render_check(label, ok, detail="", warn=False):
    cls = "check-warn" if warn else ("check-ok" if ok else "check-fail")
    icon = "⚠" if warn else ("✓" if ok else "✗")
    st.markdown(f'<div class="check-item"><div class="check-icon {cls}">{icon}</div><div style="flex:1;"><div style="font-size:.85rem;color:#e2e8f0;">{label}</div><div style="font-size:.7rem;color:#64748b;margin-top:1px;">{detail}</div></div></div>', unsafe_allow_html=True)

def render_breadth(mode, dist_pct):
    c={"rueckenwind":"#22c55e","wachsam":"#f59e0b","schutz":"#ef4444"}.get(mode,"#64748b")
    lbl,desc={"rueckenwind":("Rückenwind","≤4% vom 52W-Hoch. Breite Stärke."),"wachsam":("Wachsam","4–8% vom 52W-Hoch. Strenger auswählen."),"schutz":("Schutz",">8% vom 52W-Hoch. Kapitalschutz.")}.get(mode,("—",""))
    fp=min(100,abs(dist_pct)/12*100)
    st.markdown(f'<div class="info-card" style="background:{c}12;border-color:{c}30;"><div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;"><div style="width:12px;height:12px;border-radius:50%;background:{c};box-shadow:0 0 8px {c}60;"></div><span style="font-size:1rem;font-weight:700;color:{c};">Modus: {lbl}</span><span style="font-size:.75rem;color:#94a3b8;">{dist_pct:.1f}% vom 52W-Hoch</span></div><div style="font-size:.75rem;color:#94a3b8;margin-bottom:8px;">{desc}</div><div class="breadth-track"><div class="breadth-fill" style="width:{fp}%;"></div></div><div style="display:flex;justify-content:space-between;font-size:.65rem;color:#64748b;"><span style="color:#22c55e;">Rückenwind</span><span style="color:#f59e0b;">Wachsam</span><span style="color:#ef4444;">Schutz</span></div></div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════
def _x(idx): return [d.strftime("%Y-%m-%d") for d in idx]
def _y(s): return [None if pd.isna(v) else round(float(v),2) for v in s]

def plot_price_chart(df, show_days=90):
    dv=df.tail(show_days); x=_x(dv.index)
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(dv["Close"]),name="Kurs",line=dict(color="#e2e8f0",width=2)))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["EMA21"]),name="21-EMA",line=dict(color="#06b6d4",width=1,dash="dot")))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA50"]),name="50-SMA",line=dict(color="#f97316",width=1,dash="dot")))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA200"]),name="200-SMA",line=dict(color="#a855f7",width=1,dash="dash")))
    floors=dv["Floor_Mark"].dropna()
    if len(floors)>0: fig.add_hline(y=float(floors.iloc[-1]),line_dash="dash",line_color="#ef4444",line_width=1,annotation_text="Bodenmarke",annotation_font_color="#ef4444")
    dist=dv[dv["Is_Distribution"]==True]
    if len(dist)>0: fig.add_trace(go.Scatter(x=_x(dist.index),y=_y(dist["Close"]),name="Distributionstag",mode="markers",marker=dict(color="#ef4444",size=7,symbol="triangle-down")))
    stall=dv[dv["Is_Stall"]==True]
    if len(stall)>0: fig.add_trace(go.Scatter(x=_x(stall.index),y=_y(stall["Close"]),name="Stau-Tag",mode="markers",marker=dict(color="#f59e0b",size=6,symbol="diamond")))
    # Mark intraday reversals down
    rev_down=dv[dv["Intraday_Reversal_Down"]==True]
    if len(rev_down)>0: fig.add_trace(go.Scatter(x=_x(rev_down.index),y=_y(rev_down["High"]),name="Intraday-Umkehr ↓",mode="markers",marker=dict(color="#f97316",size=8,symbol="x")))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=30,b=0),height=380,
        legend=dict(orientation="h",yanchor="top",y=1.12,xanchor="left",x=0,font=dict(size=10,color="#94a3b8")),
        xaxis=dict(gridcolor="#1e293b",showgrid=True,tickfont=dict(size=9,color="#64748b")),
        yaxis=dict(gridcolor="#1e293b",showgrid=True,tickfont=dict(size=9,color="#64748b")),hovermode="x unified")
    return fig

def plot_volume_chart(df, show_days=90):
    dv=df.tail(show_days); x=_x(dv.index)
    pct=dv["Pct_Change"].fillna(0).values; colors=["#22c55e" if p>=0 else "#ef4444" for p in pct]
    fig=go.Figure()
    fig.add_trace(go.Bar(x=x,y=_y(dv["Volume"]),marker_color=colors,opacity=0.7))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["Vol_SMA50"]),name="50T-Schnitt",line=dict(color="#64748b",width=1,dash="dot")))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=140,showlegend=False,
        xaxis=dict(gridcolor="#1e293b",showgrid=False,tickfont=dict(size=9,color="#64748b")),
        yaxis=dict(gridcolor="#1e293b",showgrid=True,tickfont=dict(size=9,color="#64748b"),tickformat=".2s"))
    return fig

def plot_vix(df_vix, show_days=90):
    dv=df_vix.tail(show_days); x=_x(dv.index)
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(dv["Close"]),name="VIX",line=dict(color="#ef4444",width=1.5)))
    if "SMA10" in dv.columns: fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA10"]),name="10-SMA",line=dict(color="#3b82f6",width=1,dash="dot")))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=170,
        legend=dict(orientation="h",yanchor="top",y=1.15,font=dict(size=10,color="#94a3b8")),
        xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),
        yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")))
    return fig

# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════
def main():
    st.title("BÖRSE OHNE BAUCHGEFÜHL")
    st.caption("Modul 1 — Markt-Dashboard & Trendwende-Ampel v2.0")

    with st.spinner("Lade Marktdaten von Yahoo Finance …"):
        data = load_market_data()
    if not data: st.error("Keine Marktdaten verfügbar."); return

    available = [i for i in ["S&P 500","Nasdaq Composite","Russell 2000"] if i in data]
    if not available: st.error("Keine Index-Daten."); return

    c1,c2 = st.columns([3,1])
    with c1: selected = st.radio("Index",available,horizontal=True,label_visibility="collapsed")
    with c2: show_days = st.selectbox("Zeitraum",[60,90,130,200],index=1,format_func=lambda x:f"{x} Tage")

    df = add_indicators(data[selected].copy()); df = detect_distribution_days(df); df = compute_ampel(df)
    L = df.iloc[-1]; pct = L["Pct_Change"] if not np.isnan(L["Pct_Change"]) else 0.0

    # ── AMPEL ──
    st.markdown("---")
    render_ampel(L["Ampel_Phase"])
    st.markdown("")

    # ── METRICS ──
    m1,m2,m3,m4,m5 = st.columns(5)
    dc = int(L["Dist_Count_25"])
    with m1: st.metric(selected, f"{L['Close']:,.2f}", f"{pct:+.2f}%")
    with m2: st.metric("Dist.-Tage (25T)", dc, "⚠ Häufung" if dc>=4 else "OK")
    d21 = L["Dist_21EMA"]
    with m3: st.metric("21-EMA Abstand", f"{d21:.1f} ATR" if not np.isnan(d21) else "—")
    d50 = L["Dist_50SMA_pct"]
    # NEW: 50-SMA distance with index-specific thresholds
    thresh_50 = 7.0 if "Nasdaq" in selected else 5.0
    with m4: st.metric("50-SMA Abstand", f"{d50:+.1f}%" if not np.isnan(d50) else "—",
                        f"⚠ >{thresh_50:.0f}%" if (not np.isnan(d50) and d50>thresh_50) else "")
    dd = L["Dist_52w_pct"]
    with m5: st.metric("Drawdown", f"{dd:.1f}%" if not np.isnan(dd) else "—")

    # ── CHARTS ──
    st.markdown('<div class="card-label">KURSVERLAUF · GLEITENDE DURCHSCHNITTE · DISTRIBUTIONSTAGE · INTRADAY-UMKEHRUNGEN</div>', unsafe_allow_html=True)
    st.plotly_chart(plot_price_chart(df, show_days), use_container_width=True, config={"displayModeBar":False})
    st.markdown('<div class="card-label">VOLUMEN</div>', unsafe_allow_html=True)
    st.plotly_chart(plot_volume_chart(df, show_days), use_container_width=True, config={"displayModeBar":False})

    # ═══════════════════════════════════════════════════
    # WARNING SIGNALS PANEL  (NEW)
    # ═══════════════════════════════════════════════════
    st.markdown("---")
    st.markdown('<div class="info-card"><div class="card-label">FRÜHWARNZEICHEN & WARNZEICHEN FÜR KORREKTUR</div>', unsafe_allow_html=True)

    warn_count = 0

    # 1. Intraday reversals
    neg_rev = int(L.get("Neg_Reversals_10d",0))
    is_warn = neg_rev >= 3
    if is_warn: warn_count += 1
    render_check("Intraday-Umkehrungen (letzte 10 Tage)", neg_rev<3,
                 f"{neg_rev} negative Umkehrungen (starker Open → schwacher Close)", warn=is_warn)

    # 2. Closing range clustering
    low_cr = int(L.get("Low_CR_5d",0))
    is_warn = low_cr >= 3
    if is_warn: warn_count += 1
    render_check("Closing Range (letzte 5 Tage)", low_cr<3,
                 f"{low_cr} von 5 Tagen mit Schluss im unteren 25% der Spanne", warn=is_warn)

    # 3. Distribution days
    is_warn = dc >= 4
    if is_warn: warn_count += 1
    render_check("Distributionstage", dc<=3, f"{dc} im 25-Tage-Fenster (Häufung ab 4)", warn=is_warn)

    # 4. Stall days in last 10 days
    stall_10 = int(df["Is_Stall"].tail(10).sum())
    is_warn = stall_10 >= 3
    if is_warn: warn_count += 1
    render_check("Stau-Tage (letzte 10 Tage)", stall_10<3, f"{stall_10} Stau-Tage", warn=is_warn)

    # 5. Distance to 50-SMA extended
    if not np.isnan(d50):
        ext = d50 > thresh_50
        if ext: warn_count += 1
        render_check(f"Abstand zur 50-SMA ({thresh_50:.0f}%-Schwelle)", not ext,
                     f"{d50:+.1f}% (Pullback-Risiko steigt über {thresh_50:.0f}%)", warn=ext)

    # 6. Distance to 21-EMA extended (>3 ATR)
    if not np.isnan(d21):
        ext21 = d21 > 3.0
        if ext21: warn_count += 1
        render_check("Abstand zur 21-EMA (>3 ATR = überdehnt)", not ext21,
                     f"{d21:.1f} ATR (Warnung ab 3.0)", warn=ext21)

    # 7. Under key MAs
    under_21 = not np.isnan(L["EMA21"]) and L["Close"]<L["EMA21"]
    under_50 = not np.isnan(L["SMA50"]) and L["Close"]<L["SMA50"]
    under_200 = not np.isnan(L["SMA200"]) and L["Close"]<L["SMA200"]
    if under_200: warn_count += 1
    if under_50: warn_count += 1
    render_check("Kurs über 200-SMA", not under_200, "Unter 200-SMA = schwieriges Terrain" if under_200 else "OK", warn=under_200)
    render_check("Kurs über 50-SMA", not under_50, "Unter 50-SMA = Trendstärke lässt nach" if under_50 else "OK", warn=under_50)

    # 8. Declining volume on up days (failing rally)
    vol_dec = bool(L.get("Up_Vol_Declining", False))
    if vol_dec: warn_count += 1
    render_check("Volumen an Aufwärtstagen stabil/steigend", not vol_dec,
                 "Abnehmendes Volumen an Gewinntagen → fehlendes Kaufinteresse" if vol_dec else "Volumen unterstützt Rally", warn=vol_dec)

    # 9. Intermarket divergence
    div_results = detect_intermarket_divergence(data)
    if div_results:
        at_high = [r for r in div_results if r["at_20d_high"]]
        not_high = [r for r in div_results if not r["at_20d_high"]]
        has_div = len(at_high)>0 and len(not_high)>0
        if has_div: warn_count += 1
        detail_parts = [f"{r['name']}: {r['pct_from_20d_high']:+.1f}% vom 20T-Hoch" for r in div_results]
        render_check("Intermarket-Konvergenz (Indizes im Einklang)", not has_div,
                     " · ".join(detail_parts), warn=has_div)

    # 10. Sector rotation
    rotation, sector_data, spread = detect_sector_rotation(data)
    if rotation is not None:
        if rotation: warn_count += 1
        detail = f"Defensive vs. Offensive: {spread:+.1f}% Spread (10T)"
        render_check("Keine Sektorrotation in Defensive", not rotation,
                     detail + (" → Kapital fließt in sichere Häfen" if rotation else " → Offensive führt"), warn=rotation)

    # 11. Recovery ratio (failing rally check)
    rec_pct, drop_pct = detect_failing_rally(df)
    if rec_pct is not None and drop_pct is not None and drop_pct > 5:
        weak_recovery = rec_pct < 50
        if weak_recovery: warn_count += 1
        render_check("Erholungsquote ≥ 50% nach Rückgang", not weak_recovery,
                     f"Rückgang: {drop_pct:.1f}% · Erholung: {rec_pct:.0f}% davon aufgeholt", warn=weak_recovery)

    # Summary
    if warn_count == 0:
        st.markdown('<div style="text-align:center;padding:8px;color:#22c55e;font-size:.85rem;">✓ Keine aktiven Warnzeichen</div>', unsafe_allow_html=True)
    elif warn_count <= 2:
        st.markdown(f'<div style="text-align:center;padding:8px;color:#f59e0b;font-size:.85rem;">⚠ {warn_count} Warnzeichen aktiv — Aufmerksamkeit erhöhen</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div style="text-align:center;padding:8px;color:#ef4444;font-size:.85rem;">⚠ {warn_count} Warnzeichen aktiv — Risiko deutlich reduzieren</div>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════
    # TWO COLUMNS: TREND CHECK + AMPEL DETAILS
    # ═══════════════════════════════════════════════════
    cl, cr_ = st.columns(2)
    with cl:
        st.markdown('<div class="info-card"><div class="card-label">AUFWÄRTSTREND-PRÜFUNG — KURS VS. DURCHSCHNITTE</div>', unsafe_allow_html=True)
        _c=L["Close"]; _l=L["Low"]; _e21=L["EMA21"]; _s50=L["SMA50"]; _s200=L["SMA200"]
        _e21_ok=not np.isnan(_e21); _s50_ok=not np.isnan(_s50); _s200_ok=not np.isnan(_s200)

        for ma_name, ma_val, ok, held_key, consec_key in [
            ("21-EMA",_e21,_e21_ok,"EMA21_held","Consec_Low_above_21"),
            ("50-SMA",_s50,_s50_ok,"SMA50_held","Consec_Low_above_50"),
            ("200-SMA",_s200,_s200_ok,"SMA200_held","Consec_Low_above_200")]:
            render_check(f"Tagesschluss über {ma_name}", ok and _c>ma_val, f"Close {_c:,.0f} vs {ma_name} {ma_val:,.0f}" if ok else "")
            render_check(f"Tagestief über {ma_name}", ok and _l>ma_val, f"Low {_l:,.0f} vs {ma_name} {ma_val:,.0f}" if ok else "")
            render_check(f"{ma_name} wird gehalten", bool(L.get(held_key,False)),
                         f"Schlusskurs über {ma_name}" if bool(L.get(held_key,False)) else f"Schlusskurs unter {ma_name}")
            cc = int(L.get(consec_key,0))
            render_check(f"3 Tage Tief über {ma_name}", cc>=3, f"{cc} aufeinanderfolgende Tage")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="info-card"><div class="card-label">ORDNUNG DER DURCHSCHNITTE</div>', unsafe_allow_html=True)
        e_s50 = _e21_ok and _s50_ok and _e21>_s50
        e_s200 = _e21_ok and _s200_ok and _e21>_s200
        s_s200 = _s50_ok and _s200_ok and _s50>_s200
        render_check("21-EMA über 50-SMA", e_s50, f"21-EMA {_e21:,.0f} vs 50-SMA {_s50:,.0f}" if _e21_ok and _s50_ok else "")
        render_check("21-EMA über 200-SMA", e_s200, f"21-EMA {_e21:,.0f} vs 200-SMA {_s200:,.0f}" if _e21_ok and _s200_ok else "")
        render_check("50-SMA über 200-SMA", s_s200, f"50-SMA {_s50:,.0f} vs 200-SMA {_s200:,.0f}" if _s50_ok and _s200_ok else "")
        render_check("Distributionstage ≤ 3", dc<=3, f"{dc} im 25T-Fenster")
        st.markdown("</div>", unsafe_allow_html=True)

    with cr_:
        st.markdown('<div class="info-card"><div class="card-label">TRENDWENDE-AMPEL DETAILS</div>', unsafe_allow_html=True)
        _mao = _e21_ok and _s50_ok and _s200_ok and _e21>_s50 and _s50>_s200
        rows = {"Aktuelle Phase":L["Ampel_Phase"].upper().replace("AUFWAERTSTREND","AUFWÄRTSTREND"),
                "Ankertag":L["Anchor_Date"] or "— (kein aktiver Zyklus)",
                "Bodenmarke":f"{L['Floor_Mark']:.2f}" if L["Floor_Mark"] else "— (kein aktiver Zyklus)",
                "Startschuss-Tief":f"{L['Startschuss_Low']:.2f}" if L["Startschuss_Low"] else "— (kein aktiver Zyklus)",
                "MA-Ordnung":"Korrekt ✓" if _mao else "Gestört ✗"}
        st.dataframe(pd.DataFrame({"Kennzahl":rows.keys(),"Wert":rows.values()}).set_index("Kennzahl"),use_container_width=True,height=220)
        st.markdown("</div>", unsafe_allow_html=True)

        # Sector rotation detail
        if rotation is not None and sector_data:
            st.markdown('<div class="info-card"><div class="card-label">SEKTORROTATION (10-Tage-Performance)</div>', unsafe_allow_html=True)
            for group, items in sector_data.items():
                st.markdown(f"**{group}:**")
                for name, perf in items:
                    if perf is not None:
                        c = "#22c55e" if perf>0 else "#ef4444"
                        st.markdown(f'<span style="color:{c};font-size:.85rem;">{name}: {perf:+.1f}%</span>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════
    # BREADTH
    # ═══════════════════════════════════════════════════
    st.markdown("---")
    st.markdown('<div class="card-label">MARKTBREITE (Equal-Weight · 3-Tage-Stabilitätsregel)</div>', unsafe_allow_html=True)
    ep = "RSP (Equal-Weight S&P)" if "S&P" in selected else "QQEW (Equal-Weight Nasdaq)"
    es = "QQEW (Equal-Weight Nasdaq)" if "S&P" in selected else "RSP (Equal-Weight S&P)"
    bc1,bc2 = st.columns(2)
    for col,etf in [(bc1,ep),(bc2,es)]:
        with col:
            if etf in data:
                dfe=compute_breadth_mode(data[etf].copy()); le=dfe.iloc[-1]
                render_breadth(le["Breadth_Mode"],float(le["Dist_52w_pct"])); st.caption(etf)
            else: st.info(f"{etf} nicht verfügbar.")

    # ═══════════════════════════════════════════════════
    # VIX
    # ═══════════════════════════════════════════════════
    st.markdown("---")
    st.markdown('<div class="card-label">VOLATILITÄT & STIMMUNG</div>', unsafe_allow_html=True)
    vc1,vc2 = st.columns([2,1])
    with vc1:
        if "VIX" in data: st.plotly_chart(plot_vix(analyze_vix(data["VIX"].copy()),show_days),use_container_width=True,config={"displayModeBar":False})
    with vc2:
        if "VIX" in data:
            dfv=analyze_vix(data["VIX"].copy()); vl=dfv.iloc[-1]
            st.metric("VIX",f"{vl['Close']:.1f}")
            if not np.isnan(vl["SMA10"]):
                st.metric("10T-SMA",f"{vl['SMA10']:.1f}")
                pab=vl["Pct_Above_SMA10"]
                if not np.isnan(pab):
                    if pab>20: st.error(f"⚠ VIX {pab:.0f}% über SMA → Panik")
                    elif vl["Close"]>20: st.warning(f"VIX erhöht ({pab:+.0f}% vs SMA)")
                    else: st.success(f"VIX ruhig ({pab:+.0f}% vs SMA)")
        if "VXX" in data:
            dfx=data["VXX"].copy(); dfx["EMA21"]=_ema(dfx["Close"],21); xl=dfx.iloc[-1]
            above=xl["Close"]>xl["EMA21"]; rising=dfx["EMA21"].iloc[-1]>dfx["EMA21"].iloc[-5] if len(dfx)>5 else False
            if above and rising: st.warning("VIXY über steigender 21-EMA → Risk-Off")
            elif not above and not rising: st.success("VIXY unter fallender 21-EMA → Risk-On")
            else: st.info("VIXY gemischt")

    # ═══════════════════════════════════════════════════
    # DAILY CHECKLIST
    # ═══════════════════════════════════════════════════
    st.markdown("---")
    st.markdown('<div class="info-card"><div class="card-label">TÄGLICHE CHECKLISTE</div>', unsafe_allow_html=True)
    ddv=float(L["Dist_52w_pct"]) if not np.isnan(L["Dist_52w_pct"]) else 0
    render_check("Substanzielle Korrektur?", ddv<-8, f"Drawdown: {ddv:.1f}%")
    render_check("Stabilisierung erkannt?", L["Ampel_Phase"] not in ("rot",) or L["Anchor_Date"] is not None,
                 f"Ankertag: {L['Anchor_Date']}" if L["Anchor_Date"] else "Kein aktiver Ampel-Zyklus" if L["Ampel_Phase"] in ("neutral","aufwaertstrend") else "Noch keine")
    render_check("Startschuss (Phase ≥ Gelb)?", L["Ampel_Phase"] in ("gelb","gruen","aufwaertstrend"),
                 f"Phase: {L['Ampel_Phase'].upper().replace('AUFWAERTSTREND','AUFWÄRTSTREND')}")
    if ep in data:
        dfe=compute_breadth_mode(data[ep].copy()); bm=dfe.iloc[-1]["Breadth_Mode"]
        render_check("Marktbreite unterstützend?", bm!="schutz", f"Modus: {bm.capitalize()}")
    render_check("Distributionstage ≤ 3?", dc<=3, f"{dc} im 25T-Fenster")
    if "VIX" in data:
        dfv=analyze_vix(data["VIX"].copy()); ip=bool(dfv.iloc[-1].get("Is_Panic",False))
        render_check("VIX nicht in Panik?", not ip, f"VIX: {data['VIX'].iloc[-1]['Close']:.1f}")
    render_check(f"Warnzeichen unter Kontrolle?", warn_count<=2, f"{warn_count} aktive Warnzeichen")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.caption(f"Börse ohne Bauchgefühl · v2.0 · Yahoo Finance · Stand: {L.name.strftime('%d.%m.%Y')}")
    st.caption("Basierend auf dem Handelssystem von Aljoscha Groos")

if __name__ == "__main__":
    main()
