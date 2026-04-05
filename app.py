"""
Börse ohne Bauchgefühl — Modul 1: Markt-Dashboard & Trendwende-Ampel v3.0
Basierend auf dem Handelssystem von Aljoscha Groos
Inkl. Tiefenanalyse: A/D-Linie, McClellan, NH/NL, % über MAs, Fed Funds Rate
"""
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings, os, requests
warnings.filterwarnings("ignore")

st.set_page_config(page_title="Börse ohne Bauchgefühl", page_icon="🚦", layout="wide", initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════
st.markdown("""<style>
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
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════
# S&P 500 COMPONENTS (full list for breadth calculation)
# ═══════════════════════════════════════════════════════
@st.cache_data(ttl=86400, show_spinner=False)
def get_sp500_tickers():
    """Try to load current S&P 500 tickers from Wikipedia, fall back to hardcoded list."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        symbols = tables[0]["Symbol"].tolist()
        return [s.replace(".", "-") for s in symbols]
    except Exception:
        pass
    # Hardcoded fallback (503 tickers, as of early 2025)
    return [
        'AAPL','ABBV','ABT','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ',
        'AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN',
        'ANET','ANSS','AON','AOS','APA','APD','APH','APTV','ARE','ATO','ATVI','AVB','AVGO','AVY','AWK',
        'AXP','AZO','BA','BAC','BAX','BBWI','BBY','BDX','BEN','BF-B','BIO','BK','BKNG','BKR','BLK',
        'BMY','BR','BRK-B','BRO','BSX','BWA','BX','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE',
        'CBRE','CCI','CCL','CDNS','CDW','CE','CEG','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL',
        'CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COO','COP','COST','CPB','CPRT',
        'CPT','CRL','CRM','CSCO','CSGP','CSX','CTAS','CTLT','CTRA','CTSH','CTVA','CVS','CVX','CZR',
        'D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS','DISH','DLR','DLTR','DOV','DOW',
        'DPZ','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL',
        'EMN','EMR','ENPH','EOG','EPAM','EQIX','EQR','EQT','ES','ESS','ETN','ETR','ETSY','EVRG','EW',
        'EXC','EXPD','EXPE','EXR','F','FANG','FAST','FBHS','FCX','FDS','FDX','FE','FFIV','FI','FICO',
        'FIS','FISV','FITB','FLT','FMC','FOX','FOXA','FRC','FRT','FTNT','FTV','GD','GE','GEHC','GEN',
        'GILD','GIS','GL','GLW','GM','GNRC','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS',
        'HBAN','HCA','HD','HOLX','HON','HPE','HPQ','HRL','HSIC','HST','HSY','HUM','HWM','IBM','ICE',
        'IDXX','IEX','IFF','ILMN','INCY','INTC','INTU','INVH','IP','IPG','IQV','IR','IRM','ISRG','IT',
        'ITW','IVZ','J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','K','KDP','KEY','KEYS','KHC','KIM',
        'KLAC','KMB','KMI','KMX','KO','KR','L','LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT',
        'LNC','LNT','LOW','LRCX','LUMN','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD',
        'MCHP','MCK','MCO','MDLZ','MDT','MET','META','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST',
        'MO','MOH','MOS','MPC','MPWR','MRK','MRNA','MRO','MS','MSCI','MSFT','MSI','MTB','MTCH','MTD',
        'MU','NCLH','NDAQ','NDSN','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS',
        'NUE','NVDA','NVR','NWL','NWS','NWSA','NXPI','O','ODFL','OGN','OKE','OMC','ON','ORCL','ORLY',
        'OTIS','OXY','PARA','PAYC','PAYX','PCAR','PCG','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH',
        'PHM','PKG','PKI','PLD','PM','PNC','PNR','PNW','PODD','POOL','PPG','PPL','PRU','PSA','PSX',
        'PTC','PVH','PWR','PXD','PYPL','QCOM','QRVO','RCL','RE','REG','REGN','RF','RHI','RJF','RL',
        'RMD','ROK','ROL','ROP','ROST','RSG','RTX','RVTY','SBAC','SBNY','SBUX','SCHW','SEE','SHW',
        'SIVB','SJM','SLB','SNA','SNPS','SO','SPG','SPGI','SRE','STE','STT','STX','STZ','SWK','SWKS',
        'SYF','SYK','SYY','T','TAP','TDG','TDY','TECH','TEL','TER','TFC','TFX','TGT','TJX','TMO',
        'TMUS','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','TYL',
        'UAL','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VFC','VICI','VLO','VMC','VRSK',
        'VRSN','VRTX','VTR','VTRS','VZ','WAB','WAT','WBA','WBD','WDC','WEC','WELL','WFC','WHR','WM',
        'WMB','WMT','WRB','WRK','WST','WTW','WY','WYNN','XEL','XOM','XRAY','XYL','YUM','ZBH','ZBRA',
        'ZION','ZTS',
    ]

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
        "XLU (Utilities)":"XLU","XLP (Consumer Staples)":"XLP",
        "XLK (Technology)":"XLK","XLY (Consumer Discr.)":"XLY",
    }
    data = {}
    for name, sym in tickers.items():
        df = _dl(sym, start, end)
        if df is not None and len(df) > 20: data[name] = df
    return data

# ═══════════════════════════════════════════════════════
# SECTOR ANALYSIS DATA
# ═══════════════════════════════════════════════════════
SECTOR_ETFS = {
    "XLK": "Technology",
    "XLV": "Health Care",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLP": "Consumer Staples",
    "XLY": "Consumer Discretionary",
    "XLE": "Energy",
    "XLB": "Materials",
    "XLC": "Communication Services",
    "XLU": "Utilities",
    "XLRE": "Real Estate",
}

@st.cache_data(ttl=900, show_spinner=False)
def load_sector_data(lookback_days=200):
    """Load daily close prices for all sector ETFs."""
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    symbols = list(SECTOR_ETFS.keys())
    try:
        df = yf.download(symbols, start=start, end=end, progress=False, auto_adjust=True, threads=True)
        if df is None or len(df) == 0: return None
        if isinstance(df.columns, pd.MultiIndex):
            closes = df["Close"].copy()
        else:
            closes = df[["Close"]].copy()
        closes = closes.apply(pd.to_numeric, errors="coerce")
        closes.index = pd.to_datetime(closes.index)
        closes = closes.sort_index()
        return closes
    except Exception as e:
        return None

def build_sector_table(closes, mode="daily", n_periods=20):
    """Build the ranked sector performance table.
    mode: 'daily' = each column is a trading day
          'weekly' = each column is a trading week (Friday close)
    Returns a styled DataFrame ready for display.
    """
    if closes is None or len(closes) < 5: return None, None

    if mode == "weekly":
        # Resample to weekly (Friday close)
        weekly = closes.resample("W-FRI").last().dropna(how="all")
        pct = weekly.pct_change() * 100
        pct = pct.dropna(how="all")
        display_data = pct.tail(n_periods + 1)  # +1 because we show the latest + history
    else:
        pct = closes.pct_change() * 100
        pct = pct.dropna(how="all")
        display_data = pct.tail(n_periods + 1)

    if len(display_data) < 2: return None, None

    # Latest period for ranking
    latest = display_data.iloc[-1].dropna()

    # Rename columns: ETF ticker → "Sektor (ETF)"
    col_rename = {etf: f"{name} ({etf})" for etf, name in SECTOR_ETFS.items()}

    # Build the transposed table: rows = sectors, columns = dates
    result = display_data.T.copy()
    result.index = [col_rename.get(idx, idx) for idx in result.index]

    # Format date columns
    if mode == "weekly":
        result.columns = [d.strftime("KW%V %d.%m") for d in result.columns]
    else:
        result.columns = [d.strftime("%d.%m") for d in result.columns]

    # Sort rows by the latest column (best performer first)
    last_col = result.columns[-1]
    result = result.sort_values(by=last_col, ascending=False)

    # Round to 2 decimals
    result = result.round(2)

    # Also return the latest values for the ranking summary
    latest_ranked = latest.sort_values(ascending=False)
    latest_ranked.index = [col_rename.get(idx, idx) for idx in latest_ranked.index]

    return result, latest_ranked

# ═══════════════════════════════════════════════════════
# DEEP ANALYSIS: S&P 500 breadth + FRED
# ═══════════════════════════════════════════════════════
@st.cache_data(ttl=900, show_spinner=False)
def load_sp500_breadth_data(lookback_days=550):
    """Download close prices for all S&P 500 stocks."""
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    tickers = get_sp500_tickers()
    try:
        df = yf.download(tickers, start=start, end=end, progress=False, auto_adjust=True, threads=True)
        if df is None or len(df) == 0: return None
        if isinstance(df.columns, pd.MultiIndex):
            closes = df["Close"].copy()
        else:
            closes = df[["Close"]].copy()
        closes = closes.apply(pd.to_numeric, errors="coerce")
        closes.index = pd.to_datetime(closes.index)
        closes = closes.sort_index()
        # Drop columns (stocks) that are mostly NaN (delisted, etc.)
        thresh = len(closes) * 0.5  # need at least 50% of days
        closes = closes.dropna(axis=1, thresh=int(thresh))
        return closes
    except Exception as e:
        st.warning(f"Fehler beim Laden der S&P 500 Daten: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def load_fed_funds_rate(fred_key):
    """Load Federal Funds Rate from FRED API."""
    if not fred_key: return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DFEDTARU",  # Daily Fed Funds Target Rate Upper Limit
            "api_key": fred_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 500,
            "observation_start": (datetime.now() - timedelta(days=800)).strftime("%Y-%m-%d"),
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200: return None
        obs = resp.json().get("observations", [])
        if not obs: return None
        df = pd.DataFrame(obs)
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"]).set_index("date").sort_index()
        return df[["value"]].rename(columns={"value": "FedRate"})
    except: return None

def compute_breadth_from_components(closes):
    """From a DataFrame of stock closes, compute breadth indicators daily."""
    if closes is None or len(closes) < 50: return None
    pct = closes.pct_change()
    results = pd.DataFrame(index=closes.index)

    # Advancers / Decliners each day
    results["Advancers"] = (pct > 0).sum(axis=1)
    results["Decliners"] = (pct < 0).sum(axis=1)
    results["Net_Advances"] = results["Advancers"] - results["Decliners"]
    results["AD_Ratio"] = results["Advancers"] / results["Decliners"].replace(0, np.nan)

    # A/D Line (cumulative)
    results["AD_Line"] = results["Net_Advances"].cumsum()

    # McClellan Oscillator: 19-EMA minus 39-EMA of Net Advances
    results["McC_19"] = results["Net_Advances"].ewm(span=19, adjust=False).mean()
    results["McC_39"] = results["Net_Advances"].ewm(span=39, adjust=False).mean()
    results["McClellan"] = results["McC_19"] - results["McC_39"]

    # New 52-week Highs / Lows
    # For each stock: is today's close above the highest close of the PRIOR trading days?
    # Use min(252, available_days - 2) so it works even with less than a year of data
    avail = len(closes)
    nh_window = min(252, avail - 2) if avail > 22 else 20

    # Build the reference: for each day, the max/min of the PREVIOUS nh_window days
    # (excluding today). We do this by computing rolling on the shifted series.
    prev_closes = closes.shift(1)
    high_ref = prev_closes.rolling(nh_window, min_periods=20).max()
    low_ref = prev_closes.rolling(nh_window, min_periods=20).min()

    # A stock is at a new high if today's close > highest close of the prior window
    results["New_Highs"] = (closes > high_ref).sum(axis=1)
    results["New_Lows"] = (closes < low_ref).sum(axis=1)
    results["NH_NL_Ratio"] = results["New_Highs"] / results["New_Lows"].replace(0, np.nan)

    # % of stocks above 50-SMA and 200-SMA
    sma50 = closes.rolling(50, min_periods=50).mean()
    sma200 = closes.rolling(200, min_periods=200).mean()
    n_stocks = closes.count(axis=1)
    results["Pct_Above_50SMA"] = (closes > sma50).sum(axis=1) / n_stocks * 100
    results["Pct_Above_200SMA"] = (closes > sma200).sum(axis=1) / n_stocks * 100

    # Deemer Breadth Thrust: 10-day sum Adv / 10-day sum Dec
    adv_10 = results["Advancers"].rolling(10).sum()
    dec_10 = results["Decliners"].rolling(10).sum()
    results["Deemer_Ratio"] = adv_10 / dec_10.replace(0, np.nan)
    results["Breadth_Thrust"] = results["Deemer_Ratio"] > 1.97

    # Smooth for display
    results["AD_Line_SMA21"] = results["AD_Line"].rolling(21, min_periods=5).mean()
    results["McClellan_SMA10"] = results["McClellan"].rolling(10, min_periods=3).mean()

    return results

# ═══════════════════════════════════════════════════════
# TECHNICAL HELPERS
# ═══════════════════════════════════════════════════════
def _ema(s,p): return s.ewm(span=p,adjust=False).mean()
def _sma(s,p): return s.rolling(window=p,min_periods=p).mean()
def _atr(df,p=21):
    h,l,pc = df["High"],df["Low"],df["Close"].shift(1)
    return pd.concat([h-l,(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1).rolling(p,min_periods=p).mean()
def _consec(s):
    o=np.zeros(len(s),dtype=int)
    for i in range(len(s)):
        if s.iloc[i]: o[i]=o[i-1]+1 if i>0 else 1
    return pd.Series(o,index=s.index)

def add_indicators(df):
    df=df.copy()
    df["EMA21"]=_ema(df["Close"],21);df["SMA50"]=_sma(df["Close"],50);df["SMA200"]=_sma(df["Close"],200)
    df["ATR21"]=_atr(df,21);df["ATR_pct"]=df["ATR21"]/df["Close"]*100
    df["Vol_SMA50"]=_sma(df["Volume"],50);df["Pct_Change"]=df["Close"].pct_change()*100
    rng=df["High"]-df["Low"];df["Closing_Range"]=np.where(rng>0,(df["Close"]-df["Low"])/rng,0.5)
    df["Dist_21EMA"]=(df["Close"]-df["EMA21"])/df["ATR21"]
    df["Dist_50SMA_pct"]=(df["Close"]-df["SMA50"])/df["SMA50"]*100
    df["High_52w"]=df["High"].rolling(252,min_periods=1).max()
    df["Dist_52w_pct"]=(df["Close"]-df["High_52w"])/df["High_52w"]*100
    df["MA_Order"]=(df["EMA21"]>df["SMA50"])&(df["SMA50"]>df["SMA200"])
    df["Low_above_21"]=df["Low"]>df["EMA21"];df["Low_above_50"]=df["Low"]>df["SMA50"];df["Low_above_200"]=df["Low"]>df["SMA200"]
    df["Consec_Low_above_21"]=_consec(df["Low_above_21"]);df["Consec_Low_above_50"]=_consec(df["Low_above_50"]);df["Consec_Low_above_200"]=_consec(df["Low_above_200"])
    df["EMA21_held"]=df["Close"]>df["EMA21"];df["SMA50_held"]=df["Close"]>df["SMA50"];df["SMA200_held"]=df["Close"]>df["SMA200"]
    df["Intraday_Reversal_Down"]=(df["Open"]>df["Close"].shift(1))&(df["Close"]<df["Open"])&(df["Closing_Range"]<0.35)
    df["Neg_Reversals_10d"]=df["Intraday_Reversal_Down"].rolling(10,min_periods=1).sum().astype(int)
    df["Low_CR"]=df["Closing_Range"]<0.25;df["Low_CR_5d"]=df["Low_CR"].rolling(5,min_periods=1).sum().astype(int)
    is_up=df["Close"]>df["Close"].shift(1);up_vol=df["Volume"].where(is_up)
    df["Up_Vol_SMA5"]=up_vol.ffill().rolling(5,min_periods=2).mean()
    df["Up_Vol_Declining"]=df["Up_Vol_SMA5"]<df["Up_Vol_SMA5"].shift(5)
    return df

def detect_distribution_days(df):
    df=df.copy();pc=df["Close"].shift(1);pv=df["Volume"].shift(1)
    is_down=df["Close"]<pc;high_vol=(df["Volume"]>pv)|(df["Volume"]>df["Vol_SMA50"])
    df["Is_Distribution"]=is_down&high_vol
    df["Is_Stall"]=(~is_down)&(df["Pct_Change"]<0.5)&(df["Volume"]>=pv*0.95)&(df["Closing_Range"]<0.5)
    df["Dist_Count_25"]=df["Is_Distribution"].rolling(25,min_periods=1).sum().astype(int)
    return df

# ═══════════════════════════════════════════════════════
# TRENDWENDE-AMPEL v3
# ═══════════════════════════════════════════════════════
def compute_ampel(df):
    df=df.copy();n=len(df);phase="neutral"
    anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None
    phases=["neutral"]*n;anchor_dates=[None]*n;floor_marks=[None]*n;startschuss_lows=[None]*n
    c_=df["Close"].values;o_=df["Open"].values;h_=df["High"].values;l_=df["Low"].values
    v_=df["Volume"].values;pct_=df["Pct_Change"].values;cr_=df["Closing_Range"].values
    dc_=df["Dist_Count_25"].values;s50_=df["SMA50"].values;s200_=df["SMA200"].values;e21_=df["EMA21"].values
    def _clear():
        nonlocal anchor_idx,floor_mark,startschuss_idx,startschuss_low,gruen_since
        anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None
    def _corr(i):
        lb=max(0,i-60);rh=np.nanmax(h_[lb:i+1]);dd=(c_[i]-rh)/rh*100 if rh>0 else 0
        return dd<-8 or (not np.isnan(s50_[i]) and c_[i]<s50_[i] and dc_[i]>=4)
    for i in range(1,n):
        pi=pct_[i] if not np.isnan(pct_[i]) else 0.0;cri=cr_[i] if not np.isnan(cr_[i]) else 0.5
        if phase in ("neutral","aufwaertstrend"):
            if _corr(i): phase="rot";_clear()
            elif phase=="aufwaertstrend" and not np.isnan(e21_[i]) and not np.isnan(s50_[i]) and e21_[i]<s50_[i]: phase="rot";_clear()
        elif phase=="rot":
            if anchor_idx is not None and i>anchor_idx and l_[i]<floor_mark: anchor_idx=None;floor_mark=None
            if anchor_idx is None:
                if pi>0.0 or (c_[i]>o_[i] and cri>=0.5): anchor_idx=i;floor_mark=min(l_[i],l_[i-1])
            if anchor_idx is not None and i>=anchor_idx+5:
                if pi>=1.0 and v_[i]>v_[i-1] and l_[i]>=floor_mark: phase="gelb";startschuss_idx=i;startschuss_low=l_[i]
        elif phase=="gelb":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot";_clear()
            elif startschuss_idx is not None and i>startschuss_idx+2: phase="gruen";gruen_since=i
        elif phase=="gruen":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot";_clear()
            elif not np.isnan(s200_[i]) and c_[i]>s200_[i] and not np.isnan(e21_[i]) and not np.isnan(s50_[i]) and e21_[i]>s50_[i] and (gruen_since and i-gruen_since>=10): phase="aufwaertstrend";_clear()
        phases[i]=phase
        if anchor_idx is not None: anchor_dates[i]=df.index[anchor_idx].strftime("%Y-%m-%d")
        if floor_mark is not None: floor_marks[i]=round(floor_mark,2)
        if startschuss_low is not None: startschuss_lows[i]=round(startschuss_low,2)
    df["Ampel_Phase"]=phases;df["Anchor_Date"]=anchor_dates;df["Floor_Mark"]=floor_marks;df["Startschuss_Low"]=startschuss_lows
    return df

# ═══════════════════════════════════════════════════════
# BREADTH MODE (Equal-Weight, 3-day stability)
# ═══════════════════════════════════════════════════════
def compute_breadth_mode(df_ew):
    df_ew=df_ew.copy();df_ew["High_52w"]=df_ew["High"].rolling(252,min_periods=1).max()
    df_ew["Dist_52w_pct"]=(df_ew["Close"]-df_ew["High_52w"])/df_ew["High_52w"]*100
    df_ew["Raw_Mode"]=df_ew["Dist_52w_pct"].apply(lambda d:"schutz" if d<-8 else "wachsam" if d<-4 else "rueckenwind")
    modes=df_ew["Raw_Mode"].values;stable=[modes[0]];pending=None;pc=0
    for i in range(1,len(modes)):
        if modes[i]!=stable[-1]:
            if pending==modes[i]: pc+=1
            else: pending=modes[i];pc=1
            if pc>=3: stable.append(pending);pending=None;pc=0
            else: stable.append(stable[-1])
        else: stable.append(modes[i]);pending=None;pc=0
    df_ew["Breadth_Mode"]=stable;return df_ew

def analyze_vix(dv):
    dv=dv.copy();dv["SMA10"]=_sma(dv["Close"],10);dv["Pct_Above_SMA10"]=(dv["Close"]-dv["SMA10"])/dv["SMA10"]*100;dv["Is_Panic"]=dv["Pct_Above_SMA10"]>20;return dv

# ═══════════════════════════════════════════════════════
# INTERMARKET & SECTOR ROTATION & FAILING RALLY
# ═══════════════════════════════════════════════════════
def detect_intermarket_divergence(data):
    results=[]
    for name in ["S&P 500","Nasdaq Composite","Russell 2000"]:
        if name not in data: continue
        df=data[name];h20=df["High"].rolling(20,min_periods=10).max()
        results.append({"name":name,"at_20d_high":df["Close"].iloc[-1]>=h20.iloc[-2]*0.998,
                        "pct":round((df["Close"].iloc[-1]/h20.iloc[-1]-1)*100,2)})
    return results

def detect_sector_rotation(data):
    def _p(n,d=10):
        if n not in data: return None
        df=data[n];return (df["Close"].iloc[-1]/df["Close"].iloc[-d-1]-1)*100 if len(df)>d+1 else None
    d=[("XLU",_p("XLU (Utilities)")),("XLP",_p("XLP (Consumer Staples)"))];o=[("XLK",_p("XLK (Technology)")),("XLY",_p("XLY (Consumer Discr.)"))]
    dp=[p for _,p in d if p is not None];op=[p for _,p in o if p is not None]
    if not dp or not op: return None,None,None
    ad=np.mean(dp);ao=np.mean(op);return ad>ao,{"Defensiv":d,"Offensiv":o},ad-ao

def detect_failing_rally(df):
    if len(df)<30: return None,None
    r=df.tail(60);hi=r["Close"].idxmax();hv=r.loc[hi,"Close"];ah=r.loc[hi:];
    if len(ah)<5: return None,None
    li=ah["Close"].idxmin();lv=ah.loc[li,"Close"];drop=hv-lv
    if drop/hv<0.03: return None,None
    rec=(df["Close"].iloc[-1]-lv);return round(rec/drop*100,1),round(drop/hv*100,1)

# ═══════════════════════════════════════════════════════
# RENDER HELPERS
# ═══════════════════════════════════════════════════════
def render_ampel_section(L):
    """Render the full Trendwende-Ampel section with 3 lights, status, and Startschuss."""
    phase = L["Ampel_Phase"]
    anchor = L["Anchor_Date"]
    floor = L["Floor_Mark"]
    ss_low = L["Startschuss_Low"]

    # Determine which light is active and why
    phase_info = {
        "rot": {
            "active": 0, "label": "ROT — Abwarten",
            "reason": "Substanzielle Korrektur läuft." + (
                f" Ankertag: {anchor}. Bodenmarke: {floor:.0f}." if anchor and floor else
                " Warte auf Ankertag (erster positiver Schluss)."
            ),
            "action": "Nicht kaufen. Beobachte den Markt auf Stabilisierung.",
        },
        "gelb": {
            "active": 1, "label": "GELB — Startschuss",
            "reason": f"Startschuss erkannt! Ankertag: {anchor}. Validierungslinie (Startschuss-Tief): {ss_low:.0f}." if anchor and ss_low else "Startschuss aktiv.",
            "action": "Erste Position(en) eröffnen (10–30% Kapital). Nur mit klarem Setup.",
        },
        "gruen": {
            "active": 2, "label": "GRÜN — Bestätigung",
            "reason": f"Startschuss hält. Kurs über Startschuss-Tief ({ss_low:.0f})." if ss_low else "Startschuss bestätigt.",
            "action": "Frühe Bestätigungsphase. Vorsichtig Exponierung aufbauen.",
        },
        "aufwaertstrend": {
            "active": 2, "label": "AUFWÄRTSTREND ↑",
            "reason": "MA-Ordnung bestätigt (21-EMA > 50-SMA > 200-SMA). Ampel-Zyklus abgeschlossen.",
            "action": "Offensiv handeln. Gießkannenmodus: viele kleine Positionen, beste Läufer aufstocken.",
        },
        "neutral": {
            "active": -1, "label": "NEUTRAL",
            "reason": "Keine substanzielle Korrektur erkannt. Trendwende-Ampel ist nicht aktiv.",
            "action": "Normale Marktbeobachtung. Ampel greift erst bei Drawdown > 8%.",
        },
    }
    info = phase_info.get(phase, phase_info["neutral"])

    # Colors for the 3 lights
    colors_off = ["#3b1111", "#3b2d11", "#112b11"]  # dimmed versions
    colors_on = ["#ef4444", "#f59e0b", "#22c55e"]
    labels = ["ROT", "GELB", "GRÜN"]
    glow_on = ["0 0 20px #ef444480, 0 0 40px #ef444440",
               "0 0 20px #f59e0b80, 0 0 40px #f59e0b40",
               "0 0 20px #22c55e80, 0 0 40px #22c55e40"]

    # Build the 3 lights
    lights_html = ""
    for i in range(3):
        is_active = i == info["active"]
        if phase == "aufwaertstrend" and i == 2:
            bg = "#3b82f6"; glow = "0 0 20px #3b82f680, 0 0 40px #3b82f640"; is_active = True
        else:
            bg = colors_on[i] if is_active else colors_off[i]
            glow = glow_on[i] if is_active else "none"
        border = f"2px solid {colors_on[i]}40" if is_active else "2px solid #1e293b"
        lbl_c = "#e2e8f0" if is_active else "#4a5568"
        fw = "700" if is_active else "400"
        lights_html += (
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:4px;">'
            f'<div style="width:42px;height:42px;border-radius:50%;background:{bg};box-shadow:{glow};border:{border};"></div>'
            f'<div style="font-size:.6rem;color:{lbl_c};font-weight:{fw};letter-spacing:.05em;">{labels[i]}</div>'
            f'</div>'
        )

    # Startschuss pistol icon — always visible, greyed out when not triggered
    if phase in ("gelb", "gruen") and ss_low and anchor:
        startschuss_html = (
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:10px;padding:8px 12px;'
            f'background:#f59e0b12;border:1px solid #f59e0b30;border-radius:8px;">'
            f'<span style="font-size:1.4rem;">🔫</span>'
            f'<div>'
            f'<div style="font-size:.8rem;font-weight:700;color:#f59e0b;">Startschuss aktiv</div>'
            f'<div style="font-size:.7rem;color:#94a3b8;">Startschuss-Tief: {ss_low:,.2f} · Ankertag: {anchor}</div>'
            f'</div></div>'
        )
    else:
        # Greyed out, with line-through
        if phase == "rot" and anchor:
            ss_detail = f"Ankertag: {anchor} · Warte auf Tag ≥5 mit ≥1% Gewinn + Vol. &gt; Vortag"
        elif phase == "rot":
            ss_detail = "Warte auf Ankertag, dann frühestens am 5. Tag möglich"
        else:
            ss_detail = "Kein aktiver Ampel-Zyklus"
        startschuss_html = (
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:10px;padding:8px 12px;'
            f'background:#1e293b40;border:1px solid #1e293b;border-radius:8px;opacity:0.5;">'
            f'<span style="font-size:1.4rem;filter:grayscale(1);">🔫</span>'
            f'<div>'
            f'<div style="font-size:.8rem;font-weight:700;color:#64748b;text-decoration:line-through;">Startschuss</div>'
            f'<div style="font-size:.7rem;color:#4a5568;">{ss_detail}</div>'
            f'</div></div>'
        )

    # Active phase label color
    active_color = {"rot":"#ef4444","gelb":"#f59e0b","gruen":"#22c55e","aufwaertstrend":"#3b82f6","neutral":"#64748b"}.get(phase,"#64748b")

    # Build the complete HTML as a single string (no f-string indentation issues)
    html = (
        '<div class="info-card" style="padding:20px;">'
        '<div class="card-label">TRENDWENDE-AMPEL</div>'
        '<div style="display:flex;gap:20px;align-items:flex-start;flex-wrap:wrap;">'
        '<div style="display:flex;flex-direction:column;align-items:center;gap:6px;'
        'background:#0d1117;padding:16px 20px;border-radius:12px;border:1px solid #1e293b;">'
        f'<div style="display:flex;gap:12px;">{lights_html}</div>'
        '</div>'
        '<div style="flex:1;min-width:200px;">'
        f'<div style="font-size:1.1rem;font-weight:800;color:{active_color};letter-spacing:.04em;margin-bottom:6px;">'
        f'{info["label"]}</div>'
        f'<div style="font-size:.8rem;color:#e2e8f0;line-height:1.5;margin-bottom:6px;">{info["reason"]}</div>'
        f'<div style="font-size:.75rem;color:#94a3b8;line-height:1.4;padding:6px 10px;'
        f'background:{active_color}10;border-left:3px solid {active_color};border-radius:0 6px 6px 0;">'
        f'→ {info["action"]}</div>'
        f'{startschuss_html}'
        '</div>'
        '</div>'
        '</div>'
    )
    st.markdown(html, unsafe_allow_html=True)

    # Ampel details table below
    _e = L["EMA21"]; _s5 = L["SMA50"]; _s2 = L["SMA200"]
    eo = not np.isnan(_e); so = not np.isnan(_s5); s2o = not np.isnan(_s2)
    _mao = eo and so and s2o and _e > _s5 and _s5 > _s2

    details = {
        "Ankertag": anchor if anchor else "— (kein aktiver Zyklus)" if phase in ("neutral","aufwaertstrend") else "Warte auf Ankertag",
        "Bodenmarke": f"{floor:,.2f}" if floor else "—",
        "Startschuss-Tief": f"{ss_low:,.2f}" if ss_low else "—",
        "MA-Ordnung (21>50>200)": "Korrekt ✓" if _mao else "Gestört ✗",
    }

    cols = st.columns(4)
    for i, (k, v) in enumerate(details.items()):
        with cols[i]:
            st.markdown(
                f'<div style="background:#0d1117;border:1px solid #1e293b;border-radius:8px;padding:8px 12px;text-align:center;">'
                f'<div style="font-size:.6rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em;">{k}</div>'
                f'<div style="font-size:.85rem;color:#e2e8f0;font-weight:600;margin-top:4px;">{v}</div>'
                f'</div>', unsafe_allow_html=True)

def render_check(label,ok,detail="",warn=False):
    cls="check-warn" if warn else ("check-ok" if ok else "check-fail");icon="⚠" if warn else ("✓" if ok else "✗")
    st.markdown(f'<div class="check-item"><div class="check-icon {cls}">{icon}</div><div style="flex:1;"><div style="font-size:.85rem;color:#e2e8f0;">{label}</div><div style="font-size:.7rem;color:#64748b;">{detail}</div></div></div>',unsafe_allow_html=True)

def render_breadth(mode,dist_pct):
    c={"rueckenwind":"#22c55e","wachsam":"#f59e0b","schutz":"#ef4444"}.get(mode,"#64748b")
    lbl,desc={"rueckenwind":("Rückenwind","≤4%. Breite Stärke."),"wachsam":("Wachsam","4–8%. Strenger auswählen."),"schutz":("Schutz",">8%. Kapitalschutz.")}.get(mode,("—",""))
    fp=min(100,abs(dist_pct)/12*100)
    st.markdown(f'<div class="info-card" style="background:{c}12;border-color:{c}30;"><div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;"><div style="width:12px;height:12px;border-radius:50%;background:{c};"></div><span style="font-weight:700;color:{c};">Modus: {lbl}</span><span style="font-size:.75rem;color:#94a3b8;">{dist_pct:.1f}% vom 52W-Hoch</span></div><div style="font-size:.75rem;color:#94a3b8;margin-bottom:8px;">{desc}</div><div class="breadth-track"><div class="breadth-fill" style="width:{fp}%;"></div></div><div style="display:flex;justify-content:space-between;font-size:.65rem;color:#64748b;"><span style="color:#22c55e;">Rückenwind</span><span style="color:#f59e0b;">Wachsam</span><span style="color:#ef4444;">Schutz</span></div></div>',unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════
def _x(idx): return [d.strftime("%Y-%m-%d") for d in idx]
def _y(s): return [None if pd.isna(v) else round(float(v),2) for v in s]

def plot_price(df,sd=90):
    dv=df.tail(sd);x=_x(dv.index);fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(dv["Close"]),name="Kurs",line=dict(color="#e2e8f0",width=2)))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["EMA21"]),name="21-EMA",line=dict(color="#06b6d4",width=1,dash="dot")))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA50"]),name="50-SMA",line=dict(color="#f97316",width=1,dash="dot")))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA200"]),name="200-SMA",line=dict(color="#a855f7",width=1,dash="dash")))
    fl=dv["Floor_Mark"].dropna()
    if len(fl)>0: fig.add_hline(y=float(fl.iloc[-1]),line_dash="dash",line_color="#ef4444",line_width=1,annotation_text="Bodenmarke",annotation_font_color="#ef4444")
    for col,nm,clr,sym,sz in [("Is_Distribution","Dist.","#ef4444","triangle-down",7),("Is_Stall","Stau","#f59e0b","diamond",6),("Intraday_Reversal_Down","Umkehr↓","#f97316","x",8)]:
        m=dv[dv[col]==True]
        if len(m)>0: fig.add_trace(go.Scatter(x=_x(m.index),y=_y(m["Close"] if "Stall" not in nm else m["High"]),name=nm,mode="markers",marker=dict(color=clr,size=sz,symbol=sym)))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=30,b=0),height=380,legend=dict(orientation="h",yanchor="top",y=1.12,font=dict(size=9,color="#94a3b8")),xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),hovermode="x unified")
    return fig

def plot_volume(df,sd=90):
    dv=df.tail(sd);x=_x(dv.index);colors=["#22c55e" if p>=0 else "#ef4444" for p in dv["Pct_Change"].fillna(0)]
    fig=go.Figure();fig.add_trace(go.Bar(x=x,y=_y(dv["Volume"]),marker_color=colors,opacity=0.7));fig.add_trace(go.Scatter(x=x,y=_y(dv["Vol_SMA50"]),line=dict(color="#64748b",width=1,dash="dot")))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=120,showlegend=False,xaxis=dict(gridcolor="#1e293b",showgrid=False,tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b"),tickformat=".2s"))
    return fig

def plot_vix(dv,sd=90):
    d=dv.tail(sd);x=_x(d.index);fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(d["Close"]),name="VIX",line=dict(color="#ef4444",width=1.5)))
    if "SMA10" in d: fig.add_trace(go.Scatter(x=x,y=_y(d["SMA10"]),name="10-SMA",line=dict(color="#3b82f6",width=1,dash="dot")))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=160,legend=dict(orientation="h",yanchor="top",y=1.15,font=dict(size=9,color="#94a3b8")),xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")))
    return fig

def plot_breadth_deep(br,sd=90):
    """5 subplots: A/D Line, McClellan, NH/NL, % above MAs, Deemer Ratio."""
    d=br.tail(sd);x=_x(d.index)
    fig=make_subplots(rows=5,cols=1,shared_xaxes=True,vertical_spacing=0.035,
        subplot_titles=("A/D-Linie (kumulativ)","McClellan Oscillator","Neue Hochs vs. Neue Tiefs","% über gleitenden Durchschnitten","Deemer Ratio (Breitenschub)"),
        row_heights=[0.2,0.2,0.2,0.2,0.2])
    # A/D Line
    fig.add_trace(go.Scatter(x=x,y=_y(d["AD_Line"]),name="A/D-Linie",line=dict(color="#06b6d4",width=1.5)),row=1,col=1)
    fig.add_trace(go.Scatter(x=x,y=_y(d["AD_Line_SMA21"]),name="21-SMA",line=dict(color="#64748b",width=1,dash="dot")),row=1,col=1)
    # McClellan
    mc_colors=[("#22c55e" if v>=0 else "#ef4444") for v in d["McClellan"].fillna(0)]
    fig.add_trace(go.Bar(x=x,y=_y(d["McClellan"]),name="McClellan",marker_color=mc_colors,opacity=0.8),row=2,col=1)
    fig.add_hline(y=70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=2,col=1)
    fig.add_hline(y=-70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=2,col=1)
    # NH / NL
    fig.add_trace(go.Bar(x=x,y=_y(d["New_Highs"]),name="Neue Hochs",marker_color="#22c55e",opacity=0.7),row=3,col=1)
    fig.add_trace(go.Bar(x=x,y=_y(-d["New_Lows"]),name="Neue Tiefs",marker_color="#ef4444",opacity=0.7),row=3,col=1)
    # % above MAs
    fig.add_trace(go.Scatter(x=x,y=_y(d["Pct_Above_50SMA"]),name="% > 50-SMA",line=dict(color="#f97316",width=1.5)),row=4,col=1)
    fig.add_trace(go.Scatter(x=x,y=_y(d["Pct_Above_200SMA"]),name="% > 200-SMA",line=dict(color="#a855f7",width=1.5)),row=4,col=1)
    fig.add_hline(y=70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=4,col=1)
    # Deemer Ratio
    fig.add_trace(go.Scatter(x=x,y=_y(d["Deemer_Ratio"]),name="Deemer Ratio",line=dict(color="#06b6d4",width=1.5)),row=5,col=1)
    fig.add_hline(y=1.97,line_dash="dash",line_color="#22c55e",line_width=1,annotation_text="1.97 (Thrust)",annotation_font_color="#22c55e",annotation_font_size=9,row=5,col=1)
    fig.add_hline(y=1.0,line_dash="dot",line_color="#64748b",line_width=0.5,row=5,col=1)

    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=30,b=0),height=750,showlegend=False)
    for i in range(1,6): fig.update_xaxes(gridcolor="#1e293b",tickfont=dict(size=8,color="#64748b"),row=i,col=1);fig.update_yaxes(gridcolor="#1e293b",tickfont=dict(size=8,color="#64748b"),row=i,col=1)
    for ann in fig.layout.annotations: ann.font.size=10;ann.font.color="#94a3b8"
    return fig

def plot_fed_rate(fed_df,sd=200):
    d=fed_df.tail(sd);x=_x(d.index);fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(d["FedRate"]),name="Fed Funds Rate",line=dict(color="#f59e0b",width=2),fill="tozeroy",fillcolor="rgba(245,158,11,0.1)"))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=150,showlegend=False,xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b"),title="% p.a.",title_font=dict(size=9,color="#64748b")))
    return fig

# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════
def main():
    st.title("BÖRSE OHNE BAUCHGEFÜHL")

    tab1, tab2, tab3 = st.tabs(["📊 Marktanalyse", "🏭 Sektoranalyse", "📋 Aktienbewertung"])

    # ═══════════════════════════════════════════════════════
    # TAB 1: MARKTANALYSE (existing content)
    # ═══════════════════════════════════════════════════════
    with tab1:
        _tab_marktanalyse()

    # ═══════════════════════════════════════════════════════
    # TAB 2: SEKTORANALYSE
    # ═══════════════════════════════════════════════════════
    with tab2:
        _tab_sektoranalyse()

    # ═══════════════════════════════════════════════════════
    # TAB 3: AKTIENBEWERTUNG (placeholder)
    # ═══════════════════════════════════════════════════════
    with tab3:
        _tab_aktienbewertung()


# ═══════════════════════════════════════════════════════
# TAB 3: AKTIENBEWERTUNG — Stock evaluation (Book Ch.3.4–4.7)
# ═══════════════════════════════════════════════════════
@st.cache_data(ttl=900, show_spinner=False)
# ═══════════════════════════════════════════════════════
# TAB 3: AKTIENBEWERTUNG (Book Ch.3.4–4.7)
# ═══════════════════════════════════════════════════════
@st.cache_data(ttl=900, show_spinner=False)
def load_stock_full(ticker, lookback_days=500):
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start, end=end, auto_adjust=True)
        if df is None or len(df) < 20: return None, None, None, None, None
        df.index = pd.to_datetime(df.index); df = df.sort_index()
        for c in ["Open","High","Low","Close","Volume"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        info = t.info or {}
        qi = None; ai = None; ih = None
        try: qi = t.quarterly_income_stmt
        except: pass
        try: ai = t.income_stmt  # annual
        except: pass
        try: ih = t.institutional_holders
        except: pass
        return df, info, qi, ai, ih
    except: return None, None, None, None, None

@st.cache_data(ttl=900, show_spinner=False)
def load_sp500_for_rs(lookback_days=400):
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    return _dl("^GSPC", start, end)

# ── HELPERS ──
def _find_row(stmt, candidates):
    """Find a row in an income statement by trying multiple name variants."""
    if stmt is None or stmt.empty: return None
    for name in candidates:
        if name in stmt.index: return stmt.loc[name]
    return None

def _quarterly_yoy_growth(qi, field):
    """YoY growth for last 3 quarters. Returns list of (label, growth%) newest first."""
    candidates = {
        "eps": ["Diluted EPS","Basic EPS"],
        "revenue": ["Total Revenue","Revenue","Operating Revenue"],
        "net_income": ["Net Income","Net Income Common Stockholders"],
    }
    row = _find_row(qi, candidates.get(field, [field]))
    if row is None: return []
    vals = row.dropna().sort_index(ascending=False)
    if len(vals) < 5: return []
    results = []
    for i in range(min(3, len(vals) - 4)):
        cur = vals.iloc[i]; prev = vals.iloc[i + 4]
        if prev != 0 and not np.isnan(prev) and not np.isnan(cur):
            g = (cur / prev - 1) * 100
            lbl = vals.index[i].strftime("%Y-%m") if hasattr(vals.index[i], 'strftime') else str(vals.index[i])[:7]
            results.append((lbl, round(g, 1)))
    return results

def _annual_yoy_growth(ai, field):
    """YoY growth for last 3 years. Returns list of (year, growth%)."""
    candidates = {
        "eps": ["Diluted EPS","Basic EPS"],
        "revenue": ["Total Revenue","Revenue","Operating Revenue"],
    }
    row = _find_row(ai, candidates.get(field, [field]))
    if row is None: return []
    vals = row.dropna().sort_index(ascending=False)
    if len(vals) < 2: return []
    results = []
    for i in range(min(3, len(vals) - 1)):
        cur = vals.iloc[i]; prev = vals.iloc[i + 1]
        if prev != 0 and not np.isnan(prev) and not np.isnan(cur):
            g = (cur / prev - 1) * 100
            yr = vals.index[i].strftime("%Y") if hasattr(vals.index[i], 'strftime') else str(vals.index[i])[:4]
            results.append((yr, round(g, 1)))
    return results

def _sum_last_4q_eps(qi):
    """Sum of the last 4 quarterly EPS values."""
    row = _find_row(qi, ["Diluted EPS","Basic EPS"])
    if row is None: return None
    vals = row.dropna().sort_index(ascending=False)
    if len(vals) < 4: return None
    return round(float(vals.iloc[:4].sum()), 2)

def _calc_cmf(df, period=20):
    h = df["High"]; l = df["Low"]; c = df["Close"]; v = df["Volume"]
    rng = h - l
    mfm = np.where(rng > 0, ((c - l) - (h - c)) / rng, 0.0)
    mfv = mfm * v
    return pd.Series(mfv, index=df.index).rolling(period).sum() / v.rolling(period).sum()

def _cmf_rating(val):
    if np.isnan(val): return "—","Nicht verfügbar","#64748b"
    if val > 0.25: return "A","Starke Akkumulation","#22c55e"
    if val > 0.10: return "B","Moderate Akkumulation","#22c55e"
    if val > -0.10: return "C","Neutral","#f59e0b"
    if val > -0.25: return "D","Moderate Distribution","#ef4444"
    return "E","Starke Distribution","#ef4444"

def _calc_rs_rating(sc, sp):
    if sc is None or sp is None or len(sc)<252 or len(sp)<252: return None
    common = sc.index.intersection(sp.index)
    if len(common)<200: return None
    s = sc.reindex(common); m = sp.reindex(common)
    windows = [(0,63,0.4),(63,126,0.2),(126,189,0.2),(189,252,0.2)]
    score = 0
    for a,b,w in windows:
        if b > len(s): break
        sp_ = s.iloc[-a-1]/s.iloc[-b]-1 if a==0 else s.iloc[-a]/s.iloc[-b]-1
        mp_ = m.iloc[-a-1]/m.iloc[-b]-1 if a==0 else m.iloc[-a]/m.iloc[-b]-1
        score += (sp_ - mp_) * w
    return max(1, min(99, int(round(50 + score * 150))))

def _atr_category(pct):
    if np.isnan(pct): return "—","#64748b"
    if pct <= 2.5: return "Ruhig","#22c55e"
    if pct <= 4.0: return "Lebhaft","#06b6d4"
    if pct <= 8.0: return "Stürmisch","#f59e0b"
    return "Explosiv","#ef4444"

# ═══════════════════════════════════════════════════════
# FUNDAMENTAL CHECKLIST
# ═══════════════════════════════════════════════════════
def evaluate_fundamentals(info, qi, ai, ih):
    checks = []
    def _g(k, d=None):
        v = info.get(k, d) if info else d
        return v if v is not None else d

    # ── EPS: quarterly YoY (3 quarters) ──
    epsg = _quarterly_yoy_growth(qi, "eps")
    if epsg:
        details = " → ".join(f"{q}: {g:+.0f}%" for q, g in epsg)
        all20 = all(g >= 20 for _, g in epsg)
        checks.append((f"EPS ≥20% YoY ({len(epsg)}Q)", all20, details))
    else:
        eq = _g("earningsQuarterlyGrowth")
        if eq is not None:
            checks.append(("EPS ≥20% YoY (letztes Q)", eq*100 >= 20, f"{eq*100:+.1f}%"))
        else:
            checks.append(("EPS ≥20% YoY", False, "Keine Quartalsdaten"))

    # ── EPS acceleration ──
    if len(epsg) >= 2:
        # Check if growth rates are accelerating across available quarters
        accel_count = sum(1 for i in range(len(epsg)-1) if epsg[i][1] > epsg[i+1][1])
        full_accel = accel_count == len(epsg) - 1
        details = " → ".join(f"{g:+.0f}%" for _, g in reversed(epsg))
        # Book: with clean acceleration, even ~13% in latest quarter is acceptable
        latest_ok = epsg[0][1] >= 13 if full_accel else epsg[0][1] >= 20
        checks.append(("EPS-Beschleunigung", full_accel,
                       f"Verlauf: {details}" + (" (bei Beschl. ab 13% akzeptabel)" if full_accel and epsg[0][1] < 20 else "")))

    # ── EPS: annual 3-year ──
    epsg_ann = _annual_yoy_growth(ai, "eps")
    if epsg_ann and len(epsg_ann) >= 2:
        details = " · ".join(f"{yr}: {g:+.0f}%" for yr, g in epsg_ann)
        all20 = all(g >= 20 for _, g in epsg_ann)
        checks.append((f"Jährl. EPS ≥20% ({len(epsg_ann)} Jahre)", all20, details))
    else:
        eg = _g("earningsGrowth")
        if eg is not None:
            checks.append(("Jährl. EPS-Wachstum ≥20%", eg*100 >= 20, f"{eg*100:+.1f}% (nur 1 Jahr verfügbar)"))

    # ── Sum of last 4 quarterly EPS > 0 ──
    eps_sum = _sum_last_4q_eps(qi)
    if eps_sum is not None:
        checks.append(("Summe letzte 4 Quartals-EPS > 0", eps_sum > 0, f"${eps_sum:.2f}"))
    else:
        te = _g("trailingEps")
        if te is not None:
            checks.append(("Trailing EPS > 0 (Proxy für 4Q-Summe)", te > 0, f"${te:.2f}"))

    # ── Revenue: quarterly YoY (3 quarters) ──
    revg = _quarterly_yoy_growth(qi, "revenue")
    if revg:
        details = " → ".join(f"{q}: {g:+.0f}%" for q, g in revg)
        all20 = all(g >= 20 for _, g in revg)
        checks.append((f"Umsatz ≥20% YoY ({len(revg)}Q)", all20, details))
    else:
        rg = _g("revenueGrowth")
        if rg is not None:
            checks.append(("Umsatz ≥20% YoY", rg*100 >= 20, f"{rg*100:+.1f}% (nur jährlich verfügbar)"))
        else:
            checks.append(("Umsatz ≥20% YoY", False, "Keine Quartalsdaten"))

    # ── Revenue acceleration (Bonus) ──
    if len(revg) >= 2:
        accel = revg[0][1] > revg[1][1]
        checks.append(("Umsatz-Beschleunigung (Bonus)", accel,
                       f"{revg[1][1]:+.0f}% → {revg[0][1]:+.0f}%"))

    # ── Revenue: annual 3-year ──
    revg_ann = _annual_yoy_growth(ai, "revenue")
    if revg_ann and len(revg_ann) >= 2:
        details = " · ".join(f"{yr}: {g:+.0f}%" for yr, g in revg_ann)
        all20 = all(g >= 20 for _, g in revg_ann)
        checks.append((f"Jährl. Umsatz ≥20% ({len(revg_ann)}J)", all20, details))
    else:
        rg = _g("revenueGrowth")
        if rg is not None:
            checks.append(("Jährl. Umsatzwachstum ≥20%", rg*100 >= 20, f"{rg*100:+.1f}%"))

    # ── ROE ≥ 17% ──
    roe = _g("returnOnEquity")
    if roe is not None:
        checks.append(("ROE ≥17%", roe*100 >= 17, f"{roe*100:.1f}%"))
    else:
        checks.append(("ROE ≥17%", False, "Nicht verfügbar"))

    # ── Institutional holders: count + top holders list ──
    inst_pct = _g("heldPercentInstitutions")
    if ih is not None and not ih.empty:
        n_holders = len(ih)
        total_pct = inst_pct * 100 if inst_pct else 0
        top3 = ", ".join(ih["Holder"].head(3).tolist()) if "Holder" in ih.columns else ""
        checks.append(("Institutionelle Unterstützung", n_holders >= 5,
                       f"{n_holders} Top-Institutionen · {total_pct:.0f}% inst. gehalten" + (f" · Top: {top3}" if top3 else "")))
    elif inst_pct is not None:
        checks.append(("Institutionelle Beteiligung", inst_pct * 100 > 20,
                       f"{inst_pct*100:.0f}% inst. gehalten (Detailliste nicht verfügbar)"))
    else:
        checks.append(("Institutionelle Unterstützung", False, "Nicht verfügbar"))

    # ── Profit margin ──
    pm = _g("profitMargins")
    if pm is not None:
        checks.append(("Gewinnmarge positiv", pm > 0, f"{pm*100:.1f}%"))

    return checks

# ═══════════════════════════════════════════════════════
# TECHNICAL CHECKLIST
# ═══════════════════════════════════════════════════════
def evaluate_technicals(df, info, spx_df=None):
    checks = []; L = df.iloc[-1]; price = L["Close"]

    # Price ≥ $15
    checks.append(("Preis ≥ $15", price >= 15, f"${price:,.2f}"))

    # Near ATH
    h52 = df["High"].rolling(252, min_periods=20).max().iloc[-1]
    if not np.isnan(h52):
        d = (price / h52 - 1) * 100
        checks.append(("Nahe am 52W-Hoch", d > -10, f"{d:+.1f}% vom Hoch (${h52:,.2f})"))

    # Dollar volume ≥ $30M
    avg_v = df["Volume"].tail(20).mean(); dol_v = avg_v * price / 1e6
    checks.append(("Dollar-Volumen ≥ $30 Mio.", dol_v >= 30, f"${dol_v:,.0f} Mio./Tag"))

    # Up/Down Volume Ratio ≥ 1.0
    pc = df["Close"].pct_change()
    uv = df["Volume"].where(pc > 0).tail(50).sum(); dv = df["Volume"].where(pc < 0).tail(50).sum()
    if dv > 0:
        udv = uv / dv
        checks.append(("Up/Down Vol. Ratio ≥1.0", udv >= 1.0, f"{udv:.2f}" + (" (ideal ≥1.1)" if udv >= 1.1 else "")))

    # RS Rating
    rs = None
    if spx_df is not None and len(spx_df) > 200:
        rs = _calc_rs_rating(df["Close"], spx_df["Close"])
    if rs is not None:
        lbl = "Elite" if rs >= 90 else "Stark" if rs >= 80 else "Meiden (<70)" if rs < 70 else "OK"
        checks.append(("RS-Bewertung ≥80", rs >= 80, f"RS: {rs} ({lbl})"))
    elif spx_df is not None and len(df) >= 126 and len(spx_df) >= 126:
        sp = (df["Close"].iloc[-1] / df["Close"].iloc[-126] - 1) * 100
        mp = (spx_df["Close"].iloc[-1] / spx_df["Close"].iloc[-126] - 1) * 100
        checks.append(("Relative Stärke vs. S&P (6M)", sp > mp, f"Aktie: {sp:+.1f}% · S&P: {mp:+.1f}% · Diff: {sp-mp:+.1f}%"))

    # CMF Rating A or B
    cmf = _calc_cmf(df, 20); cmf_val = cmf.iloc[-1] if len(cmf) > 0 else np.nan
    rat, meaning, _ = _cmf_rating(cmf_val)
    checks.append(("CMF Rating A oder B", rat in ("A","B"), f"CMF: {cmf_val:+.3f} → {rat} ({meaning})"))

    # MAs
    e21 = df["Close"].ewm(span=21).mean().iloc[-1]
    s10 = df["Close"].rolling(10).mean().iloc[-1]
    s50 = df["Close"].rolling(50).mean().iloc[-1]
    s200 = df["Close"].rolling(200).mean().iloc[-1]

    for nm, mv in [("21-EMA", e21), ("50-SMA", s50), ("200-SMA", s200)]:
        if not np.isnan(mv): checks.append((f"Kurs über {nm}", price > mv, f"{price:,.2f} vs {mv:,.2f}"))

    if not any(np.isnan(x) for x in [e21, s50, s200]):
        checks.append(("MA-Ordnung (21>50>200)", e21 > s50 > s200, f"21:{e21:,.0f} · 50:{s50:,.0f} · 200:{s200:,.0f}"))

    # ── MA Distance warnings (book thresholds) ──
    for nm, mv, thresh in [("10-SMA", s10, 10.0), ("21-EMA", e21, 14.0), ("50-SMA", s50, 25.0), ("200-SMA", s200, 70.0)]:
        if not np.isnan(mv):
            dist = (price / mv - 1) * 100
            extended = dist > thresh or dist < -thresh
            checks.append((f"Abstand {nm} (<{thresh:.0f}%)", not extended,
                           f"{dist:+.1f}% ({'überdehnt' if dist > 0 else 'darunter'}, Schwelle: ±{thresh:.0f}%)"))

    return checks, cmf_val, rs

# ═══════════════════════════════════════════════════════
# CHART SIGNS (Table 28)
# ═══════════════════════════════════════════════════════
def evaluate_chart_signs(df):
    signs = {"positiv": [], "negativ": [], "neutral": []}
    if len(df) < 50: return signs
    c = df["Close"]; h = df["High"]; l = df["Low"]; o = df["Open"]; v = df["Volume"]
    pct = c.pct_change(); vol_avg = v.rolling(50).mean()
    ema21 = c.ewm(span=21).mean(); sma50 = c.rolling(50).mean(); sma200 = c.rolling(200).mean()
    rng = h - l; cr_s = pd.Series(np.where(rng > 0, (c - l) / rng, 0.5), index=df.index)

    # Up-vol vs down-vol days (20d)
    t20 = df.tail(20)
    uhv = ((t20["Close"] > t20["Close"].shift(1)) & (t20["Volume"] > vol_avg.tail(20))).sum()
    dhv = ((t20["Close"] < t20["Close"].shift(1)) & (t20["Volume"] > vol_avg.tail(20))).sum()
    if uhv > dhv: signs["positiv"].append(("Mehr Gewinn- als Verlusttage mit hohem Vol.", f"{uhv} vs {dhv} (20T)"))
    elif dhv > uhv: signs["negativ"].append(("Mehr Verlust- als Gewinntage mit hohem Vol.", f"{dhv} vs {uhv} (20T)"))

    a21 = (c.tail(10) > ema21.tail(10)).sum(); a50 = (c.tail(10) > sma50.tail(10)).sum()
    if a21 >= 8 and a50 >= 8: signs["positiv"].append(("Leben über den Durchschnitten", f"{a21}/10 über 21-EMA, {a50}/10 über 50-SMA"))
    elif a21 <= 2 or a50 <= 2: signs["negativ"].append(("Leben unter den Durchschnitten", f"{a21}/10 über 21-EMA, {a50}/10 über 50-SMA"))

    e21 = ema21.iloc[-1]; s50 = sma50.iloc[-1]; s200 = sma200.iloc[-1]
    if not any(np.isnan(x) for x in [e21, s50, s200]):
        if e21 > s50 > s200: signs["positiv"].append(("Durchschnitte in richtiger Ordnung", "21>50>200"))
        elif e21 < s50 < s200: signs["negativ"].append(("Durchschnitte in falscher Ordnung", "21<50<200"))
    if len(ema21) >= 10:
        eu = ema21.iloc[-1] > ema21.iloc[-10]; su = sma50.iloc[-1] > sma50.iloc[-10] if not np.isnan(sma50.iloc[-10]) else None
        if eu and su: signs["positiv"].append(("Nach oben zeigende Durchschnittslinien", ""))
        elif not eu and su is False: signs["negativ"].append(("Nach unten zeigende Durchschnittslinien", ""))

    gu = ((o.tail(20) > h.shift(1).tail(20)) & (v.tail(20) > vol_avg.tail(20))).sum()
    gd = ((o.tail(20) < l.shift(1).tail(20)) & (v.tail(20) > vol_avg.tail(20))).sum()
    if gu > 0: signs["positiv"].append(("Positive Kurslücken", f"{gu} in 20T"))
    if gd > 0: signs["negativ"].append(("Negative Kurslücken bei hohem Vol.", f"{gd} in 20T"))

    drops = pct.tail(20) < -0.005
    dl = (drops & (v.tail(20) < vol_avg.tail(20) * 0.8)).sum()
    dhh = (drops & (v.tail(20) > vol_avg.tail(20) * 1.2)).sum()
    if dl >= 3: signs["positiv"].append(("Preisrückgänge bei niedrigem Vol.", f"{dl} Tage"))
    if dhh >= 3: signs["negativ"].append(("Preisrückgänge bei hohem Vol.", f"{dhh} Tage"))
    rises = pct.tail(20) > 0.005
    rh = (rises & (v.tail(20) > vol_avg.tail(20) * 1.2)).sum()
    if rh >= 3: signs["positiv"].append(("Preissteigerungen bei hohem Vol.", f"{rh} in 20T"))

    stall = ((pct.tail(10) >= 0) & (pct.tail(10) < 0.005) & (v.tail(10) >= v.shift(1).tail(10) * 0.95) & (cr_s.tail(10) < 0.5)).sum()
    if stall >= 2: signs["negativ"].append(("Stau-Tage", f"{stall} in 10T"))

    ur = ((o.tail(10) < c.shift(1).tail(10)) & (c.tail(10) > o.tail(10)) & (cr_s.tail(10) > 0.7)).sum()
    dr2 = ((o.tail(10) > c.shift(1).tail(10)) & (c.tail(10) < o.tail(10)) & (cr_s.tail(10) < 0.3)).sum()
    if ur >= 2: signs["positiv"].append(("Upside Reversals", f"{ur} in 10T"))
    if dr2 >= 2: signs["negativ"].append(("Downside Reversals", f"{dr2} in 10T"))

    if len(c) >= 63:
        rn = c.iloc[-1] / c.iloc[-21]; rp = c.iloc[-21] / c.iloc[-63]
        if rn > rp: signs["positiv"].append(("Steigende RS-Linie", f"{rn:.3f} vs {rp:.3f}"))
        elif rn < rp * 0.95: signs["negativ"].append(("Kippende RS-Linie", f"{rn:.3f} vs {rp:.3f}"))

    avg_cr = cr_s.tail(5).mean()
    if avg_cr > 0.6: signs["positiv"].append(("Schlussposition obere 40%", f"Ø {avg_cr:.0%}"))
    elif avg_cr < 0.25: signs["negativ"].append(("Tiefe Schlussposition", f"Ø {avg_cr:.0%}"))

    if not np.isnan(s50):
        d50 = (c.iloc[-1] / s50 - 1) * 100
        if d50 > 15: signs["negativ"].append(("Großer Abstand zu Durchschnitten", f"{d50:+.1f}% zur 50-SMA"))

    wc = c.resample("W-FRI").last().dropna()
    if len(wc) >= 6 and (wc.pct_change().tail(5) > 0).all():
        signs["positiv"].append(("5 positive Wochen in Folge", ""))

    if not np.isnan(s50) and abs(c.iloc[-1] / s50 - 1) < 0.01: signs["neutral"].append(("Rückkehr zur 50-Tage-Linie", ""))
    if h.iloc[-1] <= h.iloc[-2] and l.iloc[-1] >= l.iloc[-2]: signs["neutral"].append(("Inside Day", ""))
    r5 = (h.tail(5).max() - l.tail(5).min()) / c.iloc[-1] * 100
    if r5 < 3: signs["neutral"].append(("Enge Konsolidierung", f"5T-Range: {r5:.1f}%"))
    if not np.isnan(e21) and abs(l.iloc[-1] - e21) / e21 < 0.005: signs["neutral"].append(("Test der 21-EMA", ""))
    if not np.isnan(s50) and abs(l.iloc[-1] - s50) / s50 < 0.005: signs["neutral"].append(("Test der 50-SMA", ""))
    return signs

# ═══════════════════════════════════════════════════════
# TAB LAYOUT
# ═══════════════════════════════════════════════════════
def _tab_aktienbewertung():
    st.markdown("### 📋 Aktienbewertung — Einzelaktien-Check")
    st.caption("Fundamentale Checkliste (Kap. 3.4) · Technische Checkliste (Kap. 3.5/3.6) · Chartverhalten (Tab. 28)")

    ticker = st.text_input("Ticker eingeben (z.B. NVDA, AAPL, MSFT)", value="", placeholder="NVDA").upper().strip()
    if not ticker: return

    with st.spinner(f"Lade {ticker} …"):
        df, info, qi, ai, ih = load_stock_full(ticker)
        spx_df = load_sp500_for_rs()

    if df is None or len(df) < 20:
        st.error(f"Keine Daten für '{ticker}'."); return

    L = df.iloc[-1]; name = info.get("shortName", ticker) if info else ticker
    price = L["Close"]; prev = df["Close"].iloc[-2]; chg = (price / prev - 1) * 100

    # ── Header ──
    st.markdown(
        f'<div style="font-size:1.3rem;font-weight:800;color:#e2e8f0;">{name} ({ticker})</div>'
        f'<div style="font-size:1.1rem;color:{"#22c55e" if chg>=0 else "#ef4444"};font-weight:700;">'
        f'${price:,.2f} ({chg:+.2f}%)</div>', unsafe_allow_html=True)

    # ── Key metrics row ──
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1: st.metric("Sektor", (info.get("sector","—") if info else "—")[:18])
    with k2: st.metric("Branche", (info.get("industry","—") if info else "—")[:20])

    # Closing Range
    rng_hl = L["High"] - L["Low"]; cr_today = (L["Close"] - L["Low"]) / rng_hl * 100 if rng_hl > 0 else 50
    with k3: st.metric("Closing Range", f"{cr_today:.0f}%")

    # ATR in %
    atr_s = _atr(df, 21); atr_val = atr_s.iloc[-1] if len(atr_s) > 0 else np.nan
    atr_pct = (atr_val / price * 100) if not np.isnan(atr_val) else np.nan
    cat_lbl, cat_c = _atr_category(atr_pct)
    with k4: st.metric("ATR (21T)", f"{atr_pct:.1f}%" if not np.isnan(atr_pct) else "—", cat_lbl)

    # DRR
    drr = ((df["High"] - df["Low"]) / df["Close"] * 100).tail(21).mean()
    with k5: st.metric("DRR (Ø21T)", f"{drr:.2f}%")

    # Beta
    beta = info.get("beta") if info else None
    with k6: st.metric("Beta", f"{beta:.2f}" if beta else "—", ">1.3 dynamisch" if beta and beta > 1.3 else "")

    st.markdown("---")

    # ── TWO COLUMNS ──
    col_f, col_t = st.columns(2)

    with col_f:
        st.markdown('<div class="info-card"><div class="card-label">FUNDAMENTALE CHECKLISTE (Kap. 3.4)</div>', unsafe_allow_html=True)
        fc = evaluate_fundamentals(info, qi, ai, ih)
        fok = sum(1 for _, ok, _ in fc if ok)
        for label, ok, detail in fc: render_check(label, ok, detail)
        sc = "#22c55e" if fok >= 7 else "#f59e0b" if fok >= 4 else "#ef4444"
        st.markdown(f'<div style="text-align:center;padding:8px;color:{sc};">{fok}/{len(fc)} Kriterien erfüllt</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_t:
        st.markdown('<div class="info-card"><div class="card-label">TECHNISCHE CHECKLISTE (Kap. 3.5 / 3.6)</div>', unsafe_allow_html=True)
        tc, cmf_val, rs_val = evaluate_technicals(df, info, spx_df)
        tok = sum(1 for _, ok, _ in tc if ok)
        for label, ok, detail in tc: render_check(label, ok, detail)
        sc = "#22c55e" if tok >= 10 else "#f59e0b" if tok >= 6 else "#ef4444"
        st.markdown(f'<div style="text-align:center;padding:8px;color:{sc};">{tok}/{len(tc)} Kriterien erfüllt</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # ── CHART SIGNS ──
    st.markdown('<div class="card-label">CHARTVERHALTEN — POSITIVE / NEGATIVE / NEUTRALE ZEICHEN (Tabelle 28)</div>', unsafe_allow_html=True)
    signs = evaluate_chart_signs(df)
    sc1, sc2, sc3 = st.columns(3)
    for col, key, label, color in [(sc1,"positiv","✓ POSITIV","#22c55e"),(sc2,"negativ","✗ NEGATIV","#ef4444"),(sc3,"neutral","○ NEUTRAL","#94a3b8")]:
        with col:
            st.markdown(f'<div class="info-card" style="border-color:{color}30;">'
                        f'<div class="card-label" style="color:{color};">{label}</div>', unsafe_allow_html=True)
            if signs[key]:
                for nm, dt in signs[key]:
                    st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #1e293b;">'
                                f'<div style="font-size:.8rem;color:{color};">{nm}</div>'
                                f'<div style="font-size:.65rem;color:#64748b;">{dt}</div></div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div style="color:#4a5568;font-size:.8rem;">Keine Zeichen</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    np_ = len(signs["positiv"]); nn = len(signs["negativ"]); nu = len(signs["neutral"])
    score = np_ - nn
    if score >= 3: verd = "Starkes Chartbild"; vc = "#22c55e"
    elif score >= 1: verd = "Leicht positiv"; vc = "#22c55e"
    elif score >= -1: verd = "Gemischt — Vorsicht"; vc = "#f59e0b"
    else: verd = "Schwaches Chartbild"; vc = "#ef4444"
    st.markdown(
        f'<div style="text-align:center;padding:12px;background:#111827;border:1px solid #1e293b;border-radius:10px;">'
        f'<div style="font-size:.7rem;color:#64748b;">GESAMTBEWERTUNG</div>'
        f'<div style="font-size:1rem;font-weight:700;color:{vc};">{verd}</div>'
        f'<div style="font-size:.75rem;color:#94a3b8;">{np_} Positiv · {nn} Negativ · {nu} Neutral · Score: {score:+d}</div></div>',
        unsafe_allow_html=True)


def _tab_sektoranalyse():
    """Tab 2: Sector performance ranking table."""
    st.markdown("### 🏭 Sektoranalyse — Performance-Ranking")
    st.caption("S&P 500 Sektor-ETFs gerankt nach Performance. Bester Sektor steht oben.")

    with st.spinner("Lade Sektor-Daten …"):
        sector_closes = load_sector_data()

    if sector_closes is None or len(sector_closes) < 5:
        st.error("Sektor-Daten konnten nicht geladen werden.")
        return

    # View mode selector
    sc1, sc2 = st.columns([2, 1])
    with sc1:
        mode = st.radio("Ansicht", ["Tagesansicht", "Wochenansicht"], horizontal=True, label_visibility="collapsed")
    with sc2:
        n_periods = st.selectbox("Historie", [10, 20, 30], index=1, format_func=lambda x: f"{x} {'Tage' if mode == 'Tagesansicht' else 'Wochen'}")

    is_daily = mode == "Tagesansicht"
    table, latest_ranked = build_sector_table(sector_closes, mode="daily" if is_daily else "weekly", n_periods=n_periods)

    if table is None or latest_ranked is None:
        st.warning("Nicht genug Daten für die Auswertung.")
        return

    # Determine the last trading date from the data
    last_date = sector_closes.index[-1].strftime("%d.%m.%Y")
    today = datetime.now().strftime("%d.%m.%Y")
    date_note = f"Stand: {last_date}" + ("" if last_date == today else " (letzter Handelstag)")
    st.caption(date_note)

    # ── TOP 3 / BOTTOM 3 summary ──
    top3 = latest_ranked.head(3)
    bot3 = latest_ranked.tail(3)

    tc1, tc2 = st.columns(2)
    with tc1:
        st.markdown('<div class="card-label">🏆 TOP 3 SEKTOREN</div>', unsafe_allow_html=True)
        for i, (name, val) in enumerate(top3.items()):
            medal = ["🥇", "🥈", "🥉"][i]
            c = "#22c55e" if val > 0 else "#ef4444"
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e293b;">'
                        f'<span style="font-size:.85rem;color:#e2e8f0;">{medal} {name}</span>'
                        f'<span style="font-size:.85rem;font-weight:700;color:{c};">{val:+.2f}%</span></div>',
                        unsafe_allow_html=True)
    with tc2:
        st.markdown('<div class="card-label">📉 BOTTOM 3 SEKTOREN</div>', unsafe_allow_html=True)
        for name, val in bot3.items():
            c = "#22c55e" if val > 0 else "#ef4444"
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e293b;">'
                        f'<span style="font-size:.85rem;color:#e2e8f0;">{name}</span>'
                        f'<span style="font-size:.85rem;font-weight:700;color:{c};">{val:+.2f}%</span></div>',
                        unsafe_allow_html=True)

    st.markdown("")

    # ── FULL TABLE with color coding ──
    st.markdown(f'<div class="card-label">PERFORMANCE-TABELLE ({mode.upper()} · letzte {n_periods} {"Tage" if is_daily else "Wochen"} · sortiert nach jüngstem {"Tag" if is_daily else "Woche"})</div>', unsafe_allow_html=True)

    # Style the table: green for positive, red for negative
    def _color_cell(val):
        if pd.isna(val): return ""
        if val > 1.5: return "background-color: #22c55e30; color: #22c55e; font-weight: 600"
        if val > 0: return "background-color: #22c55e15; color: #22c55e"
        if val < -1.5: return "background-color: #ef444430; color: #ef4444; font-weight: 600"
        if val < 0: return "background-color: #ef444415; color: #ef4444"
        return "color: #94a3b8"

    styled = table.style.map(_color_cell).format("{:+.2f}%", na_rep="—")

    st.dataframe(styled, use_container_width=True, height=min(500, 40 + len(table) * 38))

    # ── RANKING POSITION HISTORY ──
    st.markdown("")
    st.markdown(f'<div class="card-label">RANKING-VERLAUF (Position 1 = bester Sektor)</div>', unsafe_allow_html=True)

    # Build ranking history
    if is_daily:
        pct_all = sector_closes.pct_change() * 100
    else:
        pct_all = sector_closes.resample("W-FRI").last().pct_change() * 100
    pct_all = pct_all.dropna(how="all").tail(n_periods)

    # For each period, rank the sectors (1 = best)
    rankings = pct_all.rank(axis=1, ascending=False, method="min").astype(int)

    # Rename columns
    col_rename = {etf: f"{name} ({etf})" for etf, name in SECTOR_ETFS.items()}
    rankings.columns = [col_rename.get(c, c) for c in rankings.columns]

    # Plot ranking lines
    fig = go.Figure()
    colors_cycle = ["#06b6d4", "#ef4444", "#22c55e", "#f59e0b", "#a855f7", "#f97316",
                    "#3b82f6", "#ec4899", "#84cc16", "#64748b", "#14b8a6"]
    for i, col in enumerate(rankings.columns):
        fig.add_trace(go.Scatter(
            x=_x(rankings.index), y=_y(rankings[col]),
            name=col.split(" (")[0],  # short name
            line=dict(color=colors_cycle[i % len(colors_cycle)], width=1.5),
            mode="lines+markers", marker=dict(size=4),
        ))
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0, r=0, t=10, b=0), height=350,
        yaxis=dict(autorange="reversed", gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b"),
                   title="Rang", title_font=dict(size=9, color="#64748b"), dtick=1),
        xaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="left", x=0,
                    font=dict(size=8, color="#94a3b8")),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _tab_marktanalyse():
    """Tab 1: Full market analysis (original dashboard content)."""

    with st.spinner("Lade Marktdaten …"): data=load_market_data()
    if not data: st.error("Keine Marktdaten.");return
    available=[i for i in ["S&P 500","Nasdaq Composite","Russell 2000"] if i in data]
    if not available: st.error("Keine Index-Daten.");return

    c1,c2=st.columns([3,1])
    with c1: selected=st.radio("Index",available,horizontal=True,label_visibility="collapsed")
    with c2: sd=st.selectbox("Zeitraum",[60,90,130,200],index=1,format_func=lambda x:f"{x} Tage")

    df=add_indicators(data[selected].copy());df=detect_distribution_days(df);df=compute_ampel(df)
    L=df.iloc[-1];pct=L["Pct_Change"] if not np.isnan(L["Pct_Change"]) else 0.0

    # ── TRENDWENDE-AMPEL (eigene Kategorie) ──
    st.markdown("---")
    render_ampel_section(L)
    st.markdown("")

    # ── METRICS ──
    m1,m2,m3,m4,m5=st.columns(5);dc=int(L["Dist_Count_25"])
    with m1: st.metric(selected,f"{L['Close']:,.2f}",f"{pct:+.2f}%")
    with m2: st.metric("Dist.-Tage",dc,"⚠ Häufung" if dc>=4 else "OK")
    d21=L["Dist_21EMA"]
    with m3: st.metric("21-EMA",f"{d21:.1f} ATR" if not np.isnan(d21) else "—")
    d50=L["Dist_50SMA_pct"]; t50=7.0 if "Nasdaq" in selected else 5.0
    with m4: st.metric("50-SMA",f"{d50:+.1f}%" if not np.isnan(d50) else "—",f"⚠>{t50:.0f}%" if(not np.isnan(d50) and d50>t50) else "")
    dd=L["Dist_52w_pct"]
    with m5: st.metric("Drawdown",f"{dd:.1f}%" if not np.isnan(dd) else "—")

    # ── CHARTS ──
    st.plotly_chart(plot_price(df,sd),use_container_width=True,config={"displayModeBar":False})
    st.plotly_chart(plot_volume(df,sd),use_container_width=True,config={"displayModeBar":False})

    # ── WARNING SIGNALS ──
    st.markdown("---")
    st.markdown('<div class="info-card"><div class="card-label">FRÜHWARNZEICHEN & WARNZEICHEN</div>',unsafe_allow_html=True)
    wc=0
    def _w(label,ok,detail,check_warn=False):
        nonlocal wc
        if check_warn: wc+=1
        render_check(label,ok,detail,warn=check_warn)
    nr=int(L.get("Neg_Reversals_10d",0));w=nr>=3;_w("Intraday-Umkehrungen (10T)",nr<3,f"{nr} negative Umkehrungen",w)
    lc=int(L.get("Low_CR_5d",0));w=lc>=3;_w("Closing Range Häufung (5T)",lc<3,f"{lc}/5 Tage Schluss im unteren 25%",w)
    w=dc>=4;_w("Distributionstage",dc<=3,f"{dc} im 25T-Fenster",w)
    st10=int(df["Is_Stall"].tail(10).sum());w=st10>=3;_w("Stau-Tage (10T)",st10<3,f"{st10} Stau-Tage",w)
    if not np.isnan(d50): w=d50>t50 or d50<0;_w(f"50-SMA Abstand",not w,f"{d50:+.1f}% ({'über' if d50>0 else 'unter'} 50-SMA, Schwelle: {t50:.0f}%)",w)
    if not np.isnan(d21): w=d21>3.0 or d21<0;_w("21-EMA Abstand",not w,f"{d21:.1f} ATR ({'über' if d21>0 else 'unter'} 21-EMA, Schwelle: 3.0 ATR)",w)
    u200=not np.isnan(L["SMA200"]) and L["Close"]<L["SMA200"];u50=not np.isnan(L["SMA50"]) and L["Close"]<L["SMA50"]
    if u200: wc+=1
    if u50: wc+=1
    _w("Kurs über 200-SMA",not u200,"Unter 200-SMA" if u200 else "OK",u200)
    _w("Kurs über 50-SMA",not u50,"Unter 50-SMA" if u50 else "OK",u50)
    vd=bool(L.get("Up_Vol_Declining",False));_w("Volumen an Aufwärtstagen",not vd,"Abnehmendes Vol." if vd else "OK",vd)
    div_r=detect_intermarket_divergence(data)
    if div_r:
        ah=[r for r in div_r if r["at_20d_high"]];nh=[r for r in div_r if not r["at_20d_high"]];hd=len(ah)>0 and len(nh)>0
        _w("Intermarket-Konvergenz",not hd," · ".join(f"{r['name']}: {r['pct']:+.1f}%" for r in div_r),hd)
    rot,sd2,sp=detect_sector_rotation(data)
    if rot is not None: _w("Keine Sektorrotation in Defensive",not rot,f"Spread: {sp:+.1f}%",rot)
    rp,drp=detect_failing_rally(df)
    if rp is not None and drp and drp>5: w=rp<50;_w("Erholungsquote ≥50%",not w,f"Rückgang {drp:.1f}%, Erholung {rp:.0f}%",w)
    if wc==0: st.markdown('<div style="text-align:center;padding:8px;color:#22c55e;">✓ Keine aktiven Warnzeichen</div>',unsafe_allow_html=True)
    elif wc<=2: st.markdown(f'<div style="text-align:center;padding:8px;color:#f59e0b;">⚠ {wc} Warnzeichen</div>',unsafe_allow_html=True)
    else: st.markdown(f'<div style="text-align:center;padding:8px;color:#ef4444;">⚠ {wc} Warnzeichen — Risiko reduzieren</div>',unsafe_allow_html=True)
    st.markdown("</div>",unsafe_allow_html=True)

    # ── AUFWÄRTSTREND + AMPEL DETAILS (2 cols) ──
    cl,cr_=st.columns(2)
    with cl:
        st.markdown('<div class="info-card"><div class="card-label">AUFWÄRTSTREND-PRÜFUNG</div>',unsafe_allow_html=True)
        _c=L["Close"];_l=L["Low"];_e=L["EMA21"];_s5=L["SMA50"];_s2=L["SMA200"]
        eo=not np.isnan(_e);so=not np.isnan(_s5);s2o=not np.isnan(_s2)
        for nm,mv,ok,hk,ck in [("21-EMA",_e,eo,"EMA21_held","Consec_Low_above_21"),("50-SMA",_s5,so,"SMA50_held","Consec_Low_above_50"),("200-SMA",_s2,s2o,"SMA200_held","Consec_Low_above_200")]:
            render_check(f"Schluss über {nm}",ok and _c>mv,f"{_c:,.0f} vs {mv:,.0f}" if ok else "")
            render_check(f"Tief über {nm}",ok and _l>mv,f"{_l:,.0f} vs {mv:,.0f}" if ok else "")
            render_check(f"{nm} gehalten",bool(L.get(hk,False)),"Schlusskurs darüber" if bool(L.get(hk,False)) else "Darunter")
            cc=int(L.get(ck,0));render_check(f"3T Tief>{nm}",cc>=3,f"{cc} Tage")
        st.markdown("</div>",unsafe_allow_html=True)
        st.markdown('<div class="info-card"><div class="card-label">ORDNUNG</div>',unsafe_allow_html=True)
        render_check("21-EMA > 50-SMA",eo and so and _e>_s5,f"{_e:,.0f} vs {_s5:,.0f}" if eo and so else "")
        render_check("21-EMA > 200-SMA",eo and s2o and _e>_s2,f"{_e:,.0f} vs {_s2:,.0f}" if eo and s2o else "")
        render_check("50-SMA > 200-SMA",so and s2o and _s5>_s2,f"{_s5:,.0f} vs {_s2:,.0f}" if so and s2o else "")
        render_check("Distributionstage ≤ 3",dc<=3,f"{dc}")
        st.markdown("</div>",unsafe_allow_html=True)
    with cr_:
        # Sector rotation detail (if available)
        rot,sd2,sp=detect_sector_rotation(data)
        if rot is not None and sd2:
            st.markdown('<div class="info-card"><div class="card-label">SEKTORROTATION (10T)</div>',unsafe_allow_html=True)
            for group,items in sd2.items():
                st.markdown(f"**{group}:**")
                for name,perf in items:
                    if perf is not None:
                        c="#22c55e" if perf>0 else "#ef4444"
                        st.markdown(f'<span style="color:{c};font-size:.85rem;">{name}: {perf:+.1f}%</span>',unsafe_allow_html=True)
            st.markdown("</div>",unsafe_allow_html=True)

    # ── MARKTBREITE (Equal-Weight) ──
    st.markdown("---")
    st.markdown('<div class="card-label">MARKTBREITE — EQUAL-WEIGHT (3-Tage-Stabilitätsregel)</div>',unsafe_allow_html=True)
    ep="RSP (Equal-Weight S&P)" if "S&P" in selected else "QQEW (Equal-Weight Nasdaq)"
    es="QQEW (Equal-Weight Nasdaq)" if "S&P" in selected else "RSP (Equal-Weight S&P)"
    bc1,bc2=st.columns(2)
    for col,etf in [(bc1,ep),(bc2,es)]:
        with col:
            if etf in data: dfe=compute_breadth_mode(data[etf].copy());render_breadth(dfe.iloc[-1]["Breadth_Mode"],float(dfe.iloc[-1]["Dist_52w_pct"]));st.caption(etf)
            else: st.info(f"{etf} n/a")

    # ── VIX ──
    st.markdown("---")
    st.markdown('<div class="card-label">VOLATILITÄT & STIMMUNG</div>',unsafe_allow_html=True)
    vc1,vc2=st.columns([2,1])
    with vc1:
        if "VIX" in data: st.plotly_chart(plot_vix(analyze_vix(data["VIX"].copy()),sd),use_container_width=True,config={"displayModeBar":False})
    with vc2:
        if "VIX" in data:
            dv=analyze_vix(data["VIX"].copy());vl=dv.iloc[-1];st.metric("VIX",f"{vl['Close']:.1f}")
            if not np.isnan(vl["SMA10"]):
                pab=vl["Pct_Above_SMA10"]
                if not np.isnan(pab):
                    if pab>20: st.error(f"⚠ {pab:.0f}% über SMA → Panik")
                    elif vl["Close"]>20: st.warning(f"Erhöht ({pab:+.0f}%)")
                    else: st.success(f"Ruhig ({pab:+.0f}%)")
        if "VXX" in data:
            dx=data["VXX"].copy();dx["EMA21"]=_ema(dx["Close"],21);xl=dx.iloc[-1]
            ab=xl["Close"]>xl["EMA21"];ri=dx["EMA21"].iloc[-1]>dx["EMA21"].iloc[-5] if len(dx)>5 else False
            if ab and ri: st.warning("VIXY Risk-Off")
            elif not ab and not ri: st.success("VIXY Risk-On")
            else: st.info("VIXY gemischt")

    # ══════════════════════════════════════════════════
    # TIEFENANALYSE (on-demand)
    # ══════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 🔬 Tiefenanalyse — Marktbreite & Makro")
    st.caption("Berechnet A/D-Linie, McClellan Oscillator, Neue Hochs/Tiefs, % über MAs, Deemer Ratio aus allen S&P 500 Aktien. Optional: Fed Funds Rate über FRED API.")

    # FRED key: try secrets first, then environment, then manual input
    fred_key = ""
    try:
        fred_key = st.secrets.get("FRED_API_KEY", "")
    except Exception:
        pass
    if not fred_key:
        fred_key = os.environ.get("FRED_API_KEY", "")
    if not fred_key:
        fred_key = st.text_input("FRED API Key (optional, kostenlos von fred.stlouisfeed.org)", type="password", help="Für Fed Funds Rate. Ohne Key werden nur die Marktbreite-Indikatoren angezeigt.")

    if st.button("🔬 Tiefenanalyse starten", type="primary", use_container_width=True):
        with st.spinner("Lade S&P 500 Aktien … (kann 1–2 Min. dauern)"):
            closes = load_sp500_breadth_data()

        if closes is not None and len(closes) > 50:
            br = compute_breadth_from_components(closes)
            if br is not None and len(br) > 20:
                # Determine last valid trading day (last row with actual data)
                last_trading_date = br.index[-1].strftime("%d.%m.%Y")
                today_str = datetime.now().strftime("%d.%m.%Y")
                is_today = last_trading_date == today_str
                date_note = f"Stand: {last_trading_date}" + ("" if is_today else " (letzter Handelstag)")

                st.success(f"✓ {len(closes.columns)} Aktien geladen, {len(br)} Handelstage · {date_note}")

                # ── Breadth Charts ──
                st.plotly_chart(plot_breadth_deep(br, sd), use_container_width=True, config={"displayModeBar": False})

                # ── Latest values (use last VALID row, not today) ──
                # Drop rows where most indicators are NaN (non-trading days)
                br_valid = br.dropna(subset=["McClellan", "New_Highs"], how="all")
                if len(br_valid) == 0:
                    st.warning("Keine gültigen Handelstage gefunden.")
                    st.markdown("</div>", unsafe_allow_html=True)
                    return
                bL = br_valid.iloc[-1]
                bL_date = br_valid.index[-1].strftime("%d.%m.%Y")

                st.markdown(f'<div class="info-card"><div class="card-label">MARKTBREITE-KENNZAHLEN — S&P 500 ({len(closes.columns)} Aktien) · {bL_date}</div>', unsafe_allow_html=True)

                kb1, kb2, kb3, kb4, kb5 = st.columns(5)
                with kb1:
                    mc = bL["McClellan"]
                    st.metric("McClellan Osc.", f"{mc:.1f}" if not np.isnan(mc) else "—",
                              "Überkauft" if mc > 70 else "Überverkauft" if mc < -70 else "")
                with kb2:
                    nhr = bL["NH_NL_Ratio"]
                    nh_val = int(bL["New_Highs"]) if not np.isnan(bL["New_Highs"]) else 0
                    nl_val = int(bL["New_Lows"]) if not np.isnan(bL["New_Lows"]) else 0
                    st.metric("NH/NL Ratio", f"{nhr:.2f}" if not np.isnan(nhr) else f"{nh_val}/{nl_val}",
                              f"{nh_val} Hochs / {nl_val} Tiefs")
                with kb3:
                    p50 = bL["Pct_Above_50SMA"]
                    st.metric("% > 50-SMA", f"{p50:.0f}%" if not np.isnan(p50) else "—",
                              "Überhitzt" if p50 > 70 else "Schwach" if p50 < 30 else "")
                with kb4:
                    p200 = bL["Pct_Above_200SMA"]
                    st.metric("% > 200-SMA", f"{p200:.0f}%" if not np.isnan(p200) else "—")
                with kb5:
                    dr = bL["Deemer_Ratio"]
                    if not np.isnan(dr):
                        if dr >= 1.97: dr_label = "🚀 Sehr gut"; dr_delta = "Breakaway Momentum!"
                        elif dr >= 1.50: dr_label = f"{dr:.2f}"; dr_delta = "Gut — konstruktiv"
                        elif dr >= 1.00: dr_label = f"{dr:.2f}"; dr_delta = "Neutral"
                        else: dr_label = f"{dr:.2f}"; dr_delta = "Schlecht — schwache Breite"
                    else: dr_label = "—"; dr_delta = ""
                    st.metric("Deemer Ratio", dr_label, dr_delta)

                # Breadth Thrust check
                recent_thrust = br["Breadth_Thrust"].tail(20).any()
                if recent_thrust:
                    st.success("🚀 Breitenschub (Deemer Ratio > 1.97) in den letzten 20 Tagen erkannt! Seltenes bullisches Signal.")

                # Divergence check: Index new high but A/D line not
                if "S&P 500" in data:
                    spx = data["S&P 500"]
                    spx_at_high = spx["Close"].iloc[-1] >= spx["High"].rolling(20).max().iloc[-2] * 0.998
                    ad_at_high = br["AD_Line"].iloc[-1] >= br["AD_Line"].rolling(20).max().iloc[-2] * 0.998
                    if spx_at_high and not ad_at_high:
                        st.warning("⚠ Divergenz: S&P 500 nahe 20T-Hoch, aber A/D-Linie nicht — Marktbreite lässt nach")
                    elif spx_at_high and ad_at_high:
                        st.success("✓ S&P 500 und A/D-Linie bestätigen sich — breite Beteiligung")

                # Checklist items from deep analysis
                render_check("McClellan > 0 (bullisches Momentum)", mc > 0, f"McClellan: {mc:.1f}")
                render_check("% über 50-SMA > 50% (Mehrheit im Aufwärtstrend)", p50 > 50, f"{p50:.0f}%")
                render_check("NH/NL Ratio > 1 (mehr Hochs als Tiefs)", nhr > 1 if not np.isnan(nhr) else False, f"Ratio: {nhr:.1f}" if not np.isnan(nhr) else "—")
                render_check("Keine Divergenz Index vs. A/D-Linie",
                             not (spx_at_high and not ad_at_high) if "S&P 500" in data else True,
                             "A/D-Linie bestätigt" if "S&P 500" in data else "")
                # Deemer with 4-level rating
                if not np.isnan(dr):
                    if dr >= 1.97: dr_status = "Sehr gut (≥1.97) — 🚀 Breakaway Momentum!"
                    elif dr >= 1.50: dr_status = "Gut (1.50–1.96) — konstruktive Breite"
                    elif dr >= 1.00: dr_status = "Neutral (1.00–1.49) — leicht positiv"
                    else: dr_status = "Schlecht (<1.00) — mehr Decliners als Advancers"
                    render_check("Deemer Ratio (Breitenschub)", dr >= 1.50, f"Ratio: {dr:.2f} · {dr_status}")
                else:
                    render_check("Deemer Ratio (Breitenschub)", False, "Nicht verfügbar")

                st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.error("Konnte S&P 500 Daten nicht laden. Bitte erneut versuchen.")

        # ── Fed Funds Rate ──
        if fred_key:
            with st.spinner("Lade Fed Funds Rate …"):
                fed = load_fed_funds_rate(fred_key)
            if fed is not None and len(fed) > 10:
                st.markdown('<div class="info-card"><div class="card-label">FEDERAL FUNDS RATE (FRED)</div>', unsafe_allow_html=True)
                st.plotly_chart(plot_fed_rate(fed), use_container_width=True, config={"displayModeBar": False})
                current_rate = fed["FedRate"].iloc[-1]
                prev_rate = fed["FedRate"].iloc[-30] if len(fed) > 30 else fed["FedRate"].iloc[0]
                rate_trend = "steigend" if current_rate > prev_rate else "fallend" if current_rate < prev_rate else "stabil"
                render_check("Zinstrend nicht steigend", rate_trend != "steigend",
                             f"Fed Funds Rate: {current_rate:.2f}% ({rate_trend})",
                             warn=(rate_trend == "steigend"))
                st.markdown("</div>", unsafe_allow_html=True)
            elif fred_key:
                st.warning("FRED-Daten konnten nicht geladen werden. API-Key korrekt?")

    # ── DAILY CHECKLIST ──
    st.markdown("---")
    st.markdown('<div class="info-card"><div class="card-label">TÄGLICHE CHECKLISTE</div>',unsafe_allow_html=True)
    ddv=float(L["Dist_52w_pct"]) if not np.isnan(L["Dist_52w_pct"]) else 0
    no_correction = ddv > -8  # True = no substantial correction = normal market
    render_check("Kein substanzieller Drawdown (> -8%)", no_correction,
                 f"Drawdown: {ddv:.1f}%" + (" — Korrektur läuft, Ampel aktiv" if not no_correction else " — Markt im Normalbereich"))
    render_check("Stabilisierung?",L["Ampel_Phase"] not in ("rot",) or L["Anchor_Date"] is not None,
                 f"Ankertag: {L['Anchor_Date']}" if L["Anchor_Date"] else "Kein Zyklus" if L["Ampel_Phase"] in ("neutral","aufwaertstrend") else "Noch keine")
    render_check("Startschuss (≥Gelb)?",L["Ampel_Phase"] in ("gelb","gruen","aufwaertstrend"),f"Phase: {L['Ampel_Phase'].upper().replace('AUFWAERTSTREND','AUFWÄRTSTREND')}")
    if ep in data: dfe=compute_breadth_mode(data[ep].copy());render_check("Marktbreite?",dfe.iloc[-1]["Breadth_Mode"]!="schutz",f"Modus: {dfe.iloc[-1]['Breadth_Mode'].capitalize()}")
    render_check("Dist.-Tage ≤3?",dc<=3,f"{dc}")
    if "VIX" in data: dv=analyze_vix(data["VIX"].copy());render_check("VIX nicht Panik?",not bool(dv.iloc[-1].get("Is_Panic",False)),f"VIX: {data['VIX'].iloc[-1]['Close']:.1f}")
    render_check(f"Warnzeichen ≤2?",wc<=2,f"{wc} aktiv")
    st.markdown("</div>",unsafe_allow_html=True)

    st.markdown("---")
    st.caption(f"Börse ohne Bauchgefühl · v3.0 · Yahoo Finance + FRED · Stand: {L.name.strftime('%d.%m.%Y')}")

if __name__=="__main__": main()
