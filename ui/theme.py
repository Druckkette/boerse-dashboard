"""Theme and global Streamlit styling."""

PAGE_CONFIG = {
    "page_title": "Börse ohne Bauchgefühl",
    "page_icon": "🚦",
    "layout": "wide",
    "initial_sidebar_state": "collapsed",
}

APP_CSS = """<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');

:root {
  --bg:           #f0f2f7;
  --panel:        #ffffff;
  --panel-2:      #f7f9fc;
  --border:       #e3e8f0;
  --border-strong:#c8d2e0;
  --text:         #0d1626;
  --muted:        #5e6e89;
  --muted-light:  #8fa0b8;
  --accent:       #1d4ed8;
  --accent-hover: #1e40af;
  --accent-bg:    #eff6ff;
  --good:         #15803d;
  --good-bg:      #f0fdf4;
  --good-border:  #bbf7d0;
  --warn:         #b45309;
  --warn-bg:      #fffbeb;
  --warn-border:  #fde68a;
  --bad:          #b91c1c;
  --bad-bg:       #fef2f2;
  --bad-border:   #fecaca;

  --radius-sm:  8px;
  --radius-md:  12px;
  --radius-lg:  16px;
  --radius-xl:  20px;

  --shadow-sm:   0 1px 3px rgba(15,25,50,.07), 0 1px 2px rgba(15,25,50,.04);
  --shadow-card: 0 2px 8px rgba(15,25,50,.08), 0 1px 3px rgba(15,25,50,.05);
  --shadow-soft: 0 4px 18px rgba(15,25,50,.10), 0 2px 6px rgba(15,25,50,.06);
}

html, body, [class*="css"] { font-family: 'Inter', system-ui, sans-serif; }

/* ── App shell ── */
.stApp {
  background: var(--bg);
  color: var(--text);
  font-family: 'Inter', system-ui, sans-serif;
}
.main .block-container {
  max-width: 1280px;
  padding-top: 1.2rem;
  padding-bottom: 2.4rem;
}

/* ── Top bar ── */
.app-topbar {
  margin: 0 0 .7rem 0;
  padding: 10px 18px;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-sm);
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
}
.app-topbar__eyebrow {
  margin: 0;
  color: var(--accent);
  font-size: .63rem;
  text-transform: uppercase;
  letter-spacing: .12em;
  font-weight: 700;
  white-space: nowrap;
}
.app-topbar__title {
  margin: 0;
  font-size: 1.15rem !important;
  line-height: 1.2;
  font-weight: 800 !important;
  color: var(--text) !important;
}

/* ── Typography ── */
h1, h2, h3 { font-family: 'Inter', system-ui, sans-serif !important; letter-spacing: -0.02em; color: var(--text); }
h1 { font-size: 1.85rem !important; font-weight: 800 !important; line-height: 1.14; }
h2 { font-size: 1.26rem !important; font-weight: 700 !important; }
h3 { font-size: 1.04rem !important; font-weight: 700 !important; }
p, li, label, .stMarkdown, .stCaption { font-family: 'Inter', system-ui, sans-serif !important; color: var(--text); }
code, pre { font-family: 'JetBrains Mono', monospace !important; }

hr { border: none; border-top: 1px solid var(--border); margin: 1rem 0; }

/* ── Navigation pills ── */
[data-testid="stNavigation"] { margin-bottom: .9rem; }
[data-testid="stNavigation"] [data-baseweb="tab-list"] { gap: 5px; flex-wrap: wrap; }
[data-testid="stNavigation"] [data-baseweb="tab"] {
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--panel);
  color: var(--muted);
  padding: 6px 13px;
  min-height: 34px;
  font-size: .83rem;
  font-weight: 600;
  transition: border-color .15s, color .15s, background .15s;
  box-shadow: var(--shadow-sm);
}
[data-testid="stNavigation"] [data-baseweb="tab"]:hover {
  border-color: var(--accent);
  color: var(--accent);
  background: var(--accent-bg);
}
[data-testid="stNavigation"] [aria-selected="true"] {
  background: var(--accent);
  border-color: var(--accent);
  color: #ffffff;
  box-shadow: 0 2px 8px rgba(29,78,216,.30);
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
  gap: 6px;
  background: transparent;
  flex-wrap: wrap;
  padding: 2px;
}
.stTabs [data-baseweb="tab"] {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 999px;
  color: var(--muted);
  padding: 8px 16px;
  min-height: 38px;
  font-size: .86rem;
  font-weight: 600;
  transition: all .15s ease;
  box-shadow: var(--shadow-sm);
}
.stTabs [data-baseweb="tab"]:hover {
  border-color: var(--accent);
  color: var(--accent);
  background: var(--accent-bg);
}
.stTabs [aria-selected="true"] {
  background: var(--accent);
  border-color: var(--accent);
  color: #ffffff;
  box-shadow: 0 2px 10px rgba(29,78,216,.28);
}

/* ── Core cards ── */
.summary-hero, .change-card, .info-card, .workspace-card, .score-card {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-card);
  padding: 16px 18px;
}
.summary-hero {
  padding: 20px 24px;
  border-radius: var(--radius-xl);
  background: linear-gradient(135deg, #eef4ff 0%, #ffffff 60%);
  border: 1px solid var(--border);
  border-left: 3px solid var(--accent);
  box-shadow: var(--shadow-soft);
}
.change-card { padding: 15px 16px; }
.info-card, .workspace-card { margin-bottom: 12px; }

/* ── Streamlit Metrics ── */
.card-label,
[data-testid="stMetricLabel"],
[data-testid="stMetricValue"] { font-family: 'Inter', system-ui, sans-serif !important; }

[data-testid="stMetric"] {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 16px 18px;
  box-shadow: var(--shadow-card);
}
[data-testid="stMetricLabel"] {
  color: var(--muted) !important;
  font-size: .71rem !important;
  text-transform: uppercase;
  letter-spacing: .09em;
  font-weight: 700 !important;
  margin-bottom: .3rem;
}
[data-testid="stMetricValue"] {
  color: var(--text) !important;
  font-size: 1.42rem !important;
  line-height: 1.15 !important;
  font-weight: 800 !important;
}
[data-testid="stMetricDelta"] { font-size: .77rem !important; font-weight: 600 !important; }

/* ── Section headers ── */
.card-label {
  font-size: .69rem;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: .09em;
  margin-bottom: 8px;
  font-weight: 700;
}
.mini-help { font-size: .78rem; color: var(--muted); line-height: 1.55; margin-top: 8px; }

.section-header { display: flex; flex-direction: column; gap: 4px; margin-bottom: 12px; }
.section-eyebrow { font-size: .67rem; text-transform: uppercase; letter-spacing: .11em; color: var(--accent); font-weight: 700; }
.section-title { font-size: 1.14rem; line-height: 1.25; font-weight: 800; color: var(--text); margin: 0; }
.section-subtitle { font-size: .86rem; line-height: 1.5; color: var(--muted); margin: 0; }

/* ── Hero elements ── */
.hero-title { font-size: 1.28rem; font-weight: 800; color: var(--text); margin-bottom: 4px; line-height: 1.2; }
.hero-subtitle { font-size: .91rem; color: var(--muted); margin-bottom: 14px; line-height: 1.45; }
.hero-action {
  display: inline-flex; align-items: center; gap: 8px;
  font-size: .91rem; font-weight: 700; padding: 8px 14px;
  border-radius: 999px; margin-top: 8px; border: 1px solid transparent;
}
.hero-good  { background: var(--good-bg);  color: var(--good);  border-color: var(--good-border); }
.hero-warn  { background: var(--warn-bg);  color: var(--warn);  border-color: var(--warn-border); }
.hero-bad   { background: var(--bad-bg);   color: var(--bad);   border-color: var(--bad-border); }

/* ── Change cards ── */
.change-title  { font-size: .70rem; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; margin-bottom: 6px; font-weight: 700; }
.change-value  { font-size: 1.04rem; font-weight: 700; color: var(--text); }
.change-detail { font-size: .81rem; color: var(--muted); margin-top: 4px; line-height: 1.4; }

/* ── KPI cards ── */
.kpi-explainer {
  background: var(--panel-2);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 10px 12px;
  font-size: .8rem;
  color: var(--muted);
}
.kpi-card {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-card);
  padding: 14px 16px;
  margin-bottom: 10px;
  min-height: 100%;
  overflow-wrap: anywhere;
  transition: box-shadow .15s, border-color .15s;
}
.kpi-card:hover { box-shadow: var(--shadow-soft); border-color: var(--border-strong); }
.kpi-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
.kpi-label { font-size: .69rem; color: var(--muted); text-transform: uppercase; letter-spacing: .1em; font-weight: 700; }
.kpi-value { font-size: 1.28rem; line-height: 1.2; font-weight: 800; color: var(--text); margin-top: 7px; }
.kpi-interpretation { font-size: .85rem; line-height: 1.45; color: var(--text); margin-top: 6px; }
.kpi-copy { font-size: .79rem; line-height: 1.52; color: var(--muted); margin-top: 6px; overflow-wrap: anywhere; }

/* ── Pills ── */
.pill-wrap { display: flex; flex-wrap: wrap; gap: 7px; }
.pill {
  display: inline-flex; align-items: center; padding: 5px 11px;
  border-radius: 999px; background: var(--panel-2); border: 1px solid var(--border);
  color: var(--text); font-size: .81rem; font-weight: 500;
}
.workspace-note { font-size: .81rem; color: var(--muted); line-height: 1.5; }

/* ── Ampel (traffic light) ── */
.ampel-box {
  border-radius: var(--radius-md); padding: 16px 20px;
  display: flex; align-items: center; gap: 16px;
  background: var(--panel); border: 1px solid var(--border);
}
.ampel-dot { width: 48px; height: 48px; border-radius: 50%; flex-shrink: 0; }
.check-item { display: flex; align-items: flex-start; gap: 10px; padding: 9px 0; border-bottom: 1px solid var(--border); }
.check-item:last-child { border-bottom: none; }
.check-icon {
  width: 22px; height: 22px; border-radius: 50%; flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
  font-size: 12px; font-weight: 700;
}
.check-ok   { background: var(--good-bg); border: 1.5px solid var(--good-border); color: var(--good); }
.check-fail { background: var(--bad-bg);  border: 1.5px solid var(--bad-border);  color: var(--bad); }
.check-warn { background: var(--warn-bg); border: 1.5px solid var(--warn-border); color: var(--warn); }

/* ── Breadth track ── */
.breadth-track { height: 8px; border-radius: 4px; background: var(--border); position: relative; overflow: hidden; margin: 8px 0; }
.breadth-fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 4px; background: linear-gradient(90deg, var(--good), var(--warn), var(--bad)); transition: width .5s; }

/* ── Score card ── */
.score-card { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 14px 16px; }
.score-ring {
  width: 64px; height: 64px; border-radius: 50%; display: grid; place-items: center;
  color: var(--text); font-weight: 800; font-size: .96rem;
  background: conic-gradient(var(--accent) 0deg, var(--accent) 210deg, var(--border) 210deg 360deg);
  border: 1px solid var(--border);
  box-shadow: inset 0 0 0 7px var(--panel);
}

/* ── Status chips ── */
.status-chip {
  display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px;
  border-radius: 999px; border: 1px solid var(--border);
  background: var(--panel-2); font-size: .77rem; font-weight: 700; line-height: 1;
}
.status-good    { color: var(--good); border-color: var(--good-border); background: var(--good-bg); }
.status-warn    { color: var(--warn); border-color: var(--warn-border); background: var(--warn-bg); }
.status-bad     { color: var(--bad);  border-color: var(--bad-border);  background: var(--bad-bg); }
.status-neutral { color: var(--muted); border-color: var(--border); background: var(--panel-2); }

/* ── Layout helpers ── */
.dashboard-grid { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 12px; }
.mobile-stack > * { min-width: 0; }

/* ── Streamlit overrides ── */
[data-testid="stDataFrame"] { border-radius: var(--radius-md); overflow: hidden; border: 1px solid var(--border); }
[data-testid="stDataFrame"] table { background: var(--panel) !important; }

/* Buttons */
.stButton > button {
  background: var(--accent) !important;
  color: #fff !important;
  border: none !important;
  border-radius: var(--radius-md) !important;
  font-weight: 600 !important;
  box-shadow: 0 1px 4px rgba(29,78,216,.25) !important;
  transition: background .15s !important;
}
.stButton > button:hover { background: var(--accent-hover) !important; }

/* Secondary buttons – Verlauf / zuletzt geprüft */
.stButton > [data-testid="baseButton-secondary"] {
  background: var(--panel-2) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
  box-shadow: none !important;
}
.stButton > [data-testid="baseButton-secondary"]:hover {
  background: var(--panel) !important;
  color: var(--text) !important;
}

/* Selectbox / inputs */
[data-baseweb="select"] > div,
[data-baseweb="input"] > div {
  background: var(--panel) !important;
  border-color: var(--border) !important;
  border-radius: var(--radius-md) !important;
}

/* Sidebar */
[data-testid="stSidebar"] {
  background: var(--panel) !important;
  border-right: 1px solid var(--border);
}

/* ── Responsive ── */
@media (max-width: 980px) {
  .main .block-container { padding-top: 1rem; }
  [data-testid="stMetric"] { padding: 14px 14px; }
  [data-testid="stMetricValue"] { font-size: 1.26rem !important; }
  .summary-hero { padding: 16px 16px; border-radius: var(--radius-lg); }
  .dashboard-grid { grid-template-columns: repeat(6, minmax(0, 1fr)); }
}

@media (max-width: 640px) {
  .main .block-container { padding-top: .85rem; padding-bottom: 1.4rem; }
  .app-topbar { padding: 10px 12px 8px; }
  .app-topbar__title { font-size: 1.08rem !important; }
  .stTabs [data-baseweb="tab-list"] { gap: 5px; }
  .stTabs [data-baseweb="tab"] { flex: 1 1 calc(50% - 6px); justify-content: center; text-align: center; padding: 8px 10px; }
  [data-testid="stNavigation"] [data-baseweb="tab"] { flex: 1 1 calc(33% - 5px); justify-content: center; text-align: center; padding: 6px 6px; font-size: .77rem; }
  [data-testid="stMetric"] { border-radius: var(--radius-md); padding: 12px 12px; }
  [data-testid="stMetricLabel"] { font-size: .66rem !important; }
  [data-testid="stMetricValue"] { font-size: 1.16rem !important; }
  .hero-title { font-size: 1.12rem; }
  .dashboard-grid { grid-template-columns: 1fr; gap: 10px; }
  .mobile-stack { display: flex; flex-direction: column; gap: 10px; }
  .kpi-value { font-size: 1.12rem; }
  .kpi-copy { font-size: .82rem; }
  .stButton > button { width: 100%; }
  [data-testid="stDataFrame"] { overflow-x: auto; }
  [data-testid="stPlotlyChart"] > div { min-height: 260px; }
}
</style>"""
