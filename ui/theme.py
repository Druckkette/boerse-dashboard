"""Theme and global Streamlit styling."""

PAGE_CONFIG = {
    "page_title": "Börse ohne Bauchgefühl",
    "page_icon": "🚦",
    "layout": "wide",
    "initial_sidebar_state": "collapsed",
}

APP_CSS = """<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');

:root{
  --bg:#080f1d;
  --panel:#0f172a;
  --panel-2:#131d33;
  --border:#24324a;
  --text:#e8edf7;
  --muted:#93a1b8;
  --accent:#3b82f6;
  --good:#22c55e;
  --warn:#f59e0b;
  --bad:#ef4444;

  --radius-sm:10px;
  --radius-md:14px;
  --radius-lg:18px;
  --radius-xl:22px;

  --shadow-soft:0 8px 28px rgba(2, 8, 23, .30);
  --shadow-card:0 6px 20px rgba(2, 8, 23, .24);
}

html, body, [class*="css"]{font-family:'Inter',system-ui,sans-serif;}
.stApp{background:radial-gradient(1100px 500px at 86% -10%, rgba(59,130,246,.10), transparent 56%), var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;}
.main .block-container{max-width:1240px;padding-top:1.2rem;padding-bottom:2.2rem;}

h1,h2,h3{font-family:'Inter',system-ui,sans-serif!important;letter-spacing:-0.02em;}
h1{font-size:1.88rem!important;font-weight:800!important;line-height:1.14;}
h2{font-size:1.28rem!important;font-weight:700!important;}
h3{font-size:1.06rem!important;font-weight:700!important;}
p,li,label,.stMarkdown,.stCaption{font-family:'Inter',system-ui,sans-serif!important;}
code,pre{font-family:'JetBrains Mono',monospace!important;}

hr{border:none;border-top:1px solid var(--border);margin:1rem 0;}

/* Core cards */
.summary-hero,.change-card,.info-card,.workspace-card,.score-card{
  background:linear-gradient(180deg, rgba(255,255,255,.015), rgba(255,255,255,.005)), var(--panel);
  border:1px solid var(--border);
  border-radius:var(--radius-lg);
  box-shadow:var(--shadow-card);
  padding:16px 18px;
}
.summary-hero{
  padding:20px 22px;
  border-radius:var(--radius-xl);
  background:
    radial-gradient(620px 260px at -8% 0%, rgba(59,130,246,.18), transparent 60%),
    linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.005)),
    var(--panel);
  box-shadow:var(--shadow-soft);
}
.change-card{padding:15px 16px;}
.info-card,.workspace-card{margin-bottom:12px;}

/* Metrics */
.card-label,[data-testid="stMetricLabel"],[data-testid="stMetricValue"]{font-family:'Inter',system-ui,sans-serif!important;}
[data-testid="stMetric"]{
  background:linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.006)), var(--panel);
  border:1px solid var(--border);
  border-radius:var(--radius-lg);
  padding:16px 18px;
  box-shadow:var(--shadow-card);
}
[data-testid="stMetricLabel"]{
  color:var(--muted)!important;
  font-size:.72rem!important;
  text-transform:uppercase;
  letter-spacing:.09em;
  font-weight:600!important;
  margin-bottom:.32rem;
}
[data-testid="stMetricValue"]{
  color:var(--text)!important;
  font-size:1.45rem!important;
  line-height:1.15!important;
  font-weight:800!important;
}
[data-testid="stMetricDelta"]{font-size:.78rem!important;font-weight:600!important;}

/* Tabs */
.stTabs [data-baseweb="tab-list"]{
  gap:8px;
  background:transparent;
  flex-wrap:wrap;
  padding:2px;
}
.stTabs [data-baseweb="tab"]{
  background:var(--panel-2);
  border:1px solid var(--border);
  border-radius:999px;
  color:var(--muted);
  padding:8px 14px;
  min-height:38px;
  font-size:.86rem;
  font-weight:600;
  transition:all .2s ease;
}
.stTabs [data-baseweb="tab"]:hover{border-color:#355071;color:#c5d2e6;}
.stTabs [aria-selected="true"]{
  background:linear-gradient(180deg, rgba(59,130,246,.34), rgba(59,130,246,.20));
  border-color:#4e8ff6;
  color:#eaf2ff;
  box-shadow:0 0 0 1px rgba(78,143,246,.38) inset;
}

/* Existing class compatibility + polish */
.card-label{font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.09em;margin-bottom:8px;}
.mini-help{font-size:.78rem;color:var(--muted);line-height:1.55;margin-top:8px;}

.hero-title{font-size:1.3rem;font-weight:800;color:var(--text);margin-bottom:4px;line-height:1.2;}
.hero-subtitle{font-size:.92rem;color:var(--muted);margin-bottom:14px;line-height:1.45;}
.hero-action{display:inline-flex;align-items:center;gap:8px;font-size:.92rem;font-weight:700;padding:9px 12px;border-radius:999px;margin-top:8px;border:1px solid transparent;}
.hero-good{background:rgba(34,197,94,.14);color:#9be7b4;border-color:rgba(34,197,94,.40);}
.hero-warn{background:rgba(245,158,11,.14);color:#fdd572;border-color:rgba(245,158,11,.42);}
.hero-bad{background:rgba(239,68,68,.14);color:#ffb2b2;border-color:rgba(239,68,68,.42);}

.change-title{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;}
.change-value{font-size:1.05rem;font-weight:700;color:var(--text);}
.change-detail{font-size:.82rem;color:var(--muted);margin-top:4px;line-height:1.4;}

.kpi-explainer{background:rgba(19,29,51,.75);border:1px solid var(--border);border-radius:var(--radius-md);padding:10px 12px;font-size:.8rem;color:var(--muted);}
.pill-wrap{display:flex;flex-wrap:wrap;gap:8px;}
.pill{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:var(--panel-2);border:1px solid var(--border);color:var(--text);font-size:.82rem;}
.workspace-note{font-size:.82rem;color:var(--muted);line-height:1.5;}

.ampel-box{border-radius:var(--radius-md);padding:16px 20px;display:flex;align-items:center;gap:16px;}
.ampel-dot{width:48px;height:48px;border-radius:50%;flex-shrink:0;}
.check-item{display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);}
.check-item:last-child{border-bottom:none;}
.check-icon{width:22px;height:22px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;}
.check-ok{background:rgba(34,197,94,.15);border:1.5px solid rgba(34,197,94,.4);color:var(--good);}
.check-fail{background:rgba(239,68,68,.15);border:1.5px solid rgba(239,68,68,.4);color:var(--bad);}
.check-warn{background:rgba(245,158,11,.15);border:1.5px solid rgba(245,158,11,.4);color:var(--warn);}

.breadth-track{height:10px;border-radius:5px;background:var(--border);position:relative;overflow:hidden;margin:8px 0;}
.breadth-fill{position:absolute;left:0;top:0;bottom:0;border-radius:5px;background:linear-gradient(90deg,var(--good),var(--warn),var(--bad));transition:width .5s;}

/* New utility classes */
.section-header{display:flex;flex-direction:column;gap:5px;margin-bottom:12px;}
.section-eyebrow{font-size:.69rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);font-weight:700;}
.section-title{font-size:1.16rem;line-height:1.25;font-weight:800;color:var(--text);margin:0;}
.section-subtitle{font-size:.87rem;line-height:1.5;color:var(--muted);margin:0;}

.score-card{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:14px 16px;}
.score-ring{
  width:66px;height:66px;border-radius:50%;display:grid;place-items:center;
  color:var(--text);font-weight:800;font-size:1rem;
  background:conic-gradient(var(--accent) 0deg, var(--accent) 210deg, rgba(147,161,184,.18) 210deg 360deg);
  border:1px solid var(--border);
  box-shadow:inset 0 0 0 7px var(--panel);
}

.status-chip{display:inline-flex;align-items:center;gap:6px;padding:5px 10px;border-radius:999px;border:1px solid var(--border);background:var(--panel-2);font-size:.78rem;font-weight:700;line-height:1;}
.status-good{color:#9be7b4;border-color:rgba(34,197,94,.45);background:rgba(34,197,94,.14);}
.status-warn{color:#fdd572;border-color:rgba(245,158,11,.45);background:rgba(245,158,11,.14);}
.status-bad{color:#ffb2b2;border-color:rgba(239,68,68,.45);background:rgba(239,68,68,.14);}
.status-neutral{color:#d6dfef;border-color:rgba(147,161,184,.35);background:rgba(147,161,184,.12);}

.dashboard-grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:12px;}
.mobile-stack > *{min-width:0;}

/* Responsive refinements */
@media (max-width:980px){
  .main .block-container{padding-top:1rem;}
  [data-testid="stMetric"]{padding:14px 14px;}
  [data-testid="stMetricValue"]{font-size:1.28rem!important;}
  .summary-hero{padding:16px 16px;border-radius:var(--radius-lg);}
  .dashboard-grid{grid-template-columns:repeat(6,minmax(0,1fr));}
}

@media (max-width:640px){
  .main .block-container{padding-top:.85rem;padding-bottom:1.4rem;}
  .stTabs [data-baseweb="tab-list"]{gap:6px;}
  .stTabs [data-baseweb="tab"]{flex:1 1 calc(50% - 6px);justify-content:center;text-align:center;padding:8px 10px;}
  [data-testid="stMetric"]{border-radius:var(--radius-md);padding:12px 12px;}
  [data-testid="stMetricLabel"]{font-size:.67rem!important;}
  [data-testid="stMetricValue"]{font-size:1.18rem!important;}
  .hero-title{font-size:1.15rem;}
  .dashboard-grid{grid-template-columns:1fr;gap:10px;}
  .mobile-stack{display:flex;flex-direction:column;gap:10px;}
}
</style>"""
