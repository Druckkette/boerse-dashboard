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
  --bg:#0b1220;
  --panel:#111827;
  --panel-2:#0f172a;
  --border:#1e293b;
  --muted:#94a3b8;
  --text:#e5eefb;
  --accent:#2563eb;
  --good:#22c55e;
  --warn:#f59e0b;
  --bad:#ef4444;
}
html, body, [class*=\"css\"] {font-family:'Inter',system-ui,sans-serif;}
.stApp{background-color:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif}
.main .block-container{padding-top:1.1rem;max-width:1220px}
h1,h2,h3{font-family:'Inter',system-ui,sans-serif!important;letter-spacing:-0.02em}
h1{font-size:1.85rem!important;font-weight:800!important;background:linear-gradient(135deg,#60a5fa,#2563eb);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
h2{font-size:1.25rem!important}
h3{font-size:1.05rem!important}
p, li, label, .stMarkdown, .stCaption {font-family:'Inter',system-ui,sans-serif!important}
code, pre{font-family:'JetBrains Mono',monospace!important}
.card-label, [data-testid=\"stMetricLabel\"], [data-testid=\"stMetricValue\"]{font-family:'Inter',system-ui,sans-serif!important}
[data-testid=\"stMetric\"]{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:14px 16px;box-shadow:0 0 0 1px rgba(255,255,255,.01) inset}
[data-testid=\"stMetricLabel\"]{color:#7c8aa0!important;font-size:.72rem!important;text-transform:uppercase;letter-spacing:.08em}
[data-testid=\"stMetricValue\"]{color:var(--text)!important;font-size:1.32rem!important;font-weight:700!important}
.stTabs [data-baseweb=\"tab-list\"]{gap:6px;background:transparent;flex-wrap:wrap}
.stTabs [data-baseweb=\"tab\"]{background:var(--panel);border:1px solid var(--border);border-radius:10px;color:var(--muted);padding:8px 14px;font-size:.86rem}
.stTabs [aria-selected=\"true\"]{background:#2563eb22;border-color:#2563eb;color:#bfdbfe}
.summary-hero,.change-card,.info-card,.workspace-card{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:16px 18px}
.summary-hero{padding:18px 20px;background:linear-gradient(135deg,rgba(37,99,235,.14),rgba(30,41,59,.35))}
.ampel-box{border-radius:12px;padding:16px 20px;display:flex;align-items:center;gap:16px}
.ampel-dot{width:48px;height:48px;border-radius:50%;flex-shrink:0}
.check-item{display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)}
.check-item:last-child{border-bottom:none}
.check-icon{width:22px;height:22px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700}
.check-ok{background:#22c55e20;border:1.5px solid #22c55e50;color:var(--good)}
.check-fail{background:#ef444420;border:1.5px solid #ef444450;color:var(--bad)}
.check-warn{background:#f59e0b20;border:1.5px solid #f59e0b50;color:var(--warn)}
.info-card,.workspace-card{margin-bottom:12px}
.card-label{font-size:.7rem;color:#7c8aa0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}
.mini-help{font-size:.76rem;color:#7c8aa0;line-height:1.45;margin-top:6px}
.hero-title{font-size:1.25rem;font-weight:800;color:var(--text);margin-bottom:4px}
.hero-subtitle{font-size:.9rem;color:var(--muted);margin-bottom:14px}
.hero-action{font-size:.95rem;font-weight:700;padding:10px 12px;border-radius:10px;margin-top:10px}
.hero-good{background:#22c55e18;color:#86efac;border:1px solid #22c55e40}
.hero-warn{background:#f59e0b18;color:#fcd34d;border:1px solid #f59e0b40}
.hero-bad{background:#ef444418;color:#fca5a5;border:1px solid #ef444440}
.change-card{padding:14px 16px}
.change-title{font-size:.72rem;color:#7c8aa0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px}
.change-value{font-size:1rem;font-weight:700;color:var(--text)}
.change-detail{font-size:.8rem;color:var(--muted);margin-top:4px;line-height:1.35}
.kpi-explainer{background:rgba(15,23,42,.85);border:1px solid var(--border);border-radius:12px;padding:10px 12px;font-size:.8rem;color:var(--muted)}
.pill-wrap{display:flex;flex-wrap:wrap;gap:8px}
.pill{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:#0f172a;border:1px solid var(--border);color:var(--text);font-size:.82rem}
.workspace-note{font-size:.82rem;color:var(--muted);line-height:1.45}
.breadth-track{height:10px;border-radius:5px;background:var(--border);position:relative;overflow:hidden;margin:8px 0}
.breadth-fill{position:absolute;left:0;top:0;bottom:0;border-radius:5px;background:linear-gradient(90deg,#22c55e,#f59e0b,#ef4444);transition:width .5s}
hr{border:none;border-top:1px solid var(--border);margin:1rem 0}
</style>"""
