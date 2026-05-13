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
  --accent:       #2563eb;
  --accent-hover: #1d4ed8;
  --accent-bg:    #eff6ff;
  --good:         #16a34a;
  --good-bg:      #f0fdf4;
  --good-border:  #bbf7d0;
  --warn:         #ca8a04;
  --warn-bg:      #fffbeb;
  --warn-border:  #fde68a;
  --bad:          #dc2626;
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

html, body, [class*="css"] { font-family: 'Inter', system-ui, sans-serif; font-size: 14px; }

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
  padding: 10px 14px;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-sm);
  display: flex;
  align-items: center;
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
  line-height: 1.15;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
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
.change-card.kpi-priority { border-left: 4px solid var(--accent); padding-left: 14px; }
.change-card.kpi-priority .change-value { font-size: 1.18rem; font-weight: 800; }
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
  font-size: 11px;
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
.kpi-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .1em; font-weight: 700; }
.kpi-value { font-size: 28px; line-height: 1.15; font-weight: 800; color: var(--text); margin-top: 7px; }
.kpi-interpretation { font-size: .85rem; line-height: 1.45; color: var(--text); margin-top: 6px; }
.kpi-copy { font-size: .79rem; line-height: 1.52; color: var(--muted); margin-top: 6px; overflow-wrap: anywhere; }

/* ── Pills ── */
.pill-wrap { display: flex; flex-wrap: wrap; gap: 7px; }
.pill {
  display: inline-flex; align-items: center; padding: 5px 11px;
  border-radius: 999px; background: var(--panel-2); border: 1px solid var(--border);
  color: var(--text); font-size: .81rem; font-weight: 500;
}
.workspace-chip {
  width: 100%; justify-content: center; min-height: 34px;
  background: linear-gradient(135deg, #eef4ff 0%, #ffffff 100%);
  border-color: var(--border-strong); font-weight: 800; letter-spacing: .03em;
}
.workspace-card [data-testid="stButton"] button[kind="secondary"],
.workspace-card [data-testid="stButton"] button {
  border-radius: 999px;
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
.check-fail-critical { width:28px; height:28px; background:#dc2626; border-color:#dc2626; color:#ffffff; font-size:18px; font-weight:900; }
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


/* Mobile-first helpers */
.ampel-lights { display:flex; gap:12px; align-items:flex-start; justify-content:center; width:100%; }
.ampel-light { flex:1 1 0; min-width:0; max-width:92px; }
.ampel-light summary { list-style:none; cursor:pointer; display:flex; flex-direction:column; align-items:center; gap:4px; outline:none; }
.ampel-light__dot { width:42px; height:42px; border-radius:50%; }

/* ── Marktampel Cockpit ─────────────────────────────────────────── */
.cockpit {
  --phase-color: #94a3b8;
  --phase-rgb: 148,163,184;
  position: relative;
  display: grid;
  grid-template-columns: minmax(190px, 240px) 1fr;
  gap: 22px;
  padding: 24px;
  margin: 0 0 14px;
  border-radius: var(--radius-xl);
  background:
    radial-gradient(circle at 100% 0%, rgba(var(--phase-rgb), .18) 0%, transparent 55%),
    linear-gradient(135deg, #0f172a 0%, #1e293b 60%, #1e293b 100%);
  color: #e2e8f0;
  box-shadow: 0 14px 40px rgba(15, 25, 50, .22), 0 2px 8px rgba(15,25,50,.10);
  overflow: hidden;
  isolation: isolate;
}
.cockpit::after {
  content: ''; position: absolute; inset: 0; pointer-events: none;
  background:
    radial-gradient(1px 1px at 30% 60%, rgba(255,255,255,.05), transparent 2px),
    radial-gradient(1px 1px at 70% 20%, rgba(255,255,255,.04), transparent 2px),
    radial-gradient(1px 1px at 80% 80%, rgba(255,255,255,.03), transparent 2px);
  z-index: 0;
}
.cockpit > * { position: relative; z-index: 1; }

.cockpit__pole {
  display: flex; flex-direction: column; align-items: center; gap: 12px;
  background: linear-gradient(180deg, rgba(15,23,42,.6) 0%, rgba(15,23,42,.85) 100%);
  border: 1px solid rgba(148, 163, 184, .12);
  border-radius: var(--radius-lg);
  padding: 18px 14px;
}
.cockpit__pole-eyebrow {
  font-size: .58rem; letter-spacing: .15em; font-weight: 800;
  color: rgba(226, 232, 240, .55); text-transform: uppercase;
}
.cockpit__lights {
  display: flex; flex-direction: column; gap: 12px; align-items: center;
  background: linear-gradient(180deg, #0a0f1a 0%, #050810 100%);
  border: 2px solid rgba(148, 163, 184, .14);
  border-radius: 999px;
  padding: 16px 14px;
  box-shadow:
    inset 0 2px 6px rgba(0,0,0,.7),
    inset 0 -1px 0 rgba(255,255,255,.04),
    0 0 0 4px rgba(148,163,184,.04);
}
.cockpit__light {
  width: 54px; height: 54px; border-radius: 50%;
  background: #131722; border: 1.5px solid rgba(148,163,184,.10);
  position: relative; transition: all .25s ease;
  opacity: .35;
}
.cockpit__light::after {
  content: ''; position: absolute; inset: 18% 22% 50% 18%;
  border-radius: 50% 50% 50% 50% / 60% 60% 40% 40%;
  background: linear-gradient(180deg, rgba(255,255,255,.18), rgba(255,255,255,0));
  pointer-events: none;
}
.cockpit__light--red.is-active   { background: radial-gradient(circle at 35% 30%, #fecaca 0%, #ef4444 45%, #991b1b 100%); border-color: #ef4444; box-shadow: 0 0 22px rgba(239,68,68,.7), 0 0 50px rgba(239,68,68,.35); opacity: 1; }
.cockpit__light--yellow.is-active{ background: radial-gradient(circle at 35% 30%, #fef3c7 0%, #f59e0b 45%, #92400e 100%); border-color: #f59e0b; box-shadow: 0 0 22px rgba(245,158,11,.7), 0 0 50px rgba(245,158,11,.35); opacity: 1; }
.cockpit__light--green.is-active { background: radial-gradient(circle at 35% 30%, #bbf7d0 0%, #22c55e 45%, #166534 100%); border-color: #22c55e; box-shadow: 0 0 22px rgba(34,197,94,.7), 0 0 50px rgba(34,197,94,.35); opacity: 1; }

.cockpit__phase-chip {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 5px 12px; border-radius: 999px;
  background: rgba(var(--phase-rgb), .15);
  border: 1px solid rgba(var(--phase-rgb), .55);
  color: var(--phase-color);
  font-size: .76rem; font-weight: 800; letter-spacing: .06em;
}
.cockpit__phase-chip::before {
  content: ''; width: 7px; height: 7px; border-radius: 50%;
  background: var(--phase-color);
  box-shadow: 0 0 8px var(--phase-color);
}

.cockpit__body { display: flex; flex-direction: column; gap: 12px; min-width: 0; }
.cockpit__topline {
  display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap;
}
.cockpit__eyebrow {
  font-size: .68rem; letter-spacing: .14em; font-weight: 800;
  color: rgba(226, 232, 240, .60); text-transform: uppercase;
}
.cockpit__freshness {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: .68rem; font-weight: 600; color: rgba(226, 232, 240, .55);
  padding: 3px 9px; border-radius: 999px;
  background: rgba(148, 163, 184, .08); border: 1px solid rgba(148, 163, 184, .14);
}
.cockpit__freshness::before {
  content: ''; width: 6px; height: 6px; border-radius: 50%; background: #22c55e;
  box-shadow: 0 0 6px #22c55e;
}

.cockpit__verdict {
  font-size: 1.85rem; font-weight: 900; line-height: 1.05;
  letter-spacing: -.01em;
  color: #ffffff;
  margin: 4px 0 0;
}
.cockpit__verdict-tag {
  display: inline-block; margin-left: 10px;
  font-size: .76rem; font-weight: 800; letter-spacing: .04em;
  padding: 4px 10px; border-radius: 999px; vertical-align: middle;
  background: rgba(var(--phase-rgb), .18);
  color: var(--phase-color);
  border: 1px solid rgba(var(--phase-rgb), .45);
}

.cockpit__reasons {
  margin: 4px 0 0; padding: 0; list-style: none;
  display: flex; flex-direction: column; gap: 7px;
}
.cockpit__reasons li {
  font-size: .88rem; line-height: 1.45;
  color: rgba(226, 232, 240, .88);
  padding-left: 18px; position: relative;
}
.cockpit__reasons li::before {
  content: ''; position: absolute; left: 4px; top: 9px;
  width: 7px; height: 7px; border-radius: 50%;
  background: var(--phase-color);
  box-shadow: 0 0 6px rgba(var(--phase-rgb), .6);
}

.cockpit__action {
  margin-top: 4px;
  display: flex; align-items: flex-start; gap: 12px;
  padding: 14px 18px;
  border-radius: var(--radius-md);
  background: linear-gradient(135deg, rgba(var(--phase-rgb), .22) 0%, rgba(var(--phase-rgb), .10) 100%);
  border: 1px solid rgba(var(--phase-rgb), .40);
  color: #ffffff;
}
.cockpit__action-arrow {
  font-size: 1.4rem; line-height: 1; color: var(--phase-color);
  flex: 0 0 auto;
}
.cockpit__action-body { min-width: 0; }
.cockpit__action-label {
  display: block; font-size: .62rem; letter-spacing: .14em;
  font-weight: 800; text-transform: uppercase;
  color: var(--phase-color); margin-bottom: 4px;
}
.cockpit__action-text {
  font-size: .92rem; font-weight: 600; line-height: 1.45;
  color: rgba(255,255,255,.95);
}

.cockpit__cycle {
  display: grid; grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px; margin-top: 6px;
}
.cockpit__cycle-stat {
  background: rgba(15, 23, 42, .55);
  border: 1px solid rgba(148, 163, 184, .12);
  border-radius: var(--radius-sm);
  padding: 9px 12px; min-width: 0;
}
.cockpit__cycle-stat .lbl {
  display: block;
  font-size: .56rem; letter-spacing: .12em; font-weight: 800;
  color: rgba(226, 232, 240, .55); text-transform: uppercase;
}
.cockpit__cycle-stat .val {
  display: block; margin-top: 4px;
  font-size: .92rem; font-weight: 800; color: #ffffff;
  font-variant-numeric: tabular-nums; overflow-wrap: anywhere;
  line-height: 1.2;
}
.cockpit__cycle-stat .val em { font-style: normal; font-weight: 700; font-size: .72rem; margin-left: 5px; }
.cockpit__cycle-stat .up { color: #4ade80; }
.cockpit__cycle-stat .down { color: #f87171; }

/* ── Signal strip ──────────────────────────────────────────────── */
.signal-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin: 0 0 14px;
}
.signal-tile {
  --signal-color: var(--accent);
  --signal-rgb: 37,99,235;
  position: relative;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 14px 16px;
  box-shadow: var(--shadow-sm);
  overflow: hidden;
  transition: transform .12s ease, box-shadow .12s ease;
}
.signal-tile::before {
  content: ''; position: absolute; left: 0; top: 0; bottom: 0;
  width: 3px; background: var(--signal-color);
}
.signal-tile::after {
  content: ''; position: absolute; right: -30px; top: -30px;
  width: 90px; height: 90px; border-radius: 50%;
  background: radial-gradient(circle, rgba(var(--signal-rgb), .12) 0%, transparent 70%);
  pointer-events: none;
}
.signal-tile:hover { transform: translateY(-1px); box-shadow: var(--shadow-card); }
.signal-tile__label {
  font-size: .62rem; letter-spacing: .12em; font-weight: 800;
  color: var(--muted); text-transform: uppercase; margin-bottom: 4px;
}
.signal-tile__value {
  display: flex; align-items: baseline; gap: 6px;
  font-size: 1.22rem; font-weight: 800; line-height: 1.15;
  color: var(--text);
  font-variant-numeric: tabular-nums;
}
.signal-tile__arrow { font-size: 1.25rem; line-height: 1; color: var(--signal-color); }
.signal-tile__quality {
  display: inline-block; margin-top: 4px;
  font-size: .72rem; font-weight: 800;
  color: var(--signal-color);
}
.signal-tile__detail {
  font-size: .76rem; color: var(--muted); margin-top: 4px; line-height: 1.4;
}
.signal-tile__sub {
  font-size: .68rem; color: var(--muted-light); margin-top: 2px; line-height: 1.35;
}

/* ── MA Ribbon (replaces 4 distance tiles) ─────────────────────── */
.ma-ribbon {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: var(--radius-md);
  padding: 14px 18px;
  margin: 0 0 14px;
  box-shadow: var(--shadow-sm);
}
.ma-ribbon__head {
  display: flex; align-items: center; justify-content: space-between;
  gap: 12px; flex-wrap: wrap; margin-bottom: 12px;
}
.ma-ribbon__title {
  font-size: .68rem; font-weight: 800; letter-spacing: .12em;
  text-transform: uppercase; color: var(--muted);
}
.ma-ribbon__order {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: .72rem; font-weight: 800; letter-spacing: .04em;
  padding: 4px 10px; border-radius: 999px;
}
.ma-ribbon__order--good { color: var(--good); background: var(--good-bg); border: 1px solid var(--good-border); }
.ma-ribbon__order--bad  { color: var(--bad);  background: var(--bad-bg);  border: 1px solid var(--bad-border); }
.ma-ribbon__bars {
  display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px;
}
.ma-ribbon__bar { display: flex; flex-direction: column; gap: 7px; min-width: 0; }
.ma-ribbon__bar-head {
  display: flex; justify-content: space-between; align-items: baseline; gap: 8px;
}
.ma-ribbon__bar-name {
  font-size: .65rem; font-weight: 800; letter-spacing: .08em;
  text-transform: uppercase; color: var(--muted);
}
.ma-ribbon__bar-val {
  font-size: 1.05rem; font-weight: 800;
  color: var(--text); font-variant-numeric: tabular-nums;
}
.ma-ribbon__bar-track {
  position: relative; height: 8px; border-radius: 4px;
  background: linear-gradient(90deg,
    rgba(220, 38, 38, .18) 0%,
    rgba(248, 250, 252, .8) 48%,
    rgba(248, 250, 252, .8) 52%,
    rgba(22, 163, 74, .18) 100%);
  border: 1px solid var(--border);
  overflow: visible;
}
.ma-ribbon__bar-track::before {
  content: ''; position: absolute; left: 50%; top: -3px; bottom: -3px;
  width: 1px; background: var(--border-strong);
}
.ma-ribbon__bar-marker {
  position: absolute; top: 50%; width: 14px; height: 14px;
  border-radius: 50%; transform: translate(-50%, -50%);
  border: 2px solid #ffffff;
  box-shadow: 0 1px 4px rgba(15,25,50,.25);
}
.ma-ribbon__bar-marker--good    { background: var(--good); }
.ma-ribbon__bar-marker--warn    { background: var(--warn); }
.ma-ribbon__bar-marker--bad     { background: var(--bad); }
.ma-ribbon__bar-marker--neutral { background: var(--muted-light); }
.ma-ribbon__bar-status {
  font-size: .7rem; font-weight: 800; line-height: 1.2;
}
.ma-ribbon__bar-status--good    { color: var(--good); }
.ma-ribbon__bar-status--warn    { color: var(--warn); }
.ma-ribbon__bar-status--bad     { color: var(--bad); }
.ma-ribbon__bar-status--neutral { color: var(--muted); }

/* Cockpit responsive */
@media (max-width: 980px) {
  .cockpit { grid-template-columns: 1fr; gap: 14px; padding: 18px; border-radius: var(--radius-lg); }
  .cockpit__pole { padding: 14px; flex-direction: row; justify-content: space-between; }
  .cockpit__lights { flex-direction: row; padding: 10px 14px; }
  .cockpit__light { width: 38px; height: 38px; }
  .cockpit__verdict { font-size: 1.45rem; }
  .signal-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .ma-ribbon__bars { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 640px) {
  .cockpit { padding: 14px; }
  .cockpit__lights { padding: 8px 12px; gap: 8px; }
  .cockpit__light { width: 32px; height: 32px; }
  .cockpit__verdict { font-size: 1.25rem; }
  .cockpit__cycle { grid-template-columns: 1fr; }
  .signal-strip { grid-template-columns: 1fr; }
  .ma-ribbon__bars { grid-template-columns: 1fr; }
}
.mobile-ma-grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:8px; margin:8px 0 12px; }
.section-divider { border-top:1px solid var(--border); margin:14px 0 8px; padding-top:8px; font-size:11px; line-height:1.2; color:var(--muted); font-weight:800; letter-spacing:.1em; text-transform:uppercase; }
.sector-grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:8px; }
.sector-heading { font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; font-weight:800; margin-bottom:6px; }
.sector-badge { display:flex; align-items:center; gap:6px; padding:5px 0; font-size:14px; font-weight:700; white-space:nowrap; }
.sector-dot { width:7px; height:7px; border-radius:50%; flex:0 0 auto; }
.vol-subline { font-size:12px; color:var(--muted); line-height:1.35; margin-top:3px; }
.portfolio-health-section { margin: 14px 0 18px; }
.portfolio-health-grid { display:grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap:10px; }
.portfolio-health-card {
  background: var(--panel);
  border: 1px solid var(--border);
  border-top: 4px solid var(--border-strong);
  border-radius: var(--radius-md);
  box-shadow: var(--shadow-card);
  padding: 13px 14px;
  min-height: 142px;
  display:flex;
  flex-direction:column;
  gap:8px;
}
.portfolio-health-card__top { display:flex; align-items:flex-start; justify-content:space-between; gap:8px; }
.portfolio-health-card__label { color:var(--muted); font-size:11px; line-height:1.2; text-transform:uppercase; letter-spacing:.09em; font-weight:800; }
.portfolio-health-card__status {
  border: 1px solid var(--border);
  border-radius:999px;
  padding:3px 8px;
  font-size:11px;
  line-height:1.1;
  font-weight:800;
  color:var(--muted);
  background:var(--panel-2);
  white-space:nowrap;
}
.portfolio-health-card__value { color:var(--text); font-size:1.45rem; line-height:1.1; font-weight:850; font-variant-numeric:tabular-nums; }
.portfolio-health-card__detail { color:var(--muted); font-size:.8rem; line-height:1.45; }
.portfolio-health-card--good { border-top-color:var(--good); background:linear-gradient(180deg, var(--good-bg) 0%, var(--panel) 54%); }
.portfolio-health-card--good .portfolio-health-card__status { color:var(--good); border-color:var(--good-border); background:var(--good-bg); }
.portfolio-health-card--warn { border-top-color:var(--warn); background:linear-gradient(180deg, var(--warn-bg) 0%, var(--panel) 54%); }
.portfolio-health-card--warn .portfolio-health-card__status { color:var(--warn); border-color:var(--warn-border); background:var(--warn-bg); }
.portfolio-health-card--bad { border-top-color:var(--bad); background:linear-gradient(180deg, var(--bad-bg) 0%, var(--panel) 54%); }
.portfolio-health-card--bad .portfolio-health-card__status { color:var(--bad); border-color:var(--bad-border); background:var(--bad-bg); }
.portfolio-health-card--neutral { border-top-color:var(--accent); background:linear-gradient(180deg, var(--accent-bg) 0%, var(--panel) 54%); }
.portfolio-health-card--neutral .portfolio-health-card__status { color:var(--accent); border-color:#bfdbfe; background:var(--accent-bg); }
[data-testid="stAlert"] { border-radius:0 !important; border-left:4px solid #dc2626 !important; background:#fef2f2 !important; padding:8px 10px !important; }
[data-testid="stAlert"] p { font-size:14px !important; line-height:1.4 !important; }
div[data-testid="stButton"]:has(button[kind="primary"]) button { background:#ffffff !important; color:#2563eb !important; border:1px solid #2563eb !important; border-radius:6px !important; box-shadow:none !important; }
div[data-testid="stButton"]:has(button[kind="primary"]) button:hover { background:#eff6ff !important; }

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
.stButton > button[kind="secondary"],
.stButton > [data-testid="baseButton-secondary"] {
  background: var(--panel-2) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
  box-shadow: none !important;
}
.stButton > button[kind="secondary"]:hover,
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
  .portfolio-health-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

@media (max-width: 640px) {
  .main .block-container { padding-top: .85rem; padding-bottom: 1.4rem; }
  .app-topbar { padding: 8px 10px; }
  .app-topbar__title { font-size: 1.05rem !important; }
  .stTabs [data-baseweb="tab-list"] { gap: 5px; }
  .stTabs [data-baseweb="tab"] { flex: 1 1 calc(50% - 6px); justify-content: center; text-align: center; padding: 8px 10px; }
  [data-testid="stNavigation"] [data-baseweb="tab"] { flex: 1 1 calc(33% - 5px); justify-content: center; text-align: center; padding: 6px 6px; font-size: .77rem; }
  [data-testid="stMetric"] { border-radius: var(--radius-md); padding: 12px 12px; }
  [data-testid="stMetricLabel"] { font-size: .66rem !important; }
  [data-testid="stMetricValue"] { font-size: 1.16rem !important; }
  .hero-title { font-size: 1.12rem; }
  .dashboard-grid { grid-template-columns: 1fr; gap: 10px; }
  .mobile-stack { display: flex; flex-direction: column; gap: 10px; }
  .portfolio-health-grid { grid-template-columns: 1fr; }
  .portfolio-health-card { min-height: 0; }
  .kpi-value { font-size: 1.25rem; }
  .kpi-copy { font-size: .82rem; }
  .ampel-light__dot { width:38px; height:38px; }
  .info-card { padding: 12px 12px; }
  .stButton > button { width: 100%; }
  [data-testid="stDataFrame"] { overflow-x: auto; }
  [data-testid="stPlotlyChart"] > div { min-height: 260px; }
}
</style>"""
