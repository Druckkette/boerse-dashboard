"""Modulare Verkaufsstrategien nach *Börse ohne Bauchgefühl*."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import pandas as pd


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
def pnl_pct(position: Position, daten: pd.DataFrame) -> float: return (letzter_schlusskurs(daten) / position.einstiegspreis - 1) * 100

def drawdown_vom_peak(position: Position, daten: pd.DataFrame) -> float:
    peak = float(position.peak or daten["high"].max())
    return max(0.0, (peak - letzter_schlusskurs(daten)) / peak * 100)

def tage_unter_ma(daten: pd.DataFrame, ma: pd.Series) -> int:
    c = 0
    for price, m in zip(daten["close"].iloc[::-1], ma.iloc[::-1]):
        if pd.isna(m) or price >= m: break
        c += 1
    return c

def distribution_tage(daten: pd.DataFrame, n: int = 25) -> int:
    w = daten.tail(n).copy(); vma = w["volume"].rolling(50, min_periods=1).mean()
    return int(((w["close"].diff() < 0) & (w["volume"] >= 1.2 * vma)).sum())

def up_down_volume_ratio(daten: pd.DataFrame, n: int = 50) -> float:
    w = daten.tail(n)
    up = w.loc[w["close"] > w["open"], "volume"].sum(); dn = w.loc[w["close"] < w["open"], "volume"].sum()
    return float(up / dn) if dn else 9.99

def vol_verhaeltnis(daten: pd.DataFrame) -> float:
    return float(daten["volume"].iloc[-1] / daten["volume"].tail(50).mean()) if len(daten) >= 50 else 1.0

def atr(daten: pd.DataFrame, periode: int = 14) -> float:
    h, l, c = daten["high"], daten["low"], daten["close"]
    tr = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(periode, min_periods=periode).mean().iloc[-1]
    return float(a / c.iloc[-1] * 100) if c.iloc[-1] else 0.0

def rs_linie(daten_aktie: pd.DataFrame, daten_spy: pd.DataFrame) -> pd.Series:
    idx = daten_aktie.index.intersection(daten_spy.index)
    return (daten_aktie.loc[idx, "close"] / daten_spy.loc[idx, "close"]).dropna()

# S1..S23

def strategie_drei_stufen_nach_kauf(position, daten):
    out=[]; s=letzter_schlusskurs(daten); pnl=pnl_pct(position,daten)
    if position.tief_tag_1 and s < position.tief_tag_1: out.append(_signal("Schluss unter Tief Ausbruchstag",33,"schluss",True,position.tief_tag_0,"Kap. 5.3/6.4","erstes Drittel"))
    if position.tief_tag_0 and s < position.tief_tag_0: out.append(_signal("Schluss unter Tief Vortag",33,"schluss",True,position.einstiegspreis*0.93,"Kap. 5.3/6.4","zweites Drittel"))
    if pnl <= -7: out.append(_signal("7%-Notbremse",100,"intraday",True,None,"Kap. 6.1","Rest raus"))
    return out

def strategie_notbremse_verlust(position,daten,markt):
    p=pnl_pct(position,daten); s=-4 if markt=="Bärisch" else -5 if markt=="Unsicher" else -7
    return [_signal("Notbremse nach Verlusthöhe",100,"intraday",True,None,"Kap. 6.1",f"{s}%") ] if p<=s else [_signal("Notbremse-Marke",0,"info",False,position.einstiegspreis*(1+s/100),"Kap. 6.1","Info")]

def strategie_gewinn_in_stufen(position,daten,markt):
    p=pnl_pct(position,daten); sold=sum(position.realisierte_tranchen or []); out=[]
    nd,l,h=(10,10,15) if markt=="Bärisch" else (15,20,35)
    if p>=nd and sold==0: out.append(_signal("Gewinn-Nachdenkschwelle",0,"info",True,position.einstiegspreis*(1+l/100),"Kap. 6.2","planen"))
    if p>=l and sold<50: out.append(_signal("Pflicht-Teilverkauf Gewinnzone",33 if p<h else 50,"schluss",True,position.einstiegspreis,"Kap. 6.2","Teilverkauf"))
    return out

def strategie_21ma_bruch(position,daten,variante="gestaffelt"):
    if pnl_pct(position,daten)<=0:return []
    ma21=sma(daten["close"],21); m=float(ma21.iloc[-1]); s=letzter_schlusskurs(daten); t=tage_unter_ma(daten,ma21); out=[]
    if variante=="aggressiv" and s<m and (m-s)/m*100>=2 and vol_verhaeltnis(daten)>=1.2: out.append(_signal("21-MA Bruch aggressiv",33,"schluss",True,float(sma(daten['close'],50).iloc[-1]),"Kap. 6.2","klarer Bruch"))
    elif variante=="geduldig" and t>=3: out.append(_signal("21-MA seit 3 Tagen gebrochen",33,"schluss",True,float(daten["low"].tail(10).min()),"Kap. 6.2","bestätigt"))
    elif variante=="gestaffelt":
        if t==1: out.append(_signal("Erster Schluss unter 21-MA",25,"schluss",True,m,"Kap. 6.2","Stufe1"))
        if t==2 and s<float(daten["close"].iloc[-2]): out.append(_signal("Zweiter Tag unter 21-MA",25,"schluss",True,m,"Kap. 6.2","Stufe2"))
        if t>=3: out.append(_signal("Dritter Tag unter 21-MA",25,"schluss",True,position.einstiegspreis,"Kap. 6.2","Stufe3"))
    return out

def strategie_drawdown_vom_peak(position,daten):
    if pnl_pct(position,daten)<=0:return []
    dd=drawdown_vom_peak(position,daten); s=letzter_schlusskurs(daten); ma21=float(sma(daten["close"],21).iloc[-1]); pk=float(position.peak or daten["high"].max())
    if 8<=dd<12:return [_signal("Drawdown 8%",25,"schluss",True,pk*0.88,"Kap. 6.2","erste Sicherung")]
    if 12<=dd<15:return [_signal("Drawdown 12-15%",33,"schluss",True,pk*0.85,"Kap. 6.2","deutlich reduzieren")]
    if dd>=15 and s<ma21:return [_signal("Drawdown >15% + Trendbruch",100,"schluss",True,None,"Kap. 6.2","komplett")]
    if dd>=15:return [_signal("Drawdown >15%",50,"schluss",True,ma21,"Kap. 6.2","halbieren")]
    return []

def strategie_ma_abstand(position,daten):
    if pnl_pct(position,daten)<=0:return []
    s=letzter_schlusskurs(daten); out=[]
    for p,th,tr in [(10,10,25),(21,15,33),(50,25,33),(200,70,50)]:
        m=float(sma(daten["close"],p).iloc[-1]) if len(daten)>=p else 0
        if m and (s-m)/m*100>=th: out.append(_signal(f"{th}% über {p}-MA",100 if p==200 and (s-m)/m*100>=100 else tr,"schluss",True,m,"Kap. 6.2","Überdehnung"))
    return out

def strategie_verlusttage_haeufung(position,daten):
    if pnl_pct(position,daten)<=0 or len(daten)<15:return []
    out=[]; l3=daten.tail(3)
    if all(l3["close"].iloc[i] < l3["close"].iloc[i-1] for i in [1,2]): out.append(_signal("3 tiefere Schlusskurse",25,"schluss",True,float(daten["low"].tail(5).min()),"Kap. 6.2","Rebound fehlt"))
    fen=daten.tail(15); up=(fen["close"]>fen["open"]).sum(); dn=(fen["close"]<fen["open"]).sum()
    if dn-up>=3: out.append(_signal("Abwärtstage überwiegen",25,"schluss",True,float(fen["low"].min()),"Kap. 6.2","Persönlichkeit kippt"))
    return out

def strategie_trendlinie(position,daten,trendlinie_oben_punkte=None,trendlinie_unten_punkte=None): return []
def strategie_groesster_anstieg_volumen(position,daten):
    if pnl_pct(position,daten)<=15:return []
    ch=daten["close"].pct_change()*100; t=float(ch.iloc[-1]); mx=float(ch.iloc[1:].max())
    if t>=mx and daten["volume"].iloc[-1]>=daten["volume"].max(): return [_signal("Größter Anstieg + höchstes Volumen",33,"schluss",True,float(daten["low"].iloc[-1]),"Kap. 6.2","Klimax")]
    if t>=mx:return [_signal("Größter Tagesanstieg",20,"schluss",True,float(daten["low"].iloc[-1]),"Kap. 6.2","Vorwarnung")]
    return []

def strategie_split_anstieg(position,daten,split_datum=None): return []
def strategie_erschoepfungsluecke(position,daten): return []
def strategie_downside_reversal(position,daten): return []
def strategie_stau_tage(position,daten): return []
def strategie_rueckkehr_pivot(position,daten):
    out=[]; s=letzter_schlusskurs(daten)
    if position.tief_tag_1 and s<position.tief_tag_1: out.append(_signal("Schluss unter Tief Ausbruchstag",33,"schluss",True,position.tief_tag_0,"Kap. 6.3","erste Linie"))
    if position.tief_tag_0 and s<position.tief_tag_0: out.append(_signal("Schluss unter Tief Vortag",33,"schluss",True,position.einstiegspreis*0.93,"Kap. 6.3","zweite Linie"))
    if position.pivot and (daten["close"].tail(10)<position.pivot).all(): out.append(_signal("10 Tage unter Pivot",50,"schluss",True,position.einstiegspreis*0.93,"Kap. 6.3","Rückkehr misslingt"))
    return out

def strategie_ma_bruch_defensiv(position,daten,wochen_daten):
    out=[]; s=letzter_schlusskurs(daten); ma50=float(sma(daten["close"],50).iloc[-1]) if len(daten)>=50 else 0; ma200=float(sma(daten["close"],200).iloc[-1]) if len(daten)>=200 else 0
    if ma50 and s<ma50*0.98 and vol_verhaeltnis(daten)>=1.3: out.append(_signal("Klarer 50-MA-Bruch",50,"schluss",True,ma200 or None,"Kap. 6.3","defensiv"))
    if ma200 and s<ma200: out.append(_signal("200-MA-Bruch",100 if vol_verhaeltnis(daten)>=1.5 else 75,"schluss",True,None,"Kap. 6.3","langfristiger Bruch"))
    if len(wochen_daten)>=10 and (wochen_daten["close"].tail(8) < sma(wochen_daten["close"],10).tail(8)).all(): out.append(_signal("8 Wochen unter 10W",100,"schluss",True,None,"Kap. 6.3","voll raus"))
    return out

def strategie_drei_verlustwochen(position,wochen_daten): return []
def strategie_groesster_einbruch(position,daten,wochen_daten): return []
def strategie_rs_linie(position,daten,daten_spy,wochen_daten,wochen_daten_spy): return []
def strategie_ma_basierte_sequenz(position,daten): return []
def strategie_einfach_halbe_position(position,daten): return []
def strategie_misslungener_ausbruch_5stufen(position,daten): return []
def strategie_einfache_verluststufen(position,daten): return []
def strategie_atr_basiert(position,daten,ziel_atr_multiplikator=3): return []

def berechne_watch_signale(position,daten):
    out=[]; p=pnl_pct(position,daten); ma21=sma(daten["close"],21); t=tage_unter_ma(daten,ma21); dd=drawdown_vom_peak(position,daten)
    if p>0 and t in (1,2): out.append({"name":f"{t} Tage unter 21-MA","buch_verweis":"Kap. 6.2"})
    if p>0 and 5<=dd<8: out.append({"name":f"Drawdown {dd:.1f}%","buch_verweis":"Kap. 6.2"})
    if up_down_volume_ratio(daten,50)<1.0: out.append({"name":"Up/Down-Volume < 1.0","buch_verweis":"Kap. 5.3"})
    if distribution_tage(daten,25)>=4: out.append({"name":">=4 Distribution-Tage","buch_verweis":"Kap. 5.3"})
    return out


def verkaufs_empfehlung_gesamt(position,daten,wochen_daten,daten_spy,wochen_daten_spy,markt,industrie,aktive_strategien,strategie_optionen=None):
    opt = strategie_optionen or {}
    r = {
        "notbremse_verlust": lambda: strategie_notbremse_verlust(position,daten,markt),
        "drei_stufen_nach_kauf": lambda: strategie_drei_stufen_nach_kauf(position,daten),
        "gewinn_in_stufen": lambda: strategie_gewinn_in_stufen(position,daten,markt),
        "ma21_bruch": lambda: strategie_21ma_bruch(position,daten,opt.get("ma21_variante","gestaffelt")),
        "drawdown_vom_peak": lambda: strategie_drawdown_vom_peak(position,daten),
        "ma_abstand": lambda: strategie_ma_abstand(position,daten),
        "verlusttage_haeufung": lambda: strategie_verlusttage_haeufung(position,daten),
        "rueckkehr_pivot": lambda: strategie_rueckkehr_pivot(position,daten),
        "ma_bruch_defensiv": lambda: strategie_ma_bruch_defensiv(position,daten,wochen_daten),
        "groesster_anstieg_volumen": lambda: strategie_groesster_anstieg_volumen(position,daten),
    }
    sig=[]
    for k in aktive_strategien:
        if k in r: sig.extend(r[k]())
    killer=[s for s in sig if s["aktuell_aktiv"] and s["tranche_pct"]==100]
    if killer: total=100; reason=killer[0]["name"]
    else:
        active=[s for s in sig if s["aktuell_aktiv"] and s["tranche_pct"]>0]; ssum=sum(s["tranche_pct"] for s in active)
        if len(active)>=4 and ssum<75: ssum=75
        lv=[0,25,33,50,66,75,100]; total=max(v for v in lv if v<=min(ssum,100))
        if markt=="Bärisch" and 0<total<100: total=next((v for v in lv if v>total),100)
        reason=", ".join(x["name"] for x in active[:3]) if active else "keine aktiven Signale"
    sold=sum(position.realisierte_tranchen or [])
    return {"gesamt_tranche":total,"bereits_realisiert":sold,"jetzt_zu_verkaufen":max(0,total-sold),"haupt_grund":reason,"alle_signale":sig}
