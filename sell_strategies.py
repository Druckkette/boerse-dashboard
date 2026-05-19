"""Verkaufsstrategien nach Börse ohne Bauchgefühl (S1-S23)."""
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


def sma(s: pd.Series, p: int) -> pd.Series: return s.rolling(p, min_periods=1).mean()
def ema(s: pd.Series, p: int) -> pd.Series: return s.ewm(span=p, adjust=False).mean()
def letzter_schlusskurs(d: pd.DataFrame) -> float: return float(d["close"].iloc[-1])
def tagestief(d: pd.DataFrame) -> float: return float(d["low"].iloc[-1])
def pnl_pct(pos: Position, d: pd.DataFrame) -> float: return (letzter_schlusskurs(d) / pos.einstiegspreis - 1) * 100

def drawdown_vom_peak(pos: Position, d: pd.DataFrame) -> float:
    peak = float(pos.peak or d["high"].max())
    return max(0.0, (peak - letzter_schlusskurs(d)) / peak * 100)

def atr(d: pd.DataFrame, p: int = 14) -> float:
    h, l, c = d["high"], d["low"], d["close"]
    tr = pd.concat([(h-l), (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(p, min_periods=1).mean().iloc[-1]
    return float(a / c.iloc[-1] * 100) if c.iloc[-1] else 0.0

def rs_linie(a: pd.DataFrame, b: pd.DataFrame) -> pd.Series:
    idx = a.index.intersection(b.index)
    return (a.loc[idx, "close"] / b.loc[idx, "close"]).dropna()

def vol_verhaeltnis(d: pd.DataFrame) -> float: return float(d["volume"].iloc[-1] / d["volume"].tail(50).mean()) if len(d) else 1.0

def tage_unter_ma(d: pd.DataFrame, ma: pd.Series) -> int:
    c = 0
    for px, m in zip(d["close"].iloc[::-1], ma.iloc[::-1]):
        if px < m: c += 1
        else: break
    return c

def distribution_tage(d: pd.DataFrame, n: int = 25) -> int:
    w = d.tail(n)
    vol50 = d["volume"].rolling(50, min_periods=1).mean().reindex(w.index)
    return int(((w["close"].diff() < 0) & (w["volume"] >= 1.2 * vol50)).sum())

def up_down_volume_ratio(d: pd.DataFrame, n: int = 50) -> float:
    w = d.tail(n)
    up = w.loc[w["close"] > w["open"], "volume"].sum(); dn = w.loc[w["close"] < w["open"], "volume"].sum()
    return float(up / dn) if dn else 99.0

# S1

def strategie_drei_stufen_nach_kauf(pos, d):
    s = letzter_schlusskurs(d); p = pnl_pct(pos,d); out=[]
    if pos.tief_tag_1 and s < pos.tief_tag_1: out.append(_signal("Schluss unter Tief Ausbruchstag",33,"schluss",True,pos.tief_tag_0,"Kap. 5.3/6.4","erstes Drittel"))
    if pos.tief_tag_0 and s < pos.tief_tag_0: out.append(_signal("Schluss unter Tief Vortag",33,"schluss",True,pos.einstiegspreis*0.93,"Kap. 5.3/6.4","zweites Drittel"))
    if p <= -7: out.append(_signal("7%-Notbremse",100,"intraday",True,None,"Kap. 6.1","Rest raus"))
    return out

# S2

def strategie_notbremse_verlust(pos, d, markt):
    p = pnl_pct(pos,d); s = -4 if markt=="Bärisch" else -5 if markt=="Unsicher" else -7
    if p <= s: return [_signal("Notbremse nach Verlusthöhe",100,"intraday",True,None,"Kap. 6.1",f"{s}% erreicht")]
    return [_signal("Notbremse-Marke",0,"info",False,pos.einstiegspreis*(1+s/100),"Kap. 6.1","Info")]

# S3

def strategie_gewinn_in_stufen(pos,d,markt):
    p = pnl_pct(pos,d); sold=sum(pos.realisierte_tranchen or []); out=[]
    nd,l,h = (10,10,15) if markt=="Bärisch" else (15,20,35)
    if p>=nd and sold==0: out.append(_signal("Gewinn-Nachdenkschwelle erreicht",0,"info",True,pos.einstiegspreis*(1+l/100),"Kap. 6.2","planen"))
    if p>=l and sold<50: out.append(_signal("Pflicht-Teilverkauf Gewinnzone",33 if p<h else 50,"schluss",True,pos.einstiegspreis,"Kap. 6.2","Zone erreicht"))
    return out

# S4

def strategie_21ma_bruch(pos,d,variante="gestaffelt"):
    if pnl_pct(pos,d)<=0:return []
    ma21=sma(d["close"],21); m=float(ma21.iloc[-1]); s=letzter_schlusskurs(d); t=tage_unter_ma(d,ma21); out=[]
    if variante=="aggressiv":
        if s<m and (m-s)/m*100>=2 and vol_verhaeltnis(d)>=1.2: out.append(_signal("Deutlicher 21-MA-Bruch",33,"schluss",True,float(sma(d['close'],50).iloc[-1]),"Kap. 6.2","klarer Bruch"))
    elif variante=="geduldig":
        if t>=3: out.append(_signal("21-MA seit 3 Tagen gebrochen",33,"schluss",True,float(d["low"].tail(10).min()),"Kap. 6.2","bestätigt"))
    else:
        if t==1: out.append(_signal("Erster Schluss unter 21-MA",25,"schluss",True,m,"Kap. 6.2","Stufe1"))
        if t==2 and s<float(d["close"].iloc[-2]): out.append(_signal("Zweiter Tag unter 21-MA",25,"schluss",True,m,"Kap. 6.2","Stufe2"))
        if t>=3: out.append(_signal("Dritter Tag unter 21-MA",25,"schluss",True,pos.einstiegspreis,"Kap. 6.2","Stufe3"))
    return out

# S5

def strategie_drawdown_vom_peak(pos,d):
    if pnl_pct(pos,d)<=0:return []
    dd=drawdown_vom_peak(pos,d); ma21=float(sma(d["close"],21).iloc[-1]); pk=float(pos.peak or d["high"].max()); s=letzter_schlusskurs(d)
    if 8<=dd<12:return [_signal("Drawdown 8%",25,"schluss",True,pk*0.88,"Kap. 6.2","erste Sicherung")]
    if 12<=dd<15:return [_signal("Drawdown 12-15%",33,"schluss",True,pk*0.85,"Kap. 6.2","deutliche Reduktion")]
    if dd>=15 and s<ma21:return [_signal("Drawdown >15% + Trendbruch",100,"schluss",True,None,"Kap. 6.2","voll raus")]
    if dd>=15:return [_signal("Drawdown >15%",50,"schluss",True,ma21,"Kap. 6.2","reduzieren")]
    return []

# S6

def strategie_ma_abstand(pos,d):
    if pnl_pct(pos,d)<=0:return []
    s=letzter_schlusskurs(d); out=[]
    for p,th,tr in [(10,10,25),(21,15,33),(50,25,33),(200,70,50)]:
        m=float(sma(d["close"],p).iloc[-1])
        ab=(s-m)/m*100 if m else 0
        if ab>=th: out.append(_signal(f"{th}% über {p}-MA",100 if p==200 and ab>=100 else tr,"schluss",True,m,"Kap. 6.2","Überdehnung"))
    return out

# S7

def strategie_verlusttage_haeufung(pos,d):
    if pnl_pct(pos,d)<=0:return []
    out=[]; l3=d.tail(3)
    if len(l3)==3 and (l3["close"].iloc[1] < l3["close"].iloc[0]) and (l3["close"].iloc[2] < l3["close"].iloc[1]): out.append(_signal("3 tiefere Schlusskurse",25,"schluss",True,float(d["low"].tail(5).min()),"Kap. 6.2","Rebound fehlt"))
    f=d.tail(15); up=(f["close"]>f["open"]).sum(); dn=(f["close"]<f["open"]).sum()
    if dn-up>=3: out.append(_signal("Abwärtstage überwiegen",25,"schluss",True,float(f["low"].min()),"Kap. 6.2","Persönlichkeit kippt"))
    return out

# S8

def strategie_trendlinie(pos,d,trendlinie_oben_punkte=None,trendlinie_unten_punkte=None): return []
# S9

def strategie_groesster_anstieg_volumen(pos,d):
    if pnl_pct(pos,d)<=15:return []
    ch=d["close"].pct_change()*100; t=float(ch.iloc[-1]); m=float(ch.iloc[1:].max())
    if t>=m and d["volume"].iloc[-1]>=d["volume"].max(): return [_signal("Größter Anstieg + höchstes Volumen",33,"schluss",True,float(d["low"].iloc[-1]),"Kap. 6.2","Klimax")]
    if t>=m: return [_signal("Größter Tagesanstieg",20,"schluss",True,float(d["low"].iloc[-1]),"Kap. 6.2","Vorwarnung")]
    return []
# S10

def strategie_split_anstieg(pos,d,split_datum=None):
    if split_datum is None:return []
    ts=pd.Timestamp(split_datum)
    if ts not in d.index:return []
    days=(d.index[-1]-ts).days
    if days>14:return []
    k=float(d.loc[ts,"close"]); s=letzter_schlusskurs(d); a=(s-k)/k*100
    if a>=25:return [_signal(f"Anstieg nach Split: {a:.1f}%",50 if a>=50 else 33,"schluss",True,k,"Kap. 6.2","Split-Lauf")]
    return []
# S11

def strategie_erschoepfungsluecke(pos,d):
    if pnl_pct(pos,d)<=15 or len(d)<2:return []
    h=d.iloc[-1]; g=d.iloc[-2]; gap=(h["open"]-g["close"])/g["close"]*100; vr=vol_verhaeltnis(d); w=((h["close"]-float(pos.pivot or h["close"]))/float(pos.pivot or h["close"]))*100 if pos.pivot else 0
    if gap>=3 and vr>=1.5 and w>=30: return [_signal("Erschöpfungslücke",33,"schluss",True,float(h["low"]),"Kap. 6.2","letzte Kaufwelle")]
    return []
# S12

def strategie_downside_reversal(pos,d):
    if pnl_pct(pos,d)<=0:return []
    h=d.iloc[-1]; span=h["high"]-h["low"]; vol=vol_verhaeltnis(d); newh=h["high"]>=d["high"].tail(30).iloc[:-1].max()
    low3=h["close"] <= h["low"] + span/3 if span>0 else False
    if newh and low3 and vol>=1.2: return [_signal("Downside Reversal",33,"schluss",True,float(h["high"]),"Kap. 6.2","Umkehr am Hoch")]
    if low3 and vol>=1.2: return [_signal("Weite Umkehrkerze",20,"schluss",True,float(h["high"]),"Kap. 6.2","Warnung")]
    return []
# S13

def strategie_stau_tage(pos,d):
    if pnl_pct(pos,d)<=0:return []
    f=d.tail(10); vavg=d["volume"].tail(50).mean(); st=[]
    for _,r in f.iterrows():
        ch=(r["close"]-r["open"])/r["open"]*100
        if abs(ch)<1 and r["volume"]>=1.3*vavg: st.append(r)
    if len(st)>=2:
        lows=[float(x["low"]) for x in st]
        return [_signal(f"{len(st)} Stau-Tage",33 if drawdown_vom_peak(pos,d)<5 else 20,"schluss",True,min(lows),"Kap. 6.2","verdeckte Distribution")]
    return []
# S14

def strategie_rueckkehr_pivot(pos,d):
    s=letzter_schlusskurs(d); out=[]
    if pos.tief_tag_1 and s<pos.tief_tag_1: out.append(_signal("Schluss unter Tief Ausbruchstag",33,"schluss",True,pos.tief_tag_0,"Kap. 6.3","erste Linie"))
    if pos.tief_tag_0 and s<pos.tief_tag_0: out.append(_signal("Schluss unter Tief Vortag",33,"schluss",True,pos.einstiegspreis*0.93,"Kap. 6.3","zweite Linie"))
    if pos.pivot and (d["close"].tail(10)<pos.pivot).all(): out.append(_signal("10 Tage unter Pivot",50,"schluss",True,pos.einstiegspreis*0.93,"Kap. 6.3","Rückkehr misslingt"))
    return out
# S15

def strategie_ma_bruch_defensiv(pos,d,w):
    out=[]; s=letzter_schlusskurs(d); ma50=float(sma(d["close"],50).iloc[-1]); ma200=float(sma(d["close"],200).iloc[-1]); atrp=atr(d,14)
    if s<ma50 and (ma50-s)/ma50*100>=max(2,atrp) and vol_verhaeltnis(d)>=1.3: out.append(_signal("Klarer 50-MA-Bruch",50,"schluss",True,ma200,"Kap. 6.3","deutlich darunter"))
    if len(w)>=10 and (w["close"].tail(8)<sma(w["close"],10).tail(8)).all(): out.append(_signal("8 Wochen unter 10W",100,"schluss",True,None,"Kap. 6.3","voll raus"))
    if s<ma200: out.append(_signal("200-MA-Bruch",100 if vol_verhaeltnis(d)>=1.5 else 75,"schluss",True,None,"Kap. 6.3","langfristiger Bruch"))
    return out
# S16

def strategie_drei_verlustwochen(pos,w):
    if len(w)<3:return []
    l=w.tail(3)
    lower=(l["close"].iloc[1]<l["close"].iloc[0]) and (l["close"].iloc[2]<l["close"].iloc[1])
    vol=(l["volume"].iloc[1]>l["volume"].iloc[0]) and (l["volume"].iloc[2]>l["volume"].iloc[1])
    red=all(l["close"]<l["open"])
    if lower and vol and red:return [_signal("3 Verlustwochen + steigendes Volumen",100,"schluss",True,None,"Kap. 6.3","Verteilung")]
    if lower and vol:return [_signal("Vorbereitung Drei-Wochen-Regel",33,"schluss",True,None,"Kap. 6.3","Vorwarnstufe")]
    return []
# S17

def strategie_groesster_einbruch(pos,d,w):
    if pnl_pct(pos,d)<=10:return []
    out=[]; loss=((d["close"].shift(1)-d["close"])/d["close"].shift(1)*100).dropna(); today=float(loss.iloc[-1]); prev=float(loss.iloc[:-1].max()) if len(loss)>1 else 0
    if today>=prev and today>3: out.append(_signal("Größter Tagesverlust",50 if vol_verhaeltnis(d)>=1.5 else 33,"schluss",True,tagestief(d),"Kap. 6.3","Einbruch"))
    wl=((w["close"].shift(1)-w["close"])/w["close"].shift(1)*100).dropna()
    if len(wl)>1 and wl.iloc[-1]>=wl.iloc[:-1].max(): out.append(_signal("Größte Verlustwoche",66,"schluss",True,None,"Kap. 6.3","Rally-Ende"))
    return out
# S18

def strategie_rs_linie(pos,d,spy,w,wspy):
    p=pnl_pct(pos,d)
    if p<20: rs=rs_linie(d,spy); sp,lp=21,50; z="tag"
    elif p<80: rs=rs_linie(w,wspy); sp,lp=10,25; z="woche"
    else:
        m=d.resample("M").last().dropna(); ms=spy.resample("M").last().dropna(); rs=rs_linie(m,ms); sp,lp=12,24; z="monat"
    if len(rs)<max(sp,lp)+2:return []
    rsf=sma(rs,sp); rsl=sma(rs,lp); out=[]
    if rs.iloc[-1]<rsf.iloc[-1] and rs.iloc[-2]>=rsf.iloc[-2]: out.append(_signal(f"RS bricht {sp}-MA ({z})",20,"schluss",True,None,"Kap. 6.4","Stufe1"))
    under=(rs<rsf).iloc[::-1].cumprod().sum()
    if under>=3: out.append(_signal(f"RS 3 {z} unter {sp}-MA",30,"schluss",True,float(rsl.iloc[-1]),"Kap. 6.4","Stufe2"))
    if rs.iloc[-1]<rsl.iloc[-1]: out.append(_signal(f"RS bricht {lp}-MA ({z})",50,"schluss",True,None,"Kap. 6.4","Stufe3"))
    return out
# S19

def strategie_ma_basierte_sequenz(pos,d):
    p=pnl_pct(pos,d); s=letzter_schlusskurs(d); sold=sum(pos.realisierte_tranchen or []); ma10=float(sma(d["close"],10).iloc[-1]); ma21=float(sma(d["close"],21).iloc[-1]); ma50=float(sma(d["close"],50).iloc[-1]); out=[]
    if 20<=p<=25 and sold<33: out.append(_signal("Gewinnzone 20-25%",33,"schluss",True,ma10,"Kap. 6.4","Punkt1"))
    if ma10 and (s-ma10)/ma10*100>=10 and sold<50: out.append(_signal("10% über 10-MA",20,"schluss",True,ma10,"Kap. 6.4","Punkt2"))
    if s<ma10 and p>5: out.append(_signal("Schluss unter 10-MA",20,"schluss",True,ma21,"Kap. 6.4","Punkt3"))
    if s<ma21 and p>5: out.append(_signal("Schluss unter 21-MA",25,"schluss",True,ma50,"Kap. 6.4","Punkt4"))
    if s<ma50 and (ma50-s)/ma50*100>=2: out.append(_signal("Klarer 50-MA-Bruch",100,"schluss",True,None,"Kap. 6.4","Punkt5"))
    return out
# S20

def strategie_einfach_halbe_position(pos,d):
    p=pnl_pct(pos,d); sold=sum(pos.realisierte_tranchen or []); first=sold>=50; out=[]
    if p>=20 and not first: out.append(_signal("Erste Hälfte bei 20%+",50,"schluss",True,pos.einstiegspreis,"Kap. 6.4","Hälfte sichern"))
    if first and p>=20: out.append(_signal("Erneut 20%",50,"schluss",True,pos.einstiegspreis,"Kap. 6.4","weitere Tranche"))
    if first and -1<=p<=1: out.append(_signal("Break-Even-Stopp",100,"intraday",True,None,"Kap. 6.4","Rest raus"))
    return out
# S21

def strategie_misslungener_ausbruch_5stufen(pos,d):
    out=[]; h=d.iloc[-1]; p=pnl_pct(pos,d)
    if pos.tief_tag_1 and h["low"]<pos.tief_tag_1: out.append(_signal("Intraday unter Tief Tag 1",20,"intraday",True,pos.tief_tag_1,"Kap. 6.4","Stufe1a"))
    if pos.tief_tag_1 and h["close"]<pos.tief_tag_1: out.append(_signal("Schluss unter Tief Tag 1",20,"schluss",True,pos.tief_tag_0,"Kap. 6.4","Stufe1b"))
    if pos.tief_tag_0 and h["low"]<pos.tief_tag_0: out.append(_signal("Intraday unter Tief Tag 0",20,"intraday",True,pos.tief_tag_0,"Kap. 6.4","Stufe2a"))
    if pos.tief_tag_0 and h["close"]<pos.tief_tag_0: out.append(_signal("Schluss unter Tief Tag 0",20,"schluss",True,pos.einstiegspreis*0.93,"Kap. 6.4","Stufe2b"))
    if p<=-7: out.append(_signal("7%-Notbremse",100,"intraday",True,None,"Kap. 6.4","Stufe3"))
    return out
# S22

def strategie_einfache_verluststufen(pos,d):
    p=pnl_pct(pos,d); out=[]
    if -5<p<=-3: out.append(_signal("Verlust ≥ 3%",33,"intraday",True,pos.einstiegspreis*0.95,"Kap. 6.4","erste Tranche"))
    if -7<p<=-5: out.append(_signal("Verlust ≥ 5%",33,"intraday",True,pos.einstiegspreis*0.93,"Kap. 6.4","zweite Tranche"))
    if p<=-7: out.append(_signal("Verlust ≥ 7%",100,"intraday",True,None,"Kap. 6.4","Rest"))
    return out
# S23

def strategie_atr_basiert(pos,d,ziel_atr_multiplikator=3):
    p=pnl_pct(pos,d); s=letzter_schlusskurs(d); a=atr(d,14) or 1e-9; out=[]; gain=p/a
    if gain>=ziel_atr_multiplikator: out.append(_signal(f"{gain:.1f} ATR Gewinn",33,"schluss",True,None,"Kap. 6.4","Ziel erreicht"))
    if s<=pos.einstiegspreis*(1-1.5*a/100): out.append(_signal("Stopp bei -1.5 ATR",100,"intraday",True,None,"Kap. 6.4","ATR-Stopp"))
    e=float(ema(d["close"],21).iloc[-1]); over=((s-e)/e*100)/a if e else 0
    if over>=3: out.append(_signal(f"{over:.1f} ATR über 21-EMA",50 if over>=4 else 33,"schluss",True,e,"Kap. 6.4","überdehnt"))
    return out


def berechne_watch_signale(pos,d):
    w=[]; p=pnl_pct(pos,d); ma21=sma(d["close"],21); t=tage_unter_ma(d,ma21); dd=drawdown_vom_peak(pos,d)
    if p>0 and t in [1,2]: w.append({"name": f"{t} Tage unter 21-MA", "buch_verweis": "Kap. 6.2"})
    if p>0 and 5<=dd<8: w.append({"name": f"Drawdown {dd:.1f}%", "buch_verweis": "Kap. 6.2"})
    if up_down_volume_ratio(d,50)<1.0: w.append({"name": "Up/Down-Volume < 1.0", "buch_verweis": "Kap. 5.3"})
    if distribution_tage(d,25)>=4: w.append({"name": ">=4 Distribution-Tage", "buch_verweis": "Kap. 5.3"})
    return w


def verkaufs_empfehlung_gesamt(pos,d,w,spy,wspy,markt,industrie,aktive_strategien,strategie_optionen=None):
    opt=strategie_optionen or {}
    reg={
        "notbremse_verlust": lambda: strategie_notbremse_verlust(pos,d,markt),
        "drei_stufen_nach_kauf": lambda: strategie_drei_stufen_nach_kauf(pos,d),
        "gewinn_in_stufen": lambda: strategie_gewinn_in_stufen(pos,d,markt),
        "ma21_bruch": lambda: strategie_21ma_bruch(pos,d,opt.get("ma21_variante","gestaffelt")),
        "drawdown_vom_peak": lambda: strategie_drawdown_vom_peak(pos,d),
        "ma_abstand": lambda: strategie_ma_abstand(pos,d),
        "verlusttage_haeufung": lambda: strategie_verlusttage_haeufung(pos,d),
        "trendlinie": lambda: strategie_trendlinie(pos,d,opt.get("trendlinie_oben_punkte"),opt.get("trendlinie_unten_punkte")),
        "groesster_anstieg_volumen": lambda: strategie_groesster_anstieg_volumen(pos,d),
        "split_anstieg": lambda: strategie_split_anstieg(pos,d,opt.get("split_datum")),
        "erschoepfungsluecke": lambda: strategie_erschoepfungsluecke(pos,d),
        "downside_reversal": lambda: strategie_downside_reversal(pos,d),
        "stau_tage": lambda: strategie_stau_tage(pos,d),
        "rueckkehr_pivot": lambda: strategie_rueckkehr_pivot(pos,d),
        "ma_bruch_defensiv": lambda: strategie_ma_bruch_defensiv(pos,d,w),
        "drei_verlustwochen": lambda: strategie_drei_verlustwochen(pos,w),
        "groesster_einbruch": lambda: strategie_groesster_einbruch(pos,d,w),
        "rs_linie": lambda: strategie_rs_linie(pos,d,spy,w,wspy),
        "ma_basierte_sequenz": lambda: strategie_ma_basierte_sequenz(pos,d),
        "einfach_halbe_position": lambda: strategie_einfach_halbe_position(pos,d),
        "misslungener_ausbruch_5stufen": lambda: strategie_misslungener_ausbruch_5stufen(pos,d),
        "einfache_verluststufen": lambda: strategie_einfache_verluststufen(pos,d),
        "atr_basiert": lambda: strategie_atr_basiert(pos,d,opt.get("ziel_atr_multiplikator",3)),
    }
    alls=[]
    for k in aktive_strategien:
        if k in reg: alls.extend(reg[k]())
    killer=[s for s in alls if s["aktuell_aktiv"] and s["tranche_pct"]==100]
    if killer: total=100; reason=killer[0]["name"]
    else:
        act=[s for s in alls if s["aktuell_aktiv"] and s["tranche_pct"]>0]; sm=sum(s["tranche_pct"] for s in act)
        if len(act)>=4 and sm<75: sm=75
        lv=[0,25,33,50,66,75,100]; total=max(v for v in lv if v<=min(sm,100))
        if markt=="Bärisch" and 0<total<100: total=next((v for v in lv if v>total),100)
        reason=", ".join(x["name"] for x in act[:3]) if act else "keine aktiven Signale"
    sold=sum(pos.realisierte_tranchen or [])
    return {"gesamt_tranche": total, "bereits_realisiert": sold, "jetzt_zu_verkaufen": max(0,total-sold), "haupt_grund": reason, "alle_signale": alls}
