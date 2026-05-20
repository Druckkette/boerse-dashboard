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
def tagestief(daten: pd.DataFrame) -> float: return float(daten["low"].iloc[-1])
def pnl_pct(position: Position, daten: pd.DataFrame) -> float: return (letzter_schlusskurs(daten)/position.einstiegspreis - 1) * 100
def drawdown_vom_peak(position: Position, daten: pd.DataFrame) -> float:
    peak = float(position.peak or daten["high"].max())
    return max(0.0, (peak - letzter_schlusskurs(daten)) / peak * 100) if peak else 0.0

def vol_verhaeltnis(daten: pd.DataFrame) -> float:
    if len(daten) < 50: return 1.0
    return float(daten["volume"].iloc[-1] / daten["volume"].tail(50).mean())

def tage_unter_ma(daten: pd.DataFrame, ma: pd.Series) -> int:
    c = 0
    for x, m in zip(daten["close"].iloc[::-1], ma.iloc[::-1]):
        if pd.isna(m) or x >= m: break
        c += 1
    return c

def atr(daten: pd.DataFrame, periode: int = 14) -> float:
    h,l,c = daten["high"], daten["low"], daten["close"]
    tr = pd.concat([(h-l), (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    a = tr.rolling(periode, min_periods=periode).mean().iloc[-1]
    return float(a / c.iloc[-1] * 100) if c.iloc[-1] else 0.0

def distribution_tage(daten: pd.DataFrame, n: int = 25) -> int:
    d = daten.tail(n).copy(); v50 = daten["volume"].rolling(50, min_periods=10).mean()
    return int(((d["close"].diff() < 0) & (d["volume"] >= 1.2*v50.loc[d.index])).sum())

def up_down_volume_ratio(daten: pd.DataFrame, n: int = 50) -> float:
    d = daten.tail(n)
    up = float(d.loc[d["close"] > d["open"], "volume"].sum()); dn = float(d.loc[d["close"] < d["open"], "volume"].sum())
    return up / dn if dn else 999.0

def _none_if_nan(x): return None if pd.isna(x) else float(x)
def _linear_marke(points, idx):
    if not points or len(points) < 2: return None
    y0, y1 = float(points[0][1]), float(points[-1][1])
    n = max(1, len(points)-1)
    return y0 + (y1-y0) * (n/n)

# Strategien 1-23

def strategie_drei_stufen_nach_kauf(position: Position, daten: pd.DataFrame):
    s, pnl, out = letzter_schlusskurs(daten), pnl_pct(position, daten), []
    if position.tief_tag_1 and s < position.tief_tag_1: out.append(_signal("Schluss unter Tief Ausbruchstag",33,"schluss",True,position.tief_tag_0,"Kap. 5.3 / 6.4","Ausbruch hält nicht"))
    if position.tief_tag_0 and s < position.tief_tag_0: out.append(_signal("Schluss unter Tief Vortag",33,"schluss",True,position.einstiegspreis*0.93,"Kap. 5.3 / 6.4","Ausbruch gescheitert"))
    if pnl <= -7: out.append(_signal("7%-Notbremse",100,"intraday",True,None,"Kap. 5.3 / 6.1 / 6.4","Maximalverlust"))
    return out

def strategie_notbremse_verlust(position: Position, daten: pd.DataFrame, markt: str):
    pnl = pnl_pct(position, daten); s = -4 if markt=="Bärisch" else -5 if markt=="Unsicher" else -7
    return [_signal("Notbremse nach Verlusthöhe",100,"intraday",True,None,"Kap. 6.1",f"Verlustgrenze {s}% erreicht")] if pnl <= s else [_signal("Notbremse-Marke",0,"info",False,position.einstiegspreis*(1+s/100),"Kap. 6.1","Info-Marke")]

def strategie_gewinn_in_stufen(position: Position, daten: pd.DataFrame, markt: str):
    pnl = pnl_pct(position, daten); r = sum(position.realisierte_tranchen or []); out=[]
    nd, lo, hi = (10,10,15) if markt=="Bärisch" else (15,20,35)
    if pnl >= nd and r==0: out.append(_signal("Gewinn-Nachdenkschwelle erreicht",0,"info",True,position.einstiegspreis*(1+lo/100),"Kap. 6.2","Teilverkauf planen"))
    if pnl >= lo and r < 50: out.append(_signal("Pflicht-Teilverkauf Gewinnzone",33 if pnl<hi else 50,"schluss",True,position.einstiegspreis,"Kap. 6.2","Gewinnzone erreicht"))
    return out

def strategie_21ma_bruch(position: Position, daten: pd.DataFrame, variante: str = "gestaffelt"):
    if pnl_pct(position,daten) <= 0: return []
    ma21=sma(daten["close"],21); m=float(ma21.iloc[-1]); s=letzter_schlusskurs(daten); t=tage_unter_ma(daten,ma21); out=[]
    if variante=="aggressiv":
        br=(m-s)/m*100 if m else 0
        if s<m and br>=2 and vol_verhaeltnis(daten)>=1.2: out.append(_signal("Deutlicher 21-MA-Bruch mit Volumen",33,"schluss",True,_none_if_nan(sma(daten['close'],50).iloc[-1]),"Kap. 6.2","Klarer Bruch"))
        if t==2 and len(daten)>=2 and (daten["close"].iloc[-1]/daten["close"].iloc[-2]-1)*100<=-7: out.append(_signal("21-MA Bruch + 7% Tagesverlust",50,"intraday",True,None,"Kap. 6.2","Beschleunigte Schwäche"))
    elif variante=="geduldig":
        if t>=3: out.append(_signal("21-MA seit 3 Tagen gebrochen",33,"schluss",True,float(daten["low"].tail(10).min()),"Kap. 6.2","Bruch bestätigt"))
    else:
        if t==1: out.append(_signal("Erster Schluss unter 21-MA",25,"schluss",True,m,"Kap. 6.2","Stufe 1"))
        if t==2 and s<float(daten["close"].iloc[-2]): out.append(_signal("Zweiter Tag unter 21-MA (tiefer)",25,"schluss",True,m,"Kap. 6.2","Stufe 2"))
        if t>=3: out.append(_signal("Dritter Tag unter 21-MA",25,"schluss",True,position.einstiegspreis,"Kap. 6.2","Stufe 3"))
    return out

def strategie_drawdown_vom_peak(position: Position, daten: pd.DataFrame):
    if pnl_pct(position,daten)<=0: return []
    dd=drawdown_vom_peak(position,daten); ma21=_none_if_nan(sma(daten["close"],21).iloc[-1]); s=letzter_schlusskurs(daten); peak=float(position.peak or daten["high"].max()); out=[]
    if 8<=dd<12: out.append(_signal("Drawdown 8% vom Peak",25,"schluss",True,peak*0.88,"Kap. 6.2","erste Sicherung"))
    if 12<=dd<15: out.append(_signal("Drawdown 12-15% vom Peak",33,"schluss",True,peak*0.85,"Kap. 6.2","deutliche Reduktion"))
    if dd>=15: out.append(_signal("Drawdown >15% + Trendbruch" if (ma21 and s<ma21) else "Drawdown >15%",100 if (ma21 and s<ma21) else 50,"schluss",True,None if (ma21 and s<ma21) else ma21,"Kap. 6.2","Komplettausstieg" if (ma21 and s<ma21) else "reduzieren"))
    return out

def strategie_ma_abstand(position,daten):
    if pnl_pct(position,daten)<=0:return []
    s=letzter_schlusskurs(daten); out=[]
    for p,th,tr in [(10,10,25),(21,15,33),(50,25,33),(200,70,50)]:
        m=_none_if_nan(sma(daten["close"],p).iloc[-1]);
        if m and (s-m)/m*100>=th: out.append(_signal(f"{th}% über {p}-MA",100 if p==200 and (s-m)/m*100>=100 else tr,"schluss",True,m,"Kap. 6.2","Überdehnung"))
    return out

def strategie_verlusttage_haeufung(position,daten):
    if pnl_pct(position,daten)<=0 or len(daten)<15:return []
    out=[]; l3=daten.tail(3)
    if all(l3["close"].iloc[i] < l3["close"].iloc[i-1] for i in [1,2]) and (l3["volume"].mean()/daten["volume"].tail(50).mean() if len(daten)>=50 else 1)>=1.1: out.append(_signal("3 tiefere Schlusskurse mit Volumen",25,"schluss",True,float(daten["low"].tail(5).min()),"Kap. 6.2","Schwäche"))
    fen=daten.tail(15); up=(fen["close"]>fen["open"]).sum(); dn=(fen["close"]<fen["open"]).sum()
    if dn-up>=3: out.append(_signal("Abwärtstage überwiegen",25,"schluss",True,float(fen["low"].min()),"Kap. 6.2","Persönlichkeit kippt"))
    return out

def strategie_trendlinie(position,daten,trendlinie_oben_punkte=None,trendlinie_unten_punkte=None):
    out=[]; s=letzter_schlusskurs(daten); pnl=pnl_pct(position,daten)
    oben=_linear_marke(trendlinie_oben_punkte, len(daten)-1)
    unten=_linear_marke(trendlinie_unten_punkte, len(daten)-1)
    if oben and pnl>10 and s>oben: out.append(_signal("Schluss über oberer Trendlinie",13,"schluss",True,None,"Kap. 6.2","Überdehnt über Trendkanal"))
    if unten and s<unten:
        wr = float(daten["volume"].tail(5).mean()/daten["volume"].tail(40).mean()) if len(daten)>=40 else 1.0
        out.append(_signal("Schluss unter unterer Trendlinie",50 if wr>=1.2 else 33,"schluss",True,tagestief(daten),"Kap. 6.3","Trendlinienbruch"))
    return out

def strategie_groesster_anstieg_volumen(position,daten):
    if pnl_pct(position,daten)<=15 or len(daten)<3:return []
    ch=daten["close"].pct_change()*100; t=ch.iloc[-1]; mx=float(ch.iloc[1:].max()); v=daten["volume"].iloc[-1] >= daten["volume"].max()
    if t>=mx and v:return [_signal("Größter Anstieg + höchstes Volumen",33,"schluss",True,float(daten["low"].iloc[-1]),"Kap. 6.2","Klimax-Muster")]
    if t>=mx:return [_signal("Größter Tagesanstieg",20,"schluss",True,float(daten["low"].iloc[-1]),"Kap. 6.2","Vorwarnung")]
    return []

def strategie_split_anstieg(position,daten,split_datum=None):
    if not split_datum: return []
    split_dt=pd.Timestamp(split_datum).tz_localize(None)
    d=daten.copy(); d.index=pd.to_datetime(d.index).tz_localize(None)
    if split_dt not in d.index: return []
    if (d.index[-1]-split_dt).days > 14: return []
    kurs=float(d.loc[split_dt,"close"]); s=letzter_schlusskurs(d); an=(s/kurs-1)*100 if kurs else 0
    if an>=25: return [_signal(f"Anstieg nach Split: {an:.1f}%",50 if an>=50 else 33,"schluss",True,kurs,"Kap. 6.2","Split-Anstieg")]
    return []
def strategie_erschoepfungsluecke(position,daten):
    if pnl_pct(position,daten)<=15 or len(daten)<51 or not position.pivot: return []
    h=daten.iloc[-1]; g=daten.iloc[-2]
    gap=(h["open"]/g["close"]-1)*100 if g["close"] else 0
    vr=vol_verhaeltnis(daten); weit=(h["close"]/position.pivot-1)*100 if position.pivot else 0
    if gap>=3 and vr>=1.5 and weit>=30: return [_signal("Erschöpfungslücke",33,"schluss",True,float(h["low"]),"Kap. 6.2","Gap-up nach langem Lauf")]
    return []
def strategie_downside_reversal(position,daten):
    if pnl_pct(position,daten)<=0 or len(daten)<12:return []
    h=daten.iloc[-1]; span=h["high"]-h["low"]; avg=float((daten["high"].tail(10)-daten["low"].tail(10)).mean()); vr=vol_verhaeltnis(daten)
    if span<=0:return []
    new_high=h["high"]>=float(daten["high"].tail(30).iloc[:-1].max()) if len(daten)>=31 else False
    low_third=h["close"]<=h["low"]+span/3
    if new_high and low_third and vr>=1.2:return [_signal("Downside Reversal an neuem Hoch",33,"schluss",True,float(h["high"]),"Kap. 6.2","Umkehrsignal")]
    if span>=1.5*avg and low_third and vr>=1.2:return [_signal("Weite Umkehrkerze",20,"schluss",True,float(h["high"]),"Kap. 6.2","Warnsignal")]
    if span>=1.5*avg and h["close"]<h["low"]+span/2:return [_signal("Schluss unter Spannenmitte",15,"schluss",True,float(h["high"]),"Kap. 6.2","Warnstufe")]
    return []

def strategie_stau_tage(position,daten):
    if pnl_pct(position,daten)<=0 or len(daten)<10:return []
    fen=daten.tail(10); avgv=float(daten["volume"].tail(50).mean()) if len(daten)>=50 else float(daten["volume"].mean()); st=[]
    for _,r in fen.iterrows():
        ch=(r["close"]-r["open"])/r["open"]*100 if r["open"] else 0
        if abs(ch)<1 and (r["volume"]/avgv if avgv else 0)>=1.3: st.append(r)
    if len(st)>=2:
        dd=drawdown_vom_peak(position,daten); tr=33 if dd<5 else 20
        return [_signal(f"{len(st)} Stau-Tage in 10 Sessions",tr,"schluss",True,float(min(x["low"] for x in st)),"Kap. 6.2","Verdeckte Distribution")]
    return []

def strategie_rueckkehr_pivot(position,daten):
    s=letzter_schlusskurs(daten); out=[]; vr=vol_verhaeltnis(daten)
    if position.tief_tag_1 and s<position.tief_tag_1: out.append(_signal("Schluss unter Tief Ausbruchstag",50 if vr>=1.5 else 33,"schluss",True,position.tief_tag_0,"Kap. 6.3","Sicherheitslinie verletzt"))
    if position.tief_tag_0 and s<position.tief_tag_0: out.append(_signal("Schluss unter Tief Vortag",33,"schluss",True,position.einstiegspreis*0.93,"Kap. 6.3","zweite Linie verletzt"))
    if position.pivot:
        under=(daten["close"]<position.pivot).iloc[::-1]; c=0
        for b in under:
            if not b: break
            c+=1
        if s<position.pivot and c>=10: out.append(_signal(f"{c} Tage unter Pivot",50,"schluss",True,position.einstiegspreis*0.93,"Kap. 6.3","Rückkehr nicht gelungen"))
    return out

def strategie_ma_bruch_defensiv(position,daten,wochen_daten):
    out=[]; s=letzter_schlusskurs(daten); ma50=_none_if_nan(sma(daten["close"],50).iloc[-1]); ma200=_none_if_nan(sma(daten["close"],200).iloc[-1]); atrp=atr(daten,14); vr=vol_verhaeltnis(daten)
    if ma50 and s<ma50:
        dist=(ma50-s)/ma50*100
        if dist>=max(2,atrp) and vr>=1.3: out.append(_signal("Klarer 50-MA-Bruch mit Volumen",50,"schluss",True,ma200,"Kap. 6.3","Defensiver Verkauf"))
        elif tage_unter_ma(daten,sma(daten["close"],50))>=3: out.append(_signal("3 Tage unter 50-MA ohne Rückeroberung",33,"schluss",True,ma200,"Kap. 6.3","Drei-Tage-Frist"))
    if len(wochen_daten)>=10 and tage_unter_ma(wochen_daten.rename(columns={"close":"close"}), sma(wochen_daten["close"],10))>=8: out.append(_signal("8+ Wochen unter 10-Wochen-Linie",100,"schluss",True,None,"Kap. 6.3","Klares Schwächesignal"))
    if ma200 and s<ma200: out.append(_signal("200-MA-Bruch" + (" mit hohem Volumen" if vr>=1.5 else " ohne Volumen"),100 if vr>=1.5 else 75,"schluss",True,None,"Kap. 6.3","Langfristiger Trendbruch"))
    return out

def strategie_drei_verlustwochen(position,wochen_daten):
    if len(wochen_daten)<3:return []
    l=wochen_daten.tail(3)
    three=(l["close"].iloc[0]>l["close"].iloc[1]>l["close"].iloc[2]); vol=(l["volume"].iloc[0]<l["volume"].iloc[1]<l["volume"].iloc[2]); red=bool((l["close"]<l["open"]).all())
    if three and vol and red:return [_signal("3 Verlustwochen + steigendes Volumen",100,"schluss",True,None,"Kap. 6.3","Verteilungsmuster")]
    if three and vol:return [_signal("Vorbereitung Drei-Wochen-Regel",33,"schluss",True,None,"Kap. 6.3","Muster im Aufbau")]
    return []

def strategie_groesster_einbruch(position,daten,wochen_daten):
    if pnl_pct(position,daten)<=10 or len(daten)<4:return []
    out=[]; d=(daten["close"].shift(1)-daten["close"])/daten["close"].shift(1)*100; h=float(d.iloc[-1]); mx=float(d.iloc[1:-1].max()) if len(d)>3 else 0; vr=vol_verhaeltnis(daten)
    if h>=mx and h>3: out.append(_signal("Größter Tagesverlust" + (" + hohes Volumen" if vr>=1.5 else " seit Beginn"),50 if vr>=1.5 else 33,"schluss",True,tagestief(daten),"Kap. 6.3","Spätphasen-Warnsignal"))
    if len(wochen_daten)>=4:
        w=(wochen_daten["close"].shift(1)-wochen_daten["close"])/wochen_daten["close"].shift(1)*100; cur=float(w.iloc[-1]); mxw=float(w.iloc[1:-1].max()) if len(w)>3 else 0; wr=float(wochen_daten["volume"].iloc[-1]/wochen_daten["volume"].tail(12).mean()) if len(wochen_daten)>=12 else 1
        if cur>=mxw and wr>=1.3: out.append(_signal("Größte Verlustwoche seit Beginn",66,"schluss",True,None,"Kap. 6.3","Wahrscheinliches Rally-Ende"))
    return out

def strategie_rs_linie(
    position,
    daten,
    daten_spy,
    wochen_daten,
    wochen_daten_spy,
    pnl_tag_zu_woche=20.0,
    pnl_woche_zu_monat=80.0,
):
    if daten_spy is None or len(daten_spy)==0:return []
    pnl=pnl_pct(position,daten)
    schwelle_tag_woche=float(pnl_tag_zu_woche)
    schwelle_woche_monat=float(max(pnl_woche_zu_monat, schwelle_tag_woche))
    if pnl < schwelle_tag_woche:
        zeitebene="tag"; basis=daten; basis_spy=daten_spy; sp,lp=21,50
    elif pnl < schwelle_woche_monat:
        if wochen_daten is None or wochen_daten_spy is None or len(wochen_daten)==0 or len(wochen_daten_spy)==0:return []
        zeitebene="woche"; basis=wochen_daten; basis_spy=wochen_daten_spy; sp,lp=10,25
    else:
        zeitebene="monat"; basis=daten.resample("ME").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
        basis_spy=daten_spy.resample("ME").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}); sp,lp=12,24
    rs=(basis["close"]/basis_spy["close"].reindex(basis.index).ffill()).dropna()
    if len(rs)<max(lp+1,4):return []
    rsf=sma(rs,sp); rsl=sma(rs,lp); out=[]
    if len(rs)>=2 and rs.iloc[-1] < rsf.iloc[-1] and rs.iloc[-2] >= rsf.iloc[-2]: out.append(_signal(f"RS bricht {sp}-MA ({zeitebene})",20,"schluss",True,None,"Kap. 6.4 RS-Stufe 1","Erstes Warnsignal — RS-Linie bricht schnellen MA"))
    cnt=0
    for a,b in zip(rs.iloc[::-1], rsf.iloc[::-1]):
        if pd.isna(b) or a>=b: break
        cnt+=1
    if cnt>=3: out.append(_signal(f"RS 3 {zeitebene} unter {sp}-MA",30,"schluss",True,_none_if_nan(rsl.iloc[-1]),"Kap. 6.4 RS-Stufe 2","Bestätigte Schwäche — zweite Tranche"))
    if rs.iloc[-1] < rsl.iloc[-1]: out.append(_signal(f"RS bricht {lp}-MA ({zeitebene})",50,"schluss",True,None,"Kap. 6.4 RS-Stufe 3","Endgültiger Ausstieg — Rest verkaufen"))
    return out

def strategie_ma_basierte_sequenz(
    position,
    daten,
    gewinnzone_min_pct=20.0,
    gewinnzone_max_pct=25.0,
    gewinnzone_tranche_pct=33.0,
    ueber_ma10_pct=10.0,
    ueber_ma10_tranche_pct=20.0,
    unter_ma10_mindestgewinn_pct=5.0,
    unter_ma10_tranche_pct=20.0,
    pendel_tranche_pct=25.0,
    pendel_lookback_tage=5,
    pendel_wechsel_min=3,
    unter_ma21_mindestgewinn_pct=5.0,
    unter_ma21_tranche_pct=25.0,
    klarer_ma50_bruch_pct=2.0,
):
    pnl=pnl_pct(position,daten); s=letzter_schlusskurs(daten); r=sum(position.realisierte_tranchen or []); out=[]
    ma10=_none_if_nan(sma(daten["close"],10).iloc[-1]); ma21=_none_if_nan(sma(daten["close"],21).iloc[-1]); ma50=_none_if_nan(sma(daten["close"],50).iloc[-1])
    if gewinnzone_min_pct<=pnl<=gewinnzone_max_pct and r<gewinnzone_tranche_pct:
        out.append(_signal("Gewinnzone 20-25%",gewinnzone_tranche_pct,"schluss",True,ma10,"Kap. 6.4 Sequenz Punkt 1","Objektive Gewinnzone — ersten Teil sichern"))
    if ma10 and (s-ma10)/ma10*100>=ueber_ma10_pct and r<50:
        out.append(_signal("10% über 10-MA (überhitzt)",ueber_ma10_tranche_pct,"schluss",True,ma10,"Kap. 6.4 Sequenz Punkt 2","Anstieg überhitzt — Tranche in Stärke"))
    if ma10 and s<ma10 and pnl>unter_ma10_mindestgewinn_pct:
        pendelt=False
        n=max(int(pendel_lookback_tage),2)
        w=max(int(pendel_wechsel_min),1)
        if len(daten)>=n and len(sma(daten["close"],10))>=n:
            last_close=daten["close"].tail(n).reset_index(drop=True)
            last_ma10=sma(daten["close"],10).tail(n).reset_index(drop=True)
            wechsel=0
            for i in range(1,n):
                if pd.notna(last_ma10.iloc[i]) and pd.notna(last_ma10.iloc[i-1]):
                    now_below=last_close.iloc[i] < last_ma10.iloc[i]
                    prev_below=last_close.iloc[i-1] < last_ma10.iloc[i-1]
                    if now_below != prev_below:
                        wechsel += 1
            pendelt = wechsel >= w
        tranche=pendel_tranche_pct if pendelt else unter_ma10_tranche_pct
        out.append(_signal("Schluss unter 10-MA (pendelnd)" if pendelt else "Schluss unter 10-MA",tranche,"schluss",True,ma21,"Kap. 6.4 Sequenz Punkt 3","Pendel um 10-MA — Warnzeichen" if pendelt else "Kurzfristige Unterstützung verloren"))
    if ma21 and s<ma21 and pnl>unter_ma21_mindestgewinn_pct:
        out.append(_signal("Schluss unter 21-MA",unter_ma21_tranche_pct,"schluss",True,ma50,"Kap. 6.4 Sequenz Punkt 4","Mittelfristige Linie verloren"))
    if ma50 and s<ma50 and (ma50-s)/ma50*100>=klarer_ma50_bruch_pct:
        out.append(_signal("Klarer 50-MA-Bruch",100,"schluss",True,None,"Kap. 6.4 Sequenz Punkt 5","Mittelfristiger Trend gebrochen — alle Reste schließen"))
    return out

def strategie_einfach_halbe_position(position,daten,erste_haelfte_gewinn_pct=20.0):
    pnl=pnl_pct(position,daten); r=sum(position.realisierte_tranchen or []); out=[]; h=r>=50
    gw=max(float(erste_haelfte_gewinn_pct),0.0)
    if pnl>=gw and not h: out.append(_signal(f"Erste Hälfte bei {gw:g}%+",50,"schluss",True,position.einstiegspreis,"Kap. 6.4","Hälfte sichern"))
    if h and pnl>=20: out.append(_signal("Erneut 20% — weitere Tranche",50,"schluss",True,position.einstiegspreis,"Kap. 6.4","Zweite Gewinnmitnahme"))
    if h and -1<=pnl<=1: out.append(_signal("Break-Even-Stopp greift",100,"intraday",True,None,"Kap. 6.4","Rest auf BE"))
    return out

def strategie_misslungener_ausbruch_5stufen(position,daten):
    out=[]; h=daten.iloc[-1]; pnl=pnl_pct(position,daten); nb=position.einstiegspreis*0.93
    g=daten.iloc[-2] if len(daten)>=2 else h
    gap_down_unter_tag1 = bool(position.tief_tag_1) and h["open"] < position.tief_tag_1 and h["open"] < g["close"]
    gap_down_unter_tag0 = bool(position.tief_tag_0) and h["open"] < position.tief_tag_0
    if h["open"]<=nb:return [_signal("Gap-down durch 7%-Grenze",100,"intraday",True,None,"Kap. 6.4 Sonderfall Gap-down","Sofort schließen — nicht auf Schluss warten")]
    if gap_down_unter_tag1 or gap_down_unter_tag0:
        out.append(_signal("Gap-down unter Ausbruchsmarken",20,"intraday",True,position.tief_tag_1 if gap_down_unter_tag1 else position.tief_tag_0,"Kap. 6.4 Sonderfall Gap-down","Eröffnung bereits unter kritischer Marke"))
    tag0_unter_notbremse = bool(position.tief_tag_0) and position.tief_tag_0 <= nb
    if position.tief_tag_1 and h["low"]<position.tief_tag_1: out.append(_signal("Intraday unter Tief Tag 1",20,"intraday",True,position.tief_tag_1,"Kap. 6.4","Stufe 1a"))
    if position.tief_tag_1 and h["close"]<position.tief_tag_1: out.append(_signal("Schluss unter Tief Tag 1",20,"schluss",True,position.tief_tag_0,"Kap. 6.4","Stufe 1b"))
    if (not tag0_unter_notbremse) and position.tief_tag_0 and h["low"]<position.tief_tag_0: out.append(_signal("Intraday unter Tief Tag 0",20,"intraday",True,position.tief_tag_0,"Kap. 6.4","Stufe 2a"))
    if (not tag0_unter_notbremse) and position.tief_tag_0 and h["close"]<position.tief_tag_0: out.append(_signal("Schluss unter Tief Tag 0",20,"schluss",True,nb,"Kap. 6.4","Stufe 2b"))
    if pnl<=-7: out.append(_signal("7%-Notbremse (intraday)",100,"intraday",True,None,"Kap. 6.4","Stufe 3"))
    if (position.realisierte_tranchen or []) and position.pivot:
        d = daten[daten.index >= pd.Timestamp(position.einstiegsdatum)] if "index" in dir(daten) else daten
        closes = d["close"] if "close" in d else pd.Series(dtype=float)
        if len(closes):
            war_unter_pivot = float(closes.min()) <= float(position.pivot)
            erholt = float(closes.tail(20).max()) >= float(position.pivot) * 1.03
            jetzt_wieder = float(h["close"]) <= float(position.pivot)
            if war_unter_pivot and erholt and jetzt_wieder:
                out.append(_signal("Zweite Rückkehr zum Pivot",33,"schluss",True,nb,"Kap. 6.4 Ergänzung","Doppelter Rücklauf — klares Schwächesignal"))
    return out

def strategie_einfache_verluststufen(position,daten,verlust_stufe_1=3.0,verlust_stufe_2=5.0,verlust_stufe_3=7.0):
    pnl=pnl_pct(position,daten)
    s1=abs(float(verlust_stufe_1)); s2=max(abs(float(verlust_stufe_2)),s1); s3=max(abs(float(verlust_stufe_3)),s2)
    if -s2<pnl<=-s1:return [_signal(f"Verlust ≥ {s1:g}%",33,"intraday",True,position.einstiegspreis*(1-s2/100),"Kap. 6.4","Erste Tranche")]
    if -s3<pnl<=-s2:return [_signal(f"Verlust ≥ {s2:g}%",33,"intraday",True,position.einstiegspreis*(1-s3/100),"Kap. 6.4","Zweite Tranche")]
    if pnl<=-s3:return [_signal(f"Verlust ≥ {s3:g}%",100,"intraday",True,None,"Kap. 6.4","Rest sofort schließen")]
    return []

def strategie_atr_basiert(position,daten,ziel_atr_multiplikator=3,ueberdehnung_atr_start=3,ueberdehnung_atr_stark=4):
    p=pnl_pct(position,daten); s=letzter_schlusskurs(daten); a=atr(daten,14)
    if not a:return []
    out=[]; ga=p/a
    if ga>=ziel_atr_multiplikator: out.append(_signal(f"{ga:.1f} ATR Gewinn erreicht",33,"schluss",True,None,"Kap. 6.4",f"Ziel {ziel_atr_multiplikator} ATR erreicht — Tranche in Stärke"))
    stop=position.einstiegspreis*(1-1.5*a/100)
    if s<=stop: out.append(_signal("Stopp bei -1.5 ATR",100,"intraday",True,None,"Kap. 6.4","Volatilitätsangepasste Stoppmarke gerissen"))
    e=_none_if_nan(ema(daten["close"],21).iloc[-1])
    if e:
        ab=(s-e)/e*100/a
        if ab>=ueberdehnung_atr_start: out.append(_signal(f"{ab:.1f} ATR über 21-EMA",50 if ab>=ueberdehnung_atr_stark else 33,"schluss",True,e,"Kap. 6.4","Überdehnt — volatilitätsbereinigt überhitzt"))
    return out

def berechne_watch_signale(position,daten):
    w=[]; pnl=pnl_pct(position,daten); ma21=sma(daten["close"],21); t=tage_unter_ma(daten,ma21); dd=drawdown_vom_peak(position,daten)
    if pnl>0 and t in [1,2]: w.append({"name":f"{t} Tage unter 21-MA","buch_verweis":"Kap. 6.2"})
    if pnl>0 and 5<=dd<8: w.append({"name":f"Drawdown {dd:.1f}% vom Peak","buch_verweis":"Kap. 6.2"})
    if up_down_volume_ratio(daten,50)<1.0: w.append({"name":"Up/Down-Volume < 1.0","buch_verweis":"Kap. 5.3"})
    d=distribution_tage(daten,25)
    if d>=4: w.append({"name":f"{d} Distribution-Tage in 25 Sessions","buch_verweis":"Kap. 5.3"})
    return w


def verkaufs_empfehlung_gesamt(position: Position, daten: pd.DataFrame, wochen_daten: pd.DataFrame, daten_spy: pd.DataFrame | None, wochen_daten_spy: pd.DataFrame | None, markt: str, industrie: str, aktive_strategien: list[str], strategie_optionen: dict | None = None):
    o = strategie_optionen or {}
    r = {
        "notbremse_verlust": lambda: strategie_notbremse_verlust(position,daten,markt),
        "drei_stufen_nach_kauf": lambda: strategie_drei_stufen_nach_kauf(position,daten),
        "gewinn_in_stufen": lambda: strategie_gewinn_in_stufen(position,daten,markt),
        "ma21_bruch": lambda: strategie_21ma_bruch(position,daten,o.get("ma21_variante","gestaffelt")),
        "drawdown_vom_peak": lambda: strategie_drawdown_vom_peak(position,daten),
        "ma_abstand": lambda: strategie_ma_abstand(position,daten),
        "verlusttage_haeufung": lambda: strategie_verlusttage_haeufung(position,daten),
        "trendlinie": lambda: strategie_trendlinie(position,daten,o.get("trendlinie_oben_punkte"),o.get("trendlinie_unten_punkte")),
        "groesster_anstieg_volumen": lambda: strategie_groesster_anstieg_volumen(position,daten),
        "split_anstieg": lambda: strategie_split_anstieg(position,daten,o.get("split_datum")),
        "erschoepfungsluecke": lambda: strategie_erschoepfungsluecke(position,daten),
        "downside_reversal": lambda: strategie_downside_reversal(position,daten),
        "stau_tage": lambda: strategie_stau_tage(position,daten),
        "rueckkehr_pivot": lambda: strategie_rueckkehr_pivot(position,daten),
        "ma_bruch_defensiv": lambda: strategie_ma_bruch_defensiv(position,daten,wochen_daten),
        "drei_verlustwochen": lambda: strategie_drei_verlustwochen(position,wochen_daten),
        "groesster_einbruch": lambda: strategie_groesster_einbruch(position,daten,wochen_daten),
        "rs_linie": lambda: strategie_rs_linie(position,daten,daten_spy,wochen_daten,wochen_daten_spy,o.get("rs_pnl_tag_zu_woche",20.0),o.get("rs_pnl_woche_zu_monat",80.0)),
        "ma_basierte_sequenz": lambda: strategie_ma_basierte_sequenz(
            position,daten,
            o.get("ma_seq_gewinnzone_min_pct",20.0),
            o.get("ma_seq_gewinnzone_max_pct",25.0),
            o.get("ma_seq_gewinnzone_tranche_pct",33.0),
            o.get("ma_seq_ueber_ma10_pct",10.0),
            o.get("ma_seq_ueber_ma10_tranche_pct",20.0),
            o.get("ma_seq_unter_ma10_mindestgewinn_pct",5.0),
            o.get("ma_seq_unter_ma10_tranche_pct",20.0),
            o.get("ma_seq_pendel_tranche_pct",25.0),
            o.get("ma_seq_pendel_lookback_tage",5),
            o.get("ma_seq_pendel_wechsel_min",3),
            o.get("ma_seq_unter_ma21_mindestgewinn_pct",5.0),
            o.get("ma_seq_unter_ma21_tranche_pct",25.0),
            o.get("ma_seq_klarer_ma50_bruch_pct",2.0),
        ),
        "einfach_halbe_position": lambda: strategie_einfach_halbe_position(position,daten,o.get("erste_haelfte_gewinn_pct",20.0)),
        "misslungener_ausbruch_5stufen": lambda: strategie_misslungener_ausbruch_5stufen(position,daten),
        "einfache_verluststufen": lambda: strategie_einfache_verluststufen(position,daten,o.get("verlust_stufe_1",3.0),o.get("verlust_stufe_2",5.0),o.get("verlust_stufe_3",7.0)),
        "atr_basiert": lambda: strategie_atr_basiert(position,daten,o.get("ziel_atr_multiplikator",3),o.get("ueberdehnung_atr_start",3),o.get("ueberdehnung_atr_stark",4)),
    }
    all_signals=[]
    for k in aktive_strategien:
        if k in r: all_signals.extend(r[k]())
    killer=[s for s in all_signals if s["aktuell_aktiv"] and s["tranche_pct"]==100]
    if killer:
        ges, grund = 100, killer[0]["name"]
    else:
        act=[s for s in all_signals if s["aktuell_aktiv"] and s["tranche_pct"]>0]
        summe=sum(s["tranche_pct"] for s in act)
        if len(act)>=4 and summe<75: summe=75
        st=[0,25,33,50,66,75,100]
        ges=max(v for v in st if v<=min(summe,100))
        if markt=="Bärisch" and 0<ges<100: ges=next((v for v in st if v>ges),100)
        grund=", ".join(s["name"] for s in act[:3]) if act else "keine aktiven Signale"
    schon=sum(position.realisierte_tranchen or [])
    return {"gesamt_tranche":ges,"bereits_realisiert":schon,"jetzt_zu_verkaufen":max(0,ges-schon),"haupt_grund":grund,"alle_signale":all_signals}
