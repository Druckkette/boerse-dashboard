# Audit: Strategie 15 — Bruch 50-MA / 10-Wochen-MA / 200-MA

Geprüft wurde die Implementierung in `sell_strategies.py`, Funktion `strategie_ma_bruch_defensiv`.

## Ergebnis

Die Kernlogik ist **weitgehend umgesetzt**, aber es gibt mehrere **Abweichungen** zur vorgegebenen Spezifikation.

## Abgleich Spezifikation vs. Code

1. **50-MA-Bruch mit Volumen**
   - Spezifikation: Auslösung bei Abstand unter MA50 `>= max(2, atr_pct)` und `heute_vol_ratio >= 1.3`.
   - Code: Exakt so umgesetzt (`dist>=max(2,atrp)` und `vr>=1.3`).

2. **3-Tage-Toleranz unter 50-MA**
   - Spezifikation: Wenn kein klarer Volumenbruch, dann Signal nach `>=3` Tagen unter 50-MA.
   - Code: So umgesetzt (`tage_unter_ma(...)>=3`, Tranche 33%).

3. **10-Wochen-Linie (8 Wochen darunter)**
   - Spezifikation: aufeinanderfolgende Wochen unter 10-Wochen-MA zählen.
   - Code: Nutzt ebenfalls `tage_unter_ma` auf den Wochen-Daten; funktional passend, aber Naming/Helper ist tagesorientiert.

4. **200-MA-Bruch mit/ohne Volumen**
   - Spezifikation: Bei Bruch unter 200-MA mit Volumen (`>=1.5`) 100%, sonst 75%.
   - Code: Exakt so umgesetzt.

5. **Bestätigungssignal „200-MA dreht abwärts“**
   - Spezifikation: Zusätzliches Info-Signal (Tranche 0), wenn MA200-Neigung über 20 Perioden negativ ist und Kurs unter MA200 liegt.
   - Code: Umgesetzt — `strategie_ma_bruch_defensiv` emittiert das Signal, wenn `ma200_richtung < 0` und Kurs unter MA200.

## Abweichungen / Lücken

1. **Buchverweis nicht granular**
   - Spezifikation unterscheidet teils `Kap. 6.3 50-MA`, `Kap. 6.3 10-Wochen-Linie`, `Kap. 6.3 200-MA`.
   - Code nutzt überall nur `Kap. 6.3`.

2. **Begründungstexte vereinfacht**
   - Texte sind funktional ähnlich, aber weniger präzise als in der Spezifikation (z. B. „Defensiver Verkauf“ statt klarer Regelbegründung).

## Fazit

- **Regeltechnisch vorhanden:** 50-MA-Bruch, 3-Tage-Toleranz, 10-Wochen-Unterbietung, 200-MA-Bruch (mit/ohne Volumen), 200-MA-Neigung dreht abwärts.
- **Dokumentations-/Semantikdelta:** Buchverweise und Textbegründungen sind knapper als die Strategievorlage.
