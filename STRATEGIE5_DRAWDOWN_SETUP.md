# Strategie 5 — Starker Preisrückgang vom Peak (Kap. 6.2)

Diese Strategie ist in `sell_strategies.py` als `strategie_drawdown_vom_peak(...)` umgesetzt.

## Kernlogik

- **Nur im Gewinnfall** aktiv (`pnl_pct > 0`).
- **Drawdown vom Peak** wird als positiver Prozentwert berechnet.
- **Stufe 1 (Standard: ab 8%)**: erste Sicherung, Stopps enger, Teilverkauf.
- **Stufe 2 (Standard: ab 12%)**: deutliche Reduktion.
- **Stufe 3 (Standard: ab 15%)**:
  - mit zusätzlichem Trendbruch (Schluss < 21-EMA): Komplettausstieg,
  - ohne Trendbruch: starke Reduktion.

## Setup-/Konfigurationsmöglichkeiten

Die Strategie kann über `strategie_optionen` parametrisiert werden:

- `drawdown_stufe1_min_pct` (Default `8.0`)
- `drawdown_stufe2_min_pct` (Default `12.0`)
- `drawdown_stufe3_min_pct` (Default `15.0`)
- `drawdown_tranche_stufe1_pct` (Default `25.0`)
- `drawdown_tranche_stufe2_pct` (Default `33.0`)
- `drawdown_tranche_stufe3_ohne_trendbruch_pct` (Default `50.0`)
- `drawdown_tranche_stufe3_mit_trendbruch_pct` (Default `100.0`)

Hinweis: Schwellenwerte werden intern monoton geordnet (`Stufe1 <= Stufe2 <= Stufe3`), damit inkonsistente Eingaben robust abgefangen werden.

## Beispiel

```python
strategie_optionen = {
    "drawdown_stufe1_min_pct": 7.5,
    "drawdown_stufe2_min_pct": 11.0,
    "drawdown_stufe3_min_pct": 14.0,
    "drawdown_tranche_stufe1_pct": 20,
    "drawdown_tranche_stufe2_pct": 35,
    "drawdown_tranche_stufe3_ohne_trendbruch_pct": 60,
    "drawdown_tranche_stufe3_mit_trendbruch_pct": 100,
}
```
