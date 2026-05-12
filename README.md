# Börse ohne Bauchgefühl — Markt-Dashboard v3.0

## Deployment auf Streamlit Community Cloud

### Schritt 1: GitHub Repository
1. Gehe zu [github.com](https://github.com) → **"+" → "New repository"**
2. Name: `boerse-dashboard`, Visibility: **Private**
3. **"Create repository"**

### Schritt 2: Dateien hochladen
1. Klicke **"uploading an existing file"**
2. Lade hoch: `app.py` und `requirements.txt`
3. Klicke **"Add file" → "Create new file"**:
   - Pfad: `.streamlit/config.toml`
   - Inhalt aus der config.toml kopieren
4. **"Commit changes"**

### Schritt 3: FRED API Key als Secret hinterlegen (und regelmäßig rotieren)
1. Gehe zu [share.streamlit.io](https://share.streamlit.io)
2. Logge dich mit GitHub ein → **"New app"**
3. Wähle dein Repo, Branch `main`, Main file `app.py`
4. **WICHTIG**: Klicke auf **"Advanced settings"** bevor du deployst
5. Füge unter **"Secrets"** ein:
   ```
   FRED_API_KEY = "dein_neuer_fred_api_key"
   ```
6. Klicke **"Deploy!"**

7. Falls ein Key jemals in einem öffentlichen Artefakt auftaucht: im FRED-Portal sofort **revoke/regenerate** und anschließend den neuen Key in Streamlit Secrets ersetzen.

### Fertig!
Die App ist live unter: `https://dein-name-boerse-dashboard.streamlit.app`

## Features v3.0

### Automatisch bei jedem Aufruf:
- **Trendwende-Ampel** (Rot/Gelb/Grün/Aufwärtstrend)
- **3 Indizes** (S&P 500, Nasdaq, Russell 2000)
- **15 Punkte Aufwärtstrend-Prüfung** (alle MA-Checks)
- **11 Frühwarnzeichen** (Intraday-Umkehrungen, Closing Range, Distribution, Stau-Tage, Intermarket-Divergenz, Sektorrotation, Erholungsquote, etc.)
- **Marktbreite-Modus** (RSP/QQEW mit 3-Tage-Stabilitätsregel)
- **VIX/VIXY Analyse** (Panik-Erkennung, Risk-On/Off)

### Per Button "Tiefenanalyse":
- **A/D-Linie** (kumulativ, mit 21-SMA)
- **McClellan Oscillator** (19/39-EMA, Überkauft/Überverkauft-Zonen)
- **Neue Hochs / Neue Tiefs** Ratio
- **% der Aktien über 50-SMA und 200-SMA**
- **Breitenschub-Erkennung** (Deemer Ratio > 1.97)
- **Divergenz-Check** (Index vs. A/D-Linie)
- **Federal Funds Rate** Trend (FRED API)


## Volatilitäts-Regime: Regeln und Grenzwerte

Das Dashboard bewertet das Volatilitätsumfeld zweistufig: Zuerst wird das VIX-Regime aus dem VIX selbst abgeleitet, anschließend kombiniert das finale `Vol_Regime` VIX, VIXY und die 5-Tage-Entwicklung des S&P 500.

### 1) VIX-Regime

Für den VIX werden ein 10-Tage-EMA, ein 63-Tage-Z-Score und ein 252-Tage-Perzentilrang berechnet. Die Einordnung erfolgt in dieser Reihenfolge: Stress vor Ruhig vor Neutral.

| Regime | Bedingung |
|---|---|
| Stress | `PctRank252 >= 0.85` und `Z63 >= 1.5`, oder Fallback: `VIX Close > 20` und `VIX Close > EMA10` |
| Ruhig | `PctRank252 <= 0.25` und `Z63 <= -0.5`, oder Fallback: `VIX Close < 16` und `VIX Close < EMA10` |
| Neutral | Weder Stress noch Ruhig ist erfüllt |

Falls Stress- und Ruhebedingung theoretisch gleichzeitig wahr wären, gewinnt Stress, weil diese Bedingung in der Auswahl zuerst geprüft wird.

### 2) VIXY-Bestätigung

VIXY dient als Bestätigung, ob Volatilitätsstress auch im Futures-Produkt getragen wird.

| Signal | Bedingung |
|---|---|
| Steigender VIXY-Trend | `VIXY Close > EMA21` und `EMA21 > EMA21 vor 5 Tagen` |
| Stress-Bestätigung | `VIXY Ret_5d > 8%`, `VIXY PctRank252 > 0.70` und steigender VIXY-Trend, oder `VIXY Ret_5d > 5%` und steigender VIXY-Trend |
| Carry-Abbau / kein Stress | `VIXY Close < EMA21` und `VIXY Ret_20d < 0` |

### 3) Finales Volatilitäts-Regime

Das finale `Vol_Regime` wird in der folgenden Priorität vergeben:

| Finales Regime | Bedingung | Interpretation |
|---|---|---|
| Risk Off bestätigt | VIX ist Stress und VIXY bestätigt Stress | VIX und VIXY ziehen gleichzeitig an |
| Kurzer Volatilitätsschock | VIX ist Stress, aber VIXY bestätigt nicht | VIX springt an, Futures bestätigen aber nicht voll |
| Fragile Rally | S&P 500 steigt über 5 Tage und gleichzeitig bleibt VIX oder VIXY auffällig | Aktienmarkt steigt, Volatilität entspannt aber nicht sauber |
| Risk On / ruhig | VIX ist ruhig, VIXY baut ab und S&P 500 steigt über 5 Tage | Ruhiges Umfeld mit abbauendem VIXY |
| Neutral | Keine der obigen Bedingungen ist erfüllt | Keine klare Volatilitätslage |

Eine fragile Rally liegt konkret vor, wenn `SPX_Ret_5d > 0` gilt und zusätzlich mindestens eine der folgenden Bedingungen erfüllt ist: VIXY-Stress-Bestätigung, `VIX_Ret_5d > 0`, oder `VIXY_Ret_5d > 3%` bei gleichzeitigem `VIX_PctRank252 > 0.55`.

## Aktienanalyse: Scoring-Kriterien, Gewichtung und Schwellenwerte

Die Einzelaktien-Bewertung wird in vier Teilbereiche zerlegt und anschließend zu einem Gesamtscore (0–100) zusammengeführt.

### 1) Gewichtung der Teilbereiche im Gesamtscore

| Teilbereich | Gewicht im Gesamtscore |
|---|---:|
| Qualität | 25% |
| Wachstum | 20% |
| Trend | 35% |
| Risiko | 20% |

Formel: `Gesamtscore = 0.25*Qualität + 0.20*Wachstum + 0.35*Trend + 0.20*Risiko`.

Fehlende Teilwerte werden neutral mit `50` ersetzt, damit der Score berechenbar bleibt.

### 2) Scoring-Logik je Kriterium (100 / 60 / 25 Punkte)

Jedes Kriterium wird über Schwellen eingestuft:

- **100 Punkte**: guter Bereich
- **60 Punkte**: mittlerer Bereich
- **25 Punkte**: schwacher Bereich

Die Teilbereich-Scores sind jeweils der Mittelwert ihrer enthaltenen Kriterien.

### 3) Qualität

| Kriterium | 100 Punkte | 60 Punkte | 25 Punkte |
|---|---:|---:|---:|
| ROE (%) | ≥ 17 | ≥ 10 | < 10 |
| Bruttomarge (%) | ≥ 45 | ≥ 30 | < 30 |
| Operative Marge (%) | ≥ 18 | ≥ 10 | < 10 |
| Debt/Equity | ≤ 80 | ≤ 160 | > 160 |

### 4) Wachstum

| Kriterium | 100 Punkte | 60 Punkte | 25 Punkte |
|---|---:|---:|---:|
| Umsatzwachstum Jahr (%) | ≥ 15 | ≥ 5 | < 5 |
| Gewinnwachstum Jahr (%) | ≥ 15 | ≥ 5 | < 5 |
| Umsatzwachstum Quartal (%) | ≥ 10 | ≥ 3 | < 3 |
| Gewinnwachstum Quartal (%) | ≥ 10 | ≥ 3 | < 3 |

### 5) Trend

| Kriterium | Punkte |
|---|---:|
| Kurs über EMA21 | 100 (sonst 25) |
| Kurs über SMA50 | 100 (sonst 20) |
| Kurs über SMA200 | 100 (sonst 15) |
| RS-Rating | 100 bei ≥ 80, 60 bei ≥ 65, sonst 25 |
| RS-Trend 5W | +80 bei positiv, +30 bei negativ |
| Chartzeichen-Bonus/Malus | +75 auf Trend bei klar positiven Zeichen (Positiv > Negativ + 2) |

### 6) Risiko

| Kriterium | 100 Punkte | 60 Punkte | 25 Punkte |
|---|---:|---:|---:|
| ATR % | ≤ 2.5 | ≤ 4.5 | > 4.5 |
| Beta | ≤ 1.0 | ≤ 1.6 | > 1.6 |
| Drawdown vs. 52W-Hoch (%) absolut | ≤ 12 | ≤ 25 | > 25 |
| Abstand zur SMA50 (%) absolut | ≤ 6 | ≤ 14 | > 14 |

Zusatz-Malus: Bei klar negativen Chartzeichen (Negativ > Positiv + 2) werden dem Risiko 30 Punkte hinzugefügt.

### 7) Status-Schwellen auf Gesamtebene

| Status | Bedingung |
|---|---|
| Nicht bewertbar | zu wenig Daten (≤1 verfügbare Teilgruppen oder <120 Kursdatenpunkte) |
| Zu erweitert | Trendscore ≥ 75 und gleichzeitig deutliche Überdehnung (Abstand SMA50 ≥ 18% oder ATR-Extension ≥ 4.5) |
| Attraktiv | Gesamtscore ≥ 80 und Risikoscore ≥ 45 |
| Beobachten | Gesamtscore 60–79 |
| Zu schwach | Gesamtscore < 60 |

## Aktienbewertung-Screen: Warum ähnliche Kästen doppelt erscheinen

Im Screen gibt es **zwei Darstellungs-Ebenen** mit teilweise gleichen Begriffen:

1. **KPI-Cockpit (oben)** mit Karten wie „Qualität“, „Wachstum“, „Trend“, „Risiko“.
2. **Geführte 4er-Analysekarte (unten)** mit Karten wie „Qualität“, „Wachstum“, „Chart & Trend“, „Risiko“.

### Qualität (oben) vs. Qualität (unten)

- **Inhaltlich gleicher Teilscore**: Beide zeigen denselben `quality_score`.
- **Oben**: komprimierte Management-Sicht (Score + kurze Interpretation + Ampelton).
- **Unten**: detaillierte Aufschlüsselung der Treiber (ROE, Margen, Debt/Equity) inkl. Statuschip.

### Wachstum (oben) vs. Wachstum (unten)

- **Inhaltlich gleicher Teilscore**: Beide zeigen denselben `growth_score`.
- **Oben**: kompakte Einordnung für schnelles Urteil.
- **Unten**: Detailsicht mit Jahres- und Quartalsraten (Umsatz/Gewinn) und erklärendem Text.

### Risiko (oben) vs. Risiko (unten)

- **Inhaltlich gleicher Teilscore**: Beide zeigen denselben `risk_score`.
- **Oben**: Kurzsicht mit ATR und Drawdown als schnelle Risikoproxies.
- **Unten**: breitere Kontextsicht inkl. Beta, Drawdown, Volumenfaktor und Statuslabel.

### Trend vs. „Chart & Trend“

- **Kein separates Scoring-Modell**: „Trend“ (oben) und „Chart & Trend“ (unten) referenzieren denselben `trend_score`.
- **Warum zwei Namen?**
  - „Trend“ im KPI-Cockpit ist die Kurzbezeichnung.
  - „Chart & Trend“ in der 4er-Karte betont, dass neben MAs/RS auch Chartsignale in die Einordnung einfließen.
- **Praktische Lesart**: Oben = Überblick, unten = Erklärung desselben Scores.

## SEC 13F Institutionen-Trend

Der Workflow `.github/workflows/institutional-13f.yml` wertet die offiziellen SEC Form-13F-Datensätze aus und erzeugt gemeinsame Artefakte für Streamlit und iOS:

- `output/institutional_13f_trends.json` für App/Streamlit-Lookups
- `output/institutional_13f_trends.csv` zur Kontrolle in Tabellenform
- `output/sec13f_cusip_ticker_map.csv` als erzeugtes CUSIP→Ticker-Mapping
- `output/sec13f_unmatched_cusips.csv` für nicht zuordenbare CUSIPs

Der Lauf zählt keine Fondsnamen aus, sondern aggregiert nur je Ticker:

- Anzahl aller 13F-Halter
- Anzahl großer Institutionen ab 10 Mio. USD Positionswert
- Veränderung zum Vorquartal
- Trend `positive`, `neutral`, `negative` oder `new`

Manuell lokal:

```bash
python scripts/update_institutional_13f.py
```

Für GitHub Actions sollte optional ein Secret `SEC_USER_AGENT` mit Kontaktkennung gesetzt werden, damit der SEC-Abruf den Fair-Access-Regeln sauber entspricht.
