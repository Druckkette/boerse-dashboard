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

### Schritt 3: FRED API Key als Secret hinterlegen
1. Gehe zu [share.streamlit.io](https://share.streamlit.io)
2. Logge dich mit GitHub ein → **"New app"**
3. Wähle dein Repo, Branch `main`, Main file `app.py`
4. **WICHTIG**: Klicke auf **"Advanced settings"** bevor du deployst
5. Füge unter **"Secrets"** ein:
   ```
   FRED_API_KEY = "dein_fred_api_key"
   ```
6. Klicke **"Deploy!"**

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
