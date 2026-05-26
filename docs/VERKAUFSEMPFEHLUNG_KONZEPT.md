# Konzept: Verkaufsempfehlung vereinfachen & konsolidieren

> Status: Konzept · keine Code-Änderungen
> Branch: `claude/sales-recommendation-concept-8cW0o`
> Datum: 2026-05-26

---

## 1. Ausgangslage (Ist-Zustand)

### 1.1 Was es heute gibt

**5 Bereiche**, die sich überschneiden:

| Bereich | Datei | Funktion | Inhalt |
|---|---|---|---|
| Mein Depot → Verkaufskandidaten | `app.py:3045` | `_build_sell_candidates_table` | Tabelle: Health-Score + Tranche % + Status pro Position |
| Verkaufs-Entscheidung → Live-Monitor | `app.py:13141` | `_render_sell_decision_live_monitor` | Pro Position: alle 23 Strategien + 11 Warnungen + manuelle Overrides + Setup-Panel |
| Verkaufs-Entscheidung → Portfolio-Ranking | `app.py:13143` | `_render_sell_decision_portfolio_ranking` | Portfolio-weite Rangliste (Health-Score + Stop) |
| Verkaufs-Entscheidung → Strategien-Hub | `app.py:13145` | `_render_sell_strategy_hub` | Single-Ticker Deep-Dive in alle 23 Strategien |
| Verkaufs-Entscheidung → Post-Mortem | `app.py:13147` | `_render_sell_decision_post_mortem` | Historische Auswertung verkaufter Positionen |
| Nach dem Kauf | `app.py:13155` | `_tab_nach_kauf` | Health-Check kurz nach Einstieg |

**Drei davon nutzen dieselbe Engine** (Verkaufskandidaten / Portfolio-Ranking / Nach-Kauf-Check), zeigen aber unterschiedliche Tabellen — das ist die Hauptquelle für „warum sehe ich das dreimal?".

### 1.2 Zwei parallele Empfehlungs-Logiken

Die widersprüchliche Anzeige („Halten" bei Tranche 100 %") entsteht, weil zwei voneinander unabhängige Modelle nebeneinander leben:

| Achse | Quelle | Output | Logik |
|---|---|---|---|
| **A — Technische Signale** | `verkaufs_empfehlung_gesamt()` in `sell_strategies.py:975` | `gesamt_tranche` (0/25/33/50/66/75/100 %) | Themen-Deduplizierung → Summe → Breitenboost → Quantisierung → Bear-Eskalation |
| **B — Health-Score** | `compute_sell_health_score()` in `sell_decision_rules.py:678` | 0–100 Punkte → „Halten/Beobachten/Verkaufen" | Basis 50 ± P&L, MA-Lage, Drawdown, RS, Distribution |

**A** kann eine Position auf 100 % drücken, weil z. B. Strategie 9 (Klimax) + Strategie 18 (RS-Linie) je 50 % liefern.
**B** kann gleichzeitig „Halten" sagen, weil P&L positiv ist und der Kurs > 200-MA notiert.

Dazu kommt: Das aktive **Profil** („konservativ" = 4 Strategien, „standard" = 6, „aktiv_gewinnsicherung" = 9, „frei" = alle 23) führt dazu, dass das System „nur einen Bruchteil der Indikatoren" nutzt — der User sieht aber im Hub trotzdem alle 23. Auch das fühlt sich inkonsistent an.

### 1.3 Strategien-Inventar

23 Strategien (`sell_strategies.py:167–691`), gruppiert in 9 Themen
(`STRATEGY_THEMES`, `sell_strategies.py:11–33`):

`verlust_notbremse, pivot_fail, trendbruch, drawdown, gewinnmitnahme, ueberdehnung, klimax, umkehr, distribution, rs_schwaeche`

Mehrere Strategien gehören zum selben Thema und sind faktisch Varianten desselben Signals (z. B. Strategie 2 / 22 = „Notbremse Verlust"; Strategie 4 / 15 / 19 = „Trendbruch MA"; Strategie 20 / 3 = „Gewinnmitnahme").

---

## 2. Diagnose der drei Beschwerden

| Beschwerde | Ursache |
|---|---|
| „Viele Strategien & Indikatoren" | 23 Strategien × 3–5 Varianten (aggressiv/standard/geduldig) × 11 Warnungen. Im UI wird alles gleichzeitig angeboten, ohne dass klar wäre, welche im aktuellen Profil wirklich zählen. |
| „Halten" aber Tranche 100 % | Health-Score (Spalte „Status") und Tranche % (Spalte „Empf. Tranche") werden **unabhängig** berechnet und im selben Tabellenzeile **gleichberechtigt nebeneinander** dargestellt — ohne Schiedsrichter. |
| „Nur ein Bruchteil wird benutzt" | Das gewählte Profil filtert die Hub-Strategien (z. B. Standard = 6 von 23). Die UI zeigt aber unterschiedlich viele — Strategien-Hub: alle 23, Live-Monitor: alle aus Profil + Warnungen, Verkaufskandidaten: nur Tranche %. Es wird nirgends transparent gemacht, *welche* gerade aktiv sind. |
| „4 Bereiche sind ähnlich" | Verkaufskandidaten (Depot-Tab) + Portfolio-Ranking + Nach-Kauf-Check zeigen dieselben Spalten (Ticker, Health, Tranche, Status) aus derselben Engine. Live-Monitor + Strategien-Hub zeigen dieselbe Strategie-Liste, nur in unterschiedlichem Detailgrad. |

---

## 3. Leitprinzipien des Konzepts

1. **Eine Empfehlung pro Position** — kein Widerspruch zwischen Text-Status und Tranche-%-Zahl.
2. **Eine Ampel, eine Begründung, ein Knopf** — die Kerninformation in 3 Sekunden lesbar.
3. **Strategien sind Diagnose, nicht Empfehlung** — Strategien zeigen *warum*, die Empfehlung ist *was tun*.
4. **Ein Ort pro Frage** —
   - „Was im Depot ist gefährdet?" → **eine** Liste
   - „Warum diese Empfehlung für Ticker X?" → **ein** Drill-Down
   - „War mein Verkauf gut?" → Post-Mortem (bleibt)
5. **Profile entscheiden über Verhalten, nicht über Sichtbarkeit** — alle Strategien werden immer berechnet; das Profil bestimmt nur die Schärfe der Schwellen und das Gewicht.

---

## 4. Konzept Teil A — Einheitliche Empfehlung (Widerspruch auflösen)

### 4.1 Eine Master-Empfehlung mit hierarchischer Logik

Statt Health-Score und Tranche % nebeneinander → **eine** Spalte „Empfehlung" mit 5 möglichen Stufen, die aus beiden Achsen abgeleitet wird:

| Empfehlung | Bedingung | Aktion |
|---|---|---|
| **HALTEN** | Health ≥ 65 **und** Tranche < 25 % | Nichts tun |
| **BEOBACHTEN** | Health 40–64 **und** Tranche < 50 % | Warnung, kein Verkauf |
| **TEIL-VERKAUF (Tranche %)** | Tranche ≥ 25 % **und** Health ≥ 40 | Tranche umsetzen, Rest halten |
| **VERKAUFEN** | Tranche ≥ 75 % **oder** Health < 40 | Komplett raus (Killer-Signal bypasst Health) |
| **AKTION NÖTIG (Konflikt)** | Health ≥ 65 **und** Tranche ≥ 50 % | Explizit gekennzeichnet: „Technische Signale stark, Fundamentaldaten gut — Entscheidung User" |

→ Damit gibt es **keine** stillen Widersprüche mehr. Der Konflikt-Fall wird sichtbar gemacht statt versteckt.

### 4.2 Welche Achse gewinnt wann (Schiedsrichter-Regeln)

Reihenfolge der Prüfung in `evaluate_sell_decision()`:

1. **Killer-Signal**: Falls eine Strategie isoliert 100 % liefert (z. B. Strategie 16 „Drei Verlustwochen mit Vol", Strategie 14 „Pivot-Fail 7 % Notbremse") → **VERKAUFEN** unabhängig vom Health-Score.
2. **Health-Veto bei Halten**: Falls Health ≥ 80 **und** Tranche < 50 % → reduziere die angezeigte Tranche auf 0 % (verhindert Fehlalarme bei klar gesunden Positionen).
3. **Health-Bestätigung bei Verkauf**: Falls Health < 30 **und** Tranche < 25 % → erhöhe Empfehlung auf „BEOBACHTEN" (Health-Score erkennt Schwäche, die Strategien noch nicht).
4. **Normalfall**: Tranche % und Health-Score bestätigen sich gegenseitig → eine der ersten vier Stufen.

### 4.3 Transparenz „welche Strategien zählen gerade"

In der Detail-Ansicht **immer** drei Listen:
- **Aktive Strategien (aus Profil)**: z. B. „6 von 23 — Standard-Profil"
- **Davon ausgelöst**: konkrete Treffer mit jeweiliger Tranche
- **Inaktiv aber relevant**: Strategien außerhalb des Profils, die ausgelöst hätten — als „Hinweis ohne Wirkung" mit einem Klick „in Profil aufnehmen".

→ Damit beantwortet das UI selbst die Frage „warum nur ein Bruchteil?".

---

## 5. Konzept Teil B — Strategien-Vereinfachung

### 5.1 Verdichtung: 23 → 9 Themen-Strategien

Statt 23 Strategien einzeln zu führen, exponiere im UI **nur die 9 Themen**. Pro Thema läuft intern die jeweils beste Variante (genau das macht die Themen-Deduplizierung in `sell_strategies.py:1114` heute schon — aber nur intern). Der User sieht:

| Thema (sichtbar) | Bündelt heute (Strategien intern) |
|---|---|
| 🛑 Notbremse Verlust | 2, 22 |
| 📉 Drawdown vom Peak | 5, 17 |
| 🔻 Trendbruch (MA) | 4, 15, 19 |
| 🎯 Gewinnmitnahme | 3, 20 |
| 🚀 Überdehnung (MA-Abstand) | 6 |
| 💥 Klimax / Erschöpfung | 9, 10, 11 |
| 🔄 Umkehr / Reversal | 12, 21 |
| 📊 Distribution | 7, 13, 16 |
| 📈 Relative Stärke schwach | 18 |
| ⚓ Pivot-Bruch | 14 |

**Hinter jeder Themen-Karte**: eine Akkordeon-Klappe „Details / Variante wählen", falls Power-User justieren wollen.

### 5.2 Profile neu definieren — als Empfindlichkeit, nicht als Strategiefilter

Aktuell schalten Profile Strategien ein/aus (`konservativ` = 4 Strategien). Besser:

- **Alle 9 Themen sind immer aktiv.**
- Das Profil regelt nur die **Schwellen pro Thema** (z. B. Drawdown-Schwelle 8 % konservativ → 5 % aktiv) und die **Aggregations-Strenge** (Breitenboost ab 4 vs. 3 Themen).
- Das Profil-Label wird zur **Risikohaltung**: „Halten bis es weh tut" / „Mittelweg" / „Früh aussteigen, Gewinne sichern".

→ Damit verschwindet die Frage „warum sind nur 6 von 23 aktiv?" — alle laufen immer, das Profil bestimmt nur die Lautstärke.

### 5.3 Warnungen vs. Strategien klar trennen

Heute: 11 Warnungen leben halb separat (`warnungen`-Liste in den Profilen, `app.py:10946`). Vorschlag:

- **Strategien** → können Tranche-Prozent liefern (= harte Empfehlung)
- **Warnungen** → nur Beobachtungssignale, **niemals** Tranche-wirksam (= weicher Hinweis im Detail-Drawer)

Das ist heute zwar so gedacht, in der UI aber nicht visuell getrennt. → Eigener Tabreiter / eigene Farbe.

---

## 6. Konzept Teil C — Bereiche konsolidieren

### 6.1 Von 5+1 Screens auf 3

| Heute | Künftig | Begründung |
|---|---|---|
| Depot → Verkaufskandidaten | **bleibt** als „Kurz-Cockpit" im Depot-Tab | Mein-Depot-Nutzer wollen Status direkt sehen |
| Verkaufs-Entscheidung → Portfolio-Ranking | **wird zu** Verkaufs-Entscheidung → **Übersicht** (1 Tab) | Identische Daten wie Verkaufskandidaten, aber mit Sortierung/Filter |
| Verkaufs-Entscheidung → Live-Monitor | **wird zu** Verkaufs-Entscheidung → **Position** (Drill-Down beim Klick auf Zeile) | Hat heute schon den Setup-Panel; perfekt als Detail-Ansicht |
| Verkaufs-Entscheidung → Strategien-Hub | **entfällt als eigener Tab**, wandert als Akkordeon „📚 Regelwerk anzeigen" in die Position-Detail-Ansicht | Doppelt zum Live-Monitor; nur sinnvoll für Lernen/Lookup |
| Verkaufs-Entscheidung → Post-Mortem | **bleibt** | Klar abgegrenzter Use-Case (Rückblick) |
| Nach dem Kauf (eigener Tab) | **wird zu** Detail-Modus „Kürzlich gekauft" in der Position-Ansicht | Heute schon dieselbe Engine; Kontext = Kaufdatum < 30 Tage |

**Endergebnis: 3 sichtbare Bereiche** für Verkaufslogik:

1. **Übersicht** (Portfolio-Tabelle, eine Zeile = eine Position, sortierbar nach Empfehlung)
2. **Position** (Drill-Down: ein Ticker, alle Details, Setup, Regelwerk-Akkordeon)
3. **Post-Mortem** (Rückblick auf Verkäufe)

Plus: die **Kurz-Karte** „Verkaufskandidaten" im Depot-Tab bleibt als Einstiegspunkt, **verlinkt** aber auf die Übersicht.

### 6.2 UI-Skizze „Übersicht"

```
┌─ Verkaufs-Entscheidung · Übersicht ──────────────────────────┐
│ Profil: [Standard ▾]  Filter: [alle ▾]   🔄 Neu berechnen   │
├──────────────────────────────────────────────────────────────┤
│ Ticker │ P&L%  │ Empfehlung           │ Health │ Trigger    │
│ NVDA   │ +25%  │ 🟢 HALTEN            │  82    │ —          │
│ AAPL   │ +8%   │ 🟡 BEOBACHTEN        │  58    │ Distrib.×3 │
│ MRVL   │ -4%   │ 🟠 TEIL-VERKAUF 33%  │  45    │ 21-EMA     │
│ ASML   │ -9%   │ 🔴 VERKAUFEN         │  28    │ Drawdown   │
│ AMD    │ +15%  │ ⚠ KONFLIKT           │  78    │ Klimax+RS  │
└──────────────────────────────────────────────────────────────┘
Klick auf Zeile → öffnet Position-Detail-Drawer rechts
```

### 6.3 UI-Skizze „Position-Detail"

```
┌─ AMD — Position-Detail ──────────────────────────────────────┐
│ ⚠ KONFLIKT: Health 78 (Halten) ↔ Tranche 50% (Klimax + RS)  │
│ [Tranche 50% buchen]  [Ignorieren · 7 Tage]  [Profil ändern]│
├──────────────────────────────────────────────────────────────┤
│ Trigger heute (2 von 9 Themen):                              │
│   💥 Klimax / Erschöpfung      → 25% (Strat 9)              │
│   📈 RS-Linie schwach          → 25% (Strat 18)             │
│                                                              │
│ Inaktive Themen (5 still): MA-Bruch ok, Drawdown ok, ...    │
│ Hinweise (Warnungen): Closing-Range tief                     │
│                                                              │
│ ▸ Setup & Overrides (Pivot, Split, Variante 21-EMA)         │
│ ▸ 📚 Regelwerk (alle 23 Buch-Strategien nachlesen)          │
│ ▸ Verlauf der letzten 30 Tage                                │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. Migrationspfad (in Phasen, jede einzeln nutzbar)

### Phase 1 — „Stop-the-Bleeding" (klein, hohe Wirkung)
- In `_build_sell_candidates_table` und Portfolio-Ranking-Tabelle die Spalten **„Empfehlung" und „Status" zusammenführen** zu einer einzigen Spalte „Empfehlung" mit den 5 Stufen aus Abschnitt 4.1.
- Konflikt-Fall (Health hoch + Tranche hoch) explizit als „⚠ KONFLIKT" anzeigen.
- Aufwand: ~1 Tag. Berührt nur Anzeige-Funktionen, keine Engine.

### Phase 2 — Transparenz
- In der Detail-Ansicht (Live-Monitor) eine **Box „Aktive Themen / Inaktive Themen / Warnungen"** ergänzen.
- Profil-Auswahl mit Tooltip „Standard-Profil: 6 von 9 Themen aktiv".
- Aufwand: ~1–2 Tage.

### Phase 3 — Strategien thematisch bündeln
- Im Strategien-Hub UI-seitig die Akkordeon-Struktur „9 Themen → Varianten" einführen (kein Logik-Umbau, nur Gruppierung).
- Profile als Empfindlichkeits-Slider pro Thema neu modellieren (`sell_decision_rules.py:86–142`).
- Aufwand: ~3–5 Tage.

### Phase 4 — Bereiche konsolidieren
- Live-Monitor und Portfolio-Ranking zu einem Tab „Übersicht + Detail-Drawer" zusammenführen.
- Strategien-Hub und Nach-Kauf-Check als Modi in die Detail-Ansicht überführen.
- Aufwand: ~5–8 Tage (große UI-Umstellung, optional).

→ Phase 1 + 2 alleine lösen bereits beide Hauptbeschwerden („Widerspruch" und „Bruchteil sichtbar"). Phase 3 + 4 sind die langfristige Aufräumung.

---

## 8. Risiken & offene Fragen

- **Phase 4 ist invasiv**: Streamlit-Tabs sind tief mit `_render_*`-Funktionen verzahnt; Zusammenführung erfordert Drawer-Pattern oder zweistufige Seite. Bewertung: erst nach Phase 1–3 entscheiden.
- **Profil-Migration**: Bestehende User mit gespeichertem Profil-Namen müssen kompatibel bleiben. Mapping „alte Strategien-Liste → neue Schwellen-Profile" ist sauber zu validieren.
- **Killer-Signal-Definition**: Heute liefert die 100 %-Erkennung in `verkaufs_empfehlung_gesamt()` (line 1109) automatisch komplett-raus. Soll das auch bei sehr hohem Health-Score gelten? **Empfehlung: ja**, aber im UI mit „Override anbieten" für mind. 24h.
- **Health-Veto bei „Halten"** (Abschnitt 4.2 Regel 2): risk, dass legitime frühe Tranche-Empfehlungen unterdrückt werden. → Initial nur bei Health ≥ 80 (sehr hohe Schwelle), nicht bei ≥ 65.

---

## 9. Erfolgskriterien

Konzept ist erfolgreich umgesetzt, wenn:

- [ ] In keiner Tabelle steht jemals „Halten" neben Tranche ≥ 50 % ohne Konflikt-Markierung.
- [ ] Der User kann in der Übersicht in < 3 Sekunden sehen, was er heute tun soll.
- [ ] Im Detail-Drawer ist erkennbar, **warum** (welche Themen, welche Strategien) und **welche** anderen Themen ruhig sind.
- [ ] Es gibt höchstens 3 Tabs/Bereiche, die nach „Verkauf" aussehen — keine doppelten Tabellen mehr.
- [ ] Profile sind als Risiko-Haltung verstanden, nicht als „4 von 23 sind aktiv".
