# Mammotion Cloud Status Notes

Stand: 2026-05-15

Diese Notiz sammelt die logischen Punkte aus der aktuellen Mammotion-HA-Analyse:
was wir als beobachtet behandeln, was daraus nur abgeleitet ist, und welche
Tests noch fehlen.

## Zielbild

Die Integration muss ohne dauerhafte BLE-Verbindung zuverlässig genug erkennen:

- wenn der Mäher startet, pausiert, zurückkehrt, andockt oder wieder lädt
- wenn ein Mähvorgang wegen Laden unterbrochen ist und danach weitergehen soll
- wenn ein Mähvorgang wirklich beendet ist, damit Automationen reagieren können
- ohne das knappe Cloud/MQTT-Sendebudget unnötig zu verbrennen

BLE ist in der konkreten Installation kein verlässlicher Primärpfad: Home
Assistant ist zu weit weg, die Dock steht geschützt in einer geschlossenen
Gartenhütte. Die Cloud-Strecke muss also als Hauptpfad funktionieren.

## Gesicherte Beobachtungen

- Die Integration verwendet `pymammotion==0.7.109` und aktuell Version `0.5.55`.
- Der echte HA-Loginpfad setzt `MammotionClient(ha_version=integration.version.split("-")[0])`.
- Direktes `MammotionHTTP()` ohne `ha_version` sendet einen `ALIYUN DEMO,...`
  App-Version-Header; Mammotion beantwortet das mit `Access denied`.
- Derselbe Login mit HA-artigem App-Version-Header (`HA,2.0.5.55` oder
  `HA,2.2026.5.0`) ist erfolgreich.
- `Access denied` ist deshalb bei lokalen Login-Tests nicht automatisch ein
  Passwort- oder Accountfehler. Der Test muss den HA-Header nachbilden.
- Der separate Testaccount aus den Env-Vars kann sich anmelden, sieht über die
  geprüften Device-Endpunkte aber aktuell 0 Geräte. Das ist ein Account/Share-
  oder Testdaten-Thema und nicht derselbe Befund wie ein Loginfehler.
- In der laufenden HA-Instanz existieren Mammotion-Entities; der Mäher war beim
  letzten Check als `paused` sichtbar.

## Status-Update-Modell

MQTT verhält sich hier praktisch nicht wie ein vollständiger, dauerhaft
aktiver Zustandsbus. Die bisherige Live-Beobachtung spricht eher für:

- passives MQTT-Abonnieren allein liefert nicht zuverlässig zeitnahe Statusdaten
- ein gezielter Report-Request kann innerhalb weniger Sekunden frische Daten liefern
- ein Report-Stream kann während aktiver Phasen laufende Updates auslösen
- das Öffnen der Mammotion-App kann Updates anstoßen, vermutlich weil die App
  selbst Report-/Stream-Anfragen auslöst

Ein MQTT-Broker hat zwar Pub/Sub-Charakter, aber es gibt hier keinen sicheren
"Topic-Browse", der alle künftigen/retained Statusquellen beweist. Wildcard-
Subscriptions sehen nur passende Publikationen, sobald sie tatsächlich kommen.

## Aktuelle Logik in unserer Integration

Die aktuelle Report-Logik ist bewusst budgetschonend:

- aktive Zustände: `MODE_WORKING`, `MODE_RETURNING`, `MODE_CHARGING_PAUSE`
- aktiver Cloud-Report-Stream: 30 Minuten, Erneuerung nach 25 Minuten
- aktiver Snapshot-Takt: 30 Sekunden
- Idle-Snapshot-Takt: 15 Minuten
- Docked-Snapshot-Takt: 60 Minuten
- Job-Watch: bis zu 4 Stunden, solange ein unfertiger Job plausibel ist
- normale Pause abseits der Dock: nur 5 Minuten Grace, danach Stream stoppen
- Fehlerzustände wie Lock, Location Error, Boundary Jump: keine Dauerstreams

Die Logik unterscheidet grob:

- `working` / `returning`: schnell beobachten
- `charging_pause` oder Dock/Laden mit unfertigem Job: weiter beobachten
- `pause` abseits der Dock: kurz beobachten, dann stoppen
- Dock/Ready ohne unfertigen Job: langsam beobachten
- Fehlerzustände: sparsam bleiben, eher Snapshot als Stream
- Fehler im Feld: minimaler Keepalive per Snapshot, kein Dauerstream

Die konkrete Statusmatrix liegt in `custom_components/mammotion/report_policy.py`.
Der Coordinator extrahiert nur noch die relevanten Report-Felder und delegiert
die Entscheidung an diese Policy. Dadurch ist die Matrix separat testbar.

## Bisherige Patches

Die Integration patcht lokal mehrere `pymammotion`-Kanten:

- Cloud-Sends bekommen keinen BLE-Sync-Prefix mehr.
- Zusätzliche MQTT-Realtime-Topics werden registriert.
- `/sys/proto/...` und `down_raw` werden korrekt geroutet.
- Auth-/Rate-Limit-Zustände werden defensiver behandelt.
- Report-Streams laufen bei Recharge-Pause/unfertigem Job weiter.
- Manuelle HA-Services existieren:
  - `mammotion.request_report`
  - `mammotion.start_report_stream` mit `duration_seconds`

## Wahrscheinlichste Erklärungen für die Symptome

1. Fehlendes oder zu spätes Report-Streaming in genau den kritischen Übergängen.
   Besonders relevant: Ende des Mähens, Rückkehr zur Dock, Ladepause, Fortsetzung
   nach Ladepause.

2. Mammotion sendet Status nicht als echten Dauer-Push, sondern erst nach
   Request/Stream-Aktivierung. Das würde erklären, warum das Öffnen der App
   manchmal auch HA "belebt".

3. Der interne HA-Lawn-Mower-State kann stale sein. Wenn Aktionen zu stark auf
   diesen State vertrauen, werden gültige Kommandos oder Automationen blockiert,
   obwohl der Roboter real in einem anderen Zustand ist.

4. Einige Zustandsfelder ändern sich auch an der Dock, obwohl der Mäher physisch
   steht. Das kann aus nichtkritischen Report-Feldern, transienten MQTT-Messages
   oder aus doppelten Updatepfaden entstehen. Die Entprellung muss daher eher auf
   semantische Zustände achten als auf "irgendein Reportfeld hat sich geändert".

5. Login ist aktuell nicht die Hauptursache. Der zuvor gesehene `Access denied`
   war ein lokaler Testartefakt ohne HA-App-Version-Header.

## Offene Fragen

- Sieht der neue Testaccount die geteilten Geräte wirklich in der Mammotion-App,
  und ist es derselbe Account wie in den Env-Vars?
- Welche MQTT-Topics kommen beim echten Undock/Dock/Ende-Mähen tatsächlich an?
- Ist ein zusätzliches "watch until terminal state"-Service sinnvoller als ein
  pauschaler Stream mit fixer Dauer?
- Welche Zustände zählen für die Rollo-Automation als terminal?
  Vermutlich: wirklich gedockt/charging/ready nach Rückkehr, aber nicht
  `MODE_CHARGING_PAUSE` mit unfertigem Job.
- Können wir Kommandos wie Return-to-Dock noch stärker best-effort machen, ohne
  gefährliche Doppelkommandos oder falsche Pausen zu erzeugen?

## Naechste sinnvolle Tests

- Bei Undock/Dock einmal gezielt `mammotion.start_report_stream` auslösen und
  messen, nach wie vielen Sekunden `lawn_mower` und relevante Sensoren umspringen.
- Während eines echten Mähendes Debug-Logs für Report-Stream Start/Stop,
  `state_changed`, `sys_status`, `charge_state`, `bp_info`, Fortschritt und
  MQTT-Topic-Namen erfassen.
- Testaccount/Share getrennt prüfen: Login erfolgreich reicht nicht, wenn die
  Device-Listen leer bleiben.
- Falls die App frische Updates auslöst: parallel App öffnen und prüfen, ob HA
  danach dieselben MQTT-/Reportpfade sieht oder nur neue Snapshots bekommt.

## Moegliche naechste Code-Ideen

- Einen expliziten Watch-Service ergänzen, der für eine Automation gezielt bis
  zu einem terminalen Zustand streamt, statt dauerhaft zu pollen.
- Bei kritischen Commands nach dem Senden automatisch einen kurzen Watch/Stream
  aktivieren.
- Mehr rate-limitierte, aber verständliche Warning-Logs für:
  - Cloud-Snapshot übersprungen
  - Stream gestartet/erneuert/gestoppt
  - State bleibt stale trotz Request
  - Transport rate-limited
- Eine Diagnostikfläche ergänzen: Sendebudget, letzter Report, letzter Stream,
  letzter MQTT-Topic-Typ, letzter semantischer Statuswechsel.

## Entscheidung: Fehler-im-Feld-Keepalive

Wenn der Mäher mit einem Fehler nicht an der Ladestation steht, ist ein
Dauerstream zu teuer und riskant für das Sendebudget. Ein einzelner Report-
Snapshot ist dagegen das kleinste bekannte Cloud-Signal, das den Status
auffrischen und den Mäher eher wach halten kann.

Umgesetzt als konservativer Modus:

- `sys_status` in `MODE_LOCK`, `MODE_LOCATION_ERROR`, `MODE_BOUNDARY_JUMP`
- plus `charge_state == 0`, also nicht erkennbar ladend
- Snapshot-Kadenz: 10 Minuten
- kein Report-Stream
- vorhandener Sendebudget-Schutz bleibt aktiv

Budgetrechnung: 10 Minuten entsprechen maximal 144 Sends pro 24 Stunden, bevor
andere Aktionen dazukommen. Mit der bestehenden Reserve ist das deutlich
vorsichtiger als 5 Minuten, aber aktiver als der normale 15-Minuten-Idle-Pfad.
