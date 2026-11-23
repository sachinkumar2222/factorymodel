# Factory Model: HTTP Version

**ESP32 · Node.js backend · SQLite history · Digital Twin · OTA updates**

![Status](https://img.shields.io/badge/status-deployed-brightgreen)
![Firmware](https://img.shields.io/badge/firmware-factory--http--ota--v1.1.0-blue)
![Platform](https://img.shields.io/badge/platform-ESP32-lightgrey)

A working miniature factory demonstration model that is monitored and controlled over a **local HTTP architecture**. The original ThingsBoard-oriented ESP32 communication was migrated to a Node.js backend running on a Mini PC, so the whole system runs on the factory LAN with **no cloud or Internet dependency**.

The Factory ESP32 reports telemetry and executes commands. The Mini PC backend provides control and monitoring APIs, live WebSocket updates, a browser dashboard, Digital Twin display pages, and SQLite history for power and production.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Network Configuration](#network-configuration)
- [Repository Layout](#repository-layout)
- [Installation](#installation)
- [Backend Service](#backend-service)
- [API Reference](#api-reference)
- [Digital Twin Display URLs](#digital-twin-display-urls)
- [Dashboard](#dashboard)
- [Historical Data](#historical-data)
- [Heater Safety](#heater-safety)
- [OTA Firmware Update](#ota-firmware-update)
- [Camera Integration](#camera-integration)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
Digital Twin ──HTTP──► Factory Backend ──HTTP──► Factory ESP32
                       (Mini PC, Node.js :4000)   (sensors / actuators / safety / OTA)
                              │
                              ├── WebSocket ─► Dashboard / browser
                              ├── iframe display URLs ─► Digital Twin
                              └── SQLite ─► historical Power and Production APIs

Factory ESP32 ──HTTP──► Mini PC camera service (:8081)
```

| Layer | Component | Address / Port | Role |
|-------|-----------|----------------|------|
| Visualization | Digital Twin | Project-specific | Operator visualization, controls and iframe monitoring |
| Backend | Factory Mini PC | `10.10.12.66:4000` | HTTP API, state, commands, history, WebSocket, display routes |
| Camera | Mini PC camera services | `10.10.12.66:8081` | Camera service / recording integration |
| Controller | Factory ESP32 | `10.10.12.75` | Sensors, actuators, safety/control logic, telemetry and OTA |
| Database | SQLite | `factory_model.db` | Telemetry and historical power/production storage |

## Features

- Local **HTTP control and telemetry** between ESP32 and backend
- **Live WebSocket updates** to the dashboard and browser
- **Digital Twin integration** through control APIs and iframe display URLs
- **SQLite history** with 1H / 1D / 1W / 1M / 1Y charts
- **Heater automatic cutoff** at 33°C
- **OTA firmware updates** for the ESP32
- **Persistent product count** and lifetime count (NVS)
- Device-key authentication, static IP, and automatic backend restart (systemd)

### Monitored items

Cooling Tower 1 and 2 (temperature, humidity) · switchyard temperature · lighting power and conveyor power (INA226) · total power · product count and lifetime count · laser zones 1–4 · Person 1 / Person 2 servos · camera status · system health

### Controlled items

Conveyor · lighting · mist 1 · mist 2 · heater · Person 1 and Person 2 servos

| Person | Home | Move |
|--------|------|------|
| Person 1 | 180° | 0° |
| Person 2 | 0° | 150° |

## Network Configuration

| Device | Address | Purpose |
|--------|---------|---------|
| Factory ESP32 | `10.10.12.75/24` | Static controller address |
| Gateway | `10.10.12.1` | Factory LAN gateway |
| DNS | `10.10.12.1`, `8.8.8.8` | Name resolution |
| Mini PC (Ethernet) | `10.10.12.66/24` | Primary backend LAN address |
| Mini PC (Wi-Fi) | `10.10.12.121/24` | Secondary interface |
| Backend | `http://10.10.12.66:4000` | Factory HTTP backend |
| Camera | `http://10.10.12.66:8081` | Camera service |

> Adjust these addresses to match your own network. Keep the ESP32 on the private factory LAN.

## Repository Layout

Backend location on the Mini PC (`~/FactoryModel_HTTP_Backend/`):

```
FactoryModel_HTTP_Backend/
├── server.js            # Node.js HTTP backend
├── public/
│   └── index.html       # Monitoring / control dashboard UI
├── package.json
├── package-lock.json
└── factory_model.db     # SQLite history database
```

ESP32 firmware release: **`factory-http-ota-v1.1.0`**

## Installation

### 1. Backend (Mini PC)

```bash
cd ~/FactoryModel_HTTP_Backend
npm install
node server.js          # test run, listens on port 4000
```

### 2. ESP32 firmware

Flash `factory-http-ota-v1.1.0` to the Factory ESP32 **by USB** the first time. Subsequent updates can use [OTA](#ota-firmware-update).

### 3. Verify

Open the dashboard at `http://10.10.12.66:4000/` and check `GET /api/health`.

## Backend Service

The backend runs as a systemd service and restarts automatically after network availability and reboots.

| Item | Value |
|------|-------|
| Service | `factory-model-http.service` |
| Working directory | `/home/server/FactoryModel_HTTP_Backend` |
| ExecStart | `/usr/bin/node /home/server/FactoryModel_HTTP_Backend/server.js` |
| Port | `4000` |

```bash
sudo systemctl status factory-model-http.service
sudo systemctl restart factory-model-http.service
journalctl -u factory-model-http.service -f
```

Existing camera services (`factory-camera-web.service`, `factory-camera.service`) are separate and were left untouched.

## API Reference

### Core

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/health` | GET | Backend health and device online state |
| `/api/state` | GET | Current combined state |
| `/api/device/:id` | GET | Device information |
| `/api/device/telemetry` | POST | Receive device telemetry |
| `/api/device/:id/command` | POST | Send a device command |
| `/ws` | WebSocket | Live state updates to dashboard/browser |

### Control (all `POST`)

| Actuator | Endpoints |
|----------|-----------|
| Conveyor | `/api/control/conveyor/on` · `/api/control/conveyor/off` |
| Lighting | `/api/control/lighting/on` · `/api/control/lighting/off` |
| Mist 1 | `/api/control/mist1/on` · `/api/control/mist1/off` |
| Mist 2 | `/api/control/mist2/on` · `/api/control/mist2/off` |
| Heater | `/api/control/heater/on` · `/api/control/heater/off` |
| Person 1 | `/api/control/person1/home` · `/0` · `/180` |
| Person 2 | `/api/control/person2/home` · `/0` · `/150` |

Example:

```bash
curl -X POST http://10.10.12.66:4000/api/control/conveyor/on
```

### Monitoring (all `GET`)

| Endpoint | Data |
|----------|------|
| `/api/monitor/all` | All monitoring / state data |
| `/api/monitor/temperature` | Temperature data |
| `/api/monitor/cooling-tower-1` · `/cooling-tower-2` | Cooling tower data |
| `/api/monitor/lighting-power` · `/conveyor-power` · `/power` | Power data |
| `/api/monitor/product-count` | Product count |
| `/api/monitor/laser/1` … `/laser/4` | Laser zones |
| `/api/monitor/person1` · `/person2` | Servo / person state |
| `/api/monitor/camera` | Camera |

## Digital Twin Display URLs

Single-value display pages for embedding as iframes (base: `http://10.10.12.66:4000/display/`):

| Display | Path |
|---------|------|
| Switchyard temperature | `switchyard-temp` |
| Cooling Tower 1 temperature / humidity | `cooling-tower-1-temp` · `cooling-tower-1-humidity` |
| Cooling Tower 2 temperature / humidity | `cooling-tower-2-temp` · `cooling-tower-2-humidity` |
| Lighting / conveyor / total power | `lighting-power` · `conveyor-power` · `total-power` |
| Product count / lifetime count | `product-count` · `lifetime-product-count` |
| Conveyor / lighting status | `conveyor-status` · `lighting-status` |
| Mist 1 / mist 2 / heater status | `mist1-status` · `mist2-status` · `heater-status` |
| Laser zones 1–4 | `laser-1` … `laser-4` |
| Person 1 / Person 2 | `person1` · `person2` |
| Camera | `camera` |

## Dashboard

The dashboard (`public/index.html`) shows live temperature/humidity, power, product counts, actuator status and controls, laser zones, camera status and system health, plus the historical **Power** (conveyor, lighting, total) and **Production** (total count) graphs with ranges **1H, 1D, 1W, 1M, 1Y**.

Charts use backend SQLite history, not browser `localStorage`.

## Historical Data

Database: `~/FactoryModel_HTTP_Backend/factory_model.db`

| Table | Purpose |
|-------|---------|
| `telemetry_history` | Raw recent telemetry samples (power and production) |
| `hourly_history` | Hourly power averages and production totals |
| `daily_history` | Daily power averages/maxima and production totals |

## Heater Safety

The firmware includes an automatic heater cutoff:

- Heater **ON** and temperature **≥ 33°C** → heater turns **OFF**

## OTA Firmware Update

OTA updates only the **ESP32 firmware**. It does not update the Mini PC backend, dashboard or SQLite database.

| Endpoint | Method |
|----------|--------|
| `/ota` | GET |
| `/api/ota/status` | GET |
| `/api/ota/update` | POST |

Recommended procedure:

1. Build the new ESP32 firmware image.
2. Keep the known-good firmware as the recovery baseline.
3. Copy the new image to the Mini PC securely.
4. Verify the Factory ESP32 is reachable and healthy.
5. Check OTA status, submit the update, and let the ESP32 write and reboot.
6. Verify firmware version, health, telemetry, controls and persistent product count.
7. If OTA fails, use **USB recovery** with the known-good firmware.

## Camera Integration

The existing camera services on the Mini PC are unchanged. The ESP32 can trigger the camera over HTTP (`http://10.10.12.66:8081`), and camera status is shown in the dashboard and Digital Twin display.

## Security

- Keep the ESP32 on the private factory LAN; do not expose it to the public Internet.
- Do not publish device keys or Wi-Fi credentials. Keep them out of this repository.
- Use authentication for device and backend operations.
- Use secure remote access or VPN for remote administration.
- Restrict OTA to authorised maintenance personnel.
- Back up the known-good firmware before OTA.
- Keep a record of firmware versions and deployment dates.

## Troubleshooting

| Symptom | First checks |
|---------|--------------|
| Backend unavailable | Check `factory-model-http.service`, port 4000, Mini PC IP |
| ESP32 offline | Check ESP32 power, IP `10.10.12.75`, LAN and firmware |
| No history data | Check ESP32 telemetry POST and `factory_model.db` |
| Iframe blank | Check route, browser developer tools, returned HTML/CSS, backend reachability |
| Controls not responding | Check backend, ESP32 connectivity, route and authentication |
| Camera issue | Check camera services and port 8081 |
| OTA issue | Verify health, firmware compatibility; use USB recovery if required |

> The backend may report `online: false` while the ESP32 is not running the HTTP firmware (for example, if it still runs the earlier ThingsBoard firmware). This is expected.

## Documentation

Full technical documentation (architecture, deployment, validation checklists, software freeze/backup) is included in this repository.
