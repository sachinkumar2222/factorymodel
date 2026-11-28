require("dotenv").config();

const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const WebSocket = require("ws");
const Database = require("better-sqlite3");

// ============================================================
// SQLite historical database
// ============================================================

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "factory_model.db");
const dbDir = path.dirname(DB_PATH);
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
}
const db = new Database(DB_PATH);

// Improve reliability for concurrent telemetry + dashboard reads
db.pragma("journal_mode = WAL");
db.pragma("busy_timeout = 5000");

// Raw telemetry history
db.exec(`
  CREATE TABLE IF NOT EXISTS telemetry_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    lighting_power_w REAL,
    conveyor_power_w REAL,
    total_power_w REAL,
    product_count INTEGER,
    product_count_total INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_telemetry_history_timestamp
  ON telemetry_history(timestamp);

  CREATE TABLE IF NOT EXISTS hourly_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hour_bucket INTEGER NOT NULL UNIQUE,
    lighting_power_avg_w REAL,
    conveyor_power_avg_w REAL,
    total_power_avg_w REAL,
    production_count_total INTEGER,
    sample_count INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_hourly_history_bucket
  ON hourly_history(hour_bucket);

  CREATE TABLE IF NOT EXISTS daily_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day_date TEXT NOT NULL UNIQUE,
    lighting_power_avg_w REAL,
    lighting_power_max_w REAL,
    conveyor_power_avg_w REAL,
    conveyor_power_max_w REAL,
    total_power_avg_w REAL,
    total_power_max_w REAL,
    production_count_total INTEGER,
    sample_count INTEGER
  );

  CREATE INDEX IF NOT EXISTS idx_daily_history_date
  ON daily_history(day_date);

  -- Industrial Batch Management (MES Execution)
  CREATE TABLE IF NOT EXISTS batch_management (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL UNIQUE,
    product_name TEXT DEFAULT 'Standard Unit',
    target_units INTEGER DEFAULT 100,
    start_total_count INTEGER NOT NULL,
    current_output INTEGER DEFAULT 0,
    status TEXT DEFAULT 'RUNNING', -- RUNNING, PAUSED, COMPLETED
    start_time TEXT NOT NULL,
    end_time TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_batch_status
  ON batch_management(status);
`);

console.log(`[SQLITE] Database ready: ${DB_PATH}`);

// Preload last recorded values from DB so dashboard shows real history even before first telemetry
try {
  const lastRow = db.prepare("SELECT * FROM telemetry_history ORDER BY id DESC LIMIT 1").get();
  if (lastRow) {
    console.log(`[SQLITE] Preloaded last telemetry: Lifetime=${lastRow.product_count_total}, Power=${lastRow.total_power_w}W`);
    if (deviceState.sensors) {
      if (lastRow.product_count_total !== undefined) deviceState.sensors.productCountTotal = lastRow.product_count_total;
      if (lastRow.product_count !== undefined) deviceState.sensors.productCount = lastRow.product_count;
    }
  }
} catch (e) { }

// Prepared statement for telemetry history
const insertTelemetryHistory = db.prepare(`
  INSERT INTO telemetry_history (
    timestamp,
    lighting_power_w,
    conveyor_power_w,
    total_power_w,
    product_count,
    product_count_total
  )
  VALUES (
    @timestamp,
    @lightingPowerW,
    @conveyorPowerW,
    @totalPowerW,
    @productCount,
    @productCountTotal
  )
`);

function saveTelemetryHistory(state) {
  try {
    // Only save genuine ESP32 telemetry. The initial/default deviceState
    // must never create a fake history row.
    if (!state || !state.online || !state.sensors) {
      return false;
    }

    const sensors = state.sensors;

    const lightingPowerW = Number(sensors.lightingPowerW);
    const conveyorPowerW = Number(sensors.conveyorPowerW);
    const totalPowerW = Number(sensors.powerW);
    const productCountTotal = Number(sensors.productCountTotal);
    const productCount = Number(sensors.productCount);

    // Require the core historical values to be real numbers.
    if (
      !Number.isFinite(lightingPowerW) ||
      !Number.isFinite(conveyorPowerW) ||
      !Number.isFinite(totalPowerW) ||
      !Number.isFinite(productCountTotal)
    ) {
      return false;
    }

    insertTelemetryHistory.run({
      timestamp: Date.now() / 1000,
      lightingPowerW,
      conveyorPowerW,
      totalPowerW,
      productCount: Number.isFinite(productCount) ? productCount : null,
      productCountTotal
    });

    // Update active running batch output dynamically & auto-stop conveyor when target is reached
    try {
      const activeBatch = db.prepare("SELECT * FROM batch_management WHERE status = 'RUNNING' ORDER BY id DESC LIMIT 1").get();
      if (activeBatch && productCountTotal >= activeBatch.start_total_count) {
        const produced = productCountTotal - activeBatch.start_total_count;
        db.prepare("UPDATE batch_management SET current_output = ? WHERE id = ?").run(produced, activeBatch.id);

        if (activeBatch.target_units > 0 && produced >= activeBatch.target_units) {
          console.log(`[MES BATCH] Target reached: ${produced}/${activeBatch.target_units} pcs. Auto-stopping conveyor.`);
          db.prepare("UPDATE batch_management SET status = 'COMPLETED', end_time = ? WHERE id = ?").run(new Date().toISOString(), activeBatch.id);
          if (typeof setConveyorState === "function") {
            setConveyorState(false);
          }
        }
      }
    } catch (bErr) { }

    return true;
  } catch (error) {
    console.error("[SQLITE] Failed to save telemetry:", error.message);
    return false;
  }
}

/* ============================================================
   HISTORY AGGREGATION / RETENTION
   ============================================================ */

const RAW_RETENTION_DAYS = 7;
const RAW_RETENTION_SECONDS = RAW_RETENTION_DAYS * 24 * 60 * 60;

function refreshHourlyHistory() {
  try {
    const now = Math.floor(Date.now() / 1000);
    const currentHour = Math.floor(now / 3600) * 3600;
    const start = currentHour - (7 * 24 * 3600);

    const rows = db.prepare(`
      SELECT
        CAST(timestamp / 3600 AS INTEGER) * 3600 AS hour_bucket,
        AVG(lighting_power_w) AS lighting_power_avg_w,
        AVG(conveyor_power_w) AS conveyor_power_avg_w,
        AVG(total_power_w) AS total_power_avg_w,
        MAX(product_count_total) AS production_count_total,
        COUNT(*) AS sample_count
      FROM telemetry_history
      WHERE timestamp >= ?
      GROUP BY CAST(timestamp / 3600 AS INTEGER)
      ORDER BY hour_bucket ASC
    `).all(start);

    const upsert = db.prepare(`
      INSERT INTO hourly_history (
        hour_bucket,
        lighting_power_avg_w,
        conveyor_power_avg_w,
        total_power_avg_w,
        production_count_total,
        sample_count
      ) VALUES (
        @hour_bucket,
        @lighting_power_avg_w,
        @conveyor_power_avg_w,
        @total_power_avg_w,
        @production_count_total,
        @sample_count
      )
      ON CONFLICT(hour_bucket) DO UPDATE SET
        lighting_power_avg_w = excluded.lighting_power_avg_w,
        conveyor_power_avg_w = excluded.conveyor_power_avg_w,
        total_power_avg_w = excluded.total_power_avg_w,
        production_count_total = excluded.production_count_total,
        sample_count = excluded.sample_count
    `);

    const transaction = db.transaction((items) => {
      for (const row of items) {
        upsert.run(row);
      }
    });

    transaction(rows);
  } catch (error) {
    console.error("[SQLITE] Hourly aggregation failed:", error.message);
  }
}

function refreshDailyHistory() {
  try {
    const rows = db.prepare(`
      SELECT
        date(timestamp, 'unixepoch', 'localtime') AS day_date,
        AVG(lighting_power_w) AS lighting_power_avg_w,
        MAX(lighting_power_w) AS lighting_power_max_w,
        AVG(conveyor_power_w) AS conveyor_power_avg_w,
        MAX(conveyor_power_w) AS conveyor_power_max_w,
        AVG(total_power_w) AS total_power_avg_w,
        MAX(total_power_w) AS total_power_max_w,
        MAX(product_count_total) AS production_count_total,
        COUNT(*) AS sample_count
      FROM telemetry_history
      WHERE timestamp >= ?
      GROUP BY date(timestamp, 'unixepoch', 'localtime')
      ORDER BY day_date ASC
    `).all(Date.now() / 1000 - RAW_RETENTION_SECONDS);

    const upsert = db.prepare(`
      INSERT INTO daily_history (
        day_date,
        lighting_power_avg_w,
        lighting_power_max_w,
        conveyor_power_avg_w,
        conveyor_power_max_w,
        total_power_avg_w,
        total_power_max_w,
        production_count_total,
        sample_count
      ) VALUES (
        @day_date,
        @lighting_power_avg_w,
        @lighting_power_max_w,
        @conveyor_power_avg_w,
        @conveyor_power_max_w,
        @total_power_avg_w,
        @total_power_max_w,
        @production_count_total,
        @sample_count
      )
      ON CONFLICT(day_date) DO UPDATE SET
        lighting_power_avg_w = excluded.lighting_power_avg_w,
        lighting_power_max_w = excluded.lighting_power_max_w,
        conveyor_power_avg_w = excluded.conveyor_power_avg_w,
        conveyor_power_max_w = excluded.conveyor_power_max_w,
        total_power_avg_w = excluded.total_power_avg_w,
        total_power_max_w = excluded.total_power_max_w,
        production_count_total = excluded.production_count_total,
        sample_count = excluded.sample_count
    `);

    const transaction = db.transaction((items) => {
      for (const row of items) {
        upsert.run(row);
      }
    });

    transaction(rows);
  } catch (error) {
    console.error("[SQLITE] Daily aggregation failed:", error.message);
  }
}

function cleanupRawTelemetry() {
  try {
    const cutoff = Date.now() / 1000 - RAW_RETENTION_SECONDS;
    const result = db.prepare(`
      DELETE FROM telemetry_history
      WHERE timestamp < ?
    `).run(cutoff);

    if (result.changes > 0) {
      console.log(`[SQLITE] Removed ${result.changes} old raw telemetry rows`);
    }
  } catch (error) {
    console.error("[SQLITE] Raw telemetry cleanup failed:", error.message);
  }
}

function refreshHistoryAggregates() {
  refreshHourlyHistory();
  refreshDailyHistory();
  cleanupRawTelemetry();
}

// Refresh aggregates periodically. Historical data is read from SQLite,
// while live state remains in RAM/WebSocket.
setTimeout(() => {
  refreshHistoryAggregates();
  setInterval(refreshHistoryAggregates, 5 * 60 * 1000);
}, 1000);



const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 4000;

const DEVICE_ID = process.env.DEVICE_ID || "factory-esp32";
const ESP32_URL =
  process.env.ESP32_URL ||
  (process.env.DEVICE_IP ? `http://${process.env.DEVICE_IP}` : "http://10.10.12.75");
const DEVICE_KEY = process.env.DEVICE_KEY || "";
const CAMERA_URL = process.env.CAMERA_URL || "http://camera-recorder:8081";

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

/* ============================================================
   DEVICE STATE
   ============================================================ */

function getLastDbState() {
  try {
    const row = db.prepare("SELECT * FROM telemetry_history ORDER BY id DESC LIMIT 1").get();
    if (row) {
      return {
        powerW: row.total_power_w,
        conveyorPowerW: row.conveyor_power_w,
        lightingPowerW: row.lighting_power_w,
        productCount: row.product_count,
        productCountTotal: row.product_count_total
      };
    }
  } catch (e) { }
  return {};
}

const initialDb = getLastDbState();

let deviceState = {
  device: DEVICE_ID,
  online: false,

  network: {
    ip: null,
    gateway: null,
    rssi: null,
    wifiConnected: false
  },

  actuators: {
    conveyor: false,
    mist1: false,
    mist2: false,
    heater: false,
    lighting: false
  },

  sensors: {
    temperatureC: null,
    coolingTower1TempC: null,
    coolingTower1Humidity: null,
    coolingTower2TempC: null,
    coolingTower2Humidity: null,
    lightingPowerW: initialDb.lightingPowerW ?? null,
    conveyorPowerW: initialDb.conveyorPowerW ?? null,
    powerW: initialDb.powerW ?? null,
    productCount: initialDb.productCount ?? null,
    productCountTotal: initialDb.productCountTotal ?? null
  },

  laserZones: {
    zone1: "SAFE",
    zone2: "SAFE",
    zone3: "SAFE",
    zone4: "SAFE"
  },

  servos: {
    person1Angle: 180,
    person2Angle: 0
  },

  camera: {
    connected: false,
    recording: false,
    zone: 0
  },

  system: {
    lightingIna_ok: false,
    conveyorIna_ok: false,
    coolingTower1_ok: false,
    coolingTower2_ok: false,
    switchyardTemp_ok: false,
    uptime_ms: 0,
    freeHeap: null,
    firmwareVersion: null,
    heaterTempTrusted: false
  },

  deviceId: DEVICE_ID,
  timestamp: null,
  lastTelemetry: null,
  lastSeen: null
};

/* ============================================================
   CAMERA SERVICE HEALTH MONITOR
   ============================================================ */

async function pollCameraService() {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 2000);
    const resp = await fetch(`${CAMERA_URL}/status`, { signal: controller.signal });
    clearTimeout(timeout);
    if (resp.ok) {
      const data = await resp.json();
      const isRec = Boolean(data.recording || (data.active_recording_count > 0));
      const activeZone = data.active_recordings && Object.keys(data.active_recordings).length > 0
        ? Number(Object.keys(data.active_recordings)[0])
        : 0;

      const prevConnected = deviceState.camera?.connected;
      const prevRec = deviceState.camera?.recording;

      deviceState.camera = {
        connected: true,
        recording: isRec,
        zone: activeZone || (deviceState.camera?.zone ?? 0),
        activeRecordings: data.active_recordings || {}
      };

      if (prevConnected !== true || prevRec !== isRec) {
        broadcastState();
      }
      return;
    }
  } catch (err) {
    // service not reachable
  }

  if (deviceState.camera) {
    const prevConnected = deviceState.camera.connected;
    deviceState.camera.connected = false;
    if (prevConnected !== false) {
      broadcastState();
    }
  }
}

setInterval(pollCameraService, 4000);
pollCameraService();

/* ============================================================
   HELPER FUNCTIONS
   ============================================================ */

// Correct the swapped Cooling Tower / Mist 1 and Mist 2 sensor mapping
// at the backend boundary, so all dashboards and display APIs see the
// intended values. Only swap when both fields are present.
function normalizeCoolingTowerSensorMapping(state) {
  if (!state || typeof state !== "object") {
    return state;
  }

  const normalized = { ...state };

  if (state.sensors && typeof state.sensors === "object") {
    const sensors = { ...state.sensors };

    if (
      Object.prototype.hasOwnProperty.call(sensors, "coolingTower1TempC") &&
      Object.prototype.hasOwnProperty.call(sensors, "coolingTower2TempC")
    ) {
      const tower1Temp = sensors.coolingTower1TempC;
      sensors.coolingTower1TempC = sensors.coolingTower2TempC;
      sensors.coolingTower2TempC = tower1Temp;
    }

    if (
      Object.prototype.hasOwnProperty.call(sensors, "coolingTower1Humidity") &&
      Object.prototype.hasOwnProperty.call(sensors, "coolingTower2Humidity")
    ) {
      const tower1Humidity = sensors.coolingTower1Humidity;
      sensors.coolingTower1Humidity = sensors.coolingTower2Humidity;
      sensors.coolingTower2Humidity = tower1Humidity;
    }

    normalized.sensors = sensors;
  }

  return normalized;
}

function broadcastState() {
  const fullState = {
    ...deviceState,
    backend: { connected: true },
    esp32: { connected: Boolean(deviceState.online) }
  };
  const message = JSON.stringify({
    type: "state",
    state: fullState
  });

  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}


function monitorResponse(res, type, data) {
  res.json({
    success: true,
    type,
    deviceId: DEVICE_ID,
    timestamp: new Date().toISOString(),
    data
  });
}


async function esp32Post(endpoint, body = null, timeoutMs = 4000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const options = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Device-Key": DEVICE_KEY
      },
      signal: controller.signal
    };
    if (body !== null && body !== undefined) {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(`${ESP32_URL}${endpoint}`, options);
    clearTimeout(timer);

    const text = await response.text();

    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = {
        success: response.ok,
        raw: text
      };
    }

    if (!response.ok) {
      throw new Error(
        `ESP32 returned HTTP ${response.status}: ${text}`
      );
    }

    return data;
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}


async function setConveyorState(enable) {
  try {
    const endpoint = enable ? "/api/control/conveyor/on" : "/api/control/conveyor/off";
    console.log(`[MES AUTOMATION] Conveyor state -> ${enable ? 'ON' : 'OFF'} (${endpoint})`);
    const result = await esp32Post(endpoint);
    if (result && result.state) {
      const incomingState = normalizeCoolingTowerSensorMapping(result.state);
      deviceState = {
        ...deviceState,
        ...incomingState,
        deviceId: incomingState.deviceId || DEVICE_ID,
        online: true,
        lastSeen: new Date().toISOString()
      };
      broadcastState();
    }
    return true;
  } catch (err) {
    console.error(`[MES AUTOMATION CONVEYOR ERROR]:`, err.message);
    return false;
  }
}

async function forwardControl(req, res, endpoint) {
  try {
    const result = await esp32Post(endpoint);

    if (result && result.state) {
      const incomingState = normalizeCoolingTowerSensorMapping(result.state);

      deviceState = {
        ...deviceState,
        ...incomingState,
        deviceId: incomingState.deviceId || DEVICE_ID,
        online: true,
        lastSeen: new Date().toISOString()
      };

      broadcastState();
    }

    res.json({
      success: true,
      endpoint,
      result
    });

  } catch (error) {
    console.error(`[CONTROL ERROR] ${endpoint}`);
    console.error(error.message);

    res.status(500).json({
      success: false,
      endpoint,
      error: error.message
    });
  }
}


/* ============================================================
   HEALTH
   ============================================================ */

app.get("/api/health", (req, res) => {
  res.json({
    success: true,
    service: "Factory Model HTTP Backend",
    deviceId: DEVICE_ID,
    online: Boolean(deviceState.online),
    timestamp: new Date().toISOString()
  });
});


/* ============================================================
   COMPLETE STATE
   ============================================================ */

app.get("/api/state", (req, res) => {
  res.json({
    success: true,
    state: {
      ...deviceState,
      backend: { connected: true },
      esp32: { connected: Boolean(deviceState.online) }
    }
  });
});


/* ============================================================
   DEVICE STATE
   ============================================================ */

app.get("/api/device/:id", (req, res) => {
  if (req.params.id !== DEVICE_ID) {
    return res.status(404).json({
      success: false,
      error: "Device not found"
    });
  }

  res.json({
    success: true,
    deviceId: DEVICE_ID,
    state: deviceState
  });
});


/* ============================================================
   ESP32 TELEMETRY
   ============================================================ */

app.post("/api/device/telemetry", (req, res) => {
  try {
    const telemetry = req.body;

    const incomingState = normalizeCoolingTowerSensorMapping(
      telemetry.state || telemetry
    );

    const wasCameraConnected = deviceState.camera?.connected ?? true;

    deviceState = {
      ...deviceState,
      ...incomingState,

      deviceId:
        telemetry.deviceId ||
        incomingState.deviceId ||
        DEVICE_ID,

      timestamp:
        telemetry.timestamp ||
        incomingState.timestamp ||
        new Date().toISOString(),

      online: true,

      lastTelemetry:
        new Date().toISOString(),

      lastSeen:
        new Date().toISOString()
    };

    if (deviceState.camera) {
      deviceState.camera.connected = wasCameraConnected;
    }

    // Persist only genuine ESP32 telemetry.
    saveTelemetryHistory(deviceState);

    console.log(`[TELEMETRY] ${DEVICE_ID}`);

    if (incomingState.sensors) {
      console.log(
        `  Temperature: ${incomingState.sensors.temperatureC}`
      );

      console.log(
        `  Power: ${incomingState.sensors.powerW}`
      );

      console.log(
        `  Product Count: ${incomingState.sensors.productCount}`
      );
    }

    broadcastState();

    res.json({
      success: true
    });

  } catch (error) {
    console.error("[TELEMETRY ERROR]", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


/* ============================================================
   CONTROL APIs
   ============================================================ */

/* ---------- Conveyor ---------- */

app.post("/api/control/conveyor/on", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/conveyor/on"
  );
});

app.post("/api/control/conveyor/off", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/conveyor/off"
  );
});


/* ---------- Lighting ---------- */

app.post("/api/control/lighting/on", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/lighting/on"
  );
});

app.post("/api/control/lighting/off", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/lighting/off"
  );
});


/* ---------- Mist 1 ---------- */

app.post("/api/control/mist1/on", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/mist1/on"
  );
});

app.post("/api/control/mist1/off", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/mist1/off"
  );
});


/* ---------- Mist 2 ---------- */

app.post("/api/control/mist2/on", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/mist2/on"
  );
});

app.post("/api/control/mist2/off", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/mist2/off"
  );
});


/* ---------- Heater ---------- */

app.post("/api/control/heater/on", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/heater/on"
  );
});

app.post("/api/control/heater/off", async (req, res) => {
  await forwardControl(
    req,
    res,
    "/api/control/heater/off"
  );
});


/* ---------- Person 1 ---------- */

app.post("/api/control/person1/home", async (req, res) => {
  try {
    let result = null;
    try {
      result = await esp32Post("/api/control/person1/home");
    } catch (e1) {
      // Fallback: try direct angle endpoint 180 (home for Person 1) or device command
      try {
        result = await esp32Post("/api/control/person1/180");
      } catch (e2) {
        result = await esp32Post("/api/device/command", { command: "homePerson1" });
      }
    }

    if (!deviceState.servos) deviceState.servos = {};
    deviceState.servos.person1Angle = 180;
    if (result && result.state) {
      deviceState = {
        ...deviceState,
        ...normalizeCoolingTowerSensorMapping(result.state),
        deviceId: result.state.deviceId || DEVICE_ID,
        online: true,
        lastSeen: new Date().toISOString()
      };
    }
    broadcastState();

    res.json({
      success: true,
      message: "Person 1 returned to Home (180°)",
      angle: 180,
      state: deviceState,
      result
    });
  } catch (error) {
    console.error("[PERSON1 HOME ERROR]", error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


app.post("/api/control/person1/move", async (req, res) => {
  try {
    const currentAngle = Number(deviceState.servos?.person1Angle);
    // Person 1 range: 0° - 180°. If at or near 180, move to 0; otherwise move to 180.
    const targetAngle = currentAngle === 180 ? 0 : 180;

    let result = null;
    try {
      result = await esp32Post(`/api/control/person1/${targetAngle}`);
    } catch (e1) {
      // Fallback: try device command format
      result = await esp32Post("/api/device/command", { command: "person1", value: targetAngle });
    }

    if (!deviceState.servos) deviceState.servos = {};
    deviceState.servos.person1Angle = targetAngle;
    if (result && result.state) {
      deviceState = {
        ...deviceState,
        ...normalizeCoolingTowerSensorMapping(result.state),
        deviceId: result.state.deviceId || DEVICE_ID,
        online: true,
        lastSeen: new Date().toISOString()
      };
    }
    broadcastState();

    res.json({
      success: true,
      message: `Person 1 moved to ${targetAngle}°`,
      targetAngle,
      state: deviceState,
      result
    });
  } catch (error) {
    console.error("[PERSON1 MOVE ERROR]", error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


/* ---------- Person 2 ---------- */

app.post("/api/control/person2/home", async (req, res) => {
  try {
    let result = null;
    try {
      result = await esp32Post("/api/control/person2/home");
    } catch (e1) {
      // Fallback: try direct angle endpoint 0 (home for Person 2) or device command
      try {
        result = await esp32Post("/api/control/person2/0");
      } catch (e2) {
        result = await esp32Post("/api/device/command", { command: "homePerson2" });
      }
    }

    if (!deviceState.servos) deviceState.servos = {};
    deviceState.servos.person2Angle = 0;
    if (result && result.state) {
      deviceState = {
        ...deviceState,
        ...normalizeCoolingTowerSensorMapping(result.state),
        deviceId: result.state.deviceId || DEVICE_ID,
        online: true,
        lastSeen: new Date().toISOString()
      };
    }
    broadcastState();

    res.json({
      success: true,
      message: "Person 2 returned to Home (0°)",
      angle: 0,
      state: deviceState,
      result
    });
  } catch (error) {
    console.error("[PERSON2 HOME ERROR]", error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


app.post("/api/control/person2/move", async (req, res) => {
  try {
    const currentAngle = Number(deviceState.servos?.person2Angle);
    // Person 2 range: 0° - 150°. If at or near 0, move to 150; otherwise move to 0.
    const targetAngle = currentAngle === 0 ? 150 : 0;

    let result = null;
    try {
      result = await esp32Post(`/api/control/person2/${targetAngle}`);
    } catch (e1) {
      // Fallback: try device command format
      result = await esp32Post("/api/device/command", { command: "person2", value: targetAngle });
    }

    if (!deviceState.servos) deviceState.servos = {};
    deviceState.servos.person2Angle = targetAngle;
    if (result && result.state) {
      deviceState = {
        ...deviceState,
        ...normalizeCoolingTowerSensorMapping(result.state),
        deviceId: result.state.deviceId || DEVICE_ID,
        online: true,
        lastSeen: new Date().toISOString()
      };
    }
    broadcastState();

    res.json({
      success: true,
      message: `Person 2 moved to ${targetAngle}°`,
      targetAngle,
      state: deviceState,
      result
    });
  } catch (error) {
    console.error("[PERSON2 MOVE ERROR]", error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


/* ============================================================
   LEGACY GENERIC COMMAND
   ============================================================ */

app.post("/api/device/:id/command", async (req, res) => {
  if (req.params.id !== DEVICE_ID) {
    return res.status(404).json({
      success: false,
      error: "Device not found"
    });
  }

  try {
    const command = req.body || {};

    let endpoint = null;

    if (command.conveyor === true) {
      endpoint = "/api/control/conveyor/on";
    }

    if (command.conveyor === false) {
      endpoint = "/api/control/conveyor/off";
    }

    if (command.lighting === true) {
      endpoint = "/api/control/lighting/on";
    }

    if (command.lighting === false) {
      endpoint = "/api/control/lighting/off";
    }

    if (command.mist1 === true) {
      endpoint = "/api/control/mist1/on";
    }

    if (command.mist1 === false) {
      endpoint = "/api/control/mist1/off";
    }

    if (command.mist2 === true) {
      endpoint = "/api/control/mist2/on";
    }

    if (command.mist2 === false) {
      endpoint = "/api/control/mist2/off";
    }

    if (command.heater === true) {
      endpoint = "/api/control/heater/on";
    }

    if (command.heater === false) {
      endpoint = "/api/control/heater/off";
    }

    if (!endpoint) {
      return res.status(400).json({
        success: false,
        error: "No supported command"
      });
    }

    await forwardControl(req, res, endpoint);

  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


/* ============================================================
   HISTORICAL DATA APIs
   ============================================================ */

function normalizeHistoryRange(value) {
  const allowed = new Set(["1h", "1d", "1w", "1m", "1y"]);
  return allowed.has(value) ? value : "1h";
}

function historyStartSeconds(range) {
  const now = Date.now() / 1000;

  switch (range) {
    case "1h":
      return now - (60 * 60);
    case "1d":
      return now - (24 * 60 * 60);
    case "1w":
      return now - (7 * 24 * 60 * 60);
    case "1m":
      return now - (30 * 24 * 60 * 60);
    case "1y":
      return now - (365 * 24 * 60 * 60);
    default:
      return now - (60 * 60);
  }
}

app.get("/api/history/power", (req, res) => {
  try {
    const range = normalizeHistoryRange(req.query.range);
    const start = historyStartSeconds(range);

    let rows;

    if (range === "1h") {
      rows = db.prepare(`
        SELECT
          timestamp,
          lighting_power_w AS lightingPowerW,
          conveyor_power_w AS conveyorPowerW,
          total_power_w AS totalPowerW
        FROM telemetry_history
        WHERE timestamp >= ?
        ORDER BY timestamp ASC
      `).all(start);

      // If device is offline and last hour is empty, return the most recent 60 recorded points
      if (!rows.length) {
        rows = db.prepare(`
          SELECT
            timestamp,
            lighting_power_w AS lightingPowerW,
            conveyor_power_w AS conveyorPowerW,
            total_power_w AS totalPowerW
          FROM telemetry_history
          ORDER BY timestamp DESC
          LIMIT 60
        `).all().reverse();
      }
    } else if (range === "1d" || range === "1w") {
      rows = db.prepare(`
        SELECT
          hour_bucket AS timestamp,
          lighting_power_avg_w AS lightingPowerW,
          conveyor_power_avg_w AS conveyorPowerW,
          total_power_avg_w AS totalPowerW
        FROM hourly_history
        WHERE hour_bucket >= ?
        ORDER BY hour_bucket ASC
      `).all(start);
    } else {
      rows = db.prepare(`
        SELECT
          CAST(strftime('%s', day_date || ' 00:00:00') AS INTEGER) AS timestamp,
          lighting_power_avg_w AS lightingPowerW,
          conveyor_power_avg_w AS conveyorPowerW,
          total_power_avg_w AS totalPowerW
        FROM daily_history
        WHERE day_date >= date('now', 'localtime', ?)
        ORDER BY day_date ASC
      `).all(range === "1m" ? "-30 days" : "-365 days");
    }

    res.json({
      success: true,
      type: "power-history",
      range,
      data: rows
    });
  } catch (error) {
    console.error("[HISTORY POWER ERROR]", error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


app.get("/api/history/production", (req, res) => {
  try {
    const range = normalizeHistoryRange(req.query.range);
    const start = historyStartSeconds(range);

    let rows;

    if (range === "1h") {
      rows = db.prepare(`
        SELECT
          timestamp,
          product_count_total AS productionCount
        FROM telemetry_history
        WHERE timestamp >= ?
        ORDER BY timestamp ASC
      `).all(start);

      // If device is offline and last hour is empty, return the most recent 60 recorded points
      if (!rows.length) {
        rows = db.prepare(`
          SELECT
            timestamp,
            product_count_total AS productionCount
          FROM telemetry_history
          ORDER BY timestamp DESC
          LIMIT 60
        `).all().reverse();
      }
    } else if (range === "1d" || range === "1w") {
      rows = db.prepare(`
        SELECT
          hour_bucket AS timestamp,
          production_count_total AS productionCount
        FROM hourly_history
        WHERE hour_bucket >= ?
        ORDER BY hour_bucket ASC
      `).all(start);
    } else {
      rows = db.prepare(`
        SELECT
          CAST(strftime('%s', day_date || ' 00:00:00') AS INTEGER) AS timestamp,
          production_count_total AS productionCount
        FROM daily_history
        WHERE day_date >= date('now', 'localtime', ?)
        ORDER BY day_date ASC
      `).all(range === "1m" ? "-30 days" : "-365 days");
    }

    res.json({
      success: true,
      type: "production-history",
      range,
      data: rows
    });
  } catch (error) {
    console.error("[HISTORY PRODUCTION ERROR]", error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


/* ============================================================
   BATCH MANAGEMENT (MES) & ANALYTICS APIs
   ============================================================ */

function getActiveBatch() {
  let batch = db.prepare("SELECT * FROM batch_management WHERE status IN ('RUNNING', 'PAUSED') ORDER BY id DESC LIMIT 1").get();
  if (!batch) {
    const liveLifetime = Number(deviceState.sensors?.productCountTotal) || 0;
    const startTime = new Date().toISOString();
    const info = db.prepare(`
      INSERT INTO batch_management (batch_id, product_name, target_units, start_total_count, current_output, status, start_time)
      VALUES (?, 'Standard Unit', 100, ?, 0, 'RUNNING', ?)
    `).run('BATCH-1001', liveLifetime, startTime);
    batch = db.prepare("SELECT * FROM batch_management WHERE id = ?").get(info.lastInsertRowid);
  }
  return batch;
}

function getBatchAnalytics() {
  let hourly = [];
  try {
    const hourlyRows = db.prepare(`
      SELECT hour_bucket, production_count_total
      FROM hourly_history
      ORDER BY hour_bucket DESC
      LIMIT 13
    `).all().reverse();

    if (hourlyRows.length >= 2) {
      for (let i = 1; i < hourlyRows.length; i++) {
        const diff = Math.max(0, hourlyRows[i].production_count_total - hourlyRows[i-1].production_count_total);
        const d = new Date(hourlyRows[i].hour_bucket * 1000);
        const hourStr = d.getHours().toString().padStart(2, '0') + ':00';
        hourly.push({
          label: hourStr,
          timestamp: hourlyRows[i].hour_bucket,
          output: diff
        });
      }
    } else {
      const rawHourly = db.prepare(`
        SELECT CAST(timestamp / 3600 AS INTEGER) * 3600 AS hour_bucket,
               MAX(product_count_total) - MIN(product_count_total) AS diff
        FROM telemetry_history
        GROUP BY CAST(timestamp / 3600 AS INTEGER)
        ORDER BY hour_bucket DESC
        LIMIT 10
      `).all().reverse();
      hourly = rawHourly.map(r => {
        const d = new Date(r.hour_bucket * 1000);
        return {
          label: d.getHours().toString().padStart(2, '0') + ':00',
          timestamp: r.hour_bucket,
          output: Math.max(0, r.diff || 0)
        };
      });
    }
  } catch (e) {
    console.error("[ANALYTICS] Hourly error:", e.message);
  }

  let daily = [];
  try {
    const dailyRows = db.prepare(`
      SELECT day_date, production_count_total
      FROM daily_history
      ORDER BY day_date DESC
      LIMIT 8
    `).all().reverse();

    const monthsList = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    for (let i = 1; i < dailyRows.length; i++) {
      const diff = Math.max(0, dailyRows[i].production_count_total - dailyRows[i-1].production_count_total);
      const parts = (dailyRows[i].day_date || '').split('-');
      const formattedDate = (parts.length === 3)
        ? (parts[2] + ' ' + (monthsList[parseInt(parts[1], 10) - 1] || parts[1]))
        : dailyRows[i].day_date.slice(5);

      daily.push({
        date: dailyRows[i].day_date,
        label: formattedDate,
        output: diff,
        totalCumulative: dailyRows[i].production_count_total
      });
    }
  } catch (e) {
    console.error("[ANALYTICS] Daily error:", e.message);
  }

  let weekly = [];
  try {
    const monthsList = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const weekRows = daily.slice(-7);
    weekly = weekRows.map(d => {
      const parts = (d.date || '').split('-');
      const formattedDate = (parts.length === 3)
        ? (parts[2] + ' ' + (monthsList[parseInt(parts[1], 10) - 1] || parts[1]))
        : d.date;

      return {
        day: formattedDate,
        date: d.date,
        output: d.output
      };
    });
  } catch (e) {
    console.error("[ANALYTICS] Weekly error:", e.message);
  }

  let monthly = [];
  try {
    const monthlyRows = db.prepare(`
      SELECT substr(day_date, 1, 7) AS month_str,
             MAX(production_count_total) - MIN(production_count_total) AS output,
             MAX(production_count_total) AS month_end_total,
             COUNT(DISTINCT day_date) AS active_days
      FROM daily_history
      GROUP BY substr(day_date, 1, 7)
      ORDER BY month_str DESC
      LIMIT 6
    `).all().reverse();

    monthly = monthlyRows.map(r => ({
      month: r.month_str,
      output: Math.max(0, r.output || 0),
      endTotal: r.month_end_total,
      activeDays: r.active_days
    }));
  } catch (e) {
    console.error("[ANALYTICS] Monthly error:", e.message);
  }

  let batches = [];
  try {
    batches = db.prepare(`
      SELECT id, batch_id, product_name, target_units, start_total_count, current_output, status, start_time, end_time
      FROM batch_management
      ORDER BY id DESC
      LIMIT 6
    `).all();
  } catch (e) {
    console.error("[ANALYTICS] Batches error:", e.message);
  }

  return { hourly, daily, weekly, monthly, batches };
}

app.get("/api/batch/active", async (req, res) => {
  try {
    const batch = getActiveBatch();
    const liveLifetime = Number(deviceState.sensors?.productCountTotal) || 0;
    const liveCycle = Number(deviceState.sensors?.productCount) || 0;

    let output = batch.current_output || 0;
    if (batch.status === "RUNNING" && liveLifetime >= batch.start_total_count) {
      output = Math.max(output, liveLifetime - batch.start_total_count);
    }

    const target = Number(batch.target_units) || 100;

    // Check if target reached while running -> auto stop conveyor
    if (batch.status === "RUNNING" && target > 0 && output >= target) {
      batch.status = "COMPLETED";
      db.prepare("UPDATE batch_management SET status = 'COMPLETED', current_output = ?, end_time = ? WHERE id = ?").run(output, new Date().toISOString(), batch.id);
      if (typeof setConveyorState === "function") {
        setConveyorState(false);
      }
    }

    const progressPct = Math.min(100, Math.round((output / target) * 100));
    const remainingUnits = Math.max(0, target - output);

    res.json({
      success: true,
      batch: {
        ...batch,
        current_output: output,
        progressPct,
        remainingUnits
      },
      sensors: {
        productCount: liveCycle,
        productCountTotal: liveLifetime
      }
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post(["/api/batch/new", "/api/batch/next"], async (req, res) => {
  try {
    const liveLifetime = Number(deviceState.sensors?.productCountTotal) || 0;
    const nowIso = new Date().toISOString();

    const active = db.prepare("SELECT * FROM batch_management WHERE status IN ('RUNNING', 'PAUSED') ORDER BY id DESC LIMIT 1").get();
    if (active) {
      const finalOutput = Math.max(active.current_output || 0, liveLifetime - active.start_total_count);
      db.prepare("UPDATE batch_management SET status = 'COMPLETED', end_time = ?, current_output = ? WHERE id = ?").run(nowIso, finalOutput, active.id);
    }

    const countRow = db.prepare("SELECT COUNT(*) AS c FROM batch_management").get();
    const nextNum = 1000 + (countRow.c + 1);
    const batchId = (req.body && req.body.batchId) ? req.body.batchId : `BATCH-${nextNum}`;
    const targetUnits = (req.body && Number(req.body.targetUnits) > 0) ? Number(req.body.targetUnits) : 100;
    const productName = (req.body && req.body.productName) ? req.body.productName : "Standard Unit";

    const info = db.prepare(`
      INSERT INTO batch_management (batch_id, product_name, target_units, start_total_count, current_output, status, start_time)
      VALUES (?, ?, ?, ?, 0, 'RUNNING', ?)
    `).run(batchId, productName, targetUnits, liveLifetime, nowIso);

    // Auto-start conveyor for new batch
    if (typeof setConveyorState === "function") {
      await setConveyorState(true);
    }

    const newBatch = db.prepare("SELECT * FROM batch_management WHERE id = ?").get(info.lastInsertRowid);
    return res.json({ success: true, message: `New Batch ${batchId} initialized with target ${targetUnits} pcs. Conveyor started.`, batch: newBatch });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/api/batch/pause", async (req, res) => {
  try {
    const active = db.prepare("SELECT * FROM batch_management WHERE status = 'RUNNING' ORDER BY id DESC LIMIT 1").get();
    if (!active) {
      return res.status(400).json({ success: false, error: "No active running batch found to pause" });
    }
    const liveLifetime = Number(deviceState.sensors?.productCountTotal) || 0;
    const currentOutput = Math.max(active.current_output || 0, liveLifetime - active.start_total_count);
    db.prepare("UPDATE batch_management SET status = 'PAUSED', current_output = ? WHERE id = ?").run(currentOutput, active.id);

    // Auto-stop conveyor when paused
    if (typeof setConveyorState === "function") {
      await setConveyorState(false);
    }

    const updated = db.prepare("SELECT * FROM batch_management WHERE id = ?").get(active.id);
    return res.json({ success: true, message: "Batch paused, conveyor stopped", batch: updated });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/api/batch/resume", async (req, res) => {
  try {
    const active = db.prepare("SELECT * FROM batch_management WHERE status = 'PAUSED' ORDER BY id DESC LIMIT 1").get();
    if (!active) {
      return res.status(400).json({ success: false, error: "No paused batch found to resume" });
    }
    const liveLifetime = Number(deviceState.sensors?.productCountTotal) || 0;
    const adjustedStart = liveLifetime - (active.current_output || 0);
    db.prepare("UPDATE batch_management SET status = 'RUNNING', start_total_count = ? WHERE id = ?").run(adjustedStart, active.id);

    // Auto-resume conveyor
    if (typeof setConveyorState === "function") {
      await setConveyorState(true);
    }

    const updated = db.prepare("SELECT * FROM batch_management WHERE id = ?").get(active.id);
    return res.json({ success: true, message: "Batch resumed, conveyor started", batch: updated });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/api/batch/complete", async (req, res) => {
  try {
    const active = db.prepare("SELECT * FROM batch_management WHERE status IN ('RUNNING', 'PAUSED') ORDER BY id DESC LIMIT 1").get();
    if (!active) {
      return res.status(400).json({ success: false, error: "No active batch found to complete" });
    }
    const liveLifetime = Number(deviceState.sensors?.productCountTotal) || 0;
    const finalOutput = Math.max(active.current_output || 0, liveLifetime - active.start_total_count);
    const endTime = new Date().toISOString();
    db.prepare("UPDATE batch_management SET status = 'COMPLETED', end_time = ?, current_output = ? WHERE id = ?").run(endTime, finalOutput, active.id);

    // Auto-stop conveyor
    if (typeof setConveyorState === "function") {
      await setConveyorState(false);
    }

    const updated = db.prepare("SELECT * FROM batch_management WHERE id = ?").get(active.id);
    return res.json({ success: true, message: "Batch completed successfully, conveyor stopped", batch: updated });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/api/batch/extend", async (req, res) => {
  try {
    let active = db.prepare("SELECT * FROM batch_management ORDER BY id DESC LIMIT 1").get();
    if (!active) {
      return res.status(400).json({ success: false, error: "No batch found" });
    }
    const addUnits = Number(req.body && req.body.addUnits) || 0;
    let newTarget = Number(req.body && req.body.targetUnits) || 0;
    if (newTarget <= 0 && addUnits > 0) {
      newTarget = (Number(active.target_units) || 0) + addUnits;
    }
    if (newTarget <= 0) {
      newTarget = (Number(active.target_units) || 100) + 50;
    }

    // Update target and resume running
    db.prepare("UPDATE batch_management SET target_units = ?, status = 'RUNNING', end_time = NULL WHERE id = ?").run(newTarget, active.id);

    // Auto-start conveyor for additional pcs
    if (typeof setConveyorState === "function") {
      await setConveyorState(true);
    }

    const updated = db.prepare("SELECT * FROM batch_management WHERE id = ?").get(active.id);
    return res.json({ success: true, message: `Batch target extended to ${newTarget} pcs. Conveyor running.`, batch: updated });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/batch/analytics", (req, res) => {
  try {
    const analytics = getBatchAnalytics();
    res.json({
      success: true,
      analytics
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});


/* ============================================================
   MONITORING APIs
   ============================================================ */

app.get("/api/monitor/all", (req, res) => {
  monitorResponse(res, "all", deviceState);
});


app.get("/api/monitor/temperature", (req, res) => {
  monitorResponse(res, "temperature", {
    temperatureC:
      deviceState.sensors?.temperatureC ?? null
  });
});


app.get("/api/monitor/cooling-tower-1", (req, res) => {
  monitorResponse(res, "cooling-tower-1", {
    temperatureC:
      deviceState.sensors?.coolingTower1TempC ?? null,

    humidity:
      deviceState.sensors?.coolingTower1Humidity ?? null
  });
});


app.get("/api/monitor/cooling-tower-2", (req, res) => {
  monitorResponse(res, "cooling-tower-2", {
    temperatureC:
      deviceState.sensors?.coolingTower2TempC ?? null,

    humidity:
      deviceState.sensors?.coolingTower2Humidity ?? null
  });
});


app.get("/api/monitor/lighting-power", (req, res) => {
  monitorResponse(res, "lighting-power", {
    powerW:
      deviceState.sensors?.lightingPowerW ?? null
  });
});


app.get("/api/monitor/conveyor-power", (req, res) => {
  monitorResponse(res, "conveyor-power", {
    powerW:
      deviceState.sensors?.conveyorPowerW ?? null
  });
});


app.get("/api/monitor/power", (req, res) => {
  monitorResponse(res, "power", {
    lightingPowerW:
      deviceState.sensors?.lightingPowerW ?? null,

    conveyorPowerW:
      deviceState.sensors?.conveyorPowerW ?? null,

    totalPowerW:
      deviceState.sensors?.powerW ?? null
  });
});


app.get("/api/monitor/product-count", (req, res) => {
  monitorResponse(res, "product-count", {
    productCount:
      deviceState.sensors?.productCount ?? null,

    productCountTotal:
      deviceState.sensors?.productCountTotal ?? null
  });
});


/* ---------- Laser ---------- */

app.get("/api/monitor/laser/1", (req, res) => {
  monitorResponse(res, "laser-1", {
    zone1:
      deviceState.laserZones?.zone1 ?? null
  });
});


app.get("/api/monitor/laser/2", (req, res) => {
  monitorResponse(res, "laser-2", {
    zone2:
      deviceState.laserZones?.zone2 ?? null
  });
});


app.get("/api/monitor/laser/3", (req, res) => {
  monitorResponse(res, "laser-3", {
    zone3:
      deviceState.laserZones?.zone3 ?? null
  });
});


app.get("/api/monitor/laser/4", (req, res) => {
  monitorResponse(res, "laser-4", {
    zone4:
      deviceState.laserZones?.zone4 ?? null
  });
});


/* ---------- Person ---------- */

app.get("/api/monitor/person1", (req, res) => {
  monitorResponse(res, "person1", {
    angle:
      deviceState.servos?.person1Angle ?? null
  });
});


app.get("/api/monitor/person2", (req, res) => {
  monitorResponse(res, "person2", {
    angle:
      deviceState.servos?.person2Angle ?? null
  });
});


/* ---------- Camera ---------- */

app.get("/api/monitor/camera", (req, res) => {
  monitorResponse(res, "camera", {
    recording:
      Boolean(deviceState.camera?.recording),

    zone:
      deviceState.camera?.zone ?? 0
  });
});


/* ---------- Actuators ---------- */

app.get("/api/monitor/actuators", (req, res) => {
  monitorResponse(
    res,
    "actuators",
    deviceState.actuators
  );
});


/* ---------- System ---------- */

app.get("/api/monitor/system", (req, res) => {
  monitorResponse(res, "system", {
    ...deviceState.system,

    online:
      Boolean(deviceState.online),

    network: {
      rssi:
        deviceState.network?.rssi ?? null,

      wifiConnected:
        deviceState.network?.wifiConnected ?? null
    }
  });
});


/* ============================================================
   DT IFRAME DISPLAY ROUTES
   ============================================================ */

function iframeDisplayPage(arg1, arg2, arg3, arg4) {
  let title, valueExpression, unit, theme, icon, type, metricLabel, rowLabel;

  if (typeof arg1 === "object" && arg1 !== null) {
    title = arg1.title || "Display";
    valueExpression = arg1.valueExpression || "''";
    unit = arg1.unit || "";
    theme = arg1.theme || "emerald";
    icon = arg1.icon || "📊";
    type = arg1.type || "sensor";
    metricLabel = arg1.metricLabel || title;
    rowLabel = arg1.rowLabel || title;
  } else {
    title = arg1 || "Display";
    valueExpression = arg2 || "''";
    unit = arg3 || "";
    const options = typeof arg4 === "object" && arg4 !== null ? arg4 : {};
    const isLaser = arg4 === true || options.isLaserStatus;
    theme = options.theme || (isLaser ? "laser" : "emerald");
    icon = options.icon || (isLaser ? "🚨" : "📊");
    type = options.type || (isLaser ? "laser" : "sensor");
    metricLabel = options.metricLabel || title;
    rowLabel = options.rowLabel || title;
  }

  let bodyHtml = "";
  if (type === "sensor") {
    bodyHtml = `
      <div class="sensor-box">
        <div class="sensor-label">${metricLabel}</div>
        <div class="sensor-value-row">
          <span class="sensor-value" id="display-val">--</span>
          <span class="sensor-unit">${unit}</span>
        </div>
      </div>`;
  } else if (type === "switch") {
    bodyHtml = `
      <div class="control-row">
        <div class="btn-group">
          <span class="state-btn btn-on" id="btn-on">ON</span>
          <span class="state-btn btn-off" id="btn-off">OFF</span>
        </div>
      </div>`;
  } else if (type === "laser") {
    bodyHtml = `
      <div class="control-row">
        <div class="btn-group">
          <span class="laser-pill safe" id="laser-pill">SAFE</span>
        </div>
      </div>`;
  } else if (type === "servo") {
    const isP2 = title.includes("2");
    const personNum = isP2 ? 2 : 1;
    bodyHtml = `
      <div class="control-row">
        <div class="btn-group">
          <button class="state-btn btn-home" id="btn-home" onclick="triggerServo(${personNum}, 'home')">HOME</button>
          <button class="state-btn btn-move" id="btn-move" onclick="triggerServo(${personNum}, 'move')">MOVED</button>
        </div>
      </div>`;
  } else if (type === "camera") {
    bodyHtml = `
      <div class="control-row">
        <div class="btn-group">
          <span class="state-btn btn-rec" id="btn-rec">REC</span>
          <span class="state-btn btn-idle" id="btn-idle">IDLE</span>
        </div>
      </div>`;
  }

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${title}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    ::-webkit-scrollbar { display: none; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
    }

    /* Glassmorphic Compact Card (Cupola 360 Style) */
    .card {
      width: 330px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.3s ease, border-color 0.3s ease;
    }

    /* 1. Emerald (Vibrant Mint / Emerald Green) */
    .theme-emerald {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(52, 211, 153, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(16, 185, 129, 0.22);
    }
    .theme-emerald .sensor-label { color: #a7f3d0; }
    .theme-emerald .sensor-unit { color: #6ee7b7; }

    /* 2. Amber / Hot Flame Orange (Switchyard Temp) */
    .theme-amber {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(251, 146, 60, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(249, 115, 22, 0.22);
    }
    .theme-amber .sensor-label { color: #fed7aa; }
    .theme-amber .sensor-unit { color: #fdba74; }

    /* 3. Teal / Electric Cyan (Cooling Tower 1) -> Re-mapped to Warm Amber/Gold */
    .theme-teal {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(255, 190, 11, 0.65);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(255, 190, 11, 0.35);
    }
    .theme-teal .sensor-label { color: #fde047; }
    .theme-teal .sensor-unit { color: #fef08a; }

    /* 4. Gold / Sunburst Golden Yellow (Lighting & Power) */
    .theme-gold {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(253, 224, 71, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(234, 179, 8, 0.22);
    }
    .theme-gold .sensor-label { color: #fef08a; }
    .theme-gold .sensor-unit { color: #fde047; }

    /* 5. Rose / Ruby Crimson (Industrial Heater) */
    .theme-rose, .theme-coral {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(251, 113, 133, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(225, 29, 72, 0.22);
    }
    .theme-rose .sensor-label, .theme-coral .sensor-label { color: #fecdd3; }
    .theme-rose .sensor-unit, .theme-coral .sensor-unit { color: #fda4af; }

    /* 6. Indigo / Electric Purple (Conveyor System) */
    .theme-indigo {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(192, 132, 252, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(147, 51, 234, 0.22);
    }
    .theme-indigo .sensor-label { color: #e9d5ff; }
    .theme-indigo .sensor-unit { color: #c084fc; }

    /* 7. Blue / Deep Indigo Navy -> Re-mapped to Flame Orange */
    .theme-blue {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(255, 119, 0, 0.65);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(255, 119, 0, 0.35);
    }
    .theme-blue .sensor-label { color: #fed7aa; }
    .theme-blue .sensor-unit { color: #ffbe0b; }

    /* 8. Slate / Gunmetal Platinum (Security Camera) */
    .theme-slate {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(203, 213, 225, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(148, 163, 184, 0.22);
    }
    .theme-slate .sensor-label { color: #cbd5e1; }
    .theme-slate .sensor-unit { color: #94a3b8; }

    /* 9. Lime / Electric Spring Green */
    .theme-lime {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(190, 242, 100, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(132, 204, 22, 0.22);
    }
    .theme-lime .sensor-label { color: #d9f99d; }
    .theme-lime .sensor-unit { color: #bef264; }

    /* 10. Cyan / Royal Deep Blue (Cooling Tower 2) -> Re-mapped to Flame Orange */
    .theme-cyan {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(255, 119, 0, 0.65);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(255, 119, 0, 0.35);
    }
    .theme-cyan .sensor-label { color: #fed7aa; }
    .theme-cyan .sensor-unit { color: #ffbe0b; }

    /* 11. Violet / Purple */
    .theme-violet {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(216, 180, 254, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(168, 85, 247, 0.22);
    }
    .theme-violet .sensor-label { color: #ddd6fe; }
    .theme-violet .sensor-unit { color: #c4b5fd; }

    /* 12. Crimson / Blood Red */
    .theme-crimson {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(248, 113, 113, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(220, 38, 38, 0.22);
    }
    .theme-crimson .sensor-label { color: #fecdd3; }
    .theme-crimson .sensor-unit { color: #fda4af; }

    /* 13. Copper / Warm Bronze */
    .theme-copper {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(251, 191, 36, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(217, 119, 6, 0.22);
    }
    .theme-copper .sensor-label { color: #fde68a; }
    .theme-copper .sensor-unit { color: #fbbf24; }

    /* 15. Fuchsia / Hot Magenta Pink (Person 2 Servo) */
    .theme-fuchsia {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(244, 114, 182, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(236, 72, 153, 0.22);
    }
    .theme-fuchsia .sensor-label { color: #fbcfe8; }
    .theme-fuchsia .sensor-unit { color: #f472b6; }

    /* 16. Laser Hazard State */
    .theme-laser-safe {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(239, 68, 68, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(239, 68, 68, 0.22);
    }
    .theme-laser-intrusion {
      background: rgba(185, 28, 28, 0.28);
      border: 1.5px solid #fca5a5;
      box-shadow: 0 0 30px rgba(239, 68, 68, 0.70), inset 0 0 20px rgba(239, 68, 68, 0.35);
      animation: alert-border-pulse 0.8s infinite alternate;
    }
    @keyframes alert-border-pulse {
      0% { box-shadow: 0 0 15px rgba(239, 68, 68, 0.4), inset 0 0 15px rgba(239, 68, 68, 0.2); }
      100% { box-shadow: 0 0 30px rgba(239, 68, 68, 0.8), inset 0 0 30px rgba(239, 68, 68, 0.5); }
    }

    /* Card Header (Hidden - No top icon) */
    .card-header {
      display: none !important;
    }

    /* Sensor Box */
    .sensor-box {
      background: rgba(0, 0, 0, 0.40);
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      padding: 6px 8px;
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .sensor-label {
      font-size: 20px;
      font-weight: 800;
      letter-spacing: 0.04em;
      display: flex;
      align-items: center;
      text-transform: uppercase;
      text-shadow: 0 2px 4px rgba(0, 0, 0, 0.95);
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 5px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 34px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 12px rgba(255, 255, 255, 0.25);
    }
    .sensor-unit {
      font-size: 20px;
      font-weight: 800;
    }

    /* Control / Actuator Row */
    .control-row {
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(0, 0, 0, 0.18);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 10px;
      padding: 6px 8px;
    }
    .control-name {
      display: none !important;
    }
    .btn-group {
      display: flex;
      gap: 8px;
      align-items: center;
      width: 100%;
    }
    .state-btn {
      flex: 1;
      font-size: 17px;
      font-weight: 800;
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.25);
      color: rgba(255, 255, 255, 0.85);
      background: rgba(0, 0, 0, 0.22);
      transition: all 0.2s ease;
      line-height: 1.2;
      text-align: center;
      cursor: pointer;
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 14px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 14px rgba(220, 38, 38, 0.6);
    }
    .state-btn.active-home {
      background: linear-gradient(135deg, #ff7700, #ea580c);
      color: #ffffff;
      border-color: #fed7aa;
      box-shadow: 0 0 16px rgba(255, 119, 0, 0.6);
    }
    .state-btn.active-move {
      background: linear-gradient(135deg, #ffbe0b, #f59e0b);
      color: #000000;
      font-weight: 900;
      border-color: #fde047;
      box-shadow: 0 0 16px rgba(255, 190, 11, 0.6);
    }
    .state-btn.active-rec {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 14px rgba(220, 38, 38, 0.8);
      animation: alert-pulse 1s infinite alternate;
    }
    .state-btn.active-idle {
      background: #475569;
      color: #ffffff;
      border-color: #94a3b8;
    }

    /* Laser status pill */
    .laser-pill {
      font-size: 13px;
      font-weight: 800;
      padding: 6px 14px;
      border-radius: 8px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      transition: all 0.25s ease;
    }
    .laser-pill.safe {
      background: #10b981;
      color: #ffffff;
      border: 1px solid #34d399;
      box-shadow: 0 0 12px rgba(16, 185, 129, 0.5);
    }
    .laser-pill.intrusion {
      background: #dc2626;
      color: #ffffff;
      border: 1px solid #f87171;
      box-shadow: 0 0 16px rgba(220, 38, 38, 0.8);
      animation: alert-pulse 0.9s infinite alternate;
    }
    @keyframes alert-pulse {
      0% { opacity: 0.75; transform: scale(0.98); }
      100% { opacity: 1; transform: scale(1.02); }
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card theme-${theme}" id="card">
      ${bodyHtml}
    </div>
  </div>

  <script>
    async function updateDisplay() {
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        if (!data.success || !data.state) return;
        const state = data.state;
        let value = ${valueExpression};
        if (value === undefined || value === null || Number.isNaN(value)) {
          value = 'N/A';
        }

        const type = '${type}';
        const cardEl = document.getElementById('card');
        const urlParams = new URLSearchParams(window.location.search);
        const customTheme = urlParams.get('theme');

        if (type === 'sensor') {
          const valEl = document.getElementById('display-val');
          if (valEl) valEl.textContent = value;
          if (customTheme) cardEl.className = 'card theme-' + customTheme;
        } else if (type === 'switch') {
          const strVal = String(value).trim().toUpperCase();
          const isOn = strVal === 'ON' || strVal === 'TRUE' || strVal === '1';
          const btnOn = document.getElementById('btn-on');
          const btnOff = document.getElementById('btn-off');
          if (btnOn && btnOff) {
            btnOn.className = 'state-btn btn-on' + (isOn ? ' active-on' : '');
            btnOff.className = 'state-btn btn-off' + (!isOn ? ' active-off' : '');
          }
          if (customTheme) cardEl.className = 'card theme-' + customTheme;
        } else if (type === 'laser') {
          const normalized = String(value).trim().toUpperCase();
          const pill = document.getElementById('laser-pill');
          if (pill) {
            pill.textContent = normalized;
            if (normalized === 'SAFE') {
              pill.className = 'laser-pill safe';
              cardEl.className = 'card ' + (customTheme ? ('theme-' + customTheme) : 'theme-laser-safe');
            } else if (normalized === 'INTRUSION') {
              pill.className = 'laser-pill intrusion';
              cardEl.className = 'card ' + (customTheme ? ('theme-' + customTheme) : 'theme-laser-intrusion');
            } else {
              pill.className = 'laser-pill';
              pill.style.background = '#f59e0b';
              pill.style.color = '#fff';
            }
          }
        } else if (type === 'servo') {
          const strVal = String(value).trim().toUpperCase();
          const isHome = strVal === 'HOME' || strVal === '180' || strVal === '0';
          const btnHome = document.getElementById('btn-home');
          const btnMove = document.getElementById('btn-move');
          if (btnHome && btnMove) {
            btnHome.className = 'state-btn btn-home' + (isHome ? ' active-home' : '');
            btnMove.className = 'state-btn btn-move' + (!isHome ? ' active-move' : '');
          }
          if (customTheme) cardEl.className = 'card theme-' + customTheme;
        } else if (type === 'camera') {
          const strVal = String(value).trim().toUpperCase();
          const isRec = strVal === 'RECORDING' || strVal === 'REC' || strVal === 'TRUE';
          const btnRec = document.getElementById('btn-rec');
          const btnIdle = document.getElementById('btn-idle');
          if (btnRec && btnIdle) {
            btnRec.className = 'state-btn btn-rec' + (isRec ? ' active-rec' : '');
            btnIdle.className = 'state-btn btn-idle' + (!isRec ? ' active-idle' : '');
          }
          if (customTheme) cardEl.className = 'card theme-' + customTheme;
        }
      } catch (err) {
        console.error('Update error:', err);
      }
    }

    async function triggerServo(personNum, action) {
      const endpoint = '/api/control/person' + personNum + '/' + action;
      const btnHome = document.getElementById('btn-home');
      const btnMove = document.getElementById('btn-move');
      if (btnHome && btnMove) {
        if (action === 'home') {
          btnHome.className = 'state-btn btn-home active-home';
          btnMove.className = 'state-btn btn-move';
        } else {
          btnMove.className = 'state-btn btn-move active-move';
          btnHome.className = 'state-btn btn-home';
        }
      }
      try {
        await fetch(endpoint, { method: 'POST' });
        setTimeout(updateDisplay, 400);
      } catch (err) {
        console.error('Trigger servo error:', err);
      }
    }

    updateDisplay();
    setInterval(updateDisplay, 2000);
  </script>
</body>
</html>`;
}


/* ============================================================
   INDIVIDUAL DISPLAY ROUTES (Glassmorphic Industrial Cards)
   ============================================================ */

/* ---------- 1. Switchyard Temperature ---------- */
app.get("/display/switchyard-temp", (req, res) => {
  res.send(
    iframeDisplayPage({
      title: "Switchyard Temp",
      valueExpression: `Number(state.sensors?.temperatureC).toFixed(1)`,
      unit: " °C",
      theme: req.query.theme || "amber",
      icon: "🌡️",
      type: "sensor",
      metricLabel: "TEMPERATURE"
    })
  );
});

/* ============================================================
   COOLING TOWER UNIFIED CARD (Temp + Humidity + Mist ON/OFF)
   ============================================================ */
function coolingTowerDisplayPage(towerNum, customTheme) {
  const isTower2 = Number(towerNum) === 2;
  const bg = "linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%)";
  // Bright non-blue neon colors: Amber / Warm Gold for Tower 1, Flame Orange for Tower 2
  const border = isTower2 ? "1.5px solid rgba(255, 119, 0, 0.65)" : "1.5px solid rgba(255, 190, 11, 0.65)";
  const shadowGlow = isTower2 ? "rgba(255, 119, 0, 0.35)" : "rgba(255, 190, 11, 0.35)";
  const textShadowGlow = isTower2 ? "rgba(255, 119, 0, 0.5)" : "rgba(255, 190, 11, 0.5)";
  const titleColor = isTower2 ? "#fff7ed" : "#fefce8";
  const labelColor = isTower2 ? "#ff9e00" : "#ffbe0b";
  const unitColor = isTower2 ? "#fed7aa" : "#fde047";
  const badgeColor = isTower2 ? "#fed7aa" : "#fde047";
  const icon = isTower2 ? "❄️" : "💧";

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Cooling Tower ${towerNum}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    ::-webkit-scrollbar { display: none; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
    }

    .card {
      width: 340px;
      border-radius: 18px;
      padding: 14px 16px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: ${bg};
      border: ${border};
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px ${shadowGlow};
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: none !important;
    }
    .badge-status {
      display: none !important;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: rgba(0, 0, 0, 0.40);
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      padding: 6px 8px;
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .sensor-label {
      font-size: 20px;
      font-weight: 800;
      color: ${labelColor};
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 2px 4px rgba(0, 0, 0, 0.95);
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 32px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px ${textShadowGlow};
    }
    .sensor-unit {
      font-size: 20px;
      font-weight: 800;
      color: ${unitColor};
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.9);
    }

    .controls-section {
      display: flex;
      flex-direction: column;
      gap: 8px;
      border-top: 1px solid rgba(255, 255, 255, 0.10);
      padding-top: 8px;
    }
    .control-row {
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(0, 0, 0, 0.18);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 10px;
      padding: 6px 8px;
    }
    .control-name {
      display: none !important;
    }
    .btn-group {
      display: flex;
      gap: 8px;
      align-items: center;
      width: 100%;
    }
    .state-btn {
      flex: 1;
      font-size: 17px;
      font-weight: 800;
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.25);
      color: rgba(255, 255, 255, 0.85);
      background: rgba(0, 0, 0, 0.22);
      cursor: pointer;
      transition: all 0.2s ease;
      line-height: 1.2;
      user-select: none;
      text-align: center;
    }
    .state-btn:active {
      transform: scale(0.96);
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 14px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 14px rgba(220, 38, 38, 0.6);
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="sensor-grid">
        <div class="sensor-box">
          <span class="sensor-label">Temperature</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-temp">--</span>
            <span class="sensor-unit">°C</span>
          </div>
        </div>

        <div class="sensor-box">
          <span class="sensor-label">Humidity</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-hum">--</span>
            <span class="sensor-unit">%</span>
          </div>
        </div>
      </div>

      <div class="controls-section">
        <div class="control-row">
          <div class="btn-group">
            <button class="state-btn" id="btn-on" onclick="setMist(true)">ON</button>
            <button class="state-btn" id="btn-off" onclick="setMist(false)">OFF</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const towerNum = ${towerNum};
    const tempEl = document.getElementById('val-temp');
    const humEl = document.getElementById('val-hum');
    const btnOn = document.getElementById('btn-on');
    const btnOff = document.getElementById('btn-off');

    function updateMistButtons(isOn) {
      if (btnOn && btnOff) {
        btnOn.className = 'state-btn' + (isOn ? ' active-on' : '');
        btnOff.className = 'state-btn' + (!isOn ? ' active-off' : '');
      }
    }

    async function setMist(enable) {
      const endpoint = enable ? \`/api/control/mist\${towerNum}/on\` : \`/api/control/mist\${towerNum}/off\`;
      updateMistButtons(enable);
      try {
        await fetch(endpoint, { method: 'POST' });
      } catch (err) {
        console.error('Failed to trigger mist command:', err);
      }
    }

    async function updateDisplay() {
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        if (!data.success || !data.state) return;
        const state = data.state;
        const sensors = state.sensors || {};
        const actuators = state.actuators || {};

        const tempVal = sensors[\`coolingTower\${towerNum}TempC\`];
        const humVal = sensors[\`coolingTower\${towerNum}Humidity\`];
        const mistState = actuators[\`mist\${towerNum}\`];

        if (tempEl) {
          tempEl.textContent = (tempVal !== null && tempVal !== undefined && !Number.isNaN(Number(tempVal)))
            ? Number(tempVal).toFixed(1)
            : '--';
        }

        if (humEl) {
          humEl.textContent = (humVal !== null && humVal !== undefined && !Number.isNaN(Number(humVal)))
            ? Number(humVal).toFixed(1)
            : '--';
        }

        const isOn = mistState === true || mistState === 'ON' || mistState === 'true';
        updateMistButtons(isOn);
      } catch (err) {
        console.error('Update error:', err);
      }
    }

    updateDisplay();
    setInterval(updateDisplay, 2000);
  </script>
</body>
</html>`;
}

/* ---------- 2. Cooling Tower 1 (Unified: Temp + Humidity + Mist Control) ---------- */
app.get(["/display/cooling-tower-1", "/display/cooling-tower-1-card"], (req, res) => {
  res.send(coolingTowerDisplayPage(1, req.query.theme || "teal"));
});

app.get(["/display/cooling-tower-1-temp", "/display/cooling-tower-1-humidity"], (req, res) => {
  if (req.query.single === "1") {
    const isHumidity = req.path.includes("humidity");
    res.send(
      iframeDisplayPage({
        title: "Cooling Tower 1",
        valueExpression: isHumidity
          ? `Number(state.sensors?.coolingTower1Humidity).toFixed(1)`
          : `Number(state.sensors?.coolingTower1TempC).toFixed(1)`,
        unit: isHumidity ? " %" : " °C",
        theme: isHumidity ? "emerald" : "teal",
        icon: isHumidity ? "💧" : "🌡️",
        type: "sensor",
        metricLabel: isHumidity ? "HUMIDITY" : "TEMPERATURE"
      })
    );
  } else {
    res.send(coolingTowerDisplayPage(1, req.query.theme || "teal"));
  }
});

/* ---------- 3. Cooling Tower 2 (Unified: Temp + Humidity + Mist Control) ---------- */
app.get(["/display/cooling-tower-2", "/display/cooling-tower-2-card"], (req, res) => {
  res.send(coolingTowerDisplayPage(2, req.query.theme || "cyan"));
});

app.get(["/display/cooling-tower-2-temp", "/display/cooling-tower-2-humidity"], (req, res) => {
  if (req.query.single === "1") {
    const isHumidity = req.path.includes("humidity");
    res.send(
      iframeDisplayPage({
        title: "Cooling Tower 2",
        valueExpression: isHumidity
          ? `Number(state.sensors?.coolingTower2Humidity).toFixed(1)`
          : `Number(state.sensors?.coolingTower2TempC).toFixed(1)`,
        unit: isHumidity ? " %" : " °C",
        theme: isHumidity ? "blue" : "cyan",
        icon: isHumidity ? "💧" : "❄️",
        type: "sensor",
        metricLabel: isHumidity ? "HUMIDITY" : "TEMPERATURE"
      })
    );
  } else {
    res.send(coolingTowerDisplayPage(2, req.query.theme || "cyan"));
  }
});

/* ============================================================
   LIGHTING & POWER UNIFIED CARD (Lighting Power + Total Power + ON/OFF)
   ============================================================ */
function lightingUnifiedDisplayPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Lighting & Power</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    ::-webkit-scrollbar { display: none; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
    }

    .card {
      width: 330px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(253, 224, 71, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(234, 179, 8, 0.25);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: none !important;
    }
    .badge-status {
      display: none !important;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: rgba(0, 0, 0, 0.40);
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      padding: 6px 8px;
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .sensor-label {
      font-size: 20px;
      font-weight: 800;
      color: #fef08a;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 2px 4px rgba(0, 0, 0, 0.95);
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 32px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px rgba(234, 179, 8, 0.4);
    }
    .sensor-unit {
      font-size: 20px;
      font-weight: 800;
      color: #fde047;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.9);
    }

    .controls-section {
      display: flex;
      flex-direction: column;
      gap: 8px;
      border-top: 1px solid rgba(255, 255, 255, 0.10);
      padding-top: 8px;
    }
    .control-row {
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(0, 0, 0, 0.18);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 10px;
      padding: 6px 8px;
    }
    .control-name {
      display: none !important;
    }
    .btn-group {
      display: flex;
      gap: 8px;
      align-items: center;
      width: 100%;
    }
    .state-btn {
      flex: 1;
      font-size: 17px;
      font-weight: 800;
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.25);
      color: rgba(255, 255, 255, 0.85);
      background: rgba(0, 0, 0, 0.22);
      cursor: pointer;
      transition: all 0.2s ease;
      line-height: 1.2;
      user-select: none;
      text-align: center;
    }
    .state-btn:active {
      transform: scale(0.96);
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 14px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 14px rgba(220, 38, 38, 0.6);
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="sensor-grid">
        <div class="sensor-box">
          <span class="sensor-label">Light Power</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-light-pwr">--</span>
            <span class="sensor-unit">W</span>
          </div>
        </div>

        <div class="sensor-box">
          <span class="sensor-label">Total Power</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-total-pwr">--</span>
            <span class="sensor-unit">W</span>
          </div>
        </div>
      </div>

      <div class="controls-section">
        <div class="control-row">
          <div class="btn-group">
            <button class="state-btn" id="btn-on" onclick="setLighting(true)">ON</button>
            <button class="state-btn" id="btn-off" onclick="setLighting(false)">OFF</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const lightPwrEl = document.getElementById('val-light-pwr');
    const totalPwrEl = document.getElementById('val-total-pwr');
    const btnOn = document.getElementById('btn-on');
    const btnOff = document.getElementById('btn-off');

    function updateLightingButtons(isOn) {
      if (btnOn && btnOff) {
        btnOn.className = 'state-btn' + (isOn ? ' active-on' : '');
        btnOff.className = 'state-btn' + (!isOn ? ' active-off' : '');
      }
    }

    async function setLighting(enable) {
      const endpoint = enable ? '/api/control/lighting/on' : '/api/control/lighting/off';
      updateLightingButtons(enable);
      try {
        await fetch(endpoint, { method: 'POST' });
      } catch (err) {
        console.error('Failed to trigger lighting command:', err);
      }
    }

    async function updateDisplay() {
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        if (!data.success || !data.state) return;
        const state = data.state;
        const sensors = state.sensors || {};
        const actuators = state.actuators || {};

        const lightPwr = sensors.lightingPowerW;
        const totalPwr = sensors.powerW;
        const lightState = actuators.lighting;

        if (lightPwrEl) {
          lightPwrEl.textContent = (lightPwr !== null && lightPwr !== undefined && !Number.isNaN(Number(lightPwr)))
            ? Number(lightPwr).toFixed(2)
            : '--';
        }

        if (totalPwrEl) {
          totalPwrEl.textContent = (totalPwr !== null && totalPwr !== undefined && !Number.isNaN(Number(totalPwr)))
            ? Number(totalPwr).toFixed(2)
            : '--';
        }

        const isOn = lightState === true || lightState === 'ON' || lightState === 'true';
        updateLightingButtons(isOn);
      } catch (err) {
        console.error('Update error:', err);
      }
    }

    updateDisplay();
    setInterval(updateDisplay, 2000);
  </script>
</body>
</html>`;
}

/* ---------- 6. Factory Lighting & Power (Unified: Lighting Power + Total Power + ON/OFF) ---------- */
app.get(
  [
    "/display/lighting",
    "/display/lighting-power",
    "/display/total-power",
    "/display/lighting-status",
    "/display/lighting-card"
  ],
  (req, res) => {
    if (req.query.single === "1") {
      if (req.path.includes("total-power")) {
        res.send(
          iframeDisplayPage({
            title: "Total Power",
            valueExpression: `Number(state.sensors?.powerW).toFixed(2)`,
            unit: " W",
            theme: req.query.theme || "crimson",
            icon: "⚡",
            type: "sensor",
            metricLabel: "TOTAL POWER"
          })
        );
      } else if (req.path.includes("lighting-power")) {
        res.send(
          iframeDisplayPage({
            title: "Lighting Power",
            valueExpression: `Number(state.sensors?.lightingPowerW).toFixed(2)`,
            unit: " W",
            theme: req.query.theme || "gold",
            icon: "💡",
            type: "sensor",
            metricLabel: "POWER"
          })
        );
      } else {
        res.send(
          iframeDisplayPage({
            title: "Factory Lights",
            valueExpression: `state.actuators?.lighting ? 'ON' : 'OFF'`,
            theme: req.query.theme || "gold",
            icon: "💡",
            type: "switch",
            rowLabel: "Main Lighting"
          })
        );
      }
    } else {
      res.send(lightingUnifiedDisplayPage());
    }
  }
);

/* ============================================================
   PRODUCT COUNTS UNIFIED CARD (Batch Count + Lifetime Count)
   ============================================================ */
function productionCountsDisplayPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Product Counts</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    ::-webkit-scrollbar { display: none; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
    }

    .card {
      width: 330px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(52, 211, 153, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(16, 185, 129, 0.25);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: none !important;
    }
    .badge-status {
      display: none !important;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: transparent;
      border: none;
      padding: 4px 6px;
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .sensor-label {
      font-size: 18px;
      font-weight: 800;
      color: #a7f3d0;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.95);
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 32px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px rgba(16, 185, 129, 0.4);
    }
    .sensor-unit {
      font-size: 18px;
      font-weight: 700;
      color: #6ee7b7;
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="sensor-grid">
        <div class="sensor-box">
          <span class="sensor-label">Batch Count</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-batch-count">--</span>
            <span class="sensor-unit">pcs</span>
          </div>
        </div>

        <div class="sensor-box">
          <span class="sensor-label">total Count</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-life-count">--</span>
            <span class="sensor-unit">pcs</span>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const batchEl = document.getElementById('val-batch-count');
    const lifeEl = document.getElementById('val-life-count');

    async function updateDisplay() {
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        if (!data.success || !data.state) return;
        const state = data.state;
        const sensors = state.sensors || {};

        const batchVal = sensors.productCount;
        const lifeVal = sensors.productCountTotal;

        if (batchEl) {
          batchEl.textContent = (batchVal !== null && batchVal !== undefined && !Number.isNaN(Number(batchVal)))
            ? Number(batchVal).toFixed(0)
            : '--';
        }

        if (lifeEl) {
          lifeEl.textContent = (lifeVal !== null && lifeVal !== undefined && !Number.isNaN(Number(lifeVal)))
            ? Number(lifeVal).toFixed(0)
            : '--';
        }
      } catch (err) {
        console.error('Update error:', err);
      }
    }

    updateDisplay();
    setInterval(updateDisplay, 2000);
  </script>
</body>
</html>`;
}

/* ============================================================
   ADVANCED PRODUCTION MES TERMINAL (Batch Management & Analytics)
   ============================================================ */
function productionMesDisplayPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MES Production &amp; Batch Terminal</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow-x: hidden;
      overflow-y: auto;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      justify-content: center;
      align-items: flex-start;
      padding: 10px 0;
    }
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-thumb { background: rgba(245, 158, 11, 0.4); border-radius: 4px; }
    ::-webkit-scrollbar-track { background: transparent; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
      width: 100%;
      max-width: 530px;
      justify-content: center;
      box-sizing: border-box;
      transition: max-width 0.35s ease;
    }
    .card-wrap.expanded {
      max-width: 980px;
    }

    .card {
      width: 100%;
      border-radius: 20px;
      padding: 14px 16px;
      color: #ffffff;
      backdrop-filter: blur(22px) saturate(190%);
      -webkit-backdrop-filter: blur(22px) saturate(190%);
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.50) 0%, rgba(10, 16, 30, 0.45) 100%);
      border: 1.5px solid rgba(251, 191, 36, 0.70);
      box-shadow: 0 14px 40px -5px rgba(0, 0, 0, 0.70), 0 0 25px rgba(245, 158, 11, 0.25);
      display: flex;
      flex-direction: column;
      gap: 10px;
      box-sizing: border-box;
      overflow: hidden;
      transition: all 0.3s ease;
    }

    /* Header (Clean, with Expand Data Button) */
    .terminal-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      border-bottom: 1px solid rgba(251, 191, 36, 0.25);
      padding-bottom: 8px;
      box-sizing: border-box;
      width: 100%;
    }
    .terminal-title {
      font-size: 20px !important;
      font-weight: 800;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      color: #fef08a;
      text-shadow: 0 1px 6px rgba(0, 0, 0, 0.9);
      display: flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
      min-width: 0;
    }

    .btn-expand-arrow {
      background: rgba(251, 191, 36, 0.18);
      border: 1px solid rgba(251, 191, 36, 0.55);
      color: #fef08a;
      border-radius: 8px;
      padding: 5px 12px;
      font-size: 14px !important;
      font-weight: 800;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 5px;
      transition: all 0.2s ease;
      white-space: nowrap;
      text-transform: uppercase;
      letter-spacing: 0.02em;
      flex-shrink: 0;
      box-sizing: border-box;
    }
    .btn-expand-arrow:hover {
      background: rgba(251, 191, 36, 0.35);
      box-shadow: 0 0 10px rgba(245, 158, 11, 0.4);
    }
    .card-wrap.expanded .btn-expand-arrow {
      background: rgba(148, 163, 184, 0.2);
      border-color: rgba(148, 163, 184, 0.4);
      color: #cbd5e1;
    }

    /* Grid Layout: Collapsed (1 col) by default, 2 cols when expanded */
    .landscape-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
      align-items: stretch;
      width: 100%;
      box-sizing: border-box;
    }
    .card-wrap.expanded .landscape-grid {
      grid-template-columns: 1fr 1fr;
    }

    .col-left {
      display: flex;
      flex-direction: column;
      gap: 8px;
      min-width: 0;
      width: 100%;
      box-sizing: border-box;
    }
    .col-right {
      display: none;
      flex-direction: column;
      gap: 8px;
      min-width: 0;
      width: 100%;
      box-sizing: border-box;
    }
    .card-wrap.expanded .col-right {
      display: flex;
    }

    /* Live 2-Col Metrics (20px labels as requested) */
    .metric-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      width: 100%;
      box-sizing: border-box;
    }
    .metric-box {
      background: rgba(0, 0, 0, 0.35);
      border: 1px solid rgba(255, 255, 255, 0.10);
      border-radius: 12px;
      padding: 8px 12px;
      display: flex;
      flex-direction: column;
      gap: 2px;
      box-sizing: border-box;
      min-width: 0;
    }
    .metric-label {
      font-size: 20px !important;
      font-weight: 800;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.95);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .metric-box.orange .metric-label { color: #fed7aa; }
    .metric-box.emerald .metric-label { color: #a7f3d0; }

    .metric-value-row {
      display: flex;
      align-items: baseline;
      gap: 5px;
      margin-top: 1px;
    }
    .metric-value {
      font-size: 32px !important;
      font-weight: 900;
      letter-spacing: -0.02em;
      line-height: 1.1;
      color: #ffffff;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.9);
    }
    .metric-box.orange .metric-value {
      color: #ffbe0b;
      text-shadow: 0 2px 8px rgba(0,0,0,0.9), 0 0 14px rgba(255, 190, 11, 0.45);
    }
    .metric-box.emerald .metric-value {
      color: #34d399;
      text-shadow: 0 2px 8px rgba(0,0,0,0.9), 0 0 14px rgba(52, 211, 153, 0.45);
    }
    .metric-unit {
      font-size: 20px !important;
      font-weight: 800;
      color: rgba(255, 255, 255, 0.85);
    }

    /* Active Batch Execution Card (Proportional, no wrap) */
    .batch-panel {
      background: rgba(0, 0, 0, 0.38);
      border: 1px solid rgba(251, 191, 36, 0.35);
      border-radius: 12px;
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 8px;
      box-sizing: border-box;
      min-width: 0;
      width: 100%;
    }
    .batch-meta-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 20px;
      font-weight: 800;
    }
    .batch-id-tag {
      font-size: 20px !important;
      color: #ffbe0b;
      font-weight: 800;
      letter-spacing: 0.02em;
      display: flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
    }
    .batch-target-tag {
      font-size: 19px !important;
      font-weight: 800;
      color: #f1f5f9;
      background: rgba(255, 255, 255, 0.12);
      padding: 3px 9px;
      border-radius: 6px;
      white-space: nowrap;
    }

    /* Progress Bar */
    .progress-container {
      display: flex;
      flex-direction: column;
      gap: 5px;
      width: 100%;
      box-sizing: border-box;
    }
    .progress-bar-track {
      width: 100%;
      height: 16px;
      background: rgba(0, 0, 0, 0.6);
      border-radius: 8px;
      overflow: hidden;
      border: 1px solid rgba(255, 255, 255, 0.15);
      position: relative;
      box-sizing: border-box;
    }
    .progress-bar-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #ff7700 0%, #ffbe0b 100%);
      border-radius: 8px;
      box-shadow: 0 0 10px rgba(255, 190, 11, 0.6);
      transition: width 0.4s ease;
    }
    .progress-info-row {
      display: flex;
      justify-content: space-between;
      font-size: 18px !important;
      font-weight: 700;
      color: #cbd5e1;
      white-space: nowrap;
    }
    .progress-highlight {
      color: #ffbe0b;
      font-weight: 800;
      font-size: 18px !important;
    }

    /* Batch Action Buttons (Increased by 1px to 16px) */
    .batch-controls {
      display: flex;
      gap: 6px;
      margin-top: 3px;
      width: 100%;
      box-sizing: border-box;
    }
    .btn-action {
      flex: 1;
      padding: 8px 6px;
      font-size: 16px !important;
      font-weight: 800;
      border-radius: 8px;
      border: 1px solid transparent;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 5px;
      transition: all 0.2s ease;
      text-transform: uppercase;
      letter-spacing: 0.02em;
      white-space: nowrap;
      height: 40px;
      box-sizing: border-box;
      min-width: 0;
    }
    .btn-action:active {
      transform: scale(0.96);
    }
    .btn-pause {
      background: rgba(245, 158, 11, 0.22);
      border-color: #fbbf24;
      color: #fef08a;
    }
    .btn-pause:hover {
      background: rgba(245, 158, 11, 0.4);
      box-shadow: 0 0 10px rgba(245, 158, 11, 0.5);
    }
    .btn-extend {
      background: rgba(59, 130, 246, 0.22);
      border-color: #60a5fa;
      color: #bfdbfe;
    }
    .btn-extend:hover {
      background: rgba(59, 130, 246, 0.4);
      box-shadow: 0 0 10px rgba(59, 130, 246, 0.5);
    }
    .btn-next {
      background: linear-gradient(135deg, #ffbe0b, #f59e0b);
      border-color: #fde047;
      color: #111827;
      font-weight: 900;
    }
    .btn-next:hover {
      box-shadow: 0 0 14px rgba(255, 190, 11, 0.7);
    }

    /* Analytics Navigation Tabs (Contained, no overflow) */
    .tabs-header {
      display: flex;
      gap: 4px;
      background: rgba(0, 0, 0, 0.4);
      padding: 3px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      width: 100%;
      box-sizing: border-box;
    }
    .tab-btn {
      flex: 1;
      font-size: 14px !important;
      font-weight: 800;
      padding: 6px 4px;
      border-radius: 6px;
      border: none;
      background: transparent;
      color: #94a3b8;
      cursor: pointer;
      transition: all 0.2s ease;
      text-transform: uppercase;
      letter-spacing: 0.01em;
      white-space: nowrap;
      text-align: center;
      min-width: 0;
    }
    .tab-btn.active {
      background: rgba(251, 191, 36, 0.25);
      border: 1px solid rgba(251, 191, 36, 0.55);
      color: #fef08a;
      box-shadow: 0 0 8px rgba(245, 158, 11, 0.3);
    }

    /* Tab Content Area (Fits perfectly in card) */
    .tab-content {
      background: rgba(0, 0, 0, 0.35);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 12px;
      padding: 10px 12px;
      flex: 1;
      min-height: 250px;
      max-height: 295px;
      overflow-y: auto;
      overflow-x: hidden;
      display: flex;
      flex-direction: column;
      gap: 7px;
      width: 100%;
      box-sizing: border-box;
    }
    .tab-title {
      font-size: 19px !important;
      font-weight: 800;
      color: #fef08a;
      margin-bottom: 4px;
      white-space: nowrap;
    }
    .tab-view {
      display: none;
      flex-direction: column;
      gap: 6px;
      width: 100%;
      box-sizing: border-box;
    }
    .tab-view.active {
      display: flex;
    }

    /* Bar Chart / Item Rows (Contained & aligned, 0 overlap) */
    .bar-row {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 19px !important;
      font-weight: 800;
      width: 100%;
      box-sizing: border-box;
    }
    .bar-label {
      font-size: 19px !important;
      width: 78px;
      min-width: 78px;
      color: #cbd5e1;
      font-weight: 800;
      white-space: nowrap;
      text-align: right;
      flex-shrink: 0;
    }
    .bar-track {
      flex: 1;
      min-width: 50px;
      height: 20px;
      background: rgba(255, 255, 255, 0.08);
      border-radius: 6px;
      overflow: hidden;
      display: flex;
      box-sizing: border-box;
    }
    .bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #f59e0b, #ffbe0b);
      border-radius: 6px;
      transition: width 0.3s ease;
    }
    .bar-val {
      font-size: 19px !important;
      width: 78px;
      min-width: 78px;
      text-align: right;
      color: #fef08a;
      font-weight: 800;
      white-space: nowrap;
      flex-shrink: 0;
    }

    /* Grid for Weekly (Ample space, 0 overlap) */
    .report-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(92px, 1fr));
      gap: 8px;
      width: 100%;
      box-sizing: border-box;
    }
    .report-card {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.10);
      border-radius: 8px;
      padding: 7px 6px;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 3px;
      box-sizing: border-box;
    }
    .report-card.today {
      border-color: rgba(251, 191, 36, 0.6);
      background: rgba(251, 191, 36, 0.14);
    }
    .report-key {
      font-size: 17px !important;
      font-weight: 800;
      color: #cbd5e1;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .report-num {
      font-size: 24px !important;
      font-weight: 900;
      color: #ffbe0b;
    }
    .report-unit {
      font-size: 16px !important;
      font-weight: 700;
      color: #94a3b8;
    }

    /* Inline Action Drawer (Inside Card, no modal popup) */
    .inline-action-panel {
      display: none;
      background: rgba(15, 23, 42, 0.75);
      border: 1px solid #fbbf24;
      border-radius: 9px;
      padding: 8px 10px;
      margin-top: 4px;
      flex-direction: column;
      gap: 6px;
      box-sizing: border-box;
      width: 100%;
    }
    .inline-action-title-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .inline-action-title {
      font-size: 14px !important;
      font-weight: 800;
      color: #fef08a;
    }
    .inline-close-btn {
      cursor: pointer;
      color: #94a3b8;
      font-size: 16px;
      font-weight: 800;
      padding: 0 4px;
      transition: color 0.2s ease;
    }
    .inline-close-btn:hover {
      color: #f87171;
    }
    .inline-action-controls {
      display: flex;
      gap: 6px;
      align-items: center;
      width: 100%;
      box-sizing: border-box;
    }
    .inline-number-input {
      flex: 1;
      height: 36px;
      background: #1e293b;
      border: 1px solid #475569;
      border-radius: 6px;
      color: #ffffff;
      font-size: 16px;
      font-weight: 800;
      padding: 0 8px;
      outline: none;
      box-sizing: border-box;
    }
    .inline-number-input:focus {
      border-color: #fbbf24;
    }
  </style>
</head>
<body>
  <div class="card-wrap" id="card-wrap-box">
    <div class="card" id="mes-card">
      <!-- 1. TERMINAL HEADER (With Data Expand Arrow) -->
      <div class="terminal-header">
        <div class="terminal-title">⚙️ BATCH PRODUCTION CONTROL</div>
        <button id="btn-expand-data" class="btn-expand-arrow" onclick="toggleDataExpand()" title="Expand / Collapse Production History">
          <span id="expand-btn-text">DATA</span>
          <span id="expand-arrow-icon">❯</span>
        </button>
      </div>

      <!-- 2. LANDSCAPE 2-COLUMN LAYOUT -->
      <div class="landscape-grid">
        <!-- LEFT COLUMN: Live Metrics & Active Batch -->
        <div class="col-left">
          <!-- 2-COL LIVE METRICS (20px labels & units) -->
          <div class="metric-grid">
            <div class="metric-box orange">
              <span class="metric-label">Current Pcs</span>
              <div class="metric-value-row">
                <span class="metric-value" id="live-cycle-val">--</span>
                <span class="metric-unit">pcs</span>
              </div>
            </div>
            <div class="metric-box emerald">
              <span class="metric-label">Total Output</span>
              <div class="metric-value-row">
                <span class="metric-value" id="live-life-val">--</span>
                <span class="metric-unit">pcs</span>
              </div>
            </div>
          </div>

          <!-- ACTIVE BATCH EXECUTION & MES CONTROLS -->
          <div class="batch-panel">
            <div class="batch-meta-row">
              <div class="batch-id-tag">📦 Batch: <span id="batch-id-val">BATCH-1001</span></div>
              <div class="batch-target-tag" id="batch-target-tag">Target: <strong id="batch-target-val">100 pcs</strong></div>
            </div>

            <div class="progress-container">
              <div class="progress-bar-track">
                <div class="progress-bar-fill" id="batch-progress-fill" style="width: 0%;"></div>
              </div>
              <div class="progress-info-row">
                <span>Produced: <strong class="progress-highlight" id="batch-produced-val">0</strong> pcs (<strong id="batch-pct-val">0%</strong>)</span>
                <span>Remaining: <strong id="batch-remaining-val">100</strong> pcs</span>
              </div>
            </div>

            <div class="batch-controls">
              <button class="btn-action btn-pause" id="btn-pause-resume" onclick="handlePauseResume()">⏹ STOP</button>
              <button class="btn-action btn-extend" id="btn-extend" onclick="openInlineAction('EXTEND')">➕ ADD PCS</button>
              <button class="btn-action btn-next" id="btn-next" onclick="openInlineAction('NEW_BATCH')">🚀 NEXT BATCH</button>
            </div>

            <!-- Inline Action Drawer (Inside Card, no modal popup) -->
            <div id="inline-action-panel" class="inline-action-panel">
              <div class="inline-action-title-row">
                <span id="inline-action-title" class="inline-action-title">Configure Target Pieces:</span>
                <span class="inline-close-btn" onclick="closeInlineAction()">✕</span>
              </div>
              <div class="inline-action-controls">
                <input type="number" id="inline-action-input" class="inline-number-input" value="100" min="1" max="10000" />
                <button id="inline-action-btn" class="btn-action btn-next" style="flex: 1; height: 36px; font-size: 14px; font-weight: 800;" onclick="submitInlineAction()">Start &amp; Run</button>
              </div>
            </div>
          </div>
        </div>

        <!-- RIGHT COLUMN: Production History & Analytics Tabs -->
        <div class="col-right">
          <div class="tabs-header">
            <button class="tab-btn active" onclick="switchTab('hourly')">Hourly</button>
            <button class="tab-btn" onclick="switchTab('daily')">Daily</button>
            <button class="tab-btn" onclick="switchTab('weekly')">Weekly</button>
            <button class="tab-btn" onclick="switchTab('monthly')">Monthly</button>
            <button class="tab-btn" onclick="switchTab('batches')">Batches</button>
          </div>

          <div class="tab-content">
            <!-- View: Hourly -->
            <div class="tab-view active" id="view-hourly">
              <div class="tab-title">Hourly Output (Last 12 Hours):</div>
              <div id="hourly-list" style="display:flex; flex-direction:column; gap:6px;">
                <div style="color:#94a3b8; font-size:18px;">Loading hourly data...</div>
              </div>
            </div>

            <!-- View: Daily -->
            <div class="tab-view" id="view-daily">
              <div class="tab-title">Date-wise Production (Last 7 Days):</div>
              <div id="daily-list" style="display:flex; flex-direction:column; gap:6px;">
                <div style="color:#94a3b8; font-size:18px;">Loading daily history...</div>
              </div>
            </div>

            <!-- View: Weekly (Date-wise breakdown) -->
            <div class="tab-view" id="view-weekly">
              <div class="tab-title">Weekly Output (Date-wise Breakdown):</div>
              <div class="report-grid" id="weekly-grid">
                <div style="color:#94a3b8; font-size:18px;">Loading weekly breakdown...</div>
              </div>
            </div>

            <!-- View: Monthly -->
            <div class="tab-view" id="view-monthly">
              <div class="tab-title">Month-wise Production Reports:</div>
              <div id="monthly-list" style="display:flex; flex-direction:column; gap:6px;">
                <div style="color:#94a3b8; font-size:18px;">Loading monthly reports...</div>
              </div>
            </div>

            <!-- View: Recent Batches -->
            <div class="tab-view" id="view-batches">
              <div class="tab-title">Recent Batches Execution Log:</div>
              <div id="batches-list" style="display:flex; flex-direction:column; gap:6px;">
                <div style="color:#94a3b8; font-size:18px;">Loading batches log...</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    let activeBatchStatus = 'RUNNING';
    let currentTab = 'hourly';

    function switchTab(tabName) {
      currentTab = tabName;
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-view').forEach(v => v.classList.remove('active'));

      const btn = Array.from(document.querySelectorAll('.tab-btn')).find(b => b.textContent.toLowerCase().includes(tabName.slice(0, 4)));
      if (btn) btn.classList.add('active');

      const view = document.getElementById('view-' + tabName);
      if (view) view.classList.add('active');
    }

    async function fetchActiveBatch() {
      try {
        const res = await fetch('/api/batch/active', { cache: 'no-store' });
        const data = await res.json();
        if (!data.success) return;

        const batch = data.batch || {};
        const sensors = data.sensors || {};
        activeBatchStatus = batch.status || 'RUNNING';

        // Update live counters
        const cycleEl = document.getElementById('live-cycle-val');
        const lifeEl = document.getElementById('live-life-val');
        if (cycleEl) cycleEl.textContent = Number(sensors.productCount || 0).toFixed(0);
        if (lifeEl) lifeEl.textContent = Number(sensors.productCountTotal || 0).toFixed(0);

        // Update pause/resume (Start / Stop) button label
        const prBtn = document.getElementById('btn-pause-resume');
        if (prBtn) {
          if (activeBatchStatus === 'PAUSED' || activeBatchStatus === 'COMPLETED') {
            prBtn.textContent = '▶ START';
            prBtn.className = 'btn-action btn-next';
          } else {
            prBtn.textContent = '⏹ STOP';
            prBtn.className = 'btn-action btn-pause';
          }
        }

        // Update batch meta & progress
        const isReached = (batch.target_units > 0 && (batch.current_output || 0) >= batch.target_units);
        const isCompleted = activeBatchStatus === 'COMPLETED' || isReached;

        document.getElementById('batch-id-val').textContent = batch.batch_id || 'BATCH-1001';

        const targetTag = document.getElementById('batch-target-val');
        if (targetTag) {
          if (isReached || activeBatchStatus === 'COMPLETED') {
            targetTag.innerHTML = (batch.target_units || 100) + ' pcs <span style="color:#f87171; font-size:11px; margin-left:3px;">● STOPPED</span>';
          } else {
            targetTag.textContent = (batch.target_units || 100) + ' pcs';
          }
        }

        document.getElementById('batch-produced-val').textContent = batch.current_output || 0;
        document.getElementById('batch-pct-val').textContent = isReached ? '100% (REACHED)' : ((batch.progressPct || 0) + '%');
        document.getElementById('batch-remaining-val').textContent = batch.remainingUnits !== undefined ? batch.remainingUnits : 0;

        const fill = document.getElementById('batch-progress-fill');
        if (fill) fill.style.width = Math.min(100, Math.max(0, batch.progressPct || 0)) + '%';

      } catch (err) {
        console.error('Fetch batch error:', err);
      }
    }

    async function fetchAnalytics() {
      try {
        const res = await fetch('/api/batch/analytics', { cache: 'no-store' });
        const data = await res.json();
        if (!data.success || !data.analytics) return;

        const a = data.analytics;

        // 1. Hourly render
        const hourlyEl = document.getElementById('hourly-list');
        if (hourlyEl && Array.isArray(a.hourly) && a.hourly.length) {
          const maxOut = Math.max(1, ...a.hourly.map(h => h.output));
          hourlyEl.innerHTML = a.hourly.slice(-8).map(h => {
            const pct = Math.round((h.output / maxOut) * 100);
            return '<div class="bar-row">' +
              '<span class="bar-label">' + h.label + '</span>' +
              '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%;"></div></div>' +
              '<span class="bar-val">' + h.output + ' pcs</span>' +
            '</div>';
          }).join('');
        }

        // 2. Daily render
        const dailyEl = document.getElementById('daily-list');
        if (dailyEl && Array.isArray(a.daily) && a.daily.length) {
          const maxDay = Math.max(1, ...a.daily.map(d => d.output));
          dailyEl.innerHTML = a.daily.map(d => {
            const pct = Math.round((d.output / maxDay) * 100);
            return '<div class="bar-row">' +
              '<span class="bar-label">' + (d.label || d.date) + '</span>' +
              '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%; background: linear-gradient(90deg, #ff9e00, #34d399);"></div></div>' +
              '<span class="bar-val">' + d.output + ' pcs</span>' +
            '</div>';
          }).join('');
        }

        // 3. Weekly render (Date-wise breakdown)
        const weeklyEl = document.getElementById('weekly-grid');
        if (weeklyEl && Array.isArray(a.weekly) && a.weekly.length) {
          const todayStr = new Date().toISOString().slice(0, 10);
          weeklyEl.innerHTML = a.weekly.map(w => {
            const isToday = w.date === todayStr;
            return '<div class="report-card ' + (isToday ? 'today' : '') + '">' +
              '<span class="report-key">' + (w.day || w.date) + '</span>' +
              '<span class="report-num">' + w.output + '</span>' +
              '<span class="report-unit">pcs</span>' +
            '</div>';
          }).join('');
        }

        // 4. Monthly render
        const monthlyEl = document.getElementById('monthly-list');
        if (monthlyEl && Array.isArray(a.monthly) && a.monthly.length) {
          monthlyEl.innerHTML = a.monthly.map(m => {
            return '<div style="display:flex; justify-content:space-between; align-items:center; background:rgba(255,255,255,0.05); padding:8px 12px; border-radius:8px;">' +
              '<div style="display:flex; flex-direction:column;">' +
                '<strong style="color:#fef08a; font-size:22px;">' + m.month + '</strong>' +
                '<span style="font-size:18px; color:#cbd5e1;">' + m.activeDays + ' production days</span>' +
              '</div>' +
              '<div style="text-align:right;">' +
                '<strong style="color:#ffbe0b; font-size:23px;">' + m.output + ' pcs</strong>' +
                '<div style="font-size:18px; color:#cbd5e1;">Cumulative: ' + m.endTotal + '</div>' +
              '</div>' +
            '</div>';
          }).join('');
        }

        // 5. Batches log render
        const batchesEl = document.getElementById('batches-list');
        if (batchesEl && Array.isArray(a.batches) && a.batches.length) {
          batchesEl.innerHTML = a.batches.map(b => {
            const isRun = b.status === 'RUNNING';
            const isPau = b.status === 'PAUSED';
            const color = isRun ? '#34d399' : isPau ? '#fbbf24' : '#cbd5e1';
            return '<div style="display:flex; justify-content:space-between; align-items:center; background:rgba(255,255,255,0.05); padding:8px 12px; border-radius:8px;">' +
              '<div>' +
                '<strong style="color:#ffbe0b; font-size:22px;">' + b.batch_id + '</strong>' +
                '<div style="color:#cbd5e1; font-size:18px; font-weight:700;">Target: ' + b.target_units + ' pcs</div>' +
              '</div>' +
              '<div style="text-align:right;">' +
                '<strong style="color:#ffffff; font-size:22px;">' + b.current_output + ' pcs</strong>' +
                '<div style="color:' + color + '; font-weight:900; font-size:18px;">' + b.status + '</div>' +
              '</div>' +
            '</div>';
          }).join('');
        }

      } catch (err) {
        console.error('Fetch analytics error:', err);
      }
    }

    async function handlePauseResume() {
      const endpoint = activeBatchStatus === 'PAUSED' ? '/api/batch/resume' : '/api/batch/pause';
      try {
        await fetch(endpoint, { method: 'POST' });
        setTimeout(() => { fetchActiveBatch(); fetchAnalytics(); }, 300);
      } catch (e) {
        console.error(e);
      }
    }

    async function handleComplete() {
      if (!confirm('Mark current batch as completed and save final output?')) return;
      try {
        await fetch('/api/batch/complete', { method: 'POST' });
        setTimeout(() => { fetchActiveBatch(); fetchAnalytics(); }, 300);
      } catch (e) {
        console.error(e);
      }
    }

    function toggleDataExpand() {
      const wrap = document.getElementById('card-wrap-box');
      const btnText = document.getElementById('expand-btn-text');
      const btnIcon = document.getElementById('expand-arrow-icon');
      if (!wrap) return;

      const isExp = wrap.classList.toggle('expanded');
      if (btnText && btnIcon) {
        if (isExp) {
          btnText.textContent = 'CLOSE';
          btnIcon.textContent = '❮';
        } else {
          btnText.textContent = 'DATA';
          btnIcon.textContent = '❯';
        }
      }
    }

    let currentInlineMode = null;

    function openInlineAction(mode) {
      if (currentInlineMode === mode) {
        closeInlineAction();
        return;
      }

      currentInlineMode = mode;
      const panel = document.getElementById('inline-action-panel');
      const title = document.getElementById('inline-action-title');
      const input = document.getElementById('inline-action-input');
      const btn = document.getElementById('inline-action-btn');

      if (!panel || !title || !input || !btn) return;

      if (mode === 'EXTEND') {
        panel.style.borderColor = '#60a5fa';
        title.style.color = '#bfdbfe';
        title.textContent = '➕ Add Units to Current Batch (+pcs):';
        input.value = '50';
        btn.textContent = 'Extend & Run';
        btn.className = 'btn-action btn-extend';
      } else {
        panel.style.borderColor = '#fbbf24';
        title.style.color = '#fef08a';
        title.textContent = '🚀 Next Batch Target (pcs):';
        input.value = '100';
        btn.textContent = 'Start & Run';
        btn.className = 'btn-action btn-next';
      }

      panel.style.display = 'flex';
      setTimeout(() => {
        input.focus();
        input.select();
      }, 50);
    }

    function closeInlineAction() {
      const panel = document.getElementById('inline-action-panel');
      if (panel) panel.style.display = 'none';
      currentInlineMode = null;
    }

    async function submitInlineAction() {
      const input = document.getElementById('inline-action-input');
      const val = input ? Number(input.value) : 0;
      if (!val || val <= 0) {
        alert('Please enter a quantity greater than 0');
        return;
      }

      const mode = currentInlineMode;
      closeInlineAction();

      try {
        if (mode === 'EXTEND') {
          await fetch('/api/batch/extend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ addUnits: val })
          });
        } else {
          await fetch('/api/batch/new', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ targetUnits: val })
          });
        }
        setTimeout(() => { fetchActiveBatch(); fetchAnalytics(); }, 350);
      } catch (e) {
        console.error(e);
      }
    }

    fetchActiveBatch();
    fetchAnalytics();
    setInterval(fetchActiveBatch, 2000);
    setInterval(fetchAnalytics, 5000);
  </script>
</body>
</html>`;
}

/* ============================================================
   CONVEYOR SYSTEM UNIFIED CARD (Power + ON/OFF Status)
   ============================================================ */
function conveyorUnifiedDisplayPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Conveyor System</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    ::-webkit-scrollbar { display: none; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
    }

    .card {
      width: 330px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(192, 132, 252, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(147, 51, 234, 0.25);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: none !important;
    }
    .badge-status {
      display: none !important;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: transparent;
      border: none;
      padding: 4px 6px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .sensor-label {
      font-size: 18px;
      font-weight: 800;
      color: #c7d2fe;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.95);
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
    }
    .sensor-value {
      font-size: 32px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px rgba(99, 102, 241, 0.4);
    }
    .sensor-unit {
      font-size: 18px;
      font-weight: 700;
      color: #a5b4fc;
    }

    .controls-section {
      display: flex;
      flex-direction: column;
      gap: 8px;
      border-top: 1px solid rgba(255, 255, 255, 0.10);
      padding-top: 8px;
    }
    .control-row {
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(0, 0, 0, 0.18);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 10px;
      padding: 6px 8px;
    }
    .control-name {
      display: none !important;
    }
    .btn-group {
      display: flex;
      gap: 8px;
      align-items: center;
      width: 100%;
    }
    .state-btn {
      flex: 1;
      font-size: 17px;
      font-weight: 800;
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.25);
      color: rgba(255, 255, 255, 0.85);
      background: rgba(0, 0, 0, 0.22);
      cursor: pointer;
      transition: all 0.2s ease;
      line-height: 1.2;
      user-select: none;
      text-align: center;
    }
    .state-btn:active {
      transform: scale(0.96);
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 14px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 14px rgba(220, 38, 38, 0.6);
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="sensor-grid">
        <div class="sensor-box">
          <span class="sensor-label">Conveyor Power</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-conv-pwr">--</span>
            <span class="sensor-unit">W</span>
          </div>
        </div>
      </div>

      <div class="controls-section">
        <div class="control-row">
          <div class="btn-group">
            <button class="state-btn" id="btn-on" onclick="setConveyor(true)">ON</button>
            <button class="state-btn" id="btn-off" onclick="setConveyor(false)">OFF</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const convPwrEl = document.getElementById('val-conv-pwr');
    const btnOn = document.getElementById('btn-on');
    const btnOff = document.getElementById('btn-off');

    function updateConveyorButtons(isOn) {
      if (btnOn && btnOff) {
        btnOn.className = 'state-btn' + (isOn ? ' active-on' : '');
        btnOff.className = 'state-btn' + (!isOn ? ' active-off' : '');
      }
    }

    async function setConveyor(enable) {
      const endpoint = enable ? '/api/control/conveyor/on' : '/api/control/conveyor/off';
      updateConveyorButtons(enable);
      try {
        await fetch(endpoint, { method: 'POST' });
      } catch (err) {
        console.error('Failed to trigger conveyor command:', err);
      }
    }

    async function updateDisplay() {
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        if (!data.success || !data.state) return;
        const state = data.state;
        const sensors = state.sensors || {};
        const actuators = state.actuators || {};

        const convPwr = sensors.conveyorPowerW;
        const convState = actuators.conveyor;

        if (convPwrEl) {
          convPwrEl.textContent = (convPwr !== null && convPwr !== undefined && !Number.isNaN(Number(convPwr)))
            ? Number(convPwr).toFixed(2)
            : '--';
        }

        const isOn = convState === true || convState === 'ON' || convState === 'true';
        updateConveyorButtons(isOn);
      } catch (err) {
        console.error('Update error:', err);
      }
    }

    updateDisplay();
    setInterval(updateDisplay, 2000);
  </script>
</body>
</html>`;
}

/* ---------- 7 & 11. Conveyor System (Unified: Power + Motor ON/OFF) ---------- */
app.get(
  [
    "/display/conveyor",
    "/display/conveyor-power",
    "/display/conveyor-status",
    "/display/conveyor-card"
  ],
  (req, res) => {
    if (req.query.single === "1") {
      const isStatus = req.path.includes("status");
      if (isStatus) {
        res.send(
          iframeDisplayPage({
            title: "Conveyor",
            valueExpression: `state.actuators?.conveyor ? 'ON' : 'OFF'`,
            theme: req.query.theme || "indigo",
            icon: "🔄",
            type: "switch",
            rowLabel: "Conveyor Motor"
          })
        );
      } else {
        res.send(
          iframeDisplayPage({
            title: "Conveyor Power",
            valueExpression: `Number(state.sensors?.conveyorPowerW).toFixed(2)`,
            unit: " W",
            theme: req.query.theme || "violet",
            icon: "⚡",
            type: "sensor",
            metricLabel: "POWER"
          })
        );
      }
    } else {
      res.send(conveyorUnifiedDisplayPage());
    }
  }
);

/* ---------- 9 & 10. Production Counts (Unified: Batch Count + Lifetime Count) ---------- */
app.get(
  [
    "/display/products",
    "/display/product-count",
    "/display/lifetime-product-count",
    "/display/products-card"
  ],
  (req, res) => {
    if (req.query.single === "1") {
      const isLifetime = req.path.includes("lifetime");
      res.send(
        iframeDisplayPage({
          title: isLifetime ? "total Count" : "Batch Count",
          valueExpression: isLifetime
            ? `Number(state.sensors?.productCountTotal).toFixed(0)`
            : `Number(state.sensors?.productCount).toFixed(0)`,
          unit: " pcs",
          theme: isLifetime ? "emerald" : "lime",
          icon: isLifetime ? "🏭" : "📦",
          type: "sensor",
          metricLabel: isLifetime ? "TOTAL PRODUCTS" : "PRODUCT COUNT"
        })
      );
    } else {
      res.send(productionCountsDisplayPage());
    }
  }
);

/* ---------- 11. MES Production & Batch Management Card ---------- */
app.get(
  [
    "/display/production-mes",
    "/display/batch-mes",
    "/display/production-advanced",
    "/display/batch-management"
  ],
  (req, res) => {
    res.send(productionMesDisplayPage());
  }
);

/* ---------- 12. Lighting Status (Alias to Unified Card) ---------- */
app.get("/display/lighting-status", (req, res) => {
  if (req.query.single === "1") {
    res.send(
      iframeDisplayPage({
        title: "Factory Lights",
        valueExpression: `state.actuators?.lighting ? 'ON' : 'OFF'`,
        theme: req.query.theme || "gold",
        icon: "💡",
        type: "switch",
        rowLabel: "Main Lighting"
      })
    );
  } else {
    res.send(lightingUnifiedDisplayPage());
  }
});

/* ---------- 13. Mist 1 Status ---------- */
app.get("/display/mist1-status", (req, res) => {
  if (req.query.single === "1") {
    res.send(
      iframeDisplayPage({
        title: "Cooling Mist 1",
        valueExpression: `state.actuators?.mist1 ? 'ON' : 'OFF'`,
        theme: req.query.theme || "teal",
        icon: "🌫️",
        type: "switch",
        rowLabel: "Mist Machine 1"
      })
    );
  } else {
    res.send(coolingTowerDisplayPage(1, req.query.theme || "teal"));
  }
});

/* ---------- 14. Mist 2 Status ---------- */
app.get("/display/mist2-status", (req, res) => {
  if (req.query.single === "1") {
    res.send(
      iframeDisplayPage({
        title: "Cooling Mist 2",
        valueExpression: `state.actuators?.mist2 ? 'ON' : 'OFF'`,
        theme: req.query.theme || "cyan",
        icon: "🌫️",
        type: "switch",
        rowLabel: "Mist Machine 2"
      })
    );
  } else {
    res.send(coolingTowerDisplayPage(2, req.query.theme || "cyan"));
  }
});

/* ---------- 15. Heater Status ---------- */
app.get("/display/heater-status", (req, res) => {
  res.send(
    iframeDisplayPage({
      title: "Industrial Heater",
      valueExpression: `state.actuators?.heater ? 'ON' : 'OFF'`,
      theme: req.query.theme || "coral",
      icon: "🔥",
      type: "switch",
      rowLabel: "Heater Element"
    })
  );
});

/* ---------- 16. Laser Safety Fence (Combined 4 Zones in Single Card) ---------- */
function laserCombinedDisplayPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Laser Safety Fence</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background: transparent !important;
      overflow: hidden;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      user-select: none;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    ::-webkit-scrollbar { display: none; }

    .card-wrap {
      padding: 6px;
      display: inline-flex;
    }

    .card {
      width: 330px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: all 0.3s ease;
    }

    .theme-laser-safe {
      background: linear-gradient(145deg, rgba(15, 23, 42, 0.35) 0%, rgba(10, 16, 30, 0.30) 100%);
      border: 1.5px solid rgba(239, 68, 68, 0.55);
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.55), 0 0 18px rgba(239, 68, 68, 0.22);
    }
    .theme-laser-intrusion {
      background: rgba(185, 28, 28, 0.28);
      border: 1.5px solid #fca5a5;
      box-shadow: 0 0 30px rgba(239, 68, 68, 0.70), inset 0 0 20px rgba(239, 68, 68, 0.35);
      animation: alert-border-pulse 0.8s infinite alternate;
    }
    @keyframes alert-border-pulse {
      0% { box-shadow: 0 0 15px rgba(239, 68, 68, 0.4), inset 0 0 15px rgba(239, 68, 68, 0.2); }
      100% { box-shadow: 0 0 30px rgba(239, 68, 68, 0.8), inset 0 0 30px rgba(239, 68, 68, 0.5); }
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: center;
      border-bottom: 1px solid rgba(255, 255, 255, 0.10);
      padding-bottom: 6px;
    }
    .card-title-wrap {
      display: none !important;
    }
    .status-summary-badge {
      font-size: 15px;
      font-weight: 800;
      padding: 4px 12px;
      border-radius: 6px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      transition: all 0.2s ease;
    }
    .status-summary-badge.safe {
      background: #10b981;
      color: #ffffff;
      box-shadow: 0 0 8px rgba(16, 185, 129, 0.5);
    }
    .status-summary-badge.intrusion {
      background: #dc2626;
      color: #ffffff;
      box-shadow: 0 0 12px rgba(220, 38, 38, 0.7);
      animation: alert-pulse 0.9s infinite alternate;
    }

    /* 2x2 Zones Grid */
    .zones-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .zone-box {
      background: transparent;
      border: 1px solid rgba(255, 255, 255, 0.15);
      border-radius: 12px;
      padding: 8px 10px;
      display: flex;
      flex-direction: column;
      gap: 5px;
    }
    .zone-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .zone-name {
      font-size: 18px;
      font-weight: 800;
      color: #fed7aa;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .zone-pill {
      font-size: 15px;
      font-weight: 800;
      padding: 5px 10px;
      border-radius: 6px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      text-align: center;
      transition: all 0.25s ease;
      line-height: 1.2;
    }
    .zone-pill.safe {
      background: #10b981;
      color: #ffffff;
      border: 1px solid #34d399;
      box-shadow: 0 0 8px rgba(16, 185, 129, 0.4);
    }
    .zone-pill.intrusion {
      background: #dc2626;
      color: #ffffff;
      border: 1px solid #f87171;
      box-shadow: 0 0 14px rgba(220, 38, 38, 0.8);
      animation: alert-pulse 0.9s infinite alternate;
    }
    @keyframes alert-pulse {
      0% { opacity: 0.75; transform: scale(0.98); }
      100% { opacity: 1; transform: scale(1.02); }
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card theme-laser-safe" id="card">
      <div class="card-header">
        <span class="status-summary-badge safe" id="overall-status">SAFE</span>
      </div>

      <div class="zones-grid">
        <div class="zone-box">
          <div class="zone-header">
            <span class="zone-name">Zone 1</span>
          </div>
          <span class="zone-pill safe" id="zone1-pill">SAFE</span>
        </div>

        <div class="zone-box">
          <div class="zone-header">
            <span class="zone-name">Zone 2</span>
          </div>
          <span class="zone-pill safe" id="zone2-pill">SAFE</span>
        </div>

        <div class="zone-box">
          <div class="zone-header">
            <span class="zone-name">Zone 3</span>
          </div>
          <span class="zone-pill safe" id="zone3-pill">SAFE</span>
        </div>

        <div class="zone-box">
          <div class="zone-header">
            <span class="zone-name">Zone 4</span>
          </div>
          <span class="zone-pill safe" id="zone4-pill">SAFE</span>
        </div>
      </div>
    </div>
  </div>

  <script>
    async function updateLaserDisplay() {
      try {
        const response = await fetch('/api/state', { cache: 'no-store' });
        const data = await response.json();
        if (!data.success || !data.state) return;
        const zones = data.state.laserZones || {};

        const z1 = String(zones.zone1 || 'SAFE').trim().toUpperCase();
        const z2 = String(zones.zone2 || 'SAFE').trim().toUpperCase();
        const z3 = String(zones.zone3 || 'SAFE').trim().toUpperCase();
        const z4 = String(zones.zone4 || 'SAFE').trim().toUpperCase();

        const z1Pill = document.getElementById('zone1-pill');
        const z2Pill = document.getElementById('zone2-pill');
        const z3Pill = document.getElementById('zone3-pill');
        const z4Pill = document.getElementById('zone4-pill');
        const overall = document.getElementById('overall-status');
        const cardEl = document.getElementById('card');

        function setPill(pill, val) {
          if (!pill) return;
          pill.textContent = val;
          if (val === 'SAFE') {
            pill.className = 'zone-pill safe';
          } else if (val === 'INTRUSION') {
            pill.className = 'zone-pill intrusion';
          } else {
            pill.className = 'zone-pill';
            pill.style.background = '#f59e0b';
            pill.style.color = '#fff';
          }
        }

        setPill(z1Pill, z1);
        setPill(z2Pill, z2);
        setPill(z3Pill, z3);
        setPill(z4Pill, z4);

        const hasIntrusion = z1 === 'INTRUSION' || z2 === 'INTRUSION' || z3 === 'INTRUSION' || z4 === 'INTRUSION';
        if (hasIntrusion) {
          cardEl.className = 'card theme-laser-intrusion';
          if (overall) {
            overall.textContent = 'ALERT';
            overall.className = 'status-summary-badge intrusion';
          }
        } else {
          cardEl.className = 'card theme-laser-safe';
          if (overall) {
            overall.textContent = 'ALL SAFE';
            overall.className = 'status-summary-badge safe';
          }
        }
      } catch (err) {
        console.error('Update laser error:', err);
      }
    }

    updateLaserDisplay();
    setInterval(updateLaserDisplay, 2000);
  </script>
</body>
</html>`;
}

/* Single combined endpoint for all 4 laser zones */
app.get(["/display/laser", "/display/laser-zones", "/display/lasers"], (req, res) => {
  res.send(laserCombinedDisplayPage());
});

/* Individual aliases serve the unified 4-zone card (or single zone if ?single=1) */
app.get(["/display/laser-1", "/display/laser-2", "/display/laser-3", "/display/laser-4"], (req, res) => {
  if (req.query.single === "1") {
    const zoneNum = req.path.split("-")[1] || "1";
    res.send(
      iframeDisplayPage({
        title: `Laser Zone ${zoneNum}`,
        valueExpression: `state.laserZones?.zone${zoneNum} || 'SAFE'`,
        theme: req.query.theme || "laser",
        icon: "🚨",
        type: "laser",
        rowLabel: `Zone ${zoneNum} Fence`
      })
    );
  } else {
    res.send(laserCombinedDisplayPage());
  }
});

/* ---------- 20. Person 1 ---------- */
app.get("/display/person1", (req, res) => {
  res.send(
    iframeDisplayPage({
      title: "Person 1 Servo",
      valueExpression: `Number(state.servos?.person1Angle) === 180 ? 'HOME' : 'MOVED'`,
      theme: req.query.theme || "amber",
      icon: "🤖",
      type: "servo",
      rowLabel: "Person 1 Arm"
    })
  );
});

/* ---------- 21. Person 2 ---------- */
app.get("/display/person2", (req, res) => {
  res.send(
    iframeDisplayPage({
      title: "Person 2 Servo",
      valueExpression: `Number(state.servos?.person2Angle) === 0 ? 'HOME' : 'MOVED'`,
      theme: req.query.theme || "emerald",
      icon: "👷",
      type: "servo",
      rowLabel: "Person 2 Arm"
    })
  );
});

/* ---------- 22. Camera ---------- */
app.get("/display/camera", (req, res) => {
  res.send(
    iframeDisplayPage({
      title: "Camera",
      valueExpression: `state.camera?.recording ? 'RECORDING' : 'NOT RECORDING'`,
      theme: req.query.theme || "slate",
      icon: "📹",
      type: "camera",
      rowLabel: "Security Camera"
    })
  );
});

/* ---------- 23. Display Cards Gallery Overview ---------- */
app.get("/display", (req, res) => {
  const routes = [
    { path: "/display/production-mes", title: "MES Production & Batch Terminal", theme: "Amber Gold" },
    { path: "/display/products", title: "Product Counts (Batch + Lifetime)", theme: "Mint Green" },
    { path: "/display/conveyor", title: "Conveyor System (Power + Motor Switch)", theme: "Electric Purple" },
    { path: "/display/lighting", title: "Lighting & Power (Light W + Total W + Switch)", theme: "Golden Yellow" },
    { path: "/display/cooling-tower-1", title: "Cooling Tower 1 (Temp + Hum + Mist)", theme: "Electric Cyan" },
    { path: "/display/cooling-tower-2", title: "Cooling Tower 2 (Temp + Hum + Mist)", theme: "Royal Blue" },
    { path: "/display/laser", title: "Laser Safety Fence (All 4 Zones)", theme: "Hazard Red" },
    { path: "/display/switchyard-temp", title: "Switchyard Temp", theme: "Flame Orange" },
    { path: "/display/heater-status", title: "Industrial Heater", theme: "Ruby Crimson" },
    { path: "/display/person1", title: "Person 1 Servo", theme: "Indigo Navy" },
    { path: "/display/person2", title: "Person 2 Servo", theme: "Hot Pink" },
    { path: "/display/camera", title: "Security Camera", theme: "Gunmetal Slate" },
  ];

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Factory Model - Display Cards Gallery</title>
  <style>
    body {
      background: #0b1120;
      color: #e2e8f0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      margin: 0;
      padding: 24px;
    }
    h1 {
      font-size: 26px;
      margin-bottom: 6px;
      color: #ffffff;
    }
    p {
      color: #94a3b8;
      margin-top: 0;
      margin-bottom: 24px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(330px, 1fr));
      gap: 20px;
    }
    .preview-card {
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 16px;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .preview-card-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .preview-title {
      font-weight: 700;
      font-size: 15px;
      color: #f1f5f9;
    }
    .preview-theme {
      font-size: 11px;
      padding: 3px 8px;
      border-radius: 6px;
      background: #334155;
      color: #cbd5e1;
      font-weight: 600;
    }
    iframe {
      width: 100%;
      height: 180px;
      border: 0;
      border-radius: 12px;
      background: rgba(0,0,0,0.25);
    }
    .link-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 12px;
    }
    .link-row a {
      color: #38bdf8;
      text-decoration: none;
      word-break: break-all;
    }
    .link-row a:hover {
      text-decoration: underline;
    }
  </style>
</head>
<body>
  <h1>🏭 Factory Model — Glassmorphic Display Cards</h1>
  <p>Live Digital Twin & Cupola360 hotspot card routes with distinct color themes. Click any link to open full size.</p>
  <div class="grid">
    ${routes.map(r => `
      <div class="preview-card">
        <div class="preview-card-header">
          <span class="preview-title">${r.title}</span>
          <span class="preview-theme">${r.theme}</span>
        </div>
        <iframe src="${r.path}" loading="lazy"></iframe>
        <div class="link-row">
          <a href="${r.path}" target="_blank">${r.path} ↗</a>
        </div>
      </div>
    `).join("")}
  </div>
</body>
</html>`);
});


/* ============================================================
   WEBSOCKET
   ============================================================ */

wss.on("connection", (ws) => {

  ws.send(
    JSON.stringify({
      type: "state",
      state: deviceState
    })
  );

});


/* ============================================================
   STATIC DASHBOARD
   ============================================================ */

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);


app.get("/", (req, res) => {

  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );

});


/* ============================================================
   SERVER
   ============================================================ */

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `Factory Model HTTP Backend running on port ${PORT}`
    );

    console.log(
      `ESP32: ${ESP32_URL}`
    );

    console.log(
      `Device: ${DEVICE_ID}`
    );

  }
);
