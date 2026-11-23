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
`);

console.log(`[SQLITE] Database ready: ${DB_PATH}`);

// Preload last recorded values from DB so dashboard shows real history even before first telemetry
try {
  const lastRow = db.prepare("SELECT * FROM telemetry_history ORDER BY id DESC LIMIT 1").get();
  if (lastRow) {
    console.log(`[SQLITE] Preloaded last telemetry: Lifetime=${lastRow.product_count_total}, Power=${lastRow.total_power_w}W`);
  }
} catch (e) {}

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
  } catch (e) {}
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
    productCountTotal: null
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
  const message = JSON.stringify({
    type: "state",
    state: deviceState
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


async function esp32Post(endpoint) {
  const response = await fetch(`${ESP32_URL}${endpoint}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Device-Key": DEVICE_KEY
    }
  });

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
    state: deviceState
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
  await forwardControl(
    req,
    res,
    "/api/control/person1/home"
  );
});


app.post("/api/control/person1/move", async (req, res) => {
  try {
    const currentAngle =
      Number(deviceState.servos?.person1Angle);

    const targetAngle =
      currentAngle === 180 ? 0 : 180;

    const endpoint =
      `/api/control/person1/${targetAngle}`;

    const result = await esp32Post(endpoint);

    if (result && result.state) {
      deviceState = {
        ...deviceState,
        ...normalizeCoolingTowerSensorMapping(result.state),
        deviceId:
          result.state.deviceId || DEVICE_ID,
        online: true,
        lastSeen:
          new Date().toISOString()
      };

      broadcastState();
    }

    res.json({
      success: true,
      targetAngle,
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
  await forwardControl(
    req,
    res,
    "/api/control/person2/home"
  );
});


app.post("/api/control/person2/move", async (req, res) => {
  try {
    const currentAngle =
      Number(deviceState.servos?.person2Angle);

    const targetAngle =
      currentAngle === 0 ? 150 : 0;

    const endpoint =
      `/api/control/person2/${targetAngle}`;

    const result = await esp32Post(endpoint);

    if (result && result.state) {
      deviceState = {
        ...deviceState,
        ...normalizeCoolingTowerSensorMapping(result.state),
        deviceId:
          result.state.deviceId || DEVICE_ID,
        online: true,
        lastSeen:
          new Date().toISOString()
      };

      broadcastState();
    }

    res.json({
      success: true,
      targetAngle,
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
        <span class="control-name">${rowLabel}</span>
        <div class="btn-group">
          <span class="state-btn btn-on" id="btn-on">ON</span>
          <span class="state-btn btn-off" id="btn-off">OFF</span>
        </div>
      </div>`;
  } else if (type === "laser") {
    bodyHtml = `
      <div class="control-row">
        <span class="control-name">${rowLabel}</span>
        <div class="btn-group">
          <span class="laser-pill safe" id="laser-pill">SAFE</span>
        </div>
      </div>`;
  } else if (type === "servo") {
    bodyHtml = `
      <div class="control-row">
        <span class="control-name">${rowLabel}</span>
        <div class="btn-group">
          <span class="state-btn btn-home" id="btn-home">HOME</span>
          <span class="state-btn btn-move" id="btn-move">MOVED</span>
        </div>
      </div>`;
  } else if (type === "camera") {
    bodyHtml = `
      <div class="control-row">
        <span class="control-name">${rowLabel}</span>
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
      width: 310px;
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
      background: linear-gradient(145deg, rgba(16, 185, 129, 0.78) 0%, rgba(5, 150, 105, 0.92) 100%);
      border: 2px solid #34d399;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(16, 185, 129, 0.5);
    }
    .theme-emerald .card-title { color: #ecfdf5; }
    .theme-emerald .sensor-label { color: #a7f3d0; }
    .theme-emerald .sensor-unit { color: #6ee7b7; }

    /* 2. Amber / Hot Flame Orange (Switchyard Temp) */
    .theme-amber {
      background: linear-gradient(145deg, rgba(249, 115, 22, 0.78) 0%, rgba(194, 65, 12, 0.92) 100%);
      border: 2px solid #fb923c;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(249, 115, 22, 0.5);
    }
    .theme-amber .card-title { color: #fff7ed; }
    .theme-amber .sensor-label { color: #fed7aa; }
    .theme-amber .sensor-unit { color: #fdba74; }

    /* 3. Teal / Electric Cyan (Cooling Tower 1) */
    .theme-teal {
      background: linear-gradient(145deg, rgba(6, 182, 212, 0.78) 0%, rgba(8, 145, 178, 0.92) 100%);
      border: 2px solid #22d3ee;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(6, 182, 212, 0.5);
    }
    .theme-teal .card-title { color: #ecfeff; }
    .theme-teal .sensor-label { color: #a5f3fc; }
    .theme-teal .sensor-unit { color: #67e8f9; }

    /* 4. Gold / Sunburst Golden Yellow (Lighting & Power) */
    .theme-gold {
      background: linear-gradient(145deg, rgba(234, 179, 8, 0.78) 0%, rgba(180, 83, 9, 0.92) 100%);
      border: 2px solid #fde047;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(234, 179, 8, 0.5);
    }
    .theme-gold .card-title { color: #fefce8; }
    .theme-gold .sensor-label { color: #fef08a; }
    .theme-gold .sensor-unit { color: #fde047; }

    /* 5. Rose / Ruby Crimson (Industrial Heater) */
    .theme-rose, .theme-coral {
      background: linear-gradient(145deg, rgba(225, 29, 72, 0.78) 0%, rgba(159, 18, 57, 0.92) 100%);
      border: 2px solid #fb7185;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(225, 29, 72, 0.5);
    }
    .theme-rose .card-title, .theme-coral .card-title { color: #fff1f2; }
    .theme-rose .sensor-label, .theme-coral .sensor-label { color: #fecdd3; }
    .theme-rose .sensor-unit, .theme-coral .sensor-unit { color: #fda4af; }

    /* 6. Indigo / Electric Purple (Conveyor System) */
    .theme-indigo {
      background: linear-gradient(145deg, rgba(147, 51, 234, 0.78) 0%, rgba(109, 40, 217, 0.92) 100%);
      border: 2px solid #c084fc;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(147, 51, 234, 0.5);
    }
    .theme-indigo .card-title { color: #faf5ff; }
    .theme-indigo .sensor-label { color: #e9d5ff; }
    .theme-indigo .sensor-unit { color: #c084fc; }

    /* 7. Blue / Deep Indigo Navy (Person 1 Servo) */
    .theme-blue {
      background: linear-gradient(145deg, rgba(79, 70, 229, 0.78) 0%, rgba(49, 46, 129, 0.92) 100%);
      border: 2px solid #818cf8;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(79, 70, 229, 0.5);
    }
    .theme-blue .card-title { color: #eef2ff; }
    .theme-blue .sensor-label { color: #c7d2fe; }
    .theme-blue .sensor-unit { color: #a5b4fc; }

    /* 8. Slate / Gunmetal Platinum (Security Camera) */
    .theme-slate {
      background: linear-gradient(145deg, rgba(71, 85, 105, 0.82) 0%, rgba(30, 41, 59, 0.95) 100%);
      border: 2px solid #cbd5e1;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(148, 163, 184, 0.4);
    }
    .theme-slate .card-title { color: #f8fafc; }
    .theme-slate .sensor-label { color: #cbd5e1; }
    .theme-slate .sensor-unit { color: #94a3b8; }

    /* 9. Lime / Electric Spring Green */
    .theme-lime {
      background: linear-gradient(145deg, rgba(132, 204, 22, 0.78) 0%, rgba(77, 124, 15, 0.92) 100%);
      border: 2px solid #bef264;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(132, 204, 22, 0.5);
    }
    .theme-lime .card-title { color: #f7fee7; }
    .theme-lime .sensor-label { color: #d9f99d; }
    .theme-lime .sensor-unit { color: #bef264; }

    /* 10. Cyan / Royal Deep Blue (Cooling Tower 2) */
    .theme-cyan {
      background: linear-gradient(145deg, rgba(37, 99, 235, 0.78) 0%, rgba(29, 78, 216, 0.92) 100%);
      border: 2px solid #60a5fa;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(37, 99, 235, 0.5);
    }
    .theme-cyan .card-title { color: #eff6ff; }
    .theme-cyan .sensor-label { color: #bfdbfe; }
    .theme-cyan .sensor-unit { color: #93c5fd; }

    /* 11. Violet / Purple */
    .theme-violet {
      background: linear-gradient(145deg, rgba(168, 85, 247, 0.78) 0%, rgba(126, 34, 206, 0.92) 100%);
      border: 2px solid #d8b4fe;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(168, 85, 247, 0.5);
    }
    .theme-violet .card-title { color: #f5f3ff; }
    .theme-violet .sensor-label { color: #ddd6fe; }
    .theme-violet .sensor-unit { color: #c4b5fd; }

    /* 12. Crimson / Blood Red */
    .theme-crimson {
      background: linear-gradient(145deg, rgba(220, 38, 38, 0.78) 0%, rgba(153, 27, 27, 0.92) 100%);
      border: 2px solid #f87171;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(220, 38, 38, 0.5);
    }
    .theme-crimson .card-title { color: #fff1f2; }
    .theme-crimson .sensor-label { color: #fecdd3; }
    .theme-crimson .sensor-unit { color: #fda4af; }

    /* 13. Copper / Warm Bronze */
    .theme-copper {
      background: linear-gradient(145deg, rgba(217, 119, 6, 0.78) 0%, rgba(146, 64, 14, 0.92) 100%);
      border: 2px solid #fbbf24;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(217, 119, 6, 0.5);
    }
    .theme-copper .card-title { color: #fffbeb; }
    .theme-copper .sensor-label { color: #fde68a; }
    .theme-copper .sensor-unit { color: #fbbf24; }

    /* 15. Fuchsia / Hot Magenta Pink (Person 2 Servo) */
    .theme-fuchsia {
      background: linear-gradient(145deg, rgba(236, 72, 153, 0.80) 0%, rgba(190, 24, 93, 0.92) 100%);
      border: 2px solid #f472b6;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(236, 72, 153, 0.5);
    }
    .theme-fuchsia .card-title { color: #fdf2f8; }
    .theme-fuchsia .sensor-label { color: #fbcfe8; }
    .theme-fuchsia .sensor-unit { color: #f472b6; }

    /* 16. Laser Hazard State */
    .theme-laser-safe {
      background: linear-gradient(145deg, rgba(24, 24, 27, 0.90) 0%, rgba(9, 9, 11, 0.98) 100%);
      border: 2px solid #ef4444;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.7), 0 0 25px rgba(239, 68, 68, 0.5), inset 0 0 20px rgba(239, 68, 68, 0.2);
    }
    .theme-laser-intrusion {
      background: linear-gradient(145deg, rgba(185, 28, 28, 0.88) 0%, rgba(127, 29, 29, 0.96) 100%);
      border: 2px solid #fca5a5;
      box-shadow: 0 0 35px rgba(239, 68, 68, 0.85), inset 0 0 30px rgba(239, 68, 68, 0.5);
      animation: alert-border-pulse 0.8s infinite alternate;
    }
    @keyframes alert-border-pulse {
      0% { box-shadow: 0 0 15px rgba(239, 68, 68, 0.4), inset 0 0 15px rgba(239, 68, 68, 0.2); }
      100% { box-shadow: 0 0 30px rgba(239, 68, 68, 0.8), inset 0 0 30px rgba(239, 68, 68, 0.5); }
    }

    /* Card Header */
    .card-header {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      border-bottom: 1px solid rgba(255, 255, 255, 0.16);
      padding-bottom: 8px;
    }
    .card-title-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-icon {
      font-size: 20px;
      line-height: 1;
    }
    .card-title {
      font-size: 19px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.8);
      line-height: 1.2;
    }

    /* Sensor Box */
    .sensor-box {
      background: rgba(15, 23, 42, 0.42);
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 12px;
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 3px;
      backdrop-filter: blur(8px);
    }
    .sensor-label {
      font-size: 16px;
      font-weight: 700;
      letter-spacing: 0.02em;
      display: flex;
      align-items: center;
      text-transform: uppercase;
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 5px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 32px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 12px rgba(255, 255, 255, 0.25);
    }
    .sensor-unit {
      font-size: 17px;
      font-weight: 700;
    }

    /* Control / Actuator Row */
    .control-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: rgba(15, 23, 42, 0.38);
      border: 1px solid rgba(255, 255, 255, 0.14);
      border-radius: 10px;
      padding: 10px 12px;
      gap: 8px;
    }
    .control-name {
      font-size: 18px;
      font-weight: 700;
      color: #ffffff;
      text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
    }
    .btn-group {
      display: flex;
      gap: 6px;
      align-items: center;
    }
    .state-btn {
      font-size: 13px;
      font-weight: 800;
      padding: 6px 13px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      color: rgba(255, 255, 255, 0.45);
      background: rgba(0, 0, 0, 0.25);
      transition: all 0.2s ease;
      line-height: 1.2;
      text-align: center;
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 12px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 12px rgba(220, 38, 38, 0.6);
    }
    .state-btn.active-home {
      background: #2563eb;
      color: #ffffff;
      border-color: #60a5fa;
      box-shadow: 0 0 12px rgba(37, 99, 235, 0.6);
    }
    .state-btn.active-move {
      background: #f59e0b;
      color: #ffffff;
      border-color: #fbbf24;
      box-shadow: 0 0 12px rgba(245, 158, 11, 0.6);
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
      <div class="card-header">
        <div class="card-title-wrap">
          <span class="card-icon">${icon}</span>
          <span class="card-title">${title}</span>
        </div>
      </div>
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
  const bg = isTower2
    ? "linear-gradient(145deg, rgba(37, 99, 235, 0.78) 0%, rgba(29, 78, 216, 0.92) 100%)"
    : "linear-gradient(145deg, rgba(6, 182, 212, 0.78) 0%, rgba(8, 145, 178, 0.92) 100%)";
  const border = isTower2 ? "2px solid #60a5fa" : "2px solid #22d3ee";
  const shadowGlow = isTower2 ? "rgba(37, 99, 235, 0.5)" : "rgba(6, 182, 212, 0.5)";
  const textShadowGlow = isTower2 ? "rgba(37, 99, 235, 0.6)" : "rgba(6, 182, 212, 0.6)";
  const titleColor = isTower2 ? "#eff6ff" : "#ecfeff";
  const labelColor = isTower2 ? "#bfdbfe" : "#a5f3fc";
  const unitColor = isTower2 ? "#93c5fd" : "#67e8f9";
  const badgeColor = isTower2 ? "#bfdbfe" : "#a5f3fc";
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
      width: 320px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: ${bg};
      border: ${border};
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px ${shadowGlow};
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.16);
      padding-bottom: 8px;
    }
    .card-title-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-icon {
      font-size: 20px;
      line-height: 1;
    }
    .card-title {
      font-size: 19px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.8);
      color: ${titleColor};
      line-height: 1.2;
    }
    .badge-status {
      font-size: 11px;
      font-weight: 800;
      padding: 3px 8px;
      border-radius: 6px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      background: rgba(15, 23, 42, 0.4);
      border: 1px solid rgba(255, 255, 255, 0.16);
      color: ${badgeColor};
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: rgba(15, 23, 42, 0.42);
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 12px;
      padding: 8px 10px;
      display: flex;
      flex-direction: column;
      gap: 2px;
      backdrop-filter: blur(8px);
    }
    .sensor-label {
      font-size: 14px;
      font-weight: 700;
      color: ${labelColor};
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 28px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px ${textShadowGlow};
    }
    .sensor-unit {
      font-size: 15px;
      font-weight: 700;
      color: ${unitColor};
    }

    .controls-section {
      display: flex;
      flex-direction: column;
      gap: 8px;
      border-top: 1px solid rgba(255, 255, 255, 0.16);
      padding-top: 8px;
    }
    .control-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: rgba(15, 23, 42, 0.38);
      border: 1px solid rgba(255, 255, 255, 0.14);
      border-radius: 10px;
      padding: 8px 12px;
      gap: 8px;
    }
    .control-name {
      font-size: 16px;
      font-weight: 700;
      color: #ffffff;
      text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
    }
    .btn-group {
      display: flex;
      gap: 6px;
      align-items: center;
    }
    .state-btn {
      font-size: 13px;
      font-weight: 800;
      padding: 5px 13px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      color: rgba(255, 255, 255, 0.45);
      background: rgba(0, 0, 0, 0.25);
      cursor: pointer;
      transition: all 0.2s ease;
      line-height: 1.2;
      user-select: none;
    }
    .state-btn:active {
      transform: scale(0.96);
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 12px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 12px rgba(220, 38, 38, 0.6);
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="card-header">
        <div class="card-title-wrap">
          <span class="card-icon">${icon}</span>
          <span class="card-title">Cooling Tower ${towerNum}</span>
        </div>
        <span class="badge-status" id="badge-status">ONLINE</span>
      </div>

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
          <span class="control-name">Mist Maker</span>
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
      width: 320px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: linear-gradient(145deg, rgba(234, 179, 8, 0.78) 0%, rgba(180, 83, 9, 0.92) 100%);
      border: 2px solid #fde047;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(234, 179, 8, 0.5);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.16);
      padding-bottom: 8px;
    }
    .card-title-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-icon {
      font-size: 20px;
      line-height: 1;
    }
    .card-title {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.8);
      color: #fefce8;
      line-height: 1.2;
    }
    .badge-status {
      font-size: 11px;
      font-weight: 800;
      padding: 3px 8px;
      border-radius: 6px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      background: rgba(15, 23, 42, 0.4);
      border: 1px solid rgba(255, 255, 255, 0.16);
      color: #fde047;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: rgba(15, 23, 42, 0.42);
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 12px;
      padding: 8px 10px;
      display: flex;
      flex-direction: column;
      gap: 2px;
      backdrop-filter: blur(8px);
    }
    .sensor-label {
      font-size: 13px;
      font-weight: 700;
      color: #fef08a;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 26px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px rgba(234, 179, 8, 0.4);
    }
    .sensor-unit {
      font-size: 15px;
      font-weight: 700;
      color: #fde047;
    }

    .controls-section {
      display: flex;
      flex-direction: column;
      gap: 8px;
      border-top: 1px solid rgba(255, 255, 255, 0.16);
      padding-top: 8px;
    }
    .control-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: rgba(15, 23, 42, 0.38);
      border: 1px solid rgba(255, 255, 255, 0.14);
      border-radius: 10px;
      padding: 8px 12px;
      gap: 8px;
    }
    .control-name {
      font-size: 16px;
      font-weight: 700;
      color: #ffffff;
      text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
    }
    .btn-group {
      display: flex;
      gap: 6px;
      align-items: center;
    }
    .state-btn {
      font-size: 13px;
      font-weight: 800;
      padding: 5px 13px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      color: rgba(255, 255, 255, 0.45);
      background: rgba(0, 0, 0, 0.25);
      cursor: pointer;
      transition: all 0.2s ease;
      line-height: 1.2;
      user-select: none;
    }
    .state-btn:active {
      transform: scale(0.96);
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 12px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 12px rgba(220, 38, 38, 0.6);
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="card-header">
        <div class="card-title-wrap">
          <span class="card-icon">💡</span>
          <span class="card-title">Lighting & Power</span>
        </div>
        <span class="badge-status" id="badge-status">ONLINE</span>
      </div>

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
          <span class="control-name">Factory Lights</span>
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
      width: 320px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: linear-gradient(145deg, rgba(16, 185, 129, 0.78) 0%, rgba(5, 150, 105, 0.92) 100%);
      border: 2px solid #34d399;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(16, 185, 129, 0.5);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.16);
      padding-bottom: 8px;
    }
    .card-title-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-icon {
      font-size: 20px;
      line-height: 1;
    }
    .card-title {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.8);
      color: #ecfdf5;
      line-height: 1.2;
    }
    .badge-status {
      font-size: 11px;
      font-weight: 800;
      padding: 3px 8px;
      border-radius: 6px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      background: rgba(15, 23, 42, 0.4);
      border: 1px solid rgba(255, 255, 255, 0.16);
      color: #6ee7b7;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: rgba(15, 23, 42, 0.42);
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 12px;
      padding: 8px 10px;
      display: flex;
      flex-direction: column;
      gap: 2px;
      backdrop-filter: blur(8px);
    }
    .sensor-label {
      font-size: 13px;
      font-weight: 700;
      color: #a7f3d0;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
      margin-top: 2px;
    }
    .sensor-value {
      font-size: 26px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px rgba(16, 185, 129, 0.4);
    }
    .sensor-unit {
      font-size: 15px;
      font-weight: 700;
      color: #6ee7b7;
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="card-header">
        <div class="card-title-wrap">
          <span class="card-icon">🏭</span>
          <span class="card-title">Product Counts</span>
        </div>
        <span class="badge-status" id="badge-status">ONLINE</span>
      </div>

      <div class="sensor-grid">
        <div class="sensor-box">
          <span class="sensor-label">Batch Count</span>
          <div class="sensor-value-row">
            <span class="sensor-value" id="val-batch-count">--</span>
            <span class="sensor-unit">pcs</span>
          </div>
        </div>

        <div class="sensor-box">
          <span class="sensor-label">Lifetime Count</span>
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
      width: 320px;
      border-radius: 18px;
      padding: 13px 15px;
      color: #ffffff;
      backdrop-filter: blur(20px) saturate(190%);
      -webkit-backdrop-filter: blur(20px) saturate(190%);
      background: linear-gradient(145deg, rgba(147, 51, 234, 0.78) 0%, rgba(109, 40, 217, 0.92) 100%);
      border: 2px solid #c084fc;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.6), 0 0 25px rgba(147, 51, 234, 0.5);
      display: flex;
      flex-direction: column;
      gap: 10px;
      user-select: none;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.16);
      padding-bottom: 8px;
    }
    .card-title-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-icon {
      font-size: 20px;
      line-height: 1;
    }
    .card-title {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.8);
      color: #eef2ff;
      line-height: 1.2;
    }
    .badge-status {
      font-size: 11px;
      font-weight: 800;
      padding: 3px 8px;
      border-radius: 6px;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      background: rgba(15, 23, 42, 0.4);
      border: 1px solid rgba(255, 255, 255, 0.16);
      color: #a5b4fc;
    }

    .sensor-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    .sensor-box {
      background: rgba(15, 23, 42, 0.42);
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 12px;
      padding: 8px 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      backdrop-filter: blur(8px);
    }
    .sensor-label {
      font-size: 14px;
      font-weight: 700;
      color: #c7d2fe;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .sensor-value-row {
      display: flex;
      align-items: baseline;
      gap: 4px;
    }
    .sensor-value {
      font-size: 26px;
      font-weight: 800;
      color: #ffffff;
      letter-spacing: -0.02em;
      line-height: 1.1;
      text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8), 0 0 10px rgba(99, 102, 241, 0.4);
    }
    .sensor-unit {
      font-size: 15px;
      font-weight: 700;
      color: #a5b4fc;
    }

    .controls-section {
      display: flex;
      flex-direction: column;
      gap: 8px;
      border-top: 1px solid rgba(255, 255, 255, 0.16);
      padding-top: 8px;
    }
    .control-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: rgba(15, 23, 42, 0.38);
      border: 1px solid rgba(255, 255, 255, 0.14);
      border-radius: 10px;
      padding: 8px 12px;
      gap: 8px;
    }
    .control-name {
      font-size: 16px;
      font-weight: 700;
      color: #ffffff;
      text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
    }
    .btn-group {
      display: flex;
      gap: 6px;
      align-items: center;
    }
    .state-btn {
      font-size: 13px;
      font-weight: 800;
      padding: 5px 13px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.22);
      color: rgba(255, 255, 255, 0.45);
      background: rgba(0, 0, 0, 0.25);
      cursor: pointer;
      transition: all 0.2s ease;
      line-height: 1.2;
      user-select: none;
    }
    .state-btn:active {
      transform: scale(0.96);
    }
    .state-btn.active-on {
      background: #10b981;
      color: #ffffff;
      border-color: #34d399;
      box-shadow: 0 0 12px rgba(16, 185, 129, 0.6);
    }
    .state-btn.active-off {
      background: #dc2626;
      color: #ffffff;
      border-color: #f87171;
      box-shadow: 0 0 12px rgba(220, 38, 38, 0.6);
    }
  </style>
</head>
<body>
  <div class="card-wrap">
    <div class="card" id="card">
      <div class="card-header">
        <div class="card-title-wrap">
          <span class="card-icon">🔄</span>
          <span class="card-title">Conveyor System</span>
        </div>
        <span class="badge-status" id="badge-status">ONLINE</span>
      </div>

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
          <span class="control-name">Conveyor Motor</span>
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
          title: isLifetime ? "Lifetime Count" : "Batch Count",
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
      width: 320px;
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
      background: linear-gradient(145deg, rgba(24, 24, 27, 0.90) 0%, rgba(9, 9, 11, 0.98) 100%);
      border: 2px solid #ef4444;
      box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.7), 0 0 25px rgba(239, 68, 68, 0.5), inset 0 0 20px rgba(239, 68, 68, 0.2);
    }
    .theme-laser-intrusion {
      background: linear-gradient(145deg, rgba(185, 28, 28, 0.88) 0%, rgba(127, 29, 29, 0.96) 100%);
      border: 2px solid #fca5a5;
      box-shadow: 0 0 35px rgba(239, 68, 68, 0.85), inset 0 0 30px rgba(239, 68, 68, 0.5);
      animation: alert-border-pulse 0.8s infinite alternate;
    }
    @keyframes alert-border-pulse {
      0% { box-shadow: 0 0 15px rgba(239, 68, 68, 0.4), inset 0 0 15px rgba(239, 68, 68, 0.2); }
      100% { box-shadow: 0 0 30px rgba(239, 68, 68, 0.8), inset 0 0 30px rgba(239, 68, 68, 0.5); }
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.16);
      padding-bottom: 8px;
    }
    .card-title-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card-icon {
      font-size: 20px;
      line-height: 1;
    }
    .card-title {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      text-shadow: 0 1px 4px rgba(0, 0, 0, 0.8);
      color: #ffffff;
    }
    .status-summary-badge {
      font-size: 11px;
      font-weight: 800;
      padding: 3px 8px;
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
      background: rgba(15, 23, 42, 0.42);
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 12px;
      padding: 8px 10px;
      display: flex;
      flex-direction: column;
      gap: 5px;
      backdrop-filter: blur(8px);
    }
    .zone-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .zone-name {
      font-size: 14px;
      font-weight: 700;
      color: #fed7aa;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .zone-pill {
      font-size: 11px;
      font-weight: 800;
      padding: 4px 8px;
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
        <div class="card-title-wrap">
          <span class="card-icon">🚨</span>
          <span class="card-title">Laser Safety Fence</span>
        </div>
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
      theme: req.query.theme || "blue",
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
      theme: req.query.theme || "fuchsia",
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
