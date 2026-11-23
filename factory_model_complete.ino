#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <INA226_WE.h>
#include <Adafruit_SHT31.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESP32Servo.h>
#include <ArduinoJson.h>
#include <Update.h>
#include <HTTPClient.h>
#include <esp_task_wdt.h>
#include <esp_ota_ops.h>
#include <Preferences.h>

// ============================================================================
// CONFIGURATION
// ============================================================================
namespace WifiConfig {
  constexpr char SSID[]     = "OOMNI-EYE-2.4GHz";
  constexpr char PASSWORD[] = "Admin@2026";

  // --------------------------------------------------------------------------
  // STATIC IP CONFIGURATION
  // --------------------------------------------------------------------------
  // Factory Model ESP32
  // Static IP : 10.10.12.75
  // Gateway   : 10.10.12.1
  // Subnet    : 255.255.255.0
  // DNS       : 10.10.12.1 / 8.8.8.8
  // --------------------------------------------------------------------------
  const IPAddress LOCAL_IP(10, 10, 12, 75);
  const IPAddress GATEWAY(10, 10, 12, 1);
  const IPAddress SUBNET(255, 255, 255, 0);
  const IPAddress PRIMARY_DNS(10, 10, 12, 1);
  const IPAddress SECONDARY_DNS(8, 8, 8, 8);
}

namespace ThingsBoardConfig {
  constexpr char SERVER[]    = "10.10.10.52";
  constexpr uint16_t PORT    = 1883;
  constexpr char TOKEN[]     = "C5cOYHn2LGzMBLgjnm6l";
  constexpr char CLIENT_ID[] = "FactoryModel";

  // --- OTA Configuration ---
  constexpr int OTA_PORT       = 8081;
  constexpr char FW_TITLE[]    = "FactoryModelFirmware";
  constexpr char FW_VERSION[]  = "1.0.1";

  // --- LWT (Last Will and Testament) ---
  constexpr char LWT_TOPIC[]   = "v1/devices/me/telemetry";
  constexpr char LWT_MESSAGE[] = "{\"status\":\"offline\"}";
  constexpr int  LWT_QOS       = 1;
  constexpr bool LWT_RETAIN    = false;
}

// ============================================================================
// CAMERA SERVER CONFIGURATION
// ============================================================================
namespace CameraConfig {

  constexpr bool ENABLED = true;

  // Factory Model Mini PC
  constexpr char SERVER_URL[] =
      "http://10.10.12.66:8081/intrusion";

  constexpr uint16_t HTTP_TIMEOUT_MS = 1500;
}

// ============================================================================
// RELIABILITY CONFIGURATION
// ============================================================================
constexpr unsigned long OTA_STALL_TIMEOUT_MS = 15000;

constexpr unsigned long WIFI_RETRY_INTERVAL_MS = 15000;

constexpr unsigned long TELEMETRY_STALE_MS = 45000;

// ============================================================================
// NVS CONFIGURATION
// ============================================================================
namespace NvsConfig {
  constexpr char NAMESPACE[]       = "factory";
  constexpr char KEY_TOTAL_COUNT[] = "totalCnt";
}

constexpr uint8_t COUNT_SAVE_EVERY_N = 5;

constexpr unsigned long COUNT_SAVE_INTERVAL_MS = 30000;

// ============================================================================
// NETWORK OBJECTS
// ============================================================================
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
Preferences nvs;

// ============================================================================
// HARDWARE PIN MAP
// ============================================================================
namespace Pins {

  constexpr uint8_t SDA_1 = 21;
  constexpr uint8_t SCL_1 = 22;

  constexpr uint8_t SDA_2 = 26;
  constexpr uint8_t SCL_2 = 25;

  constexpr uint8_t SWITCHYARD_TEMP = 4;
  constexpr uint8_t E18_D80NK       = 13;

  constexpr uint8_t LASER_SW1 = 27;
  constexpr uint8_t LASER_SW2 = 32;
  constexpr uint8_t LASER_SW3 = 33;
  constexpr uint8_t LASER_SW4 = 34;

  constexpr uint8_t MOTOR_ENA = 19;

  constexpr uint8_t MIST1 = 14;
  constexpr uint8_t MIST2 = 23;

  constexpr uint8_t HEATER_RELAY = 18;

  constexpr uint8_t LIGHTING_MOSFET = 12;

  constexpr uint8_t PERSON1_SERVO = 16;
  constexpr uint8_t PERSON2_SERVO = 17;

  constexpr uint8_t RED_LED   = 5;
  constexpr uint8_t GREEN_LED = 2;
  constexpr uint8_t BUZZER    = 15;
}

constexpr uint8_t INA226_ADDR = 0x45;
constexpr uint8_t SHT31_ADDR  = 0x44;

// ============================================================================
// PERIPHERAL INSTANCES
// ============================================================================
TwoWire I2C_Zone1 = TwoWire(0);
TwoWire I2C_Zone2 = TwoWire(1);

INA226_WE lightingIna(&I2C_Zone1, INA226_ADDR);
INA226_WE conveyorIna(&I2C_Zone2, INA226_ADDR);

Adafruit_SHT31 coolingTower1Sht(&I2C_Zone1);
Adafruit_SHT31 coolingTower2Sht(&I2C_Zone2);

OneWire oneWire(Pins::SWITCHYARD_TEMP);
DallasTemperature switchyardTempSensor(&oneWire);

// ============================================================================
// SYSTEM STATE
// ============================================================================
struct SensorHealth {
  bool lightingIna = false;
  bool conveyorIna = false;
  bool coolingTower1 = false;
  bool coolingTower2 = false;
  bool switchyardTemp = false;
} sensorOK;

struct SensorReadings {

  float lightingVoltage = 0;
  float lightingCurrent = 0;
  float lightingPower   = 0;

  float conveyorVoltage = 0;
  float conveyorCurrent = 0;
  float conveyorPower   = 0;

  float coolingTower1Temp = 0;
  float coolingTower1Hum  = 0;

  float coolingTower2Temp = 0;
  float coolingTower2Hum  = 0;

  float switchyardTemp = 0;

} readings;

struct SystemState {

  bool conveyorOn = false;

  bool mist1On = false;
  bool mist2On = false;

  bool heaterOn = false;

  bool lightingOn = false;

  bool redLedOn = false;
  bool greenLedOn = false;

  bool intrusion = false;

  unsigned long productCount = 0;
  unsigned long productCountTotal = 0;

} state;

// ============================================================================
// PRODUCT COUNT PERSISTENCE
// ============================================================================
uint8_t pendingCountWrites = 0;

unsigned long lastCountSaveAt = 0;

// ============================================================================
// SMOOTH SERVO
// ============================================================================
class SmoothServo {

  public:

    void attach(
      uint8_t pin,
      int minUs = 500,
      int maxUs = 2400
    ) {

      _servo.setPeriodHertz(50);
      _servo.attach(pin, minUs, maxUs);
    }

    void begin(int startAngle) {

      _current = constrain(startAngle, 0, 180);

      _target = _current;

      _servo.write(_current);
    }

    void moveTo(int targetAngle) {

      _target = constrain(targetAngle, 0, 180);
    }

    void update() {

      if (_current == _target)
        return;

      if (millis() - _lastStepAt < _stepDelayMs)
        return;

      _lastStepAt = millis();

      _current +=
        (_target > _current) ? 1 : -1;

      _servo.write(_current);
    }

    int position() const {
      return _current;
    }

    bool isMoving() const {
      return _current != _target;
    }

  private:

    Servo _servo;

    int _current = 0;
    int _target = 0;

    unsigned long _lastStepAt = 0;

    unsigned long _stepDelayMs = 15;
};

SmoothServo person1Servo;
SmoothServo person2Servo;

// ============================================================================
// TIMING
// ============================================================================
constexpr unsigned long SENSOR_SAMPLE_INTERVAL_MS = 2000;

unsigned long lastSensorSample = 0;

unsigned long lastWifiAttempt = 0;

unsigned long lastMqttAttempt = 0;

unsigned long lastSuccessfulPublish = 0;

bool otaInProgress = false;

bool otaCheckedThisSession = false;

// ============================================================================
// LASER EDGE DETECTION
// ============================================================================
bool previousLaserState[4] = {
  false,
  false,
  false,
  false
};

bool laserStateInitialized = false;

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================
void initGpio();
void initServos();
void initI2C();
void initSensors();
void initWifi();
void initMqtt();

void maintainConnection();

void mqttCallback(
  char *topic,
  byte *payload,
  unsigned int length
);

void handleRpc(
  const String &method,
  JsonVariant params,
  int requestId
);

void requestFirmwareAttributes();

void performOtaUpdate(
  const String &fwTitle,
  const String &fwVersion,
  const String &fwChecksum,
  const String &fwChecksumAlgo,
  long fwSize
);

void reportFirmwareState(
  const char* fwState,
  const char* error = nullptr
);

void reportCurrentFirmwareInfo();

void publishOnlineStatus();

void sampleSensors();

void monitorProductCount();

void loadPersistedProductCount();

void savePersistedProductCount(
  bool force = false
);

void resetProductCount();

void logSensorData();

void publishTelemetry();

void checkLaserFence();

bool laserBlocked(
  uint8_t pin
);

void setConveyor(bool on);

void setMist1(bool on);

void setMist2(bool on);

void setHeater(bool on);

void setLighting(bool on);

void setPerson1Angle(int angle);

void setPerson2Angle(int angle);

void togglePerson1();

void togglePerson2();

void homePerson1();

void homePerson2();

void buildStateJson(
  JsonDocument &doc
);

void handleSerialCommand(
  char c
);

void triggerCameraRecording(
  uint8_t zone
);

// ============================================================================
// SETUP
// ============================================================================
void setup() {

  Serial.begin(115200);

  delay(300);

  Serial.println(
    F("\n==================================================")
  );

  Serial.println(
    F("       FACTORY MODEL - COMPLETE SYSTEM")
  );

  Serial.println(
    F("       (Professional Cloud + OTA Edition)")
  );

  Serial.printf(
    "       Firmware Version: %s\n",
    ThingsBoardConfig::FW_VERSION
  );

  Serial.println(
    F("       Static IP: 10.10.12.75")
  );

  Serial.println(
    F("==================================================")
  );

  // --------------------------------------------------------------------------
  // WATCHDOG
  // --------------------------------------------------------------------------
  esp_task_wdt_config_t twdt_config = {

    .timeout_ms = 10000,

    .idle_core_mask = (1 << 0),

    .trigger_panic = true
  };

  esp_err_t wdtInitResult =
      esp_task_wdt_init(&twdt_config);

  if (wdtInitResult == ESP_ERR_INVALID_STATE) {

    esp_task_wdt_reconfigure(
      &twdt_config
    );
  }

  esp_task_wdt_add(NULL);

  // --------------------------------------------------------------------------
  // HARDWARE INITIALIZATION
  // --------------------------------------------------------------------------
  initGpio();

  initServos();

  initI2C();

  initSensors();

  loadPersistedProductCount();

  // --------------------------------------------------------------------------
  // NETWORK INITIALIZATION
  // --------------------------------------------------------------------------
  initWifi();

  initMqtt();

  // --------------------------------------------------------------------------
  // BOOT DIAGNOSTICS
  // --------------------------------------------------------------------------
  const esp_partition_t *bootPart =
      esp_ota_get_running_partition();

  Serial.printf(
    "[SYS] Running partition: %s | Reset reason: %d\n",
    bootPart ? bootPart->label : "unknown",
    (int)esp_reset_reason()
  );

  Serial.printf(
    "[CAMERA] Intrusion recording: %s (%s)\n",
    CameraConfig::ENABLED
      ? "ENABLED"
      : "DISABLED",
    CameraConfig::SERVER_URL
  );

  Serial.println(
    F("System ready. Connecting to Network...")
  );
}

// ============================================================================
// MAIN LOOP
// ============================================================================
void loop() {

  esp_task_wdt_reset();

  maintainConnection();

  mqtt.loop();

  person1Servo.update();

  person2Servo.update();

  monitorProductCount();

  savePersistedProductCount();

  checkLaserFence();

  if (Serial.available()) {

    handleSerialCommand(
      Serial.read()
    );
  }

  if (
    millis() - lastSensorSample >=
    SENSOR_SAMPLE_INTERVAL_MS
  ) {

    lastSensorSample = millis();

    sampleSensors();

    logSensorData();

    publishTelemetry();
  }
}

// ============================================================================
// GPIO INITIALIZATION
// ============================================================================
void initGpio() {

  pinMode(
    Pins::E18_D80NK,
    INPUT_PULLUP
  );

  pinMode(
    Pins::LASER_SW1,
    INPUT
  );

  pinMode(
    Pins::LASER_SW2,
    INPUT
  );

  pinMode(
    Pins::LASER_SW3,
    INPUT
  );

  pinMode(
    Pins::LASER_SW4,
    INPUT
  );

  pinMode(
    Pins::MOTOR_ENA,
    OUTPUT
  );

  pinMode(
    Pins::MIST1,
    OUTPUT
  );

  pinMode(
    Pins::MIST2,
    OUTPUT
  );

  pinMode(
    Pins::HEATER_RELAY,
    OUTPUT
  );

  pinMode(
    Pins::LIGHTING_MOSFET,
    OUTPUT
  );

  pinMode(
    Pins::RED_LED,
    OUTPUT
  );

  pinMode(
    Pins::GREEN_LED,
    OUTPUT
  );

  ledcAttach(
    Pins::BUZZER,
    2000,
    8
  );

  ledcWriteTone(
    Pins::BUZZER,
    0
  );

  digitalWrite(
    Pins::MOTOR_ENA,
    LOW
  );

  digitalWrite(
    Pins::MIST1,
    LOW
  );

  digitalWrite(
    Pins::MIST2,
    LOW
  );

  digitalWrite(
    Pins::HEATER_RELAY,
    HIGH
  );

  digitalWrite(
    Pins::LIGHTING_MOSFET,
    LOW
  );

  digitalWrite(
    Pins::RED_LED,
    HIGH
  );

  digitalWrite(
    Pins::GREEN_LED,
    LOW
  );
}

// ============================================================================
// SERVO INITIALIZATION
// ============================================================================
void initServos() {

  ESP32PWM::allocateTimer(1);

  ESP32PWM::allocateTimer(2);

  person1Servo.attach(
    Pins::PERSON1_SERVO,
    500,
    2400
  );

  person2Servo.attach(
    Pins::PERSON2_SERVO,
    500,
    2400
  );

  // Person 1 home = 180°
  person1Servo.begin(180);

  // Retained from uploaded firmware
  person2Servo.begin(180);
}

// ============================================================================
// I2C INITIALIZATION
// ============================================================================
void initI2C() {

  I2C_Zone1.begin(
    Pins::SDA_1,
    Pins::SCL_1,
    100000
  );

  I2C_Zone2.begin(
    Pins::SDA_2,
    Pins::SCL_2,
    100000
  );
}

// ============================================================================
// SENSOR INITIALIZATION
// ============================================================================
void initSensors() {

  sensorOK.lightingIna =
      lightingIna.init();

  if (sensorOK.lightingIna) {

    lightingIna.setResistorRange(
      0.1,
      1.0
    );
  }

  sensorOK.coolingTower1 =
      coolingTower1Sht.begin(
        SHT31_ADDR
      );

  sensorOK.conveyorIna =
      conveyorIna.init();

  if (sensorOK.conveyorIna) {

    conveyorIna.setResistorRange(
      0.1,
      1.0
    );
  }

  sensorOK.coolingTower2 =
      coolingTower2Sht.begin(
        SHT31_ADDR
      );

  switchyardTempSensor.begin();

  switchyardTempSensor.setWaitForConversion(
    false
  );

  sensorOK.switchyardTemp =
      (
        switchyardTempSensor.getDeviceCount()
        > 0
      );

  if (sensorOK.switchyardTemp) {

    switchyardTempSensor.requestTemperatures();
  }

  if (!sensorOK.lightingIna)

    Serial.println(
      F("[WARN] Lighting INA226 not detected.")
    );

  if (!sensorOK.conveyorIna)

    Serial.println(
      F("[WARN] Conveyor INA226 not detected.")
    );

  if (!sensorOK.coolingTower1)

    Serial.println(
      F("[WARN] Cooling Tower 1 SHT31 not detected.")
    );

  if (!sensorOK.coolingTower2)

    Serial.println(
      F("[WARN] Cooling Tower 2 SHT31 not detected.")
    );

  if (!sensorOK.switchyardTemp)

    Serial.println(
      F("[WARN] Switchyard DS18B20 not detected.")
    );
}

// ============================================================================
// WIFI INITIALIZATION - STATIC IP
// ============================================================================
void initWifi() {

  WiFi.mode(WIFI_STA);

  // Disable modem sleep for stable MQTT keepalive.
  WiFi.setSleep(false);

  Serial.println(
    F("[WIFI] Configuring static IP...")
  );

  if (
    !WiFi.config(
      WifiConfig::LOCAL_IP,
      WifiConfig::GATEWAY,
      WifiConfig::SUBNET,
      WifiConfig::PRIMARY_DNS,
      WifiConfig::SECONDARY_DNS
    )
  ) {

    Serial.println(
      F("[WIFI] Static IP configuration FAILED!")
    );

  } else {

    Serial.print(
      F("[WIFI] Static IP configured: ")
    );

    Serial.println(
      WifiConfig::LOCAL_IP
    );
  }

  Serial.print(
    F("[WIFI] Connecting to SSID: ")
  );

  Serial.println(
    WifiConfig::SSID
  );

  WiFi.begin(
    WifiConfig::SSID,
    WifiConfig::PASSWORD
  );

  Serial.print(
    F("[WIFI] Target IP: ")
  );

  Serial.println(
    WifiConfig::LOCAL_IP
  );
}

// ============================================================================
// MQTT INITIALIZATION
// ============================================================================
void initMqtt() {

  mqtt.setServer(
    ThingsBoardConfig::SERVER,
    ThingsBoardConfig::PORT
  );

  mqtt.setCallback(
    mqttCallback
  );

  mqtt.setBufferSize(1024);

  mqtt.setSocketTimeout(8);
}

// ============================================================================
// PRODUCT COUNT
// ============================================================================
void monitorProductCount() {

  static unsigned long lastDebounceTime = 0;

  static bool lastRawState = false;

  static bool steadyState = false;

  bool rawState =
      (
        digitalRead(
          Pins::E18_D80NK
        ) == LOW
      );

  if (rawState != lastRawState) {

    lastDebounceTime = millis();
  }

  if (
    millis() - lastDebounceTime > 25
  ) {

    if (rawState != steadyState) {

      steadyState = rawState;

      if (
        steadyState &&
        state.conveyorOn
      ) {

        state.productCount++;

        state.productCountTotal++;

        pendingCountWrites++;

        savePersistedProductCount();

        Serial.print(
          F("\n[+] Product detected! Run: ")
        );

        Serial.print(
          state.productCount
        );

        Serial.print(
          F(" | Lifetime Total: ")
        );

        Serial.println(
          state.productCountTotal
        );
      }
    }
  }

  lastRawState = rawState;
}

// ============================================================================
// NVS
// ============================================================================
void loadPersistedProductCount() {

  nvs.begin(
    NvsConfig::NAMESPACE,
    false
  );

  state.productCountTotal =
      nvs.getULong(
        NvsConfig::KEY_TOTAL_COUNT,
        0
      );

  lastCountSaveAt = millis();

  Serial.print(
    F("[NVS] Loaded lifetime product count: ")
  );

  Serial.println(
    state.productCountTotal
  );
}

void savePersistedProductCount(
  bool force
) {

  bool countThreshold =
      pendingCountWrites >=
      COUNT_SAVE_EVERY_N;

  bool intervalHit =
      (
        millis() - lastCountSaveAt >=
        COUNT_SAVE_INTERVAL_MS
      )
      &&
      pendingCountWrites > 0;

  if (
    !force &&
    !countThreshold &&
    !intervalHit
  )
    return;

  nvs.putULong(
    NvsConfig::KEY_TOTAL_COUNT,
    state.productCountTotal
  );

  pendingCountWrites = 0;

  lastCountSaveAt = millis();
}

void resetProductCount() {

  state.productCount = 0;

  state.productCountTotal = 0;

  pendingCountWrites = 0;

  savePersistedProductCount(true);

  Serial.println(
    F("[NVS] Product count reset (run + lifetime total).")
  );
}

// ============================================================================
// SENSOR SAMPLING
// ============================================================================
void sampleSensors() {

  if (sensorOK.lightingIna) {

    readings.lightingVoltage =
        lightingIna.getBusVoltage_V();

    readings.lightingCurrent =
        lightingIna.getCurrent_mA();

    readings.lightingPower =
        readings.lightingVoltage *
        (
          readings.lightingCurrent /
          1000.0f
        );
  }

  if (sensorOK.conveyorIna) {

    readings.conveyorVoltage =
        conveyorIna.getBusVoltage_V();

    readings.conveyorCurrent =
        conveyorIna.getCurrent_mA();

    readings.conveyorPower =
        readings.conveyorVoltage *
        (
          readings.conveyorCurrent /
          1000.0f
        );
  }

  if (sensorOK.coolingTower1) {

    float t =
        coolingTower1Sht.readTemperature();

    float h =
        coolingTower1Sht.readHumidity();

    if (!isnan(t))
      readings.coolingTower1Temp = t;

    if (!isnan(h))
      readings.coolingTower1Hum = h;
  }

  if (sensorOK.coolingTower2) {

    float t =
        coolingTower2Sht.readTemperature();

    float h =
        coolingTower2Sht.readHumidity();

    if (!isnan(t))
      readings.coolingTower2Temp = t;

    if (!isnan(h))
      readings.coolingTower2Hum = h;
  }

  if (sensorOK.switchyardTemp) {

    float t =
        switchyardTempSensor.getTempCByIndex(0);

    if (
      t != DEVICE_DISCONNECTED_C
    )
      readings.switchyardTemp = t;

    switchyardTempSensor.requestTemperatures();
  }
}

// ============================================================================
// LASER
// ============================================================================
bool laserBlocked(
  uint8_t pin
) {

  return digitalRead(pin) == HIGH;
}

// ============================================================================
// CAMERA RECORDING
// ============================================================================
void triggerCameraRecording(
  uint8_t zone
) {

  if (!CameraConfig::ENABLED) {

    Serial.println(
      F("[CAMERA] Trigger skipped - camera recording disabled.")
    );

    return;
  }

  if (
    zone < 1 ||
    zone > 4
  ) {

    Serial.println(
      F("[CAMERA] Invalid zone.")
    );

    return;
  }

  if (
    WiFi.status() != WL_CONNECTED
  ) {

    Serial.println(
      F("[CAMERA] Wi-Fi not connected. Camera trigger skipped.")
    );

    return;
  }

  HTTPClient http;

  Serial.println(F(""));
  Serial.println(
    F("==============================================")
  );

  Serial.println(
    F("[CAMERA] SWITCHYARD INTRUSION")
  );

  Serial.print(
    F("[CAMERA] Zone: ")
  );

  Serial.println(zone);

  http.setConnectTimeout(
    CameraConfig::HTTP_TIMEOUT_MS
  );

  http.setTimeout(
    CameraConfig::HTTP_TIMEOUT_MS
  );

  if (
    !http.begin(
      CameraConfig::SERVER_URL
    )
  ) {

    Serial.println(
      F("[CAMERA] HTTP connection initialization failed.")
    );

    Serial.println(
      F("==============================================")
    );

    return;
  }

  http.addHeader(
    "Content-Type",
    "application/json"
  );

  String payload =
      "{\"switchyardIntrusion\":true,\"switchyardZone\":" +
      String(zone) +
      "}";

  Serial.print(
    F("[CAMERA] POST: ")
  );

  Serial.println(payload);

  esp_task_wdt_reset();

  unsigned long callStart =
      millis();

  int httpCode =
      http.POST(payload);

  unsigned long callDuration =
      millis() - callStart;

  esp_task_wdt_reset();

  Serial.print(
    F("[CAMERA] HTTP response: ")
  );

  Serial.print(httpCode);

  Serial.print(
    F(" | Call took: ")
  );

  Serial.print(callDuration);

  Serial.println(
    F(" ms")
  );

  if (httpCode > 0) {

    String response =
        http.getString();

    Serial.print(
      F("[CAMERA] Server response: ")
    );

    Serial.println(response);

    if (
      httpCode == HTTP_CODE_OK
    ) {

      Serial.println(
        F("[CAMERA] Recording request accepted.")
      );

    } else if (
      httpCode == HTTP_CODE_CONFLICT
    ) {

      Serial.println(
        F("[CAMERA] Camera is already recording.")
      );

    } else {

      Serial.println(
        F("[CAMERA] Server returned an error.")
      );
    }

  } else {

    Serial.print(
      F("[CAMERA] HTTP request failed: ")
    );

    Serial.println(
      http.errorToString(httpCode)
    );
  }

  http.end();

  Serial.println(
    F("==============================================")
  );
}

// ============================================================================
// LASER FENCE
// ============================================================================
void checkLaserFence() {

  bool currentLaserState[4];

  currentLaserState[0] =
      laserBlocked(
        Pins::LASER_SW1
      );

  currentLaserState[1] =
      laserBlocked(
        Pins::LASER_SW2
      );

  currentLaserState[2] =
      laserBlocked(
        Pins::LASER_SW3
      );

  currentLaserState[3] =
      laserBlocked(
        Pins::LASER_SW4
      );

  if (!laserStateInitialized) {

    for (
      uint8_t i = 0;
      i < 4;
      i++
    ) {

      previousLaserState[i] =
          currentLaserState[i];
    }

    laserStateInitialized = true;
  }

  for (
    uint8_t i = 0;
    i < 4;
    i++
  ) {

    bool intrusionStarted =
        (
          !previousLaserState[i] &&
          currentLaserState[i]
        );

    if (intrusionStarted) {

      uint8_t zone = i + 1;

      Serial.println(F(""));

      Serial.println(
        F("******** INTRUSION DETECTED ********")
      );

      Serial.print(
        F("Switchyard Zone: ")
      );

      Serial.println(zone);

      triggerCameraRecording(
        zone
      );
    }

    previousLaserState[i] =
        currentLaserState[i];
  }

  state.intrusion =
      currentLaserState[0] ||
      currentLaserState[1] ||
      currentLaserState[2] ||
      currentLaserState[3];

  if (state.intrusion) {

    static unsigned long lastToneToggle = 0;

    static bool highTone = false;

    if (
      millis() - lastToneToggle >= 200
    ) {

      lastToneToggle = millis();

      highTone = !highTone;

      ledcWriteTone(
        Pins::BUZZER,
        highTone
          ? 2500
          : 1500
      );
    }

  } else {

    ledcWriteTone(
      Pins::BUZZER,
      0
    );
  }
}

// ============================================================================
// ACTUATOR CONTROLS
// ============================================================================
void setConveyor(
  bool on
) {

  state.conveyorOn = on;

  digitalWrite(
    Pins::MOTOR_ENA,
    on ? HIGH : LOW
  );

  digitalWrite(
    Pins::GREEN_LED,
    on ? HIGH : LOW
  );

  digitalWrite(
    Pins::RED_LED,
    on ? LOW : HIGH
  );

  state.greenLedOn = on;

  state.redLedOn = !on;

  Serial.println(
    on
      ? F("Conveyor Status : Running")
      : F("Conveyor Status : Stopped")
  );
}

void setMist1(
  bool on
) {

  state.mist1On = on;

  digitalWrite(
    Pins::MIST1,
    on ? HIGH : LOW
  );
}

void setMist2(
  bool on
) {

  state.mist2On = on;

  digitalWrite(
    Pins::MIST2,
    on ? HIGH : LOW
  );
}

void setHeater(
  bool on
) {

  state.heaterOn = on;

  digitalWrite(
    Pins::HEATER_RELAY,
    on ? LOW : HIGH
  );
}

void setLighting(
  bool on
) {

  state.lightingOn = on;

  digitalWrite(
    Pins::LIGHTING_MOSFET,
    on ? HIGH : LOW
  );
}

void setPerson1Angle(
  int angle
) {

  person1Servo.moveTo(
    angle
  );
}

void setPerson2Angle(
  int angle
) {

  person2Servo.moveTo(
    angle
  );
}

void togglePerson1() {

  if (
    person1Servo.position() == 0
  )
    setPerson1Angle(180);
  else
    setPerson1Angle(0);
}

void togglePerson2() {

  if (
    person2Servo.position() == 180
  )
    setPerson2Angle(0);
  else
    setPerson2Angle(180);
}

void homePerson1() {

  person1Servo.moveTo(180);

  Serial.println(
    F("Person 1 returned to Home position (180°)")
  );
}

void homePerson2() {

  person2Servo.moveTo(180);

  Serial.println(
    F("Person 2 returned to Home position (180°)")
  );
}

// ============================================================================
// NETWORK / CLOUD
// ============================================================================
void maintainConnection() {

  if (
    WiFi.status() != WL_CONNECTED
  ) {

    if (
      WiFi.status() != WL_IDLE_STATUS &&
      millis() - lastWifiAttempt >=
      WIFI_RETRY_INTERVAL_MS
    ) {

      lastWifiAttempt = millis();

      Serial.println(
        F("[WIFI] Retrying connection...")
      );

      WiFi.disconnect();

      WiFi.begin(
        WifiConfig::SSID,
        WifiConfig::PASSWORD
      );
    }

    return;
  }

  // --------------------------------------------------------------------------
  // MQTT stale connection detection
  // --------------------------------------------------------------------------
  if (
    mqtt.connected() &&
    lastSuccessfulPublish != 0 &&
    millis() - lastSuccessfulPublish >
    TELEMETRY_STALE_MS
  ) {

    Serial.println(
      F("[MQTT] Telemetry stale - forcing reconnect.")
    );

    mqtt.disconnect();
  }

  // --------------------------------------------------------------------------
  // MQTT connection
  // --------------------------------------------------------------------------
  if (!mqtt.connected()) {

    if (
      millis() - lastMqttAttempt >= 5000
    ) {

      lastMqttAttempt = millis();

      esp_task_wdt_reset();

      bool connected =
          mqtt.connect(
            ThingsBoardConfig::CLIENT_ID,
            ThingsBoardConfig::TOKEN,
            nullptr,
            ThingsBoardConfig::LWT_TOPIC,
            ThingsBoardConfig::LWT_QOS,
            ThingsBoardConfig::LWT_RETAIN,
            ThingsBoardConfig::LWT_MESSAGE
          );

      Serial.printf(
        "[MQTT] Connect attempt: %s (rc=%d)\n",
        connected
          ? "OK"
          : "FAILED",
        mqtt.state()
      );

      if (connected) {

        mqtt.subscribe(
          "v1/devices/me/rpc/request/+"
        );

        mqtt.subscribe(
          "v1/devices/me/attributes"
        );

        mqtt.subscribe(
          "v1/devices/me/attributes/response/+"
        );

        static bool rollbackChecked = false;

        if (!rollbackChecked) {

          rollbackChecked = true;

          const esp_partition_t *running =
              esp_ota_get_running_partition();

          esp_ota_img_states_t otaState;

          if (
            esp_ota_get_state_partition(
              running,
              &otaState
            ) == ESP_OK &&
            otaState ==
            ESP_OTA_IMG_PENDING_VERIFY
          ) {

            esp_ota_mark_app_valid_cancel_rollback();

            Serial.println(
              F("[OTA] New firmware confirmed working - rollback cancelled.")
            );
          }
        }

        if (!otaCheckedThisSession) {

          otaCheckedThisSession = true;

          requestFirmwareAttributes();
        }

        publishOnlineStatus();

        reportCurrentFirmwareInfo();

        publishTelemetry();
      }

      esp_task_wdt_reset();
    }
  }
}

// ============================================================================
// ONLINE STATUS
// ============================================================================
void publishOnlineStatus() {

  if (!mqtt.connected())
    return;

  mqtt.publish(
    ThingsBoardConfig::LWT_TOPIC,
    "{\"status\":\"online\"}"
  );
}

// ============================================================================
// STATE JSON
// ============================================================================
void buildStateJson(
  JsonDocument &doc
) {

  doc["productCount"] =
      state.productCount;

  doc["productCountTotal"] =
      state.productCountTotal;

  doc["laserSwitchyard1"] =
      laserBlocked(Pins::LASER_SW1)
        ? "Intrusion"
        : "Safe";

  doc["laserSwitchyard2"] =
      laserBlocked(Pins::LASER_SW2)
        ? "Intrusion"
        : "Safe";

  doc["laserSwitchyard3"] =
      laserBlocked(Pins::LASER_SW3)
        ? "Intrusion"
        : "Safe";

  doc["laserSwitchyard4"] =
      laserBlocked(Pins::LASER_SW4)
        ? "Intrusion"
        : "Safe";

  doc["lightingPower"] =
      readings.lightingPower;

  doc["conveyorPower"] =
      readings.conveyorPower;

  doc["coolingTower1Temp"] =
      readings.coolingTower1Temp;

  doc["coolingTower1Hum"] =
      readings.coolingTower1Hum;

  doc["coolingTower2Temp"] =
      readings.coolingTower2Temp;

  doc["coolingTower2Hum"] =
      readings.coolingTower2Hum;

  doc["switchyardTemp"] =
      readings.switchyardTemp;

  doc["motor"] =
      state.conveyorOn;

  doc["conveyorStatus"] =
      state.conveyorOn
        ? "Conveyor running"
        : "Conveyor stop";

  doc["mist1"] =
      state.mist1On;

  doc["mist2"] =
      state.mist2On;

  doc["heater"] =
      state.heaterOn;

  doc["lighting"] =
      state.lightingOn;

  doc["person1Servo"] =
      person1Servo.position();

  doc["person2Servo"] =
      person2Servo.position();

  doc["intrusion"] =
      state.intrusion;

  doc["lightingIna_ok"] =
      sensorOK.lightingIna;

  doc["conveyorIna_ok"] =
      sensorOK.conveyorIna;

  doc["coolingTower1_ok"] =
      sensorOK.coolingTower1;

  doc["coolingTower2_ok"] =
      sensorOK.coolingTower2;

  doc["switchyardTemp_ok"] =
      sensorOK.switchyardTemp;
}

// ============================================================================
// TELEMETRY
// ============================================================================
void publishTelemetry() {

  if (!mqtt.connected()) {

    Serial.println(
      F("[DIAG] Skipped publish - not connected")
    );

    return;
  }

  JsonDocument doc;

  buildStateJson(doc);

  String payload;

  serializeJson(
    doc,
    payload
  );

  if (
    mqtt.publish(
      "v1/devices/me/telemetry",
      payload.c_str()
    )
  ) {

    lastSuccessfulPublish =
        millis();

  } else {

    Serial.println(
      F("[DIAG] Publish FAILED")
    );
  }
}

// ============================================================================
// OTA
// ============================================================================
void reportFirmwareState(
  const char* fwState,
  const char* error
) {

  JsonDocument doc;

  doc["fw_state"] =
      fwState;

  if (error != nullptr) {

    doc["fw_error"] =
        error;
  }

  char payload[256];

  size_t written =
      serializeJson(
        doc,
        payload,
        sizeof(payload)
      );

  (void)written;

  mqtt.publish(
    "v1/devices/me/attributes",
    payload
  );

  Serial.printf(
    "[OTA] State: %s%s%s\n",
    fwState,
    error ? " - " : "",
    error ? error : ""
  );
}

// ============================================================================
// CURRENT FIRMWARE INFO
// ============================================================================
void reportCurrentFirmwareInfo() {

  if (!mqtt.connected())
    return;

  JsonDocument doc;

  doc["fw_title"] =
      ThingsBoardConfig::FW_TITLE;

  doc["fw_version"] =
      ThingsBoardConfig::FW_VERSION;

  char payload[128];

  serializeJson(
    doc,
    payload,
    sizeof(payload)
  );

  mqtt.publish(
    "v1/devices/me/attributes",
    payload
  );
}

// ============================================================================
// OTA ATTRIBUTE REQUEST
// ============================================================================
void requestFirmwareAttributes() {

  JsonDocument reqDoc;

  reqDoc["sharedKeys"] =
      "fw_title,fw_version,fw_checksum,fw_checksum_algorithm,fw_size";

  char payload[128];

  serializeJson(
    reqDoc,
    payload,
    sizeof(payload)
  );

  mqtt.publish(
    "v1/devices/me/attributes/request/1",
    payload
  );

  Serial.println(
    F("[OTA] Requested firmware shared attributes.")
  );
}

// ============================================================================
// OTA UPDATE
// ============================================================================
void performOtaUpdate(
  const String &fwTitle,
  const String &fwVersion,
  const String &fwChecksum,
  const String &fwChecksumAlgo,
  long fwSize
) {

  if (otaInProgress)
    return;

  otaInProgress = true;

  Serial.printf(
    "[OTA] Update available: %s (current: %s)\n",
    fwVersion.c_str(),
    ThingsBoardConfig::FW_VERSION
  );

  reportFirmwareState(
    "DOWNLOADING"
  );

  if (
    fwChecksumAlgo != "MD5"
  ) {

    reportFirmwareState(
      "FAILED",
      "Unsupported checksum algorithm (expected MD5)"
    );

    otaInProgress = false;

    return;
  }

  String url =
      "http://" +
      String(ThingsBoardConfig::SERVER) +
      ":" +
      String(ThingsBoardConfig::OTA_PORT) +
      "/api/v1/" +
      String(ThingsBoardConfig::TOKEN) +
      "/firmware?title=" +
      fwTitle +
      "&version=" +
      fwVersion;

  WiFiClient otaClient;

  HTTPClient http;

  http.setConnectTimeout(10000);

  http.setTimeout(10000);

  http.begin(
    otaClient,
    url
  );

  esp_task_wdt_reset();

  int httpCode =
      http.GET();

  esp_task_wdt_reset();

  if (
    httpCode != HTTP_CODE_OK
  ) {

    Serial.printf(
      "[OTA] HTTP GET failed, code: %d\n",
      httpCode
    );

    reportFirmwareState(
      "FAILED",
      "HTTP download request failed"
    );

    http.end();

    otaInProgress = false;

    return;
  }

  int contentLen =
      http.getSize();

  if (
    contentLen <= 0 ||
    (
      fwSize > 0 &&
      contentLen != fwSize
    )
  ) {

    reportFirmwareState(
      "FAILED",
      "Content length mismatch"
    );

    http.end();

    otaInProgress = false;

    return;
  }

  if (
    !Update.begin(contentLen)
  ) {

    reportFirmwareState(
      "FAILED",
      "Insufficient OTA partition space"
    );

    http.end();

    otaInProgress = false;

    return;
  }

  Update.setMD5(
    fwChecksum.c_str()
  );

  WiFiClient *stream =
      http.getStreamPtr();

  uint8_t buf[512];

  int written = 0;

  unsigned long lastProgressLog =
      millis();

  unsigned long lastByteReceivedAt =
      millis();

  while (
    http.connected() &&
    written < contentLen
  ) {

    size_t avail =
        stream->available();

    if (avail) {

      int toRead =
          avail > sizeof(buf)
            ? sizeof(buf)
            : avail;

      int n =
          stream->readBytes(
            buf,
            toRead
          );

      if (n > 0) {

        lastByteReceivedAt =
            millis();

        if (
          Update.write(buf, n) !=
          (size_t)n
        ) {

          reportFirmwareState(
            "FAILED",
            "Flash write error"
          );

          Update.abort();

          http.end();

          otaInProgress = false;

          return;
        }

        written += n;
      }
    }

    if (
      millis() -
      lastByteReceivedAt >
      OTA_STALL_TIMEOUT_MS
    ) {

      reportFirmwareState(
        "FAILED",
        "Transfer stalled - no data received"
      );

      Update.abort();

      http.end();

      otaInProgress = false;

      return;
    }

    esp_task_wdt_reset();

    if (mqtt.connected()) {

      mqtt.loop();
    }

    if (
      millis() -
      lastProgressLog >
      3000
    ) {

      lastProgressLog =
          millis();

      Serial.printf(
        "[OTA] Progress: %d / %d bytes\n",
        written,
        contentLen
      );
    }

    delay(1);
  }

  http.end();

  if (
    written != contentLen
  ) {

    reportFirmwareState(
      "FAILED",
      "Download incomplete"
    );

    Update.abort();

    otaInProgress = false;

    return;
  }

  reportFirmwareState(
    "VERIFIED"
  );

  reportFirmwareState(
    "UPDATING"
  );

  if (
    !Update.end(true)
  ) {

    reportFirmwareState(
      "FAILED",
      Update.errorString()
    );

    otaInProgress = false;

    return;
  }

  reportFirmwareState(
    "UPDATED"
  );

  Serial.println(
    F("[OTA] Update successful. Rebooting...")
  );

  savePersistedProductCount(
    true
  );

  delay(1000);

  esp_restart();
}

// ============================================================================
// MQTT CALLBACK
// ============================================================================
void mqttCallback(
  char *topic,
  byte *payload,
  unsigned int length
) {

  String topicStr(topic);

  String msg;

  msg.reserve(
    length + 1
  );

  for (
    unsigned int i = 0;
    i < length;
    i++
  ) {

    msg +=
        (char)payload[i];
  }

  if (
    topicStr.startsWith(
      "v1/devices/me/attributes"
    )
  ) {

    JsonDocument attrDoc;

    if (
      deserializeJson(
        attrDoc,
        msg
      )
    )
      return;

    JsonVariant attrs;

    if (
      attrDoc.containsKey("shared")
    ) {

      attrs =
          attrDoc["shared"];

    } else {

      attrs =
          attrDoc.as<JsonVariant>();
    }

    if (
      attrs.containsKey("fw_title") &&
      attrs.containsKey("fw_version") &&
      attrs.containsKey("fw_checksum") &&
      attrs.containsKey("fw_checksum_algorithm")
    ) {

      String fwTitle =
          attrs["fw_title"].as<String>();

      String fwVersion =
          attrs["fw_version"].as<String>();

      String fwChecksum =
          attrs["fw_checksum"].as<String>();

      String fwAlgo =
          attrs["fw_checksum_algorithm"].as<String>();

      long fwSize =
          attrs.containsKey("fw_size")
            ? attrs["fw_size"].as<long>()
            : 0;

      if (
        fwTitle ==
          ThingsBoardConfig::FW_TITLE &&
        fwVersion !=
          String(
            ThingsBoardConfig::FW_VERSION
          )
      ) {

        performOtaUpdate(
          fwTitle,
          fwVersion,
          fwChecksum,
          fwAlgo,
          fwSize
        );
      }
    }

    return;
  }

  int requestId =
      topicStr
        .substring(
          topicStr.lastIndexOf('/') + 1
        )
        .toInt();

  JsonDocument rpcDoc;

  if (
    !deserializeJson(
      rpcDoc,
      msg
    )
  ) {

    handleRpc(
      rpcDoc["method"] | "",
      rpcDoc["params"],
      requestId
    );
  }
}

// ============================================================================
// RPC
// ============================================================================
void handleRpc(
  const String &method,
  JsonVariant params,
  int requestId
) {

  if (
    method == "setConveyor"
  )

    setConveyor(
      params.as<bool>()
    );

  else if (
    method == "setMist1"
  )

    setMist1(
      params.as<bool>()
    );

  else if (
    method == "setMist2"
  )

    setMist2(
      params.as<bool>()
    );

  else if (
    method == "setHeater"
  )

    setHeater(
      params.as<bool>()
    );

  else if (
    method == "setLighting"
  )

    setLighting(
      params.as<bool>()
    );

  else if (
    method == "setPerson1" ||
    method == "setServo1"
  )

    setPerson1Angle(
      params.as<int>()
    );

  else if (
    method == "setPerson2" ||
    method == "setServo2"
  )

    setPerson2Angle(
      params.as<int>()
    );

  else if (
    method == "homePerson1" ||
    method == "homeServo1"
  )

    homePerson1();

  else if (
    method == "homePerson2" ||
    method == "homeServo2"
  )

    homePerson2();

  else if (
    method == "resetProductCount"
  )

    resetProductCount();

  else if (
    method == "reboot"
  ) {

    Serial.println(
      F("[SYS] Remote reboot triggered via RPC!")
    );

    savePersistedProductCount(
      true
    );

    delay(1000);

    esp_restart();
  }

  if (
    requestId >= 0 &&
    mqtt.connected()
  ) {

    JsonDocument stateDoc;

    buildStateJson(
      stateDoc
    );

    String response;

    serializeJson(
      stateDoc,
      response
    );

    mqtt.publish(
      (
        "v1/devices/me/rpc/response/" +
        String(requestId)
      ).c_str(),
      response.c_str()
    );
  }

  publishTelemetry();
}

// ============================================================================
// LOGGING
// ============================================================================
void logSensorData() {

  Serial.println(
    F("--------------------------------------------------")
  );

  Serial.print(
    F("Conveyor Status     : ")
  );

  Serial.println(
    state.conveyorOn
      ? F("Conveyor running")
      : F("Conveyor stop")
  );

  Serial.print(
    F("Product Count       : ")
  );

  Serial.println(
    state.productCount
  );

  Serial.print(
    F("Switchyard Temp     : ")
  );

  Serial.print(
    readings.switchyardTemp,
    2
  );

  Serial.println(
    F(" C")
  );

  Serial.print(
    F("Laser Switchyard 1  : ")
  );

  Serial.println(
    laserBlocked(Pins::LASER_SW1)
      ? F("Intrusion")
      : F("Safe")
  );

  Serial.print(
    F("Laser Switchyard 2  : ")
  );

  Serial.println(
    laserBlocked(Pins::LASER_SW2)
      ? F("Intrusion")
      : F("Safe")
  );

  Serial.print(
    F("Laser Switchyard 3  : ")
  );

  Serial.println(
    laserBlocked(Pins::LASER_SW3)
      ? F("Intrusion")
      : F("Safe")
  );

  Serial.print(
    F("Laser Switchyard 4  : ")
  );

  Serial.println(
    laserBlocked(Pins::LASER_SW4)
      ? F("Intrusion")
      : F("Safe")
  );

  Serial.print(
    F("Lighting Power      : ")
  );

  Serial.print(
    readings.lightingPower,
    3
  );

  Serial.println(
    F(" W")
  );

  Serial.print(
    F("Conveyor Power      : ")
  );

  Serial.print(
    readings.conveyorPower,
    3
  );

  Serial.println(
    F(" W")
  );

  Serial.print(
    F("Cooling Tower 1     : ")
  );

  Serial.print(
    readings.coolingTower1Temp,
    2
  );

  Serial.print(
    F(" C, ")
  );

  Serial.print(
    readings.coolingTower1Hum,
    2
  );

  Serial.println(
    F(" %RH")
  );

  Serial.print(
    F("Cooling Tower 2     : ")
  );

  Serial.print(
    readings.coolingTower2Temp,
    2
  );

  Serial.print(
    F(" C, ")
  );

  Serial.print(
    readings.coolingTower2Hum,
    2
  );

  Serial.println(
    F(" %RH")
  );

  // --------------------------------------------------------------------------
  // NETWORK DIAGNOSTICS
  // --------------------------------------------------------------------------
  Serial.print(
    F("WiFi Status         : ")
  );

  Serial.print(
    WiFi.status()
  );

  Serial.print(
    F(" | IP: ")
  );

  Serial.print(
    WiFi.localIP()
  );

  Serial.print(
    F(" | RSSI: ")
  );

  Serial.print(
    WiFi.RSSI()
  );

  Serial.print(
    F(" | MQTT Connected: ")
  );

  Serial.print(
    mqtt.connected()
  );

  Serial.print(
    F(" | MQTT State: ")
  );

  Serial.print(
    mqtt.state()
  );

  Serial.print(
    F(" | Free Heap: ")
  );

  Serial.println(
    ESP.getFreeHeap()
  );

  Serial.println(
    F("--------------------------------------------------")
  );
}

// ============================================================================
// SERIAL COMMANDS
// ============================================================================
void handleSerialCommand(
  char command
) {

  switch (command) {

    case 'm':
      setConveyor(
        !state.conveyorOn
      );
      break;

    case '1':
      setMist1(
        !state.mist1On
      );
      break;

    case '2':
      setMist2(
        !state.mist2On
      );
      break;

    case 'h':
      setHeater(
        !state.heaterOn
      );
      break;

    case 'l':
      setLighting(
        !state.lightingOn
      );
      break;

    case 'p':
      togglePerson1();
      break;

    case 'o':
      togglePerson2();
      break;

    case 't':
      sampleSensors();
      logSensorData();
      publishTelemetry();
      break;

    case 'n':

      Serial.printf(
        "[DIAG] WiFi status: %d | IP: %s | RSSI: %d\n",
        WiFi.status(),
        WiFi.localIP().toString().c_str(),
        WiFi.RSSI()
      );

      Serial.printf(
        "[DIAG] MQTT connected: %d | MQTT state: %d\n",
        mqtt.connected(),
        mqtt.state()
      );

      Serial.printf(
        "[DIAG] Free heap: %u | Uptime: %lus | Last publish: %lus ago\n",
        ESP.getFreeHeap(),
        millis() / 1000,
        lastSuccessfulPublish == 0
          ? 0
          : (
              millis() -
              lastSuccessfulPublish
            ) / 1000
      );

      break;

    default:
      break;
  }
}
