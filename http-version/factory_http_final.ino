#include <WiFi.h>
#include <Wire.h>
#include <INA226_WE.h>
#include <Adafruit_SHT31.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ESP32Servo.h>
#include <ArduinoJson.h>
#include <HTTPClient.h>
#include <WebServer.h>
#include <Preferences.h>
#include <Update.h>
#include <esp_ota_ops.h>
#include "secrets.h"

// ============================================================================
// FACTORY MODEL - HTTP VERSION
// Same hardware, sensors, camera trigger, NVS counting and actuator logic as
// the deployed firmware. ThingsBoard/MQTT replaced by HTTP.
// ============================================================================

// Change this string whenever a new firmware build is created.
constexpr char FIRMWARE_VERSION[] = "factory-http-ota-v1.1.0";

// ============================================================================
// WIFI
// ============================================================================
namespace WifiConfig {
  // Credentials live in secrets.h (keep that file out of version control).
  constexpr const char* SSID     = Secrets::WIFI_SSID;
  constexpr const char* PASSWORD = Secrets::WIFI_PASSWORD;

  const IPAddress LOCAL_IP(10, 10, 12, 75);
  const IPAddress GATEWAY(10, 10, 12, 1);
  const IPAddress SUBNET(255, 255, 255, 0);
  const IPAddress PRIMARY_DNS(10, 10, 12, 1);
  const IPAddress SECONDARY_DNS(8, 8, 8, 8);
}

// ============================================================================
// BACKEND
// ============================================================================
namespace BackendConfig {
  constexpr char SERVER_URL[] = "http://10.10.12.66:4000";
  constexpr char DEVICE_ID[]  = "factory-esp32";

  // Defined in secrets.h; must match the DEVICE_KEY in the Mini PC backend .env.
  constexpr const char* DEVICE_KEY = Secrets::DEVICE_KEY;

  constexpr uint16_t HTTP_TIMEOUT_MS = 1500;
  constexpr char TELEMETRY_PATH[] = "/api/device/telemetry";
}

// ============================================================================
// CAMERA SERVER
// ============================================================================
namespace CameraConfig {
  constexpr bool ENABLED = true;
  constexpr char SERVER_URL[] = "http://10.10.12.66:8081/intrusion";
  constexpr uint16_t HTTP_TIMEOUT_MS = 1500;
}

// ============================================================================
// RELIABILITY / SAFETY
// ============================================================================
constexpr unsigned long WIFI_RETRY_INTERVAL_MS = 15000;
constexpr unsigned long TELEMETRY_INTERVAL_MS = 2000;
constexpr float HEATER_CUTOFF_TEMP_C = 33.0f;

// Heater may only run while the switchyard sensor gives fresh, valid readings.
constexpr unsigned long TEMP_STALE_MS = 10000;

// Network task (telemetry + camera trigger run off the main loop)
constexpr unsigned long NET_BACKOFF_STEP_MS = 2000;
constexpr unsigned long NET_BACKOFF_MAX_MS  = 10000;
constexpr uint32_t NET_TASK_STACK = 8192;
constexpr uint8_t CAMERA_QUEUE_LEN = 8;

// ============================================================================
// NVS
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
WebServer server(80);
Preferences nvs;

// Hand-off between the main loop and the network task
QueueHandle_t cameraQueue = nullptr;
SemaphoreHandle_t telemetryMutex = nullptr;
String pendingTelemetry;
bool telemetryPending = false;

// OTA state
bool otaUploadAuthorized = false;
size_t otaBytesWritten = 0;
bool otaUpdateStarted = false;
bool otaUpdateFailed = false;
String otaUpdateError;

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

  constexpr uint8_t HEATER_RELAY    = 18;
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
// PERIPHERALS
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
// STATE
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
  float lightingPower = 0;

  float conveyorVoltage = 0;
  float conveyorCurrent = 0;
  float conveyorPower = 0;

  float coolingTower1Temp = 0;
  float coolingTower1Hum = 0;
  float coolingTower2Temp = 0;
  float coolingTower2Hum = 0;

  float switchyardTemp = 0;
} readings;

struct SystemState {
  bool conveyorOn = false;
  bool mist1On = false;
  bool mist2On = false;
  bool heaterOn = false;
  bool lightingOn = false;
  bool intrusion = false;

  unsigned long productCount = 0;
  unsigned long productCountTotal = 0;
} state;

uint8_t pendingCountWrites = 0;
unsigned long lastCountSaveAt = 0;

// ============================================================================
// SMOOTH SERVO
// ============================================================================
class SmoothServo {
public:
  void attach(uint8_t pin, int minUs = 500, int maxUs = 2400) {
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
    if (_current == _target) return;
    if (millis() - _lastStepAt < _stepDelayMs) return;

    _lastStepAt = millis();
    _current += (_target > _current) ? 1 : -1;
    _servo.write(_current);
  }

  int position() const { return _current; }
  bool isMoving() const { return _current != _target; }
  int targetPosition() const { return _target; }

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
// TIMING / LASER / CAMERA STATE
// ============================================================================
unsigned long lastSensorSample = 0;
unsigned long lastWifiAttempt = 0;
unsigned long lastTelemetrySend = 0;

unsigned long lastSwitchyardTempAt = 0;
bool switchyardTempEverValid = false;
bool firmwareMarkedValid = false;

bool previousLaserState[4] = {false, false, false, false};
bool laserStateInitialized = false;

bool cameraRecording = false;
uint8_t cameraZone = 0;
unsigned long cameraRecordingUntil = 0;
constexpr unsigned long CAMERA_RECORDING_STATE_MS = 15000;

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================
void initGpio();
void initServos();
void initI2C();
void initSensors();
void initWifi();
void initHttpServer();
void maintainWifi();

void sampleSensors();
void monitorProductCount();
void loadPersistedProductCount();
void savePersistedProductCount(bool force = false);
void resetProductCount();
void logSensorData();

bool laserBlocked(uint8_t pin);
void checkLaserFence();
void queueCameraTrigger(uint8_t zone);
void sendCameraTrigger(uint8_t zone);

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

void buildStateJson(JsonObject doc);
void queueTelemetry();
bool postTelemetry(const String &payload);
void networkTask(void *param);
void markFirmwareValid();

bool heaterTempTrusted();
const char* heaterBlockReason();
void sendHeaterBlocked(const char* reason);

void handleHealth();
void handleStatus();
void handleDeviceCommand();
void handleReboot();
void handleOtaStatus();
void handleOtaUpdateComplete();
void handleOtaUpload();

void handleConveyorOn();
void handleConveyorOff();
void handleMist1On();
void handleMist1Off();
void handleMist2On();
void handleMist2Off();
void handleHeaterOn();
void handleHeaterOff();
void handleLightingOn();
void handleLightingOff();
void handlePerson1Home();
void handlePerson1Zero();
void handlePerson1Max();
void handlePerson2Home();
void handlePerson2Zero();
void handlePerson2Max();

void handleMonitorAll();
void handleMonitorTemperature();
void handleMonitorCoolingTower1();
void handleMonitorCoolingTower2();
void handleMonitorLightingPower();
void handleMonitorConveyorPower();
void handleMonitorTotalPower();
void handleMonitorProductCount();
void handleMonitorLaser1();
void handleMonitorLaser2();
void handleMonitorLaser3();
void handleMonitorLaser4();
void handleMonitorPerson1();
void handleMonitorPerson2();
void handleMonitorCamera();

void handleSerialCommand(char c);

// ============================================================================
// SETUP
// ============================================================================
void setup() {
  Serial.begin(115200);
  delay(300);

  Serial.println(F("\n=================================================="));
  Serial.println(F("       FACTORY MODEL - HTTP VERSION"));
  Serial.print(F("       Firmware: "));
  Serial.println(FIRMWARE_VERSION);
  Serial.println(F("       Static IP: 10.10.12.75"));
  Serial.println(F("       Backend : http://10.10.12.66:4000"));
  Serial.println(F("       Camera  : http://10.10.12.66:8081"));
  Serial.println(F("=================================================="));

  initGpio();
  initServos();
  initI2C();
  initSensors();
  loadPersistedProductCount();

  initWifi();
  initHttpServer();

  cameraQueue = xQueueCreate(CAMERA_QUEUE_LEN, sizeof(uint8_t));
  telemetryMutex = xSemaphoreCreateMutex();
  xTaskCreatePinnedToCore(networkTask, "netTask", NET_TASK_STACK, nullptr, 1, nullptr, 0);

  Serial.println(F("[SYS] System ready."));
}

// ============================================================================
// MAIN LOOP
// ============================================================================
void loop() {
  server.handleClient();
  maintainWifi();

  person1Servo.update();
  person2Servo.update();

  monitorProductCount();
  savePersistedProductCount();
  checkLaserFence();

  // Keep camera state aligned with the Mini PC 15-second recording window.
  if (cameraRecording && millis() >= cameraRecordingUntil) {
    cameraRecording = false;
    cameraZone = 0;
  }

  if (millis() - lastSensorSample >= TELEMETRY_INTERVAL_MS) {
    lastSensorSample = millis();
    sampleSensors();

    // Local safety: heater cuts off at/above the threshold, or when the
    // switchyard sensor is missing / no longer producing fresh readings.
    if (state.heaterOn) {
      const char* reason = heaterBlockReason();
      if (reason) {
        Serial.print(F("[SAFETY] Heater cutoff: "));
        Serial.println(reason);
        setHeater(false);
      }
    }

    logSensorData();
  }

  if (millis() - lastTelemetrySend >= TELEMETRY_INTERVAL_MS) {
    lastTelemetrySend = millis();
    queueTelemetry();
  }

  if (Serial.available()) {
    handleSerialCommand(Serial.read());
  }

  markFirmwareValid();

  delay(2);
}

// ============================================================================
// INITIALIZATION
// ============================================================================
void initGpio() {
  pinMode(Pins::E18_D80NK, INPUT_PULLUP);

  pinMode(Pins::LASER_SW1, INPUT);
  pinMode(Pins::LASER_SW2, INPUT);
  pinMode(Pins::LASER_SW3, INPUT);
  pinMode(Pins::LASER_SW4, INPUT);

  pinMode(Pins::MOTOR_ENA, OUTPUT);
  pinMode(Pins::MIST1, OUTPUT);
  pinMode(Pins::MIST2, OUTPUT);
  pinMode(Pins::HEATER_RELAY, OUTPUT);
  pinMode(Pins::LIGHTING_MOSFET, OUTPUT);

  pinMode(Pins::RED_LED, OUTPUT);
  pinMode(Pins::GREEN_LED, OUTPUT);

  ledcAttach(Pins::BUZZER, 2000, 8);
  ledcWriteTone(Pins::BUZZER, 0);

  digitalWrite(Pins::MOTOR_ENA, LOW);
  digitalWrite(Pins::MIST1, LOW);
  digitalWrite(Pins::MIST2, LOW);

  // Heater relay is active LOW: HIGH = OFF.
  digitalWrite(Pins::HEATER_RELAY, HIGH);
  digitalWrite(Pins::LIGHTING_MOSFET, LOW);

  digitalWrite(Pins::RED_LED, HIGH);
  digitalWrite(Pins::GREEN_LED, LOW);
}

// Person 1: home 180, range 0-180.  Person 2: home 0, range 0-150.
void initServos() {
  ESP32PWM::allocateTimer(1);
  ESP32PWM::allocateTimer(2);

  person1Servo.attach(Pins::PERSON1_SERVO, 500, 2400);
  person2Servo.attach(Pins::PERSON2_SERVO, 500, 2400);

  person1Servo.begin(180);
  person2Servo.begin(0);
}

void initI2C() {
  I2C_Zone1.begin(Pins::SDA_1, Pins::SCL_1, 100000);
  I2C_Zone2.begin(Pins::SDA_2, Pins::SCL_2, 100000);
}

void initSensors() {
  sensorOK.lightingIna = lightingIna.init();
  if (sensorOK.lightingIna) {
    lightingIna.setResistorRange(0.1, 1.0);  // 0.1 ohm shunt
  }

  sensorOK.coolingTower1 = coolingTower1Sht.begin(SHT31_ADDR);

  sensorOK.conveyorIna = conveyorIna.init();
  if (sensorOK.conveyorIna) {
    conveyorIna.setResistorRange(0.1, 1.0);  // 0.1 ohm shunt
  }

  sensorOK.coolingTower2 = coolingTower2Sht.begin(SHT31_ADDR);

  switchyardTempSensor.begin();
  switchyardTempSensor.setWaitForConversion(false);
  sensorOK.switchyardTemp = (switchyardTempSensor.getDeviceCount() > 0);
  if (sensorOK.switchyardTemp) {
    switchyardTempSensor.requestTemperatures();
  }

  if (!sensorOK.lightingIna)    Serial.println(F("[WARN] Lighting INA226 not detected."));
  if (!sensorOK.conveyorIna)    Serial.println(F("[WARN] Conveyor INA226 not detected."));
  if (!sensorOK.coolingTower1)  Serial.println(F("[WARN] Cooling Tower 1 SHT31 not detected."));
  if (!sensorOK.coolingTower2)  Serial.println(F("[WARN] Cooling Tower 2 SHT31 not detected."));
  if (!sensorOK.switchyardTemp) Serial.println(F("[WARN] Switchyard DS18B20 not detected."));
}

// ============================================================================
// WIFI
// ============================================================================
void initWifi() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);

  if (!WiFi.config(WifiConfig::LOCAL_IP, WifiConfig::GATEWAY, WifiConfig::SUBNET,
                   WifiConfig::PRIMARY_DNS, WifiConfig::SECONDARY_DNS)) {
    Serial.println(F("[WIFI] Static IP configuration FAILED!"));
  } else {
    Serial.print(F("[WIFI] Static IP configured: "));
    Serial.println(WifiConfig::LOCAL_IP);
  }

  Serial.print(F("[WIFI] Connecting to SSID: "));
  Serial.println(WifiConfig::SSID);

  WiFi.begin(WifiConfig::SSID, WifiConfig::PASSWORD);
}

void maintainWifi() {
  if (WiFi.status() == WL_CONNECTED) return;

  if (millis() - lastWifiAttempt >= WIFI_RETRY_INTERVAL_MS) {
    lastWifiAttempt = millis();

    Serial.println(F("[WIFI] Retrying connection..."));
    WiFi.disconnect();
    WiFi.begin(WifiConfig::SSID, WifiConfig::PASSWORD);
  }
}

// ============================================================================
// HTTP SERVER
// ============================================================================
void initHttpServer() {
  const char* headerKeys[] = {"X-Device-Key", "Content-Length"};
  server.collectHeaders(headerKeys, 2);

  server.on("/", HTTP_GET, []() {
    String html =
      "<!doctype html><html><body>"
      "<h2>Factory Model ESP32 HTTP</h2>"
      "<p>Device: factory-esp32</p>"
      "<p>IP: " + WiFi.localIP().toString() + "</p>"
      "<p><a href='/api/health'>Health</a></p>"
      "<p><a href='/api/status'>Status</a></p>"
      "<p>Firmware: " + String(FIRMWARE_VERSION) + "</p>"
      "<p>OTA status: <a href='/api/ota/status'>/api/ota/status</a></p>"
      "<p>Digital Twin monitor base: /api/monitor/</p>"
      "<p>Digital Twin control base: /api/control/</p>"
      "</body></html>";

    server.send(200, "text/html", html);
  });

  server.on("/api/health", HTTP_GET, handleHealth);
  server.on("/api/status", HTTP_GET, handleStatus);
  server.on("/api/telemetry", HTTP_GET, handleStatus);  // same payload as status

  server.on("/api/device/command", HTTP_POST, handleDeviceCommand);
  server.on("/api/reboot", HTTP_POST, handleReboot);

  // OTA: GET status is read-only; POST update accepts a .bin firmware image.
  server.on("/api/ota/status", HTTP_GET, handleOtaStatus);
  server.on("/api/ota/update", HTTP_POST, handleOtaUpdateComplete, handleOtaUpload);
  server.on("/ota", HTTP_GET, []() {
    const char* html = R"HTML(
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Factory Model OTA</title>
<style>body{font-family:Arial,sans-serif;max-width:650px;margin:40px auto;padding:0 18px}input,button{font-size:16px;padding:10px;margin:8px 0;width:100%;box-sizing:border-box}button{cursor:pointer}.box{border:1px solid #ccc;border-radius:10px;padding:20px}#status{white-space:pre-wrap;margin-top:15px}</style>
</head>
<body>
<div class="box">
<h2>Factory Model ESP32 OTA</h2>
<p>Firmware update page. Upload only a trusted <b>.bin</b> built for this ESP32.</p>
<label>Device key</label>
<input id="key" type="password" autocomplete="off" placeholder="X-Device-Key">
<label>Firmware .bin</label>
<input id="file" type="file" accept=".bin,application/octet-stream">
<button onclick="upload()">Upload Firmware</button>
<div id="status">Ready.</div>
</div>
<script>
async function upload(){
 const key=document.getElementById('key').value;
 const file=document.getElementById('file').files[0];
 const out=document.getElementById('status');
 if(!key||!file){out.textContent='Enter the device key and select a .bin file.';return;}
 if(!confirm('Upload '+file.name+' and reboot the ESP32 after a successful update?')) return;
 out.textContent='Uploading...';
 const fd=new FormData(); fd.append('firmware',file,file.name);
 try{
   const r=await fetch('/api/ota/update',{method:'POST',headers:{'X-Device-Key':key},body:fd});
   const t=await r.text();
   out.textContent='HTTP '+r.status+'\n'+t+'\n\nThe ESP32 may be rebooting. Wait about 10 seconds, then refresh /api/health.';
 }catch(e){out.textContent='Upload error: '+e;}
}
</script>
</body>
</html>
)HTML";
    server.send(200, "text/html", html);
  });

  // Digital Twin control (POST, X-Device-Key required)
  server.on("/api/control/conveyor/on", HTTP_POST, handleConveyorOn);
  server.on("/api/control/conveyor/off", HTTP_POST, handleConveyorOff);
  server.on("/api/control/mist1/on", HTTP_POST, handleMist1On);
  server.on("/api/control/mist1/off", HTTP_POST, handleMist1Off);
  server.on("/api/control/mist2/on", HTTP_POST, handleMist2On);
  server.on("/api/control/mist2/off", HTTP_POST, handleMist2Off);
  server.on("/api/control/heater/on", HTTP_POST, handleHeaterOn);
  server.on("/api/control/heater/off", HTTP_POST, handleHeaterOff);
  server.on("/api/control/lighting/on", HTTP_POST, handleLightingOn);
  server.on("/api/control/lighting/off", HTTP_POST, handleLightingOff);
  server.on("/api/control/person1/home", HTTP_POST, handlePerson1Home);
  server.on("/api/control/person1/0", HTTP_POST, handlePerson1Zero);
  server.on("/api/control/person1/180", HTTP_POST, handlePerson1Max);
  server.on("/api/control/person2/home", HTTP_POST, handlePerson2Home);
  server.on("/api/control/person2/0", HTTP_POST, handlePerson2Zero);
  server.on("/api/control/person2/150", HTTP_POST, handlePerson2Max);

  // Digital Twin monitoring (GET, X-Device-Key required)
  server.on("/api/monitor/all", HTTP_GET, handleMonitorAll);
  server.on("/api/monitor/temperature", HTTP_GET, handleMonitorTemperature);
  server.on("/api/monitor/cooling-tower-1", HTTP_GET, handleMonitorCoolingTower1);
  server.on("/api/monitor/cooling-tower-2", HTTP_GET, handleMonitorCoolingTower2);
  server.on("/api/monitor/lighting-power", HTTP_GET, handleMonitorLightingPower);
  server.on("/api/monitor/conveyor-power", HTTP_GET, handleMonitorConveyorPower);
  server.on("/api/monitor/power", HTTP_GET, handleMonitorTotalPower);
  server.on("/api/monitor/product-count", HTTP_GET, handleMonitorProductCount);
  server.on("/api/monitor/laser/1", HTTP_GET, handleMonitorLaser1);
  server.on("/api/monitor/laser/2", HTTP_GET, handleMonitorLaser2);
  server.on("/api/monitor/laser/3", HTTP_GET, handleMonitorLaser3);
  server.on("/api/monitor/laser/4", HTTP_GET, handleMonitorLaser4);
  server.on("/api/monitor/person1", HTTP_GET, handleMonitorPerson1);
  server.on("/api/monitor/person2", HTTP_GET, handleMonitorPerson2);
  server.on("/api/monitor/camera", HTTP_GET, handleMonitorCamera);

  server.begin();
  Serial.println(F("[HTTP] Device API started on port 80."));
}

// ============================================================================
// AUTHENTICATION
// ============================================================================
bool authorizedRequest() {
  if (!server.hasHeader("X-Device-Key")) {
    server.send(401, "application/json",
                "{\"success\":false,\"error\":\"Missing X-Device-Key\"}");
    return false;
  }

  if (server.header("X-Device-Key") != String(BackendConfig::DEVICE_KEY)) {
    server.send(403, "application/json",
                "{\"success\":false,\"error\":\"Invalid device key\"}");
    return false;
  }

  return true;
}

// ============================================================================
// STATE JSON
// ============================================================================
void buildStateJson(JsonObject doc) {
  doc["device"] = BackendConfig::DEVICE_ID;

  JsonObject network = doc["network"].to<JsonObject>();
  network["ip"] = WiFi.localIP().toString();
  network["gateway"] = WiFi.gatewayIP().toString();
  network["rssi"] = WiFi.RSSI();
  network["wifiConnected"] = WiFi.status() == WL_CONNECTED;

  JsonObject actuators = doc["actuators"].to<JsonObject>();
  actuators["conveyor"] = state.conveyorOn;
  actuators["mist1"] = state.mist1On;
  actuators["mist2"] = state.mist2On;
  actuators["heater"] = state.heaterOn;
  actuators["lighting"] = state.lightingOn;

  JsonObject sensors = doc["sensors"].to<JsonObject>();
  sensors["temperatureC"] = readings.switchyardTemp;
  sensors["coolingTower1TempC"] = readings.coolingTower1Temp;
  sensors["coolingTower1Humidity"] = readings.coolingTower1Hum;
  sensors["coolingTower2TempC"] = readings.coolingTower2Temp;
  sensors["coolingTower2Humidity"] = readings.coolingTower2Hum;
  sensors["lightingPowerW"] = readings.lightingPower;
  sensors["conveyorPowerW"] = readings.conveyorPower;
  sensors["powerW"] = readings.lightingPower + readings.conveyorPower;
  sensors["productCount"] = state.productCount;
  sensors["productCountTotal"] = state.productCountTotal;

  JsonObject laser = doc["laserZones"].to<JsonObject>();
  laser["zone1"] = laserBlocked(Pins::LASER_SW1) ? "INTRUSION" : "SAFE";
  laser["zone2"] = laserBlocked(Pins::LASER_SW2) ? "INTRUSION" : "SAFE";
  laser["zone3"] = laserBlocked(Pins::LASER_SW3) ? "INTRUSION" : "SAFE";
  laser["zone4"] = laserBlocked(Pins::LASER_SW4) ? "INTRUSION" : "SAFE";

  JsonObject servos = doc["servos"].to<JsonObject>();
  servos["person1Angle"] = person1Servo.position();
  servos["person2Angle"] = person2Servo.position();

  JsonObject camera = doc["camera"].to<JsonObject>();
  camera["recording"] = cameraRecording;
  camera["zone"] = cameraZone;

  JsonObject health = doc["system"].to<JsonObject>();
  health["lightingIna_ok"] = sensorOK.lightingIna;
  health["conveyorIna_ok"] = sensorOK.conveyorIna;
  health["coolingTower1_ok"] = sensorOK.coolingTower1;
  health["coolingTower2_ok"] = sensorOK.coolingTower2;
  health["switchyardTemp_ok"] = sensorOK.switchyardTemp;
  health["uptime_ms"] = millis();
  health["freeHeap"] = ESP.getFreeHeap();
  health["firmwareVersion"] = FIRMWARE_VERSION;
  health["heaterTempTrusted"] = heaterTempTrusted();
}

// ============================================================================
// HEALTH / STATUS
// ============================================================================
void handleHealth() {
  StaticJsonDocument<512> doc;

  doc["success"] = true;
  doc["service"] = "factory-model-esp32";
  doc["status"] = "ok";
  doc["device"] = BackendConfig::DEVICE_ID;
  doc["wifiConnected"] = WiFi.status() == WL_CONNECTED;
  doc["ip"] = WiFi.localIP().toString();
  doc["uptimeMs"] = millis();

  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

// Also serves /api/telemetry.
void handleStatus() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<4096> doc;
  doc["success"] = true;

  JsonObject stateObj = doc["state"].to<JsonObject>();
  buildStateJson(stateObj);

  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

// ============================================================================
// DIGITAL TWIN - CONTROL ENDPOINTS
// ============================================================================
void sendIndependentControlResponse(const char* command, bool value) {
  StaticJsonDocument<1024> response;
  response["success"] = true;
  response["command"] = command;
  response["value"] = value;

  JsonObject stateObj = response["state"].to<JsonObject>();
  buildStateJson(stateObj);

  String out;
  serializeJson(response, out);
  server.send(200, "application/json", out);
}

void handleConveyorOn() {
  if (!authorizedRequest()) return;
  setConveyor(true);
  sendIndependentControlResponse("conveyor", true);
}

void handleConveyorOff() {
  if (!authorizedRequest()) return;
  setConveyor(false);
  sendIndependentControlResponse("conveyor", false);
}

void handleMist1On() {
  if (!authorizedRequest()) return;
  setMist1(true);
  sendIndependentControlResponse("mist1", true);
}

void handleMist1Off() {
  if (!authorizedRequest()) return;
  setMist1(false);
  sendIndependentControlResponse("mist1", false);
}

void handleMist2On() {
  if (!authorizedRequest()) return;
  setMist2(true);
  sendIndependentControlResponse("mist2", true);
}

void handleMist2Off() {
  if (!authorizedRequest()) return;
  setMist2(false);
  sendIndependentControlResponse("mist2", false);
}

void handleHeaterOn() {
  if (!authorizedRequest()) return;

  const char* reason = heaterBlockReason();
  if (reason) {
    sendHeaterBlocked(reason);
    return;
  }

  setHeater(true);
  sendIndependentControlResponse("heater", true);
}

void handleHeaterOff() {
  if (!authorizedRequest()) return;
  setHeater(false);
  sendIndependentControlResponse("heater", false);
}

void handleLightingOn() {
  if (!authorizedRequest()) return;
  setLighting(true);
  sendIndependentControlResponse("lighting", true);
}

void handleLightingOff() {
  if (!authorizedRequest()) return;
  setLighting(false);
  sendIndependentControlResponse("lighting", false);
}

void handlePerson1Home() {
  if (!authorizedRequest()) return;
  homePerson1();
  sendIndependentControlResponse("person1_home", true);
}

void handlePerson1Zero() {
  if (!authorizedRequest()) return;
  setPerson1Angle(0);
  sendIndependentControlResponse("person1", true);
}

void handlePerson1Max() {
  if (!authorizedRequest()) return;
  setPerson1Angle(180);
  sendIndependentControlResponse("person1", true);
}

void handlePerson2Home() {
  if (!authorizedRequest()) return;
  homePerson2();
  sendIndependentControlResponse("person2_home", true);
}

void handlePerson2Zero() {
  if (!authorizedRequest()) return;
  setPerson2Angle(0);
  sendIndependentControlResponse("person2", true);
}

void handlePerson2Max() {
  if (!authorizedRequest()) return;
  setPerson2Angle(150);
  sendIndependentControlResponse("person2", true);
}

// ============================================================================
// DIGITAL TWIN - MONITORING ENDPOINTS
// ============================================================================
void sendMonitorResponse(JsonDocument& doc) {
  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

void handleMonitorAll() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<4096> doc;
  JsonObject stateObj = doc.to<JsonObject>();
  buildStateJson(stateObj);
  sendMonitorResponse(doc);
}

void handleMonitorTemperature() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["temperatureC"] = readings.switchyardTemp;
  doc["heaterOn"] = state.heaterOn;
  doc["heaterCutoffC"] = HEATER_CUTOFF_TEMP_C;
  doc["heaterTempTrusted"] = heaterTempTrusted();
  sendMonitorResponse(doc);
}

void handleMonitorCoolingTower1() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["temperatureC"] = readings.coolingTower1Temp;
  doc["humidity"] = readings.coolingTower1Hum;
  doc["sensorOk"] = sensorOK.coolingTower1;
  sendMonitorResponse(doc);
}

void handleMonitorCoolingTower2() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["temperatureC"] = readings.coolingTower2Temp;
  doc["humidity"] = readings.coolingTower2Hum;
  doc["sensorOk"] = sensorOK.coolingTower2;
  sendMonitorResponse(doc);
}

void handleMonitorLightingPower() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["voltageV"] = readings.lightingVoltage;
  doc["currentmA"] = readings.lightingCurrent;
  doc["powerW"] = readings.lightingPower;
  doc["lightingOn"] = state.lightingOn;
  doc["sensorOk"] = sensorOK.lightingIna;
  sendMonitorResponse(doc);
}

void handleMonitorConveyorPower() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["voltageV"] = readings.conveyorVoltage;
  doc["currentmA"] = readings.conveyorCurrent;
  doc["powerW"] = readings.conveyorPower;
  doc["conveyorOn"] = state.conveyorOn;
  doc["sensorOk"] = sensorOK.conveyorIna;
  sendMonitorResponse(doc);
}

void handleMonitorTotalPower() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["lightingPowerW"] = readings.lightingPower;
  doc["conveyorPowerW"] = readings.conveyorPower;
  doc["powerW"] = readings.lightingPower + readings.conveyorPower;
  sendMonitorResponse(doc);
}

void handleMonitorProductCount() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["productCount"] = state.productCount;
  doc["productCountTotal"] = state.productCountTotal;
  doc["conveyorOn"] = state.conveyorOn;
  sendMonitorResponse(doc);
}

void sendLaserMonitor(uint8_t zone, uint8_t pin) {
  StaticJsonDocument<512> doc;
  bool intrusion = laserBlocked(pin);

  doc["success"] = true;
  doc["zone"] = zone;
  doc["status"] = intrusion ? "INTRUSION" : "SAFE";
  doc["intrusion"] = intrusion;
  sendMonitorResponse(doc);
}

void handleMonitorLaser1() {
  if (!authorizedRequest()) return;
  sendLaserMonitor(1, Pins::LASER_SW1);
}

void handleMonitorLaser2() {
  if (!authorizedRequest()) return;
  sendLaserMonitor(2, Pins::LASER_SW2);
}

void handleMonitorLaser3() {
  if (!authorizedRequest()) return;
  sendLaserMonitor(3, Pins::LASER_SW3);
}

void handleMonitorLaser4() {
  if (!authorizedRequest()) return;
  sendLaserMonitor(4, Pins::LASER_SW4);
}

void handleMonitorPerson1() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["angle"] = person1Servo.position();
  doc["targetAngle"] = person1Servo.targetPosition();
  doc["homeAngle"] = 180;
  doc["moving"] = person1Servo.isMoving();
  sendMonitorResponse(doc);
}

void handleMonitorPerson2() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["angle"] = person2Servo.position();
  doc["targetAngle"] = person2Servo.targetPosition();
  doc["homeAngle"] = 0;
  doc["moving"] = person2Servo.isMoving();
  sendMonitorResponse(doc);
}

void handleMonitorCamera() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["recording"] = cameraRecording;
  doc["zone"] = cameraZone;
  doc["recordDurationSeconds"] = 15;
  sendMonitorResponse(doc);
}

// ============================================================================
// BACKEND COMMAND HANDLER
// Examples: {"command":"conveyor","value":true}, {"command":"person1","value":90},
// {"command":"homePerson1"}, {"command":"resetProductCount"}, {"command":"reboot"}
// ============================================================================
void handleDeviceCommand() {
  if (!authorizedRequest()) return;

  if (!server.hasArg("plain")) {
    server.send(400, "application/json",
                "{\"success\":false,\"error\":\"Missing JSON body\"}");
    return;
  }

  StaticJsonDocument<1024> req;
  DeserializationError err = deserializeJson(req, server.arg("plain"));

  if (err) {
    server.send(400, "application/json",
                "{\"success\":false,\"error\":\"Invalid JSON\"}");
    return;
  }

  const char *command = req["command"] | "";

  if (strlen(command) == 0) {
    server.send(400, "application/json",
                "{\"success\":false,\"error\":\"Missing command\"}");
    return;
  }

  bool value = req["value"] | false;

  if (strcmp(command, "conveyor") == 0) {
    setConveyor(value);
  }
  else if (strcmp(command, "mist1") == 0) {
    setMist1(value);
  }
  else if (strcmp(command, "mist2") == 0) {
    setMist2(value);
  }
  else if (strcmp(command, "heater") == 0) {
    // A remote ON request cannot bypass the local safety checks.
    const char* reason = value ? heaterBlockReason() : nullptr;
    if (reason) {
      sendHeaterBlocked(reason);
      return;
    }
    setHeater(value);
  }
  else if (strcmp(command, "lighting") == 0) {
    setLighting(value);
  }
  else if (strcmp(command, "person1") == 0 ||
           strcmp(command, "setPerson1") == 0 ||
           strcmp(command, "setServo1") == 0) {
    int angle = req["value"] | 180;
    setPerson1Angle(constrain(angle, 0, 180));
  }
  else if (strcmp(command, "person2") == 0 ||
           strcmp(command, "setPerson2") == 0 ||
           strcmp(command, "setServo2") == 0) {
    int angle = req["value"] | 0;
    setPerson2Angle(constrain(angle, 0, 150));
  }
  else if (strcmp(command, "homePerson1") == 0 ||
           strcmp(command, "homeServo1") == 0) {
    homePerson1();
  }
  else if (strcmp(command, "homePerson2") == 0 ||
           strcmp(command, "homeServo2") == 0) {
    homePerson2();
  }
  else if (strcmp(command, "resetProductCount") == 0) {
    resetProductCount();
  }
  else if (strcmp(command, "reboot") == 0) {
    savePersistedProductCount(true);

    StaticJsonDocument<512> response;
    response["success"] = true;
    response["command"] = command;
    response["rebooting"] = true;

    String out;
    serializeJson(response, out);
    server.send(200, "application/json", out);

    delay(500);
    ESP.restart();
    return;
  }
  else {
    server.send(404, "application/json",
                "{\"success\":false,\"error\":\"Unknown command\"}");
    return;
  }

  StaticJsonDocument<4096> response;
  response["success"] = true;
  response["command"] = command;

  JsonObject stateObj = response["state"].to<JsonObject>();
  buildStateJson(stateObj);

  String out;
  serializeJson(response, out);
  server.send(200, "application/json", out);
}

// ============================================================================
// OTA STATUS / UPDATE
// Example:
// curl -X POST -H "X-Device-Key: <key>" \
//   -F "firmware=@factory_model_http_ota.bin" \
//   http://10.10.12.75/api/ota/update
// ============================================================================
// Returns true if the request carries the right key; otherwise records why.
bool otaCheckAuthorization() {
  if (!server.hasHeader("X-Device-Key")) {
    otaUpdateError = "Missing X-Device-Key";
    return false;
  }

  if (server.header("X-Device-Key") != String(BackendConfig::DEVICE_KEY)) {
    otaUpdateError = "Invalid device key";
    return false;
  }

  return true;
}

void handleOtaStatus() {
  if (!authorizedRequest()) return;

  StaticJsonDocument<768> doc;
  doc["success"] = true;
  doc["device"] = BackendConfig::DEVICE_ID;
  doc["firmwareVersion"] = FIRMWARE_VERSION;
  doc["freeSketchSpace"] = ESP.getFreeSketchSpace();
  doc["sketchSize"] = ESP.getSketchSize();
  doc["freeHeap"] = ESP.getFreeHeap();
  doc["otaUpdateRunning"] = otaUpdateStarted;
  doc["otaBytesWritten"] = otaBytesWritten;

  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);
}

void handleOtaUpload() {
  HTTPUpload& upload = server.upload();

  if (upload.status == UPLOAD_FILE_START) {
    otaBytesWritten = 0;
    otaUpdateStarted = false;
    otaUpdateFailed = false;
    otaUpdateError = "";
    otaUploadAuthorized = otaCheckAuthorization();

    if (!otaUploadAuthorized) {
      otaUpdateFailed = true;
      Serial.println(F("[OTA] Upload rejected: authentication failed."));
      return;
    }

    // Reject images that cannot fit before touching flash.
    size_t contentLength = server.header("Content-Length").toInt();
    if (contentLength > ESP.getFreeSketchSpace()) {
      otaUpdateFailed = true;
      otaUpdateError = "Firmware too large for OTA partition";
      Serial.println(F("[OTA] Upload rejected: image too large."));
      return;
    }

    Serial.print(F("[OTA] Starting firmware upload: "));
    Serial.println(upload.filename);

    if (!Update.begin(UPDATE_SIZE_UNKNOWN)) {
      otaUpdateFailed = true;
      otaUpdateError = Update.errorString();
      Serial.print(F("[OTA] Update.begin failed: "));
      Serial.println(otaUpdateError);
      return;
    }

    otaUpdateStarted = true;
    return;
  }

  // Client dropped the connection mid-upload: close the update cleanly.
  if (upload.status == UPLOAD_FILE_ABORTED) {
    if (otaUpdateStarted) Update.abort();
    otaUpdateStarted = false;
    otaUpdateFailed = true;
    otaUpdateError = "Upload aborted";
    Serial.println(F("[OTA] Upload aborted by client."));
    return;
  }

  if (!otaUploadAuthorized || otaUpdateFailed) return;

  if (upload.status == UPLOAD_FILE_WRITE) {
    size_t written = Update.write(upload.buf, upload.currentSize);
    if (written != upload.currentSize) {
      otaUpdateFailed = true;
      otaUpdateStarted = false;
      otaUpdateError = Update.errorString();
      Update.abort();
      Serial.print(F("[OTA] Write failed: "));
      Serial.println(otaUpdateError);
      return;
    }

    otaBytesWritten += written;
    return;
  }

  if (upload.status == UPLOAD_FILE_END) {
    if (!Update.end(true)) {
      otaUpdateFailed = true;
      otaUpdateError = Update.errorString();
      otaUpdateStarted = false;
      Serial.print(F("[OTA] Update.end failed: "));
      Serial.println(otaUpdateError);
      return;
    }

    Serial.print(F("[OTA] Firmware written successfully: "));
    Serial.print(otaBytesWritten);
    Serial.println(F(" bytes."));
  }
}

void handleOtaUpdateComplete() {
  // Consume the flag so a later request can never reuse a stale authorization.
  bool authorized = otaUploadAuthorized;
  otaUploadAuthorized = false;

  if (!authorized) {
    // Sends 401/403 for a bad key; a valid key with no file gets 400.
    if (authorizedRequest()) {
      server.send(400, "application/json",
                  "{\"success\":false,\"error\":\"No firmware file received\"}");
    }
    return;
  }

  if (otaUpdateFailed || !otaUpdateStarted) {
    String error = otaUpdateError.length() ? otaUpdateError : "OTA update failed";
    StaticJsonDocument<512> doc;
    doc["success"] = false;
    doc["error"] = error;
    doc["bytesWritten"] = otaBytesWritten;

    String out;
    serializeJson(doc, out);
    server.send(500, "application/json", out);
    return;
  }

  StaticJsonDocument<512> doc;
  doc["success"] = true;
  doc["rebooting"] = true;
  doc["bytesWritten"] = otaBytesWritten;
  doc["firmwareVersion"] = FIRMWARE_VERSION;
  doc["message"] = "Firmware update successful. ESP32 will reboot now.";

  String out;
  serializeJson(doc, out);
  server.send(200, "application/json", out);

  savePersistedProductCount(true);  // do not lose pending counts across the OTA reboot

  delay(500);
  ESP.restart();
}

// ============================================================================
// REBOOT
// ============================================================================
void handleReboot() {
  if (!authorizedRequest()) return;

  savePersistedProductCount(true);

  server.send(200, "application/json",
              "{\"success\":true,\"rebooting\":true}");

  delay(500);
  ESP.restart();
}

// ============================================================================
// BACKEND TELEMETRY + CAMERA TRIGGER (non-blocking)
// The main loop only builds the payload / queues the zone number. A FreeRTOS
// task on core 0 does the blocking HTTP calls, so a slow or offline Mini PC can
// no longer stall the laser fence, servos or the local web server.
// ============================================================================
void queueTelemetry() {
  StaticJsonDocument<4096> doc;
  doc["deviceId"] = BackendConfig::DEVICE_ID;
  doc["timestamp"] = millis();

  JsonObject stateObj = doc["state"].to<JsonObject>();
  buildStateJson(stateObj);

  String payload;
  serializeJson(doc, payload);

  if (xSemaphoreTake(telemetryMutex, pdMS_TO_TICKS(10)) == pdTRUE) {
    pendingTelemetry = payload;  // newest snapshot wins
    telemetryPending = true;
    xSemaphoreGive(telemetryMutex);
  }
}

bool postTelemetry(const String &payload) {
  HTTPClient http;
  http.setConnectTimeout(BackendConfig::HTTP_TIMEOUT_MS);
  http.setTimeout(BackendConfig::HTTP_TIMEOUT_MS);

  String url = String(BackendConfig::SERVER_URL) + String(BackendConfig::TELEMETRY_PATH);

  if (!http.begin(url)) {
    Serial.println(F("[HTTP] Telemetry begin failed."));
    return false;
  }

  http.addHeader("Content-Type", "application/json");
  http.addHeader("X-Device-Key", BackendConfig::DEVICE_KEY);

  int code = http.POST(payload);
  bool ok = (code > 0 && code < 400);

  if (code < 0) {
    Serial.print(F("[HTTP] Telemetry failed: "));
    Serial.println(http.errorToString(code));
  }
  else if (code >= 400) {
    Serial.print(F("[HTTP] Telemetry server error: "));
    Serial.println(code);
    Serial.println(http.getString());
  }

  http.end();
  return ok;
}

// Queue a camera trigger for the network task (called from the laser fence).
void queueCameraTrigger(uint8_t zone) {
  if (!CameraConfig::ENABLED) {
    Serial.println(F("[CAMERA] Disabled."));
    return;
  }

  if (zone < 1 || zone > 4) {
    Serial.println(F("[CAMERA] Invalid zone."));
    return;
  }

  if (xQueueSend(cameraQueue, &zone, 0) != pdTRUE) {
    Serial.println(F("[CAMERA] Queue full, trigger dropped."));
  }
}

// Runs in the network task. ESP32 sends the physical zone number; the Mini PC
// camera service applies its own zone_remap (including 1 -> 4).
void sendCameraTrigger(uint8_t zone) {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println(F("[CAMERA] Wi-Fi unavailable."));
    return;
  }

  HTTPClient http;
  http.setConnectTimeout(CameraConfig::HTTP_TIMEOUT_MS);
  http.setTimeout(CameraConfig::HTTP_TIMEOUT_MS);

  if (!http.begin(CameraConfig::SERVER_URL)) {
    Serial.println(F("[CAMERA] HTTP begin failed."));
    return;
  }

  http.addHeader("Content-Type", "application/json");

  String payload =
      "{\"switchyardIntrusion\":true,\"switchyardZone\":" + String(zone) + "}";

  Serial.print(F("[CAMERA] POST: "));
  Serial.println(payload);

  int httpCode = http.POST(payload);

  Serial.print(F("[CAMERA] HTTP response: "));
  Serial.println(httpCode);

  if (httpCode > 0) {
    String response = http.getString();
    Serial.print(F("[CAMERA] Response: "));
    Serial.println(response);

    if (httpCode == 200) {
      cameraRecording = true;
      cameraZone = zone;
      cameraRecordingUntil = millis() + CAMERA_RECORDING_STATE_MS;
    }
    else if (httpCode == 409) {
      Serial.println(F("[CAMERA] Already recording."));
    }
  }
  else {
    Serial.print(F("[CAMERA] Request failed: "));
    Serial.println(http.errorToString(httpCode));
  }

  http.end();
}

void networkTask(void *param) {
  unsigned long nextTelemetryAt = 0;
  uint8_t failures = 0;
  uint8_t zone = 0;

  for (;;) {
    // Camera triggers have priority; the queue wait doubles as the idle sleep.
    if (xQueueReceive(cameraQueue, &zone, pdMS_TO_TICKS(50)) == pdTRUE) {
      sendCameraTrigger(zone);
      continue;
    }

    // Stay off the network while flashing, while backing off, or while offline.
    if (otaUpdateStarted) continue;
    if ((long)(millis() - nextTelemetryAt) < 0) continue;
    if (WiFi.status() != WL_CONNECTED) continue;

    String payload;
    bool havePayload = false;

    if (xSemaphoreTake(telemetryMutex, pdMS_TO_TICKS(10)) == pdTRUE) {
      if (telemetryPending) {
        payload = pendingTelemetry;
        telemetryPending = false;
        havePayload = true;
      }
      xSemaphoreGive(telemetryMutex);
    }

    if (!havePayload) continue;

    if (postTelemetry(payload)) {
      failures = 0;
    } else {
      if (failures < 5) failures++;
      unsigned long backoff = NET_BACKOFF_STEP_MS * failures;
      if (backoff > NET_BACKOFF_MAX_MS) backoff = NET_BACKOFF_MAX_MS;
      nextTelemetryAt = millis() + backoff;
    }
  }
}

// Confirms the running image once it has stayed up with Wi-Fi for 30 s.
// Only has an effect if the bootloader was built with app rollback enabled.
void markFirmwareValid() {
  if (firmwareMarkedValid) return;
  if (millis() < 30000 || WiFi.status() != WL_CONNECTED) return;

  esp_ota_mark_app_valid_cancel_rollback();
  firmwareMarkedValid = true;
}

// ============================================================================
// PRODUCT COUNT
// ============================================================================
void monitorProductCount() {
  static unsigned long lastDebounceTime = 0;
  static bool lastRawState = false;
  static bool steadyState = false;

  bool rawState = (digitalRead(Pins::E18_D80NK) == LOW);

  if (rawState != lastRawState) {
    lastDebounceTime = millis();
  }

  if (millis() - lastDebounceTime > 25) {
    if (rawState != steadyState) {
      steadyState = rawState;

      if (steadyState && state.conveyorOn) {
        state.productCount++;
        state.productCountTotal++;

        pendingCountWrites++;
        savePersistedProductCount();

        Serial.print(F("[+] Product detected! Run: "));
        Serial.print(state.productCount);
        Serial.print(F(" | Lifetime Total: "));
        Serial.println(state.productCountTotal);
      }
    }
  }

  lastRawState = rawState;
}

void loadPersistedProductCount() {
  nvs.begin(NvsConfig::NAMESPACE, false);

  state.productCountTotal = nvs.getULong(NvsConfig::KEY_TOTAL_COUNT, 0);
  lastCountSaveAt = millis();

  Serial.print(F("[NVS] Loaded lifetime product count: "));
  Serial.println(state.productCountTotal);
}

void savePersistedProductCount(bool force) {
  bool countThreshold = pendingCountWrites >= COUNT_SAVE_EVERY_N;
  bool intervalHit = (millis() - lastCountSaveAt >= COUNT_SAVE_INTERVAL_MS) &&
                     pendingCountWrites > 0;

  if (!force && !countThreshold && !intervalHit) return;

  nvs.putULong(NvsConfig::KEY_TOTAL_COUNT, state.productCountTotal);

  pendingCountWrites = 0;
  lastCountSaveAt = millis();
}

void resetProductCount() {
  state.productCount = 0;
  state.productCountTotal = 0;
  pendingCountWrites = 0;

  savePersistedProductCount(true);

  Serial.println(F("[NVS] Product count reset."));
}

// ============================================================================
// SENSOR SAMPLING
// ============================================================================
void sampleSensors() {
  if (sensorOK.lightingIna) {
    readings.lightingVoltage = lightingIna.getBusVoltage_V();
    readings.lightingCurrent = lightingIna.getCurrent_mA();
    readings.lightingPower = readings.lightingVoltage * (readings.lightingCurrent / 1000.0f);
  }

  if (sensorOK.conveyorIna) {
    readings.conveyorVoltage = conveyorIna.getBusVoltage_V();
    readings.conveyorCurrent = conveyorIna.getCurrent_mA();
    readings.conveyorPower = readings.conveyorVoltage * (readings.conveyorCurrent / 1000.0f);
  }

  if (sensorOK.coolingTower1) {
    float t = coolingTower1Sht.readTemperature();
    float h = coolingTower1Sht.readHumidity();
    if (!isnan(t)) readings.coolingTower1Temp = t;
    if (!isnan(h)) readings.coolingTower1Hum = h;
  }

  if (sensorOK.coolingTower2) {
    float t = coolingTower2Sht.readTemperature();
    float h = coolingTower2Sht.readHumidity();
    if (!isnan(t)) readings.coolingTower2Temp = t;
    if (!isnan(h)) readings.coolingTower2Hum = h;
  }

  if (sensorOK.switchyardTemp) {
    float t = switchyardTempSensor.getTempCByIndex(0);
    if (t != DEVICE_DISCONNECTED_C) {
      readings.switchyardTemp = t;
      lastSwitchyardTempAt = millis();
      switchyardTempEverValid = true;
    }
    switchyardTempSensor.requestTemperatures();
  }
}

// ============================================================================
// LASER FENCE
// ============================================================================
bool laserBlocked(uint8_t pin) {
  return digitalRead(pin) == HIGH;
}

void checkLaserFence() {
  bool currentLaserState[4];

  currentLaserState[0] = laserBlocked(Pins::LASER_SW1);
  currentLaserState[1] = laserBlocked(Pins::LASER_SW2);
  currentLaserState[2] = laserBlocked(Pins::LASER_SW3);
  currentLaserState[3] = laserBlocked(Pins::LASER_SW4);

  if (!laserStateInitialized) {
    for (uint8_t i = 0; i < 4; i++) {
      previousLaserState[i] = currentLaserState[i];
    }
    laserStateInitialized = true;
  }

  for (uint8_t i = 0; i < 4; i++) {
    bool intrusionStarted = (!previousLaserState[i] && currentLaserState[i]);

    if (intrusionStarted) {
      uint8_t zone = i + 1;

      Serial.println(F("\n******** INTRUSION DETECTED ********"));
      Serial.print(F("Switchyard Zone: "));
      Serial.println(zone);

      queueCameraTrigger(zone);
    }

    previousLaserState[i] = currentLaserState[i];
  }

  state.intrusion = currentLaserState[0] || currentLaserState[1] ||
                    currentLaserState[2] || currentLaserState[3];

  if (state.intrusion) {
    static unsigned long lastToneToggle = 0;
    static bool highTone = false;

    if (millis() - lastToneToggle >= 200) {
      lastToneToggle = millis();
      highTone = !highTone;
      ledcWriteTone(Pins::BUZZER, highTone ? 2500 : 1500);
    }
  }
  else {
    ledcWriteTone(Pins::BUZZER, 0);
  }
}

// ============================================================================
// ACTUATORS
// ============================================================================
void setConveyor(bool on) {
  state.conveyorOn = on;

  digitalWrite(Pins::MOTOR_ENA, on ? HIGH : LOW);
  digitalWrite(Pins::GREEN_LED, on ? HIGH : LOW);
  digitalWrite(Pins::RED_LED, on ? LOW : HIGH);

  Serial.println(on ? F("Conveyor Status : Running")
                    : F("Conveyor Status : Stopped"));
}

void setMist1(bool on) {
  state.mist1On = on;
  digitalWrite(Pins::MIST1, on ? HIGH : LOW);
}

void setMist2(bool on) {
  state.mist2On = on;
  digitalWrite(Pins::MIST2, on ? HIGH : LOW);
}

// True only while the DS18B20 is present and has produced a recent valid reading.
bool heaterTempTrusted() {
  return sensorOK.switchyardTemp && switchyardTempEverValid &&
         (millis() - lastSwitchyardTempAt < TEMP_STALE_MS);
}

// Returns why the heater must not run, or nullptr if it is allowed.
const char* heaterBlockReason() {
  if (!heaterTempTrusted())
    return "Heater blocked: switchyard temperature sensor missing or stale";
  if (readings.switchyardTemp >= HEATER_CUTOFF_TEMP_C)
    return "Heater blocked: temperature is at or above 33 C";
  return nullptr;
}

void sendHeaterBlocked(const char* reason) {
  setHeater(false);

  StaticJsonDocument<384> response;
  response["success"] = false;
  response["command"] = "heater";
  response["value"] = false;
  response["error"] = reason;

  String out;
  serializeJson(response, out);
  server.send(409, "application/json", out);
}

void setHeater(bool on) {
  // Local safety always wins.
  if (on) {
    const char* reason = heaterBlockReason();
    if (reason) {
      on = false;
      Serial.print(F("[SAFETY] Heater ON request rejected. "));
      Serial.println(reason);
    }
  }

  state.heaterOn = on;

  // Relay is active LOW.
  digitalWrite(Pins::HEATER_RELAY, on ? LOW : HIGH);
}

void setLighting(bool on) {
  state.lightingOn = on;
  digitalWrite(Pins::LIGHTING_MOSFET, on ? HIGH : LOW);
}

// ============================================================================
// SERVOS
// ============================================================================
void setPerson1Angle(int angle) {
  person1Servo.moveTo(constrain(angle, 0, 180));
}

void setPerson2Angle(int angle) {
  person2Servo.moveTo(constrain(angle, 0, 150));
}

void togglePerson1() {
  setPerson1Angle(person1Servo.position() == 0 ? 180 : 0);
}

void togglePerson2() {
  setPerson2Angle(person2Servo.position() == 0 ? 150 : 0);
}

void homePerson1() {
  person1Servo.moveTo(180);
  Serial.println(F("Person 1 Home = 180"));
}

void homePerson2() {
  person2Servo.moveTo(0);
  Serial.println(F("Person 2 Home = 0"));
}

// ============================================================================
// SERIAL LOGGING
// ============================================================================
void logSensorData() {
  Serial.println(F("--------------------------------------------------"));

  Serial.print(F("Conveyor Status     : "));
  Serial.println(state.conveyorOn ? F("Conveyor running") : F("Conveyor stop"));

  Serial.print(F("Product Count       : "));
  Serial.println(state.productCount);

  Serial.print(F("Switchyard Temp     : "));
  Serial.print(readings.switchyardTemp, 2);
  Serial.println(F(" C"));

  Serial.print(F("Laser Switchyard 1  : "));
  Serial.println(laserBlocked(Pins::LASER_SW1) ? F("Intrusion") : F("Safe"));

  Serial.print(F("Laser Switchyard 2  : "));
  Serial.println(laserBlocked(Pins::LASER_SW2) ? F("Intrusion") : F("Safe"));

  Serial.print(F("Laser Switchyard 3  : "));
  Serial.println(laserBlocked(Pins::LASER_SW3) ? F("Intrusion") : F("Safe"));

  Serial.print(F("Laser Switchyard 4  : "));
  Serial.println(laserBlocked(Pins::LASER_SW4) ? F("Intrusion") : F("Safe"));

  Serial.print(F("Lighting Power      : "));
  Serial.print(readings.lightingPower, 3);
  Serial.println(F(" W"));

  Serial.print(F("Conveyor Power      : "));
  Serial.print(readings.conveyorPower, 3);
  Serial.println(F(" W"));

  Serial.print(F("Cooling Tower 1     : "));
  Serial.print(readings.coolingTower1Temp, 2);
  Serial.print(F(" C, "));
  Serial.print(readings.coolingTower1Hum, 2);
  Serial.println(F(" %RH"));

  Serial.print(F("Cooling Tower 2     : "));
  Serial.print(readings.coolingTower2Temp, 2);
  Serial.print(F(" C, "));
  Serial.print(readings.coolingTower2Hum, 2);
  Serial.println(F(" %RH"));

  Serial.print(F("WiFi Status         : "));
  Serial.print(WiFi.status());
  Serial.print(F(" | IP: "));
  Serial.print(WiFi.localIP());
  Serial.print(F(" | RSSI: "));
  Serial.print(WiFi.RSSI());
  Serial.print(F(" | Free Heap: "));
  Serial.println(ESP.getFreeHeap());

  Serial.println(F("--------------------------------------------------"));
}

// ============================================================================
// SERIAL COMMANDS
// m=conveyor 1=mist1 2=mist2 h=heater l=lighting p=person1 o=person2
// t=sample+send telemetry n=network diagnostics
// ============================================================================
void handleSerialCommand(char command) {
  switch (command) {
    case 'm': setConveyor(!state.conveyorOn); break;
    case '1': setMist1(!state.mist1On); break;
    case '2': setMist2(!state.mist2On); break;
    case 'h': setHeater(!state.heaterOn); break;
    case 'l': setLighting(!state.lightingOn); break;
    case 'p': togglePerson1(); break;
    case 'o': togglePerson2(); break;

    case 't':
      sampleSensors();
      logSensorData();
      queueTelemetry();
      break;

    case 'n':
      Serial.printf("[DIAG] WiFi: %d | IP: %s | RSSI: %d | Heap: %u\n",
                    WiFi.status(),
                    WiFi.localIP().toString().c_str(),
                    WiFi.RSSI(),
                    ESP.getFreeHeap());
      break;

    default:
      break;
  }
}
