/*
 * Ultimate Dual Native CAN Datalogger (ESP32-C6)
 * Features:
 * 1. Dual CAN Bus Logging (TWAI v2 API) to SD Card
 * 2. WiFi Telemetry to ThingsBoard (1Hz Sampling)
 * 3. Raw Data Telemetry (Hex String format)
 */

#include <Arduino.h>
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include "driver/twai.h"
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include <WiFi.h>
#include <PubSubClient.h> // 請安裝 PubSubClient 函式庫

// ================= 使用者設定區 (請修改這裡) =================

// 1. 無線網路設定
const char* kWifiSsid      = "William Huang"; 
const char* kWifiPass      = "Qwer2wer";

// 2. ThingsBoard 設定
const char* kTbServer      = "thingsboard.cloud"; // 官方雲端版
const int   kTbPort        = 1883;
// 從你的截圖中 VR8_Datalogger 裝置裡複製 Access Token 貼在下面
const char* kTbToken       = "NVREE119"; 

// ================= 硬體腳位定義 (ESP32-C6) =================

// SD Card (SPI)
constexpr int kSpiSck  = 19;
constexpr int kSpiMiso = 20;
constexpr int kSpiMosi = 21;
constexpr int kSdCs    = 4;

// CAN Bus Pins
constexpr gpio_num_t kCan0Tx = GPIO_NUM_2; 
constexpr gpio_num_t kCan0Rx = GPIO_NUM_3;
constexpr gpio_num_t kCan1Tx = GPIO_NUM_10; 
constexpr gpio_num_t kCan1Rx = GPIO_NUM_11;

// ================= 系統常數 =================
constexpr size_t kMaxQueueSize = 500;
constexpr uint32_t kSpiFreq = 15000000; // 15MHz
constexpr uint32_t kFileBufferSize = 32768; // 32KB Buffer
constexpr uint32_t kFlushIntervalMs = 500;

// ================= 資料結構 =================

// 接收用的暫存結構
typedef struct {
  uint32_t id;
  uint8_t length;
  uint8_t data[8];
  uint32_t timestamp_ms;
  bool isExtended;
  uint8_t source; // 0 = CAN0, 1 = CAN1
} CanMessage_t;

// 儲存與遙測用的結構 (Packed)
#pragma pack(push, 1)
struct LogEntry {
  uint32_t timestamp;
  uint32_t id;        
  uint8_t len;
  uint8_t data[8];
};
#pragma pack(pop)

// ================= 全域物件 =================
twai_handle_t hTwai0;
twai_handle_t hTwai1;

QueueHandle_t canQueue;
SemaphoreHandle_t telemetryMutex; // 保護共享數據
char currentFilename[64];
uint32_t messagesLogged = 0;
File logFile;

// 遙測共享變數 (存放最新的一筆 CAN 資料)
LogEntry globalLatestEntry;
bool hasNewTelemetryData = false;

// 網路物件
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

// ================= 函式宣告 =================
bool initSDCard();
bool initDualTwai();
void createNewLogFile();
void can0ReceiverTask(void *pvParameters);
void can1ReceiverTask(void *pvParameters);
void fileWriterTask(void *pvParameters);
void telemetryTask(void *pvParameters); 
void connectToCloud();
void safeDelay(uint32_t ms);

// ================= Main Setup =================
void setup() {
  Serial.begin(115200);
  while (!Serial) safeDelay(10);
  Serial.println("\n=== VR8 Datalogger Starting (ESP32-C6) ===");

  // 1. 初始化 Mutex
  telemetryMutex = xSemaphoreCreateMutex();

  // 2. 初始化 WiFi (在這裡等待連線以便觀察 Serial)
  Serial.print("[WiFi] Connecting to "); 
  Serial.print(kWifiSsid);
  WiFi.begin(kWifiSsid, kWifiPass);
  
  // 嘗試連線 10 秒，如果連不上就先跳過，避免卡死 SD 卡功能
  int retry = 0;
  while (WiFi.status() != WL_CONNECTED && retry < 20) {
    delay(500);
    Serial.print(".");
    retry++;
  }
  
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n[WiFi] Connected! IP Address: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\n[WiFi] Connection failed (or timeout). Will retry in background.");
  }

  // 3. 設定 MQTT
  mqttClient.setServer(kTbServer, kTbPort);
  // 加大 MQTT Packet Size 以防 JSON 字串太長
  mqttClient.setBufferSize(512); 

  // 4. 初始化硬體 (SPI / SD)
  SPI.begin(kSpiSck, kSpiMiso, kSpiMosi);
  pinMode(kSdCs, OUTPUT); digitalWrite(kSdCs, HIGH);

  if (!initSDCard()) {
    Serial.println("[ERROR] SD Init Failed!");
  } else {
    Serial.println("[OK] SD Card Initialized!");
  }

  // 5. 初始化 CAN Bus
  if(initDualTwai()){
     Serial.println("[OK] Dual TWAI Started!");
  } else {
     Serial.println("[ERROR] TWAI Init Failed");
  }

  canQueue = xQueueCreate(kMaxQueueSize, sizeof(CanMessage_t));

  if (SD.cardType() != CARD_NONE) {
    createNewLogFile();
  }

  // 6. 啟動任務 (ESP32-C6 單核心)
  // SD 寫入 (優先級 2)
  xTaskCreatePinnedToCore(fileWriterTask, "Writer", 8192, NULL, 2, NULL, ARDUINO_RUNNING_CORE);
  // CAN 接收 (優先級 3 - 最高)
  xTaskCreatePinnedToCore(can0ReceiverTask, "Can0RX", 4096, NULL, 3, NULL, ARDUINO_RUNNING_CORE);
  xTaskCreatePinnedToCore(can1ReceiverTask, "Can1RX", 4096, NULL, 3, NULL, ARDUINO_RUNNING_CORE);
  // 遙測上傳 (優先級 1 - 最低，不影響記錄)
  xTaskCreatePinnedToCore(telemetryTask, "Telemetry", 6144, NULL, 1, NULL, ARDUINO_RUNNING_CORE);

  Serial.println("[System] All Tasks Running...");
}

void loop() {
  // 主迴圈留空即可
  vTaskDelay(pdMS_TO_TICKS(1000));
}

// ================= 任務實作 =================

// 檔案寫入任務 + 更新遙測數據
void fileWriterTask(void *pvParameters) {
  uint8_t *buf = (uint8_t *)malloc(kFileBufferSize);
  if (!buf) {
    Serial.println("[FATAL] Malloc failed for file buffer");
    vTaskDelete(NULL);
  }
  
  size_t pos = 0;
  uint32_t lastFlush = millis();
  logFile = SD.open(currentFilename, FILE_APPEND);

  while(1) {
    CanMessage_t msg;
    if(xQueueReceive(canQueue, &msg, pdMS_TO_TICKS(10))) {
      
      // 建構 LogEntry
      LogEntry e;
      e.timestamp = msg.timestamp_ms;
      e.id = msg.id;
      e.len = msg.length;
      memcpy(e.data, msg.data, 8);
      
      // 處理 ID 格式 (加上 Source 和 Ext 標記)
      if(msg.isExtended) e.id |= 0x80000000;
      if(msg.source == 1) e.id |= 0x40000000; 
      
      size_t eSize = sizeof(LogEntry);

      // 寫入 SD 卡 Buffer
      if(pos + eSize >= kFileBufferSize) {
         if (logFile) {
            logFile.write(buf, pos);
            logFile.flush();
            messagesLogged += (pos/sizeof(LogEntry));
         }
         pos = 0;
         lastFlush = millis();
      }
      memcpy(buf + pos, &e, eSize);
      pos += eSize;

      // --- 更新遙測數據 ---
      // 只要有新數據，就更新到全域變數，供遙測任務抓取
      if (xSemaphoreTake(telemetryMutex, 0) == pdTRUE) {
        memcpy(&globalLatestEntry, &e, sizeof(LogEntry));
        hasNewTelemetryData = true;
        xSemaphoreGive(telemetryMutex);
      }
    }
    
    // 定時 Flush
    if(millis() - lastFlush > kFlushIntervalMs) {
      if(pos > 0 && logFile) {
        logFile.write(buf, pos);
        logFile.flush();
        messagesLogged += (pos/sizeof(LogEntry));
        pos = 0;
      }
      lastFlush = millis();
    }
  }
}

// 遙測任務：負責連網與傳送 MQTT
void telemetryTask(void *pvParameters) {
  char jsonBuffer[256];
  char dataHexStr[17]; // 8 bytes * 2 chars + null
  
  while(1) {
    // 1. 檢查 WiFi 狀態
    if (WiFi.status() == WL_CONNECTED) {
      
      // 2. 檢查 MQTT 連線
      if (!mqttClient.connected()) {
        connectToCloud();
      }
      
      if (mqttClient.connected()) {
        mqttClient.loop(); // 維持心跳

        // 3. 讀取最新數據快照
        LogEntry snap;
        bool doSend = false;
        
        if (xSemaphoreTake(telemetryMutex, pdMS_TO_TICKS(10)) == pdTRUE) {
           if (hasNewTelemetryData) {
             memcpy(&snap, &globalLatestEntry, sizeof(LogEntry));
             // hasNewTelemetryData = false; // 可選：如果不希望重複發送相同數據，可以設為 false
             doSend = true;
           }
           xSemaphoreGive(telemetryMutex);
        }

        if (doSend) {
          // 4. 轉換 Data 為 Hex 字串 (例如 "0A1B2C...")
          memset(dataHexStr, 0, sizeof(dataHexStr));
          for(int i=0; i<snap.len; i++) {
            sprintf(&dataHexStr[i*2], "%02X", snap.data[i]);
          }

          // 5. 處理 ID (去除標記，只顯示純 CAN ID)
          uint32_t rawId = snap.id & 0x1FFFFFFF;
          uint8_t src = (snap.id & 0x40000000) ? 1 : 0;

          // 6. 打包 JSON
          // 格式: {"ts":12345, "can_id":256, "len":8, "data":"AABBCC...", "src":0, "log_count":100}
          snprintf(jsonBuffer, sizeof(jsonBuffer), 
            "{\"ts\":%lu, \"can_id\":%lu, \"len\":%u, \"data_hex\":\"%s\", \"src\":%u, \"log_count\":%lu}", 
            snap.timestamp, 
            rawId, 
            snap.len, 
            dataHexStr, 
            src,
            messagesLogged
          );

          // 7. 發送並顯示在 Serial Monitor
          if (mqttClient.publish("v1/devices/me/telemetry", jsonBuffer)) {
             Serial.printf("[MQTT] Sent ID: %lu | Data: %s\n", rawId, dataHexStr);
          } else {
             Serial.println("[MQTT] Publish Failed");
          }
        }
      }
    } else {
      // WiFi 斷線時的提示 (降低顯示頻率)
      static uint32_t lastWifiCheck = 0;
      if (millis() - lastWifiCheck > 5000) {
        Serial.println("[WiFi] Lost connection. Reconnecting...");
        WiFi.reconnect();
        lastWifiCheck = millis();
      }
    }

    // 每秒執行一次 (1Hz)
    vTaskDelay(pdMS_TO_TICKS(1000));
  }
}

// MQTT 連線邏輯
void connectToCloud() {
  Serial.print("[MQTT] Connecting to ThingsBoard...");
  
  // 這裡產生隨機 Client ID
  String clientId = "ESP32-C6-" + String(random(0xffff), HEX);
  
  // 使用 Token 進行認證 (Token 填在 User 欄位，Password 留空)
  if (mqttClient.connect(clientId.c_str(), kTbToken, NULL)) {
    Serial.println(" Connected!");
  } else {
    Serial.print(" Failed, rc=");
    Serial.print(mqttClient.state());
    Serial.println(" (Retrying in 5 seconds)");
    vTaskDelay(pdMS_TO_TICKS(5000));
  }
}

// ================= 其餘初始化函式 =================

bool initSDCard() {
  if (!SD.begin(kSdCs, SPI, kSpiFreq)) return false;
  if (!SD.exists("/CAN_LOGS")) SD.mkdir("/CAN_LOGS");
  return true;
}

bool initDualTwai() {
  // CAN 0
  twai_general_config_t g_config0 = TWAI_GENERAL_CONFIG_DEFAULT(kCan0Tx, kCan0Rx, TWAI_MODE_NORMAL);
  g_config0.controller_id = 0;
  g_config0.rx_queue_len = 100;
  twai_timing_config_t t_config = TWAI_TIMING_CONFIG_500KBITS();
  twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();
  if (twai_driver_install_v2(&g_config0, &t_config, &f_config, &hTwai0) != ESP_OK) return false;
  if (twai_start_v2(hTwai0) != ESP_OK) return false;

  // CAN 1
  twai_general_config_t g_config1 = TWAI_GENERAL_CONFIG_DEFAULT(kCan1Tx, kCan1Rx, TWAI_MODE_NORMAL);
  g_config1.controller_id = 1;
  g_config1.rx_queue_len = 100;
  if (twai_driver_install_v2(&g_config1, &t_config, &f_config, &hTwai1) != ESP_OK) return false;
  if (twai_start_v2(hTwai1) != ESP_OK) return false;

  return true;
}

void createNewLogFile() {
  int fileIndex = 0;
  while (true) {
    snprintf(currentFilename, sizeof(currentFilename), "/CAN_LOGS/LOG_%03d.bin", fileIndex);
    if (!SD.exists(currentFilename)) break; 
    fileIndex++;
    if(fileIndex > 999) fileIndex = 0;
  }
  File f = SD.open(currentFilename, FILE_WRITE);
  if(f) {
    f.close();
    Serial.printf("Created new log file: %s\n", currentFilename);
  }
}

void can0ReceiverTask(void *pvParameters) {
  twai_message_t msg;
  CanMessage_t rx;
  while(1) {
    if(twai_receive_v2(hTwai0, &msg, pdMS_TO_TICKS(10)) == ESP_OK) {
      rx.timestamp_ms = millis();
      rx.source = 0;
      rx.id = msg.identifier;
      rx.isExtended = msg.extd;
      rx.length = msg.data_length_code;
      if(rx.isExtended) rx.id &= 0x1FFFFFFF; else rx.id &= 0x7FF;
      memcpy(rx.data, msg.data, rx.length);
      xQueueSend(canQueue, &rx, 0);
    }
  }
}

void can1ReceiverTask(void *pvParameters) {
  twai_message_t msg;
  CanMessage_t rx;
  while(1) {
    if(twai_receive_v2(hTwai1, &msg, pdMS_TO_TICKS(10)) == ESP_OK) {
      rx.timestamp_ms = millis();
      rx.source = 1;
      rx.id = msg.identifier;
      rx.isExtended = msg.extd;
      rx.length = msg.data_length_code;
      if(rx.isExtended) rx.id &= 0x1FFFFFFF; else rx.id &= 0x7FF;
      memcpy(rx.data, msg.data, rx.length);
      xQueueSend(canQueue, &rx, 0);
    }
  }
}

void safeDelay(uint32_t ms) { vTaskDelay(pdMS_TO_TICKS(ms)); }