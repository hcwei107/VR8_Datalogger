/*
 * Ultimate Dual Native CAN Datalogger (ESP32-C6 Exclusive)
 * Platform: Nano ESP32-C6 (RISC-V Single Core)
 * * Hardware:
 * - 2x Native TWAI Controllers (No MCP2515 needed)
 * - 1x SD Card (SPI Mode)
 *
 * Logic:
 * - Source 0: TWAI Controller 0
 * - Source 1: TWAI Controller 1
 * - Storage Logic: Unchanged (Queue -> File)
 */

#include <Arduino.h>
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include "driver/twai.h" // Native CAN Driver
#include <freertos/queue.h>

// ----------------- Pin Definitions (ESP32-C6) -----------------

// 1. SD Card (Shared SPI Bus)
constexpr int kSpiSck  = 19;
constexpr int kSpiMiso = 20;
constexpr int kSpiMosi = 21;
constexpr int kSdCs    = 4;

// 2. Dual CAN Pins (Requires 3.3V Transceivers like VP230)
// Channel 0
constexpr gpio_num_t kCan0Tx = GPIO_NUM_2; 
constexpr gpio_num_t kCan0Rx = GPIO_NUM_3;
// Channel 1
constexpr gpio_num_t kCan1Tx = GPIO_NUM_10; 
constexpr gpio_num_t kCan1Rx = GPIO_NUM_11;

// ----------------- System Constants -----------------
constexpr size_t kMaxQueueSize = 500;
constexpr uint32_t kSpiFreq = 15000000; 
constexpr uint32_t kFileBufferSize = 32768; 
constexpr uint32_t kFlushIntervalMs = 500;

// ----------------- Data Structures -----------------
// Matches original structure strictly
typedef struct {
  uint32_t id;
  uint8_t length;
  uint8_t data[8];
  uint32_t timestamp_ms;
  bool isExtended;
  uint8_t source; // 0 = CAN0, 1 = CAN1
} CanMessage_t;

#pragma pack(push, 1)
struct LogEntry {
  uint32_t timestamp;
  uint32_t id;        
  uint8_t len;
  uint8_t data[8];
};
#pragma pack(pop)

// ----------------- Global Objects -----------------
// TWAI Handles for v2 API
twai_handle_t hTwai0;
twai_handle_t hTwai1;

// OS Objects
QueueHandle_t canQueue;
char currentFilename[64];
uint32_t messagesLogged = 0;
File logFile;

// ----------------- Function Prototypes -----------------
bool initSDCard();
bool initDualTwai();
void createNewLogFile();
void can0ReceiverTask(void *pvParameters); // Replaces twaiReceiverTask
void can1ReceiverTask(void *pvParameters); // Replaces mcpReceiverTask
void fileWriterTask(void *pvParameters);
void safeDelay(uint32_t ms);

// ----------------- Main Setup -----------------
void setup() {
  Serial.begin(115200);
  while (!Serial) safeDelay(10);
  Serial.println("\n=== Dual Native CAN Datalogger (ESP32-C6) ===");

  // Initialize Common SPI Bus (Only for SD now)
  SPI.begin(kSpiSck, kSpiMiso, kSpiMosi);
  pinMode(kSdCs, OUTPUT); digitalWrite(kSdCs, HIGH);

  // 1. Initialize SD Card
  if (!initSDCard()) {
    Serial.println("[ERROR] SD Init Failed!");
  } else {
    Serial.println("[OK] SD Card Initialized!");
  }

  // 2. Initialize Dual TWAI
  if(initDualTwai()){
     Serial.println("[OK] Dual TWAI Controllers Started!");
  } else {
     Serial.println("[ERROR] TWAI Init Failed");
  }

  // 3. Create OS Objects
  canQueue = xQueueCreate(kMaxQueueSize, sizeof(CanMessage_t));

  // 4. Create File
  if (SD.cardType() != CARD_NONE) {
    createNewLogFile();
  }

  // 5. Start Tasks
  // Core 0 (ARDUINO_RUNNING_CORE) is used for all since C6 is single core
  xTaskCreatePinnedToCore(fileWriterTask, "Writer", 8192, NULL, 2, NULL, ARDUINO_RUNNING_CORE);
  xTaskCreatePinnedToCore(can0ReceiverTask, "Can0RX", 4096, NULL, 3, NULL, ARDUINO_RUNNING_CORE);
  xTaskCreatePinnedToCore(can1ReceiverTask, "Can1RX", 4096, NULL, 3, NULL, ARDUINO_RUNNING_CORE);

  Serial.println("[System] Running...");
}

void loop() {
  static uint32_t lastPrint = 0;
  if (millis() - lastPrint > 2000) {
    Serial.printf("Logged: %lu | Queue: %u | File: %s\n", 
      messagesLogged, uxQueueMessagesWaiting(canQueue), currentFilename);
    
    // Optional: Check status of drivers
    twai_status_info_t status;
    if(twai_get_status_info_v2(hTwai0, &status) == ESP_OK) {
        // Serial.printf("CAN0 Bus State: %d\n", status.state);
    }
    
    lastPrint = millis();
  }
  vTaskDelay(pdMS_TO_TICKS(100));
}

// ----------------- Initialization -----------------
bool initSDCard() {
  if (!SD.begin(kSdCs, SPI, kSpiFreq)) {
    return false;
  }
  if (!SD.exists("/CAN_LOGS")) {
     SD.mkdir("/CAN_LOGS");
  }
  return true;
}

bool initDualTwai() {
  // --- Configure CAN 0 ---
  twai_general_config_t g_config0 = TWAI_GENERAL_CONFIG_DEFAULT(kCan0Tx, kCan0Rx, TWAI_MODE_NORMAL);
  g_config0.controller_id = 0; // Explicitly Select Controller 0
  g_config0.rx_queue_len = 100;

  twai_timing_config_t t_config = TWAI_TIMING_CONFIG_500KBITS(); // Both 500K
  twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();

  // Install v2 (Handle based)
  if (twai_driver_install_v2(&g_config0, &t_config, &f_config, &hTwai0) != ESP_OK) {
    Serial.println("Failed to install CAN0");
    return false;
  }
  if (twai_start_v2(hTwai0) != ESP_OK) {
    Serial.println("Failed to start CAN0");
    return false;
  }

  // --- Configure CAN 1 ---
  twai_general_config_t g_config1 = TWAI_GENERAL_CONFIG_DEFAULT(kCan1Tx, kCan1Rx, TWAI_MODE_NORMAL);
  g_config1.controller_id = 1; // Explicitly Select Controller 1
  g_config1.rx_queue_len = 100;

  // Install v2
  if (twai_driver_install_v2(&g_config1, &t_config, &f_config, &hTwai1) != ESP_OK) {
    Serial.println("Failed to install CAN1");
    return false;
  }
  if (twai_start_v2(hTwai1) != ESP_OK) {
    Serial.println("Failed to start CAN1");
    return false;
  }

  return true;
}

void createNewLogFile() {
  int fileIndex = 0;
  while (true) {
    snprintf(currentFilename, sizeof(currentFilename), "/CAN_LOGS/LOG_%03d.bin", fileIndex);
    if (!SD.exists(currentFilename)) {
      break; 
    }
    fileIndex++;
    if(fileIndex > 999) fileIndex = 0;
  }

  File f = SD.open(currentFilename, FILE_WRITE);
  if(f) {
    f.close();
    Serial.printf("Created new log file: %s\n", currentFilename);
  } else {
    Serial.println("Failed to create file!");
  }
}

// ----------------- Tasks -----------------

// Task for Controller 0 (Native)
void can0ReceiverTask(void *pvParameters) {
  twai_message_t msg;
  CanMessage_t rx;
  while(1) {
    // Use v2 API with handle 0
    if(twai_receive_v2(hTwai0, &msg, pdMS_TO_TICKS(10)) == ESP_OK) {
      rx.timestamp_ms = millis();
      rx.source = 0; // Mark as CAN 0
      
      rx.id = msg.identifier;
      rx.isExtended = msg.extd;
      rx.length = msg.data_length_code;
      
      // Standardize ID format for storage
      if(rx.isExtended) rx.id &= 0x1FFFFFFF;
      else rx.id &= 0x7FF;
      
      memcpy(rx.data, msg.data, rx.length);
      xQueueSend(canQueue, &rx, 0);
    }
  }
}

// Task for Controller 1 (Native - Replacing MCP2515)
void can1ReceiverTask(void *pvParameters) {
  twai_message_t msg;
  CanMessage_t rx;
  while(1) {
    // Use v2 API with handle 1
    if(twai_receive_v2(hTwai1, &msg, pdMS_TO_TICKS(10)) == ESP_OK) {
      rx.timestamp_ms = millis();
      rx.source = 1; // Mark as CAN 1
      
      rx.id = msg.identifier;
      rx.isExtended = msg.extd;
      rx.length = msg.data_length_code;
      
      if(rx.isExtended) rx.id &= 0x1FFFFFFF;
      else rx.id &= 0x7FF;
      
      memcpy(rx.data, msg.data, rx.length);
      xQueueSend(canQueue, &rx, 0);
    }
  }
}

void fileWriterTask(void *pvParameters) {
  uint8_t *buf = (uint8_t *)malloc(kFileBufferSize);

  if (buf == NULL) {
      Serial.println("[ERROR] Malloc failed! Writer Task Suspended.");
      vTaskDelete(NULL);
  }
  
  size_t pos = 0;
  uint32_t lastFlush = millis();

  // 初始開啟
  logFile = SD.open(currentFilename, FILE_APPEND);
  if (!logFile) Serial.println("[ERROR] Failed to open file initially");

  while(1) {
    CanMessage_t msg;
    
    // Receive from Queue
    if(xQueueReceive(canQueue, &msg, pdMS_TO_TICKS(10))) {
      LogEntry e;
      e.timestamp = msg.timestamp_ms;
      e.id = msg.id;
      e.len = msg.length;
      memcpy(e.data, msg.data, 8);
      
      if(msg.isExtended) e.id |= 0x80000000;
      if(msg.source == 1) e.id |= 0x40000000; 
      
      size_t eSize = sizeof(LogEntry);

      // --- Buffer Logic ---
      if(pos + eSize >= kFileBufferSize) {
        // Buffer Full -> Flush
        bool writeSuccess = false;
        
        // 嘗試寫入
        if (logFile) {
            size_t written = logFile.write(buf, pos);
            if (written == pos) {
                logFile.flush();
                messagesLogged += (pos/sizeof(LogEntry));
                writeSuccess = true;
            } else {
                Serial.println("[WARN] Partial write or write failed!");
                logFile.close(); // 強制關閉以重置
            }
        }

        // 如果寫入失敗或檔案未開啟，嘗試救援
        if (!writeSuccess) {
            Serial.println("[WARN] Re-opening file...");
            logFile = SD.open(currentFilename, FILE_APPEND);
            if (logFile) {
                // 重開成功，補寫資料
                logFile.write(buf, pos);
                logFile.flush();
                messagesLogged += (pos/sizeof(LogEntry));
                Serial.println("[INFO] File recovered.");
            } else {
                Serial.println("[ERROR] File recovery failed! Data lost.");
                // 這裡可以加一個 LED 亮紅燈
            }
        }

        pos = 0;
        lastFlush = millis();
      }
      
      memcpy(buf + pos, &e, eSize);
      pos += eSize;
    }
    
    // --- Time-based flush (邏輯同上，必須嚴謹) ---
    if(millis() - lastFlush > kFlushIntervalMs) {
      if(pos > 0) {
        bool writeSuccess = false;
        if (logFile) { 
          size_t written = logFile.write(buf, pos);
          if (written == pos) {
              logFile.flush(); 
              messagesLogged += (pos/sizeof(LogEntry));
              writeSuccess = true;
          } else {
              logFile.close();
          }
        }
        
        if (!writeSuccess) {
            logFile = SD.open(currentFilename, FILE_APPEND);
            if(logFile) {
                logFile.write(buf, pos);
                logFile.flush();
                messagesLogged += (pos/sizeof(LogEntry));
            }
        }
        pos = 0; 
      }
      lastFlush = millis();
    }
  }
}

void safeDelay(uint32_t ms) { vTaskDelay(pdMS_TO_TICKS(ms)); }