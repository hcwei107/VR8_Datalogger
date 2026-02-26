/*
 * Ultimate Dual SPI Datalogger (Fixed Version)
 * Platform: ESP32 Dev Module
 * Fixes Applied:
 * 1. [Critical] ISR "Sticky Interrupt" fix (prevent deadlock).
 * 2. [Performance] Keep file open (flush instead of close).
 * 3. [Stability] Pre-charge CS pins to prevent SPI bus contention.
 */

#include <Arduino.h>
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include <driver/twai.h>
#include <freertos/queue.h>
#include <mcp_can.h>

// ----------------- Pin Definitions -----------------

// 1. SD Card (Mapping to HSPI Hardware)
constexpr int kSdCs   = 4;   
constexpr int kSdSck  = 18;
constexpr int kSdMiso = 19;
constexpr int kSdMosi = 23;

// 2. MCP2515 (Mapping to VSPI/Global Hardware)
constexpr int kMcpCs   = 27;
constexpr int kMcpSck  = 14;
constexpr int kMcpMiso = 26; 
constexpr int kMcpMosi = 13;
constexpr int kMcpInt  = 32; 

// 3. Built-in TWAI (CJMCU-2551)
constexpr gpio_num_t kTwaiTx = GPIO_NUM_2; 
constexpr gpio_num_t kTwaiRx = GPIO_NUM_15; 

// ----------------- System Constants -----------------
constexpr size_t kMaxQueueSize = 500;
constexpr uint32_t kSpiFreq = 15000000;      // 4MHz for SD stability
constexpr uint32_t kFileBufferSize = 32768; // 32KB Buffer
constexpr uint32_t kFlushIntervalMs = 500;  // Force flush every 500ms

// ----------------- Data Structures -----------------
typedef struct {
  uint32_t id;
  uint8_t length;
  uint8_t data[8];
  uint32_t timestamp_ms;
  bool isExtended;
  uint8_t source; 
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
// MCP2515 uses default global SPI (VSPI)
MCP_CAN CAN_HSPI(kMcpCs); 

// SD Card uses a separate HSPI instance
SPIClass *hspi = NULL;

// OS Objects
QueueHandle_t canQueue;
SemaphoreHandle_t mcpIntSemaphore;
char currentFilename[64];
uint32_t messagesLogged = 0;

// [Fix] Global file object to avoid repeated open/close
File logFile; 

// ----------------- ISR (Interrupt Service Routine) -----------------
void IRAM_ATTR mcpISR() {
  BaseType_t xHigherPriorityTaskWoken = pdFALSE;
  xSemaphoreGiveFromISR(mcpIntSemaphore, &xHigherPriorityTaskWoken);
  if (xHigherPriorityTaskWoken) {
    portYIELD_FROM_ISR();
  }
}

// ----------------- Function Prototypes -----------------
bool initSDCard();
bool initMcp2515();
bool initTwai();
void createNewLogFile();
void twaiReceiverTask(void *pvParameters);
void mcpReceiverTask(void *pvParameters);
void fileWriterTask(void *pvParameters);
void safeDelay(uint32_t ms);

// ----------------- Main Setup -----------------
void setup() {
  Serial.begin(115200);
  while (!Serial) safeDelay(10);
  Serial.println("\n=== Ultimate Dual SPI Datalogger (Fixed) ===");

  // [Fix] Pre-charge CS pins to prevent SPI bus contention
  pinMode(kSdCs, OUTPUT); digitalWrite(kSdCs, HIGH);
  pinMode(kMcpCs, OUTPUT); digitalWrite(kMcpCs, HIGH);

  // 1. Initialize SD Card (HSPI)
  if (!initSDCard()) {
    Serial.println("[ERROR] SD Init Failed! (Check Wiring & 5V Power)");
  } else {
    Serial.println("[OK] SD Card Initialized Successfully (HSPI)!");
  }

  // 2. Initialize MCP2515 (VSPI)
  if (!initMcp2515()) {
    Serial.println("[ERROR] MCP2515 Fail");
  } else {
    Serial.println("[OK] MCP2515 Initialized OK (VSPI) + Interrupt Mode!");
  }

  // 3. Initialize TWAI
  if(initTwai()){
     Serial.println("[OK] TWAI (Built-in CAN) Initialized OK!");
  } else {
     Serial.println("[ERROR] TWAI Fail");
  }

  // 4. Create OS Objects
  canQueue = xQueueCreate(kMaxQueueSize, sizeof(CanMessage_t));
  
  // 5. Create File
  if (SD.cardType() != CARD_NONE) {
    createNewLogFile();
  }

  // 6. Start Tasks
  // File Writer (Priority 2)
  xTaskCreatePinnedToCore(fileWriterTask, "Writer", 8192, NULL, 2, NULL, 0);
  // MCP Receiver (Priority 3 - High)
  xTaskCreatePinnedToCore(mcpReceiverTask, "McpRX", 4096, NULL, 3, NULL, 1);
  // TWAI Receiver (Priority 3 - High)
  xTaskCreatePinnedToCore(twaiReceiverTask, "TwaiRX", 4096, NULL, 3, NULL, 1);

  Serial.println("[System] Running...");
}

void loop() {
  static uint32_t lastPrint = 0;
  if (millis() - lastPrint > 2000) {
    Serial.printf("Logged: %lu | Queue: %u | File: %s\n", 
      messagesLogged, uxQueueMessagesWaiting(canQueue), currentFilename);
    lastPrint = millis();
  }
  vTaskDelay(100);
}

// ----------------- Initialization -----------------
bool initSDCard() {
  hspi = new SPIClass(HSPI); 
  hspi->begin(kSdSck, kSdMiso, kSdMosi, kSdCs);
  pinMode(kSdCs, OUTPUT);
  digitalWrite(kSdCs, HIGH);
  delay(50); 

  if (!SD.begin(kSdCs, *hspi, kSpiFreq)) {
    return false;
  }
  
  if (!SD.exists("/CAN_LOGS")) {
     SD.mkdir("/CAN_LOGS");
  }
  return true;
}

bool initMcp2515() {
  SPI.begin(kMcpSck, kMcpMiso, kMcpMosi, kMcpCs);
  
  pinMode(kMcpInt, INPUT_PULLUP);
  mcpIntSemaphore = xSemaphoreCreateBinary();
  attachInterrupt(digitalPinToInterrupt(kMcpInt), mcpISR, FALLING);

  // 8MHz Crystal Setting (Check your crystal!)
  if (CAN_HSPI.begin(MCP_ANY, CAN_500KBPS, MCP_8MHZ) == CAN_OK) { 
    CAN_HSPI.setMode(MCP_NORMAL);
    return true;
  }
  return false;
}

bool initTwai() {
  twai_general_config_t g_config = TWAI_GENERAL_CONFIG_DEFAULT(kTwaiTx, kTwaiRx, TWAI_MODE_NORMAL);
  g_config.rx_queue_len = 200;
  twai_timing_config_t t_config = TWAI_TIMING_CONFIG_500KBITS();
  twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();
  return (twai_driver_install(&g_config, &t_config, &f_config) == ESP_OK && twai_start() == ESP_OK);
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

  // [Fix] Only create file, do not keep open. Let Task handle it.
  File f = SD.open(currentFilename, FILE_WRITE);
  if(f) {
    f.close();
    Serial.printf("Created new log file: %s\n", currentFilename);
  } else {
    Serial.println("Failed to create file!");
  }
}

// ----------------- Tasks -----------------
void twaiReceiverTask(void *pvParameters) {
  twai_message_t msg;
  CanMessage_t rx;
  while(1) {
    if(twai_receive(&msg, pdMS_TO_TICKS(10)) == ESP_OK) {
      rx.timestamp_ms = millis();
      rx.source = 0; 
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

void mcpReceiverTask(void *pvParameters) {
  while(1) {
    // Wait for ISR signal
    if (xSemaphoreTake(mcpIntSemaphore, portMAX_DELAY) == pdTRUE) {
      
      long unsigned int id;
      unsigned char len;
      unsigned char buf[8];
      
      // [Fix] Key fix: Use do-while to ensure we clear buffer if INT is still LOW
      // This prevents "Sticky Interrupt" deadlock
      do {
          while(CAN_HSPI.checkReceive() == CAN_MSGAVAIL) {
            CAN_HSPI.readMsgBuf(&id, &len, buf);
            CanMessage_t rx;
            rx.timestamp_ms = millis();
            rx.source = 1; 
            
            rx.isExtended = (id & 0x80000000) ? true : false;
            rx.id = id & 0x1FFFFFFF;
            rx.length = len;
            memcpy(rx.data, buf, len);
            xQueueSend(canQueue, &rx, 0);
          }
      } while(digitalRead(kMcpInt) == LOW); // Double Check Pin State
    }
  }
}

void fileWriterTask(void *pvParameters) {
  uint8_t *buf = (uint8_t *)malloc(kFileBufferSize);
  
  // [Fix] Memory check
  if (buf == NULL) {
      Serial.println("[ERROR] Malloc failed! Writer Task Suspended.");
      vTaskDelete(NULL);
  }
  
  size_t pos = 0;
  uint32_t lastFlush = millis();

  // [Fix] Performance: Keep file open
  logFile = SD.open(currentFilename, FILE_APPEND);
  if (!logFile) Serial.println("[ERROR] Failed to open file for appending");
  
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
      
      // If buffer full, flush to SD
      if(pos + eSize >= kFileBufferSize) {
        if (logFile) {
            logFile.write(buf, pos);
            logFile.flush(); // [Fix] Flush only, don't close
            messagesLogged += (pos/sizeof(LogEntry));
        } else {
            // Re-open if accidentally closed
            logFile = SD.open(currentFilename, FILE_APPEND);
        }
        pos = 0;
        lastFlush = millis(); // Reset flush timer
      }
      
      memcpy(buf + pos, &e, eSize);
      pos += eSize;
    }
    
    // Time-based flush
    if(millis() - lastFlush > kFlushIntervalMs) {
      if(pos > 0) {
        if (logFile) { 
          logFile.write(buf, pos); 
          logFile.flush(); // [Fix] Flush only
          messagesLogged += (pos/sizeof(LogEntry));
        } else {
          logFile = SD.open(currentFilename, FILE_APPEND);
        }
        pos = 0; 
      }
      lastFlush = millis();
    }
  }
}

void safeDelay(uint32_t ms) { vTaskDelay(pdMS_TO_TICKS(ms)); }