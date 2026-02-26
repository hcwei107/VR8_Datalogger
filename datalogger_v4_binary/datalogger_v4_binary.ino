#include "FS.h"
#include "SD.h"
#include "SPI.h"

#include <driver/twai.h>  // Changed from CAN.h to TWAI
#include <RTClib.h>
#include <freertos/queue.h>

// ----------------- Pin Configuration -----------------
constexpr int kSckPin = GPIO_NUM_18;    // SPI Clock
constexpr int kMisoPin = GPIO_NUM_19;   // SPI MISO
constexpr int kMosiPin = GPIO_NUM_23;   // SPI MOSI
constexpr int kCsPin = GPIO_NUM_4;      // SD Card Chip Select
constexpr gpio_num_t kCanTxPin = GPIO_NUM_2;   // CAN Transmit (TWAI_TX)
constexpr gpio_num_t kCanRxPin = GPIO_NUM_15;  // CAN Receive (TWAI_RX)

// ----------------- Constants -----------------
constexpr size_t kMaxQueueSize = 3000;          // Increased queue size
constexpr uint32_t kWriteIntervalMs = 100;      // (Binary模式下此參數主要由FlushInterval控制)
// 根據你的回饋，維持 15MHz
constexpr uint32_t kSpiFrequency = 15000000;    
constexpr size_t kMaxLineLength = 128;          // Max line length (Binary模式較少用)
constexpr size_t kFileBufferSize = 4096;        // (舊Buffer參數，Binary模式改用動態配置)
constexpr uint32_t kHealthCheckInterval = 30000; // System health check interval
constexpr uint32_t kMaxStandardId = 0x7FF;      // Maximum 11-bit standard ID
constexpr uint32_t kMaxExtendedId = 0x1FFFFFFF; // Maximum 29-bit extended ID

// ----------------- Data Structures -----------------
typedef struct {
  uint32_t id;             // CAN message ID
  uint8_t length;          // Data length
  uint8_t data[8];         // CAN data bytes
  uint32_t timestamp_ms;   // Millisecond timestamp
  bool isExtended;         // Extended frame flag
} CanMessage_t;

// [新增] 二進制紀錄用的緊湊結構 (強制對齊為 1 byte)
// 總共 17 Bytes (4 + 4 + 1 + 8)
#pragma pack(push, 1)
struct LogEntry {
  uint32_t timestamp; // 4 bytes (時間戳記)
  uint32_t id;        // 4 bytes (CAN ID, 最高位元可用來標記 Extended)
  uint8_t len;        // 1 byte  (資料長度)
  uint8_t data[8];    // 8 bytes (資料內容)
};
#pragma pack(pop)

/* ───── Reference points captured once at start ───── */
DateTime      initial_dt;          // Wall-clock at t0
unsigned long start_ms;            // millis() value at t0

// ----------------- Global Variables -----------------
RTC_DS3231 rtc;
QueueHandle_t canQueue;
TaskHandle_t writeTaskHandle;
SemaphoreHandle_t sdMutex;
char currentFilename[64];
bool systemActive = true;
uint32_t lastHealthCheck = 0;
uint32_t messagesLogged = 0;
uint32_t writeErrors = 0;

// ----------------- Function Prototypes -----------------
bool initSDCard();
bool initRTC();
bool initCAN();
void createNewLogFile();
void canReceiverTask(void *pvParameters);
void fileWriterTask(void *pvParameters);
void systemHealthCheck();
// void formatCanMessage(...); // Binary模式不再需要轉字串函式
void safeDelay(uint32_t ms);

// ----------------- Main Functions -----------------
void setup() {
  Serial.begin(115200);
  while (!Serial) {
    safeDelay(10);
  }
  Serial.println("\nUniversal CAN Datalogger (Binary Mode) Initializing...");

  // Create mutex for SD card access
  sdMutex = xSemaphoreCreateMutex();
  if (sdMutex == NULL) {
    Serial.println("FATAL: Failed to create SD mutex");
    while (1) safeDelay(1000);
  }

  // Initialize hardware components
  if (!initRTC()) {
    Serial.println("FATAL: RTC initialization failed");
    while (1) safeDelay(1000);
  }
    /* Capture both reference clocks *at the same moment*. */
  initial_dt = rtc.now();  // wall-clock (seconds resolution)
  start_ms   = millis();   // monotonic (millisecond resolution)

  while (!initSDCard()) {
    Serial.println("FATAL: SD Card initialization failed");
    safeDelay(1000);
  }

  if (!initCAN()) {
    Serial.println("FATAL: CAN initialization failed");
    while (1) safeDelay(1000);
  }

  // Create message queue
  canQueue = xQueueCreate(kMaxQueueSize, sizeof(CanMessage_t));
  if (canQueue == NULL) {
    Serial.println("FATAL: Failed to create message queue");
    while (1) safeDelay(1000);
  }

  // Create new log file (Binary)
  createNewLogFile();

  // Create tasks
  xTaskCreatePinnedToCore(
    canReceiverTask,    // Task function
    "CAN_RX",           // Task name
    4096,               // Stack size
    NULL,               // Parameters
    2,                  // Priority (higher than writer)
    NULL,               // Task handle
    0                   // Core (0 or 1)
  );

  xTaskCreatePinnedToCore(
    fileWriterTask,     // Task function
    "FileWriter",       // Task name
    8192,               // Larger stack for file operations
    NULL,               // Parameters
    1,                  // Priority
    &writeTaskHandle,   // Task handle
    1                   // Core (different from receiver)
  );

  Serial.println("Universal CAN Datalogger ready (Binary Mode)");
}

void loop() {
  // Main loop handles system health checks
  static uint32_t lastPrint = 0;
  uint32_t now = millis();

  //掉包報錯
  uint32_t alerts_triggered;
  
  twai_read_alerts(&alerts_triggered, 0); 

  if (alerts_triggered & TWAI_ALERT_RX_FIFO_OVERRUN) {
    Serial.println("CRITICAL: Hardware FIFO Overrun! (CPU 太忙或中斷被卡住)");
  }
  
  if (alerts_triggered & TWAI_ALERT_RX_QUEUE_FULL) {
    Serial.println("WARNING: Driver RX Queue Full! (建議加大 rx_queue_len)");
  }
  //掉包報錯

  // [重要] 建議保持註解掉 Health Check，避免 30 秒時的資源衝突
  /*
  if (now - lastHealthCheck > kHealthCheckInterval) {
    systemHealthCheck();
    lastHealthCheck = now;
  }
  */

  // Status reporting
  if (now - lastPrint > 5000) {
    Serial.printf("Status: Messages logged: %lu, Queue: %u\n",
                  messagesLogged, uxQueueMessagesWaiting(canQueue));
    lastPrint = now;
  }

  safeDelay(100);
}

// ----------------- Hardware Initialization -----------------
bool initSDCard() {
  SPI.begin(kSckPin, kMisoPin, kMosiPin, kCsPin);
  
  if (!SD.begin(kCsPin, SPI, kSpiFrequency)) {
    Serial.println("SD Card initialization failed");
    return false;
  }

  uint8_t cardType = SD.cardType();
  if (cardType == CARD_NONE) {
    Serial.println("No SD card detected");
    return false;
  }

  Serial.printf("SD Card Type: %s\n", 
    cardType == CARD_MMC ? "MMC" :
    cardType == CARD_SD ? "SDSC" :
    cardType == CARD_SDHC ? "SDHC" : "UNKNOWN");

  uint64_t cardSize = SD.cardSize() / (1024 * 1024);
  Serial.printf("SD Card Size: %lluMB\n", cardSize);

  // Create CAN_LOGS directory if it doesn't exist
  if (xSemaphoreTake(sdMutex, portMAX_DELAY) == pdTRUE) {
    if (!SD.exists("/CAN_LOGS")) {
      SD.mkdir("/CAN_LOGS");
    }
    xSemaphoreGive(sdMutex);
  }

  return true;
}

bool initRTC() {
  if (!rtc.begin()) {
    Serial.println("Couldn't find RTC");
    return false;
  }

  rtc.disable32K();
  rtc.writeSqwPinMode(DS3231_OFF);

  DateTime now = rtc.now();
  Serial.printf("RTC initialized. Current time: %04d-%02d-%02d %02d:%02d:%02d\n",
                now.year(), now.month(), now.day(),
                now.hour(), now.minute(), now.second());

  return true;
}

bool initCAN() {
  twai_general_config_t g_config = {
    .mode = TWAI_MODE_NORMAL,
    .tx_io = kCanTxPin,
    .rx_io = kCanRxPin,
    .clkout_io = TWAI_IO_UNUSED,
    .bus_off_io = TWAI_IO_UNUSED,
    .tx_queue_len = 5,
    .rx_queue_len = 100,    // 維持 100
    .alerts_enabled = TWAI_ALERT_RX_QUEUE_FULL | TWAI_ALERT_RX_FIFO_OVERRUN, // 開啟警報
    .clkout_divider = 0
  };
  
  twai_timing_config_t t_config = TWAI_TIMING_CONFIG_500KBITS();
  twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();

  if (twai_driver_install(&g_config, &t_config, &f_config) != ESP_OK) {
    Serial.println("Failed to install TWAI driver");
    return false;
  }

  if (twai_start() != ESP_OK) {
    Serial.println("Failed to start TWAI controller");
    return false;
  }

  Serial.println("TWAI initialized for both standard and extended IDs");
  return true;
}

// ----------------- File Management -----------------
void createNewLogFile() {
  DateTime now = rtc.now();
  // [修改] 副檔名改為 .bin
  snprintf(currentFilename, sizeof(currentFilename),
           "/CAN_LOGS/CAN_%04d%02d%02d_%02d%02d%02d.bin",
           now.year(), now.month(), now.day(),
           now.hour(), now.minute(), now.second());

  if (xSemaphoreTake(sdMutex, portMAX_DELAY) == pdTRUE) {
    File file = SD.open(currentFilename, FILE_WRITE);
    if (file) {
      // [修改] Binary 檔不需要 CSV 表頭，這裡保持空白
      file.close();
      Serial.printf("Created new binary log file: %s\n", currentFilename);
    } else {
      Serial.println("Failed to create log file");
      systemActive = false;
    }
    xSemaphoreGive(sdMutex);
  }
}

// ----------------- Task Functions -----------------
void canReceiverTask(void *pvParameters) {
  CanMessage_t rxMsg;
  twai_message_t message;
  
  while (1) {
    // 嘗試接收，Timeout 設為 10ms (如果沒資料就休息，有資料就繼續)
    // 這裡我們把主動等待放在 twai_receive 裡面，而不是外面的 vTaskDelay
    if (twai_receive(&message, pdMS_TO_TICKS(10)) == ESP_OK) {
      rxMsg.id = message.identifier;
      rxMsg.isExtended = message.extd;
      rxMsg.length = message.data_length_code;
      rxMsg.timestamp_ms = millis();
      
      if (rxMsg.isExtended) {
        rxMsg.id &= 0x1FFFFFFF;
      } else {
        rxMsg.id &= 0x7FF;
      }
      
      if (!message.rtr && rxMsg.length > 0) {
        memcpy(rxMsg.data, message.data, rxMsg.length);
      }

      // 丟進 Queue，如果不成功 (滿了) 也不要卡住，繼續下一筆
      if (xQueueSend(canQueue, &rxMsg, 0) != pdTRUE) {
        // Serial.println("Q Full"); // 盡量不要在 ISR 或高頻任務印字
      }
    }
    
    // [重要] 這裡原本有的 vTaskDelay(1); 必須刪掉！！！
    // 讓 Task 一直跑，直到沒資料 (twai_receive 會自己 Block 住)
  }
}

// [核心修改] Binary Writer Task (32KB Buffer + 500ms Flush + Binary Struct)
void fileWriterTask(void *pvParameters) {
  // 1. 配置 32KB 超大緩衝區
  const size_t kBigBufferSize = 32768; 
  uint8_t *fileBuffer = (uint8_t *)malloc(kBigBufferSize);
  
  if (fileBuffer == NULL) {
      Serial.println("FATAL: Malloc failed! Not enough RAM.");
      vTaskDelete(NULL);
  }

  size_t bufferPos = 0;
  const uint32_t kFlushIntervalMs = 500; 
  uint32_t lastFlushTime = millis(); // 改為 millis() 初始值

  File logFile;
  if (xSemaphoreTake(sdMutex, portMAX_DELAY) == pdTRUE) {
    logFile = SD.open(currentFilename, FILE_APPEND);
    xSemaphoreGive(sdMutex);
  }

  while (1) {
    CanMessage_t msg;
    
    // [修改點] 聰明的等待機制
    // 這裡我們等待 Queue 有資料進來，最多等 10ms (pdMS_TO_TICKS(10))
    // 如果有資料 -> 馬上處理 (不 Delay)
    // 如果沒資料 -> Task 進入 Blocked 狀態 (省電，讓出 CPU)
    // 這樣既不會空轉，也不會被 vTaskDelay(1) 鎖死速度
    if (xQueueReceive(canQueue, &msg, pdMS_TO_TICKS(10)) == pdTRUE) {
      
      // ----------- 處理資料 (保持不變) -----------
      LogEntry entry;
      entry.timestamp = msg.timestamp_ms;
      entry.id = msg.id;
      if (msg.isExtended) entry.id |= 0x80000000; 
      entry.len = msg.length;
      memcpy(entry.data, msg.data, 8);
      size_t entrySize = sizeof(LogEntry);

      if (bufferPos + entrySize >= kBigBufferSize) { 
        if (xSemaphoreTake(sdMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
           logFile.write(fileBuffer, bufferPos);
           bufferPos = 0;
           xSemaphoreGive(sdMutex);
        }
      }
      
      if (bufferPos + entrySize < kBigBufferSize) {
        memcpy(fileBuffer + bufferPos, &entry, entrySize);
        bufferPos += entrySize;
      }
      // -----------------------------------------
    }
    
    // 定時 Flush 機制 (保持不變)
    uint32_t now = millis();
    if (now - lastFlushTime > kFlushIntervalMs) {
      if (xSemaphoreTake(sdMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        if (bufferPos > 0) {
           logFile.write(fileBuffer, bufferPos);
           bufferPos = 0;
        }
        logFile.flush(); 
        lastFlushTime = now;
        messagesLogged++; 
        xSemaphoreGive(sdMutex);
      }
    }
    
    // [重要] 把原本這裡的 vTaskDelay(1) 刪掉！
  }
  free(fileBuffer); 
}


// ----------------- Helper Functions -----------------
void systemHealthCheck() {
  if (xSemaphoreTake(sdMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    if (!SD.exists(currentFilename)) {
      Serial.println("Log file missing, attempting recovery");
      createNewLogFile();
    }
    xSemaphoreGive(sdMutex);
  }
  
  UBaseType_t queueCount = uxQueueMessagesWaiting(canQueue);
  if (queueCount > kMaxQueueSize * 0.9) {
    Serial.printf("WARNING: High queue usage: %u/%d\n", queueCount, kMaxQueueSize);
  }
  
  if (!rtc.begin()) {
    Serial.println("RTC communication failed");
    systemActive = false;
  }
}

void safeDelay(uint32_t ms) {
  uint32_t start = millis();
  while (millis() - start < ms) {
    vTaskDelay(1);
  }
}