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
constexpr uint32_t kWriteIntervalMs = 100;      // More frequent writes
constexpr uint32_t kSpiFrequency = 15E6;        // Max SPI speed for SD card
constexpr size_t kMaxLineLength = 128;          // Max line length in log file
constexpr size_t kFileBufferSize = 4096;        // File write buffer size
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
void formatCanMessage(const CanMessage_t &msg, char *buffer, size_t size);
void safeDelay(uint32_t ms);

// ----------------- Main Functions -----------------
void setup() {
  Serial.begin(115200);
  while (!Serial) {
    safeDelay(10);
  }
  Serial.println("\nUniversal CAN Datalogger Initializing...");

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

  // Create new log file
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

  Serial.println("Universal CAN Datalogger ready");
  Serial.println("Configured to log both standard and extended IDs");
}

void loop() {
  // Main loop handles system health checks
  static uint32_t lastPrint = 0;
  uint32_t now = millis();

  // Periodic health check
  if (now - lastHealthCheck > kHealthCheckInterval) {
    systemHealthCheck();
    lastHealthCheck = now;
  }

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

  // if (rtc.lostPower()) {
  //   Serial.println("RTC lost power, setting to compile time");
  //   rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
  // }
  // rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));


  // Disable unnecessary RTC features to save power
  rtc.disable32K();
  rtc.writeSqwPinMode(DS3231_OFF);

  DateTime now = rtc.now();
  Serial.printf("RTC initialized. Current time: %04d-%02d-%02d %02d:%02d:%02d\n",
               now.year(), now.month(), now.day(),
               now.hour(), now.minute(), now.second());

  return true;
}

bool initCAN() {
  // Initialize configuration structures using macro initializers
  twai_general_config_t g_config = {
    .mode = TWAI_MODE_NORMAL,
    .tx_io = kCanTxPin,
    .rx_io = kCanRxPin,
    .clkout_io = TWAI_IO_UNUSED,
    
    .bus_off_io = TWAI_IO_UNUSED,
    .tx_queue_len = 5,
    .rx_queue_len = 5,
    .alerts_enabled = TWAI_ALERT_NONE,
    .clkout_divider = 0
  };
  
  twai_timing_config_t t_config = TWAI_TIMING_CONFIG_500KBITS();
  twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();

  // Install TWAI driver
  if (twai_driver_install(&g_config, &t_config, &f_config) != ESP_OK) {
    Serial.println("Failed to install TWAI driver");
    return false;
  }

  // Start TWAI driver
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
  snprintf(currentFilename, sizeof(currentFilename),
           "/CAN_LOGS/CAN_%04d%02d%02d_%02d%02d%02d.csv",
           now.year(), now.month(), now.day(),
           now.hour(), now.minute(), now.second());

  if (xSemaphoreTake(sdMutex, portMAX_DELAY) == pdTRUE) {
    File file = SD.open(currentFilename, FILE_WRITE);
    if (file) {
      file.println("Timestamp,ID,Extended,Length,Data");
      file.close();
      Serial.printf("Created new log file: %s\n", currentFilename);
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
    // Receive TWAI message
    if (twai_receive(&message, pdMS_TO_TICKS(10)) == ESP_OK) {
      rxMsg.id = message.identifier;
      rxMsg.isExtended = message.extd;
      rxMsg.length = message.data_length_code;
      rxMsg.timestamp_ms = millis();
      
      // Mask ID based on frame type
      if (rxMsg.isExtended) {
        rxMsg.id &= 0x1FFFFFFF; // Ensure 29-bit ID
      } else {
        rxMsg.id &= 0x7FF;      // Ensure 11-bit ID
      }
      
      if (!message.rtr && rxMsg.length > 0) {
        // Copy data bytes
        memcpy(rxMsg.data, message.data, rxMsg.length);
      }

      // Add to queue with timeout
      if (xQueueSend(canQueue, &rxMsg, 10) != pdTRUE) {
        Serial.println("WARNING: CAN message queue full");
      }
    }
    
    vTaskDelay(1);
  }
}

// [Rest of the file remains the same...]
// fileWriterTask, formatCanMessage, systemHealthCheck, safeDelay functions
// remain unchanged from the original code


void fileWriterTask(void *pvParameters) {
  char lineBuffer[kMaxLineLength];
  char fileBuffer[kFileBufferSize];
  size_t bufferPos = 0;
  uint32_t lastWriteTime = 0;
  
  while (1) {
    CanMessage_t msg;
    
    // Process messages from queue
    while (xQueueReceive(canQueue, &msg, 0) == pdTRUE) {
      formatCanMessage(msg, lineBuffer, sizeof(lineBuffer));
      size_t lineLen = strlen(lineBuffer);
      
      // Check if buffer has space
      if (bufferPos + lineLen >= sizeof(fileBuffer)) {
        // Write buffer to file
        if (xSemaphoreTake(sdMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
          File file = SD.open(currentFilename, FILE_APPEND);
          if (file) {
            if (file.write((uint8_t*)fileBuffer, bufferPos) == bufferPos) {
              file.close();
              bufferPos = 0;
              messagesLogged++;
            } else {
              writeErrors++;
              file.close();
            }
          } else {
            writeErrors++;
          }
          xSemaphoreGive(sdMutex);
        }
      }
      
      // Add line to buffer
      if (bufferPos + lineLen < sizeof(fileBuffer)) {
        memcpy(fileBuffer + bufferPos, lineBuffer, lineLen);
        bufferPos += lineLen;
      }
    }
    
    // Periodic flush if buffer has data
    uint32_t now = millis();
    if (bufferPos > 0 && (now - lastWriteTime > kWriteIntervalMs)) {
      if (xSemaphoreTake(sdMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        File file = SD.open(currentFilename, FILE_APPEND);
        if (file) {
          if (file.write((uint8_t*)fileBuffer, bufferPos) == bufferPos) {
            file.close();
            bufferPos = 0;
            messagesLogged++;
            lastWriteTime = now;
          } else {
            writeErrors++;
            file.close();
          }
        } else {
          writeErrors++;
        }
        xSemaphoreGive(sdMutex);
      }
    }
    
    vTaskDelay(1);
  }
}

// ----------------- Helper Functions -----------------
void formatCanMessage(const CanMessage_t &msg, char *buffer, size_t size) {
  // DateTime now = rtc.now();
  int msg_timestamp_ms = msg.timestamp_ms - start_ms;

  // /* ------------------------------------------------------------------
  //  * 2) Turn that relative stamp into an absolute DateTime (+ millis).
  //  * ------------------------------------------------------------------ */
  // TimeSpan delta(msg_timestamp_ms / 1000);    // whole-second offset
  // DateTime abs_dt = initial_dt + delta;       // add to reference
  // uint16_t abs_ms = msg_timestamp_ms % 1000;  // sub-second remainder

  // /* ------------------------------------------------------------------
  //  * 3) Format the timestamp the same way you did before.
  //  * ------------------------------------------------------------------ */
  // snprintf(buffer, sizeof(buffer),
  //          "%04d-%02d-%02d %02d:%02d:%02d.%03u,",
  //          abs_dt.year(), abs_dt.month(), abs_dt.day(),
  //          abs_dt.hour(), abs_dt.minute(), abs_dt.second(),
  //          abs_ms);
  snprintf(buffer, size,
           "%d,",
           msg_timestamp_ms);

  // // Format timestamp with milliseconds
  // snprintf(buffer, size, "%04d-%02d-%02d %02d:%02d:%02d.%03d,",
  //          now.year(), now.month(), now.day(),
  //          now.hour(), now.minute(), now.second(),
  //          msg.timestamp_ms % 1000);
  
  size_t pos = strlen(buffer);
  
  // Format message ID (hex) and type
  if (msg.isExtended) {
    snprintf(buffer + pos, size - pos, "0x%08X,1,%d,",
             msg.id, msg.length);
  } else {
    snprintf(buffer + pos, size - pos, "0x%03X,0,%d,",
             msg.id, msg.length);
  }
  
  pos = strlen(buffer);
  
  // Format data bytes (hex)
  for (int i = 0; i < msg.length && pos < size - 3; i++) {
    snprintf(buffer + pos, size - pos, "%02X", msg.data[i]);
    pos += 2;
  }
  
  // Add newline
  if (pos < size - 1) {
    buffer[pos] = '\n';
    buffer[pos + 1] = '\0';
  }
}

void systemHealthCheck() {
  // Check SD card
  if (xSemaphoreTake(sdMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
    if (!SD.exists(currentFilename)) {
      Serial.println("Log file missing, attempting recovery");
      createNewLogFile();
    }
    xSemaphoreGive(sdMutex);
  }
  
  // Check queue status
  UBaseType_t queueCount = uxQueueMessagesWaiting(canQueue);
  if (queueCount > kMaxQueueSize * 0.9) {
    Serial.printf("WARNING: High queue usage: %u/%d\n", queueCount, kMaxQueueSize);
  }
  
  // Check RTC
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