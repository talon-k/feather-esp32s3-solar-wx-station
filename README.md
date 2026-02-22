# Feather ESP32-S3 Solar Weather Station

Solar-powered outdoor weather station firmware built for CircuitPython on the Adafruit Feather ESP32-S3.

## What This Project Does

- Reads environmental data from a BME280 sensor
- Tracks battery state with MAX17048 fuel gauge support in firmware
- Keeps time with a DS3231 RTC (with NTP resync)
- Publishes telemetry to MQTT (with Home Assistant discovery)
- Logs operational data and health info to microSD
- Uses sleep/wake cycles for low-power operation on solar + LiPo

## Hardware (Current Build)

- **Main MCU:** Adafruit Feather ESP32-S3 (4MB Flash / 2MB PSRAM, Product ID 5477)
  - <https://learn.adafruit.com/adafruit-esp32-s3-feather>
- **RTC:** Adafruit DS3231 Precision RTC STEMMA QT (Product ID 5188)
  - <https://learn.adafruit.com/adafruit-ds3231-precision-rtc-breakout>
- **Weather Sensor:** Adafruit BME280 I2C/SPI STEMMA QT (Product ID 2652)
  - <https://learn.adafruit.com/adafruit-bme280-humidity-barometric-pressure-temperature-sensor-breakout>
- **Storage:** Adafruit MicroSD Breakout Board+ (Product ID 254)
  - <https://learn.adafruit.com/adafruit-micro-sd-breakout-board-card-tutorial>
- **Solar/LiPo Charger:** Adafruit bq24074 USB/DC/Solar charger (Product ID 4755)
  - <https://learn.adafruit.com/adafruit-bq24074-universal-usb-dc-solar-charger-breakout/design-notes>
- **Solar Panel:** Voltaic 6V 2W ETFE panel
- **Battery:** 6000mAh LiPo

## Repository Layout

- `firmware/code.py` - Active weather station firmware (CircuitPython)

## Firmware Notes

- **Current firmware in repo:** `v1.5.5`
- Written for CircuitPython behavior and constraints on embedded hardware
- Includes watchdog management, Wi-Fi/MQTT retry logic, RTC/NTP sync, timezone/sun cache handling, and SD-based health logging

## Quick Start

1. Clone the repository.

```bash
git clone https://github.com/talon-k/feather-esp32s3-solar-wx-station.git
cd feather-esp32s3-solar-wx-station
```

2. Install CircuitPython on your Feather ESP32-S3 and mount `CIRCUITPY`.
3. Copy `firmware/code.py` to the root of the `CIRCUITPY` drive as `code.py`.
4. Create/update `secrets.py` with Wi-Fi and MQTT credentials.
5. Ensure required CircuitPython libraries are present in `CIRCUITPY/lib` (for example: BME280, MiniMQTT, Requests, NTP, DS3231, MAX1704x, SD card support).
6. Reboot the board and monitor serial logs.

## MQTT / Home Assistant

The firmware publishes state under a configurable topic base and can publish Home Assistant discovery entities for diagnostics and weather telemetry.

## Project Status

Active firmware development and field tuning are ongoing. Hardware and power-management assumptions in this repo reflect the current deployed stack listed above.
