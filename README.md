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
- `firmware/secrets.py` - Credentials/config dictionary for Wi-Fi and MQTT
- `firmware/settings.toml` - CircuitPython environment variables (OWM + timezone, optional Wi-Fi override)

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
4. Copy `firmware/secrets.py` and `firmware/settings.toml` to the `CIRCUITPY` root, then edit values.
5. Ensure required CircuitPython libraries are present in `CIRCUITPY/lib`.
6. Reboot the board and monitor serial logs.

## Required CircuitPython Libraries

Install these from the CircuitPython bundle into `CIRCUITPY/lib`:

- `adafruit_bme280`
- `adafruit_bus_device`
- `adafruit_ds3231.mpy`
- `adafruit_max1704x.mpy`
- `adafruit_minimqtt`
- `adafruit_ntp.mpy`
- `adafruit_register`
- `adafruit_requests.mpy`
- `adafruit_sdcard.mpy`

## Configuration Files

The firmware expects both files next to `code.py` on the board.

- `secrets.py` keys:
  - `ssid`
  - `password`
  - `mqtt_broker`
  - `mqtt_port`
  - `mqtt_username`
  - `mqtt_password`
- `settings.toml` keys:
  - `OW_API_KEY`
  - `OW_LAT`
  - `OW_LON`
  - `LOCAL_TZ`
  - Optional: `CIRCUITPY_WIFI_SSID`, `CIRCUITPY_WIFI_PASSWORD`

Keeping dummy `secrets.py` and `settings.toml` under `firmware/` is intentional, since deployment is a straight copy to the board root. Treat repo versions as templates only and never commit real credentials.

## MQTT / Home Assistant

The firmware publishes state under a configurable topic base and can publish Home Assistant discovery entities for diagnostics and weather telemetry.

## References

- Adafruit Feather ESP32-S3: <https://learn.adafruit.com/adafruit-esp32-s3-feather>
- Adafruit DS3231 Precision RTC: <https://learn.adafruit.com/adafruit-ds3231-precision-rtc-breakout>
- Adafruit BME280 Sensor: <https://learn.adafruit.com/adafruit-bme280-humidity-barometric-pressure-temperature-sensor-breakout>
- Adafruit MicroSD Breakout Board+: <https://learn.adafruit.com/adafruit-micro-sd-breakout-board-card-tutorial>
- Adafruit bq24074 Charger: <https://learn.adafruit.com/adafruit-bq24074-universal-usb-dc-solar-charger-breakout/design-notes>
- CircuitPython docs: <https://docs.circuitpython.org/en/latest/>
- CircuitPython library bundle: <https://circuitpython.org/libraries>
- Home Assistant MQTT Discovery: <https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery>
- OpenWeather API: <https://openweathermap.org/current>
- WorldTimeAPI: <https://worldtimeapi.org/>

## Project Status

Active firmware development and field tuning are ongoing. Hardware and power-management assumptions in this repo reflect the current deployed stack listed above.
