# Feather ESP32-S3 Solar Weather Station

This repository contains firmware and hardware design files for a solar-powered weather station
based on the Adafruit Feather ESP32-S3, with an external TLV3011-based UVLO module to protect
the LiPo pack during extended low-sun periods.

## Layout

- `firmware/`
  - `main.py` — current firmware for the Feather ESP32-S3
  - `archive/` — older firmware versions (if needed later)
- `hardware/`
  - `uvlo-board/` — PCB design files, schematics, and Gerbers for the TLV3011 UVLO board
- `docs/`
  - Additional documentation, notes, wiring diagrams, etc.

## UVLO Module (TLV3011) -- work in progress

External board that monitors the battery (BAT) line and controls the Feather EN pin:

- Turn-OFF (falling): ~3.35 V
- Turn-ON (rising): ~3.65 V
- Designed for very low quiescent current for solar/battery applications.

## Firmware Overview

- Target: Adafruit Feather ESP32-S3
- Language: CircuitPython / MicroPython-style Python
- Responsibilities:
  - Read sensors (weather station data)
  - Manage low-power behavior together with the UVLO board
  - Log/transmit measurements

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/talon-k/feather-esp32s3-solar-wx-station.git
   cd feather-esp32s3-solar-wx-station
2. Copy firmware/code.py to the root of your Feather ESP32-S3 CIRCUITPY drive.
