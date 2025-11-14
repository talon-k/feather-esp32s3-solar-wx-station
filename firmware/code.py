# ============================================================
# Feather ESP32-S3 BME280 → MQTT (Home Assistant Discovery)
# Firmware Version: v1.5.2b-alt  (2025-11-02)
#
# CHANGELOG (highlights):
# v1.5.2b-alt - Implement robust Watchdog Manager
#           • Replaces ad-hoc WDT calls with a central WatchdogMgr.
#           • WD is never touched when ENABLE_WATCHDOG=False.
#           • Prevents hidden arming of RAISE/RESET when WD is disabled.
#           • Safe sleep-time timeout widening + unified feed path.
# v1.5.2a — Friendly-format local sunrise/sunset text
#           • Keeps Option A (plain text) for local sunrise/sunset sensors.
#           • Adds human-readable format:
#             e.g. "2025-10-14 17:13:21 CDT (-05:00)"
#           • Removes HA timestamp device_class to prevent 'unavailable' states.
#           • Adds `_tz_offset_str()` + `_format_local_display()` helpers.
#           • Minor internal cleanup; no schema, payload, or topic changes.
# v1.5.2 - Timezone-aware night mode & daily sun refresh @ 03:00 local
#           • Adds TZ support via WorldTimeAPI (no API key required).
#           • New USER CONFIG: LOCAL_TZ (IANA/Olson ID, e.g., "America/Chicago").
#           • New HA sensors: local_date (date), local_sunrise, local_sunset (ISO local).
#           • Night/day evaluation now uses LOCAL time
#           • Decouples sun refresh from NTP; schedules OWM sun refresh once daily
#           • Robust RTC epoch handling for CircuitPython 9.x (no Y2K offset).
#           • Cleanup: removed unused helpers; simplified SD + logging paths
# v1.5.1f — Previous baseline (night-mode investigation, logging hardening)
# v1.5.1e — Night-mode fix + epoch correction
# ============================================================

FW_VERSION = "v1.5.2b-alt"

import time
import json
import math
import board
import wifi
import socketpool
import alarm
import microcontroller
import neopixel
import gc
import sys
import binascii
import os as _os
from os import getenv
from digitalio import DigitalInOut, Direction, Pull
from microcontroller import watchdog as WDT
from watchdog import WatchDogMode
from busio import I2C as BusI2C
import busio
import storage
import traceback

from adafruit_minimqtt.adafruit_minimqtt import MQTT
from adafruit_bme280.advanced import Adafruit_BME280_I2C
import adafruit_max1704x
import adafruit_ds3231
import adafruit_ntp
import ssl
import adafruit_requests
import adafruit_sdcard

print(f"BOOT: entered code.py {FW_VERSION} — starting (pre-SD)")

# ---------- USER CONFIG ----------
CLIENT_ID = "feather-esp32s3-bme280"
TOPIC_BASE = "sensors/feather01/bme280"
QOS = 0

# Home Assistant MQTT Discovery
ENABLE_HA_DISCOVERY = True
DISCOVERY_PREFIX = "homeassistant"
DEVICE_ID = "feather01_bme280"

# Publish cadence (base)
PUBLISH_PERIOD_SEC = 180        # nominal daytime cadence
BATTERY_LOW_THRESHOLD = 25.0
LOW_BATT_EXTRA_SLEEP = 900      # +15 min when battery low
SITE_ALT_M = 270                # elevation (m) for sea-level pressure calc

# Sleep behavior
SLEEP_MODE = 'light'            # 'none' | 'light' | 'deep'

# Night mode
NIGHT_MODE_ENABLED = True
NIGHT_SLEEP_SEC = 360           # longer cadence at night

# OpenWeatherMap (OWM) + WorldTimeAPI
OW_API_KEY = getenv("OW_API_KEY") or None
OW_LAT = getenv("OW_LAT") or None
OW_LON = getenv("OW_LON") or None
LOCAL_TZ = getenv("LOCAL_TZ") or "America/Chicago"  # IANA tz database ID
OW_REFRESH_LOCAL_HOUR = 3       # fetch new sun times daily at ~03:00 local

# I2C
I2C_FREQ_HZ = 50_000

# Recovery thresholds
MAX_CONSEC_FAILS = 5
LONG_SLEEP_AFTER_FAILS = 600

# Network/MQTT robustness
WIFI_SETTLE_SEC = 2
MQTT_KEEPALIVE = 120
MQTT_SOCKET_TIMEOUT = 30
MQTT_RECV_TIMEOUT = 35

# RTC sync/refresh windows (24h)
RTC_RESYNC_S = 24 * 3600
RESYNC_CYCLES = max(1, int(RTC_RESYNC_S / PUBLISH_PERIOD_SEC))

# NVM write throttling
_HEARTBEAT_FLUSH_INTERVAL = 10

# Optional daily HA discovery refresh
DAILY_DISCOVERY_REFRESH_S = 24 * 3600
DAILY_DISCOVERY_REFRESH_BOOT_THRESHOLD = max(1, int(DAILY_DISCOVERY_REFRESH_S / PUBLISH_PERIOD_SEC))

# Debug logging
DEBUG = True

# Watchdog
ENABLE_WATCHDOG = True
WDT_TIMEOUT_S = 8

# --- SD logging (SPI) ---
ENABLE_SD_LOGGING = True
SD_CS_PIN = getattr(board, "D10")
SD_SPI_BAUD = 8_000_000
SD_MOUNT = "/sd"
SD_LOG_DIR = "/sd/logs"
SD_SAMPLE_DIR = "/sd/samples"
SD_HEALTH_DIR = "/sd/health"
SD_CRASH_DIR = "/sd/crash"
SD_STATE_DIR = "/sd/state"
SD_OK = False
LOG_RETENTION_S = 72 * 3600

_SD_SPI = None
_SD_CS = None
_SD_SDOBJ = None
_SD_VFS = None

# ---------- Globals ----------
LAST_ERROR = ""
DEVICE_INFO_TOPIC  = f"{TOPIC_BASE}/device_info"
DEVICE_AVAIL_TOPIC = f"{TOPIC_BASE}/device/availability"
fetch_tz_ok = False

# State file names (all under SD_STATE_DIR)
DISCOVERY_HASH_FILE       = SD_STATE_DIR + "/ha_discovery.sha"
DISCOVERY_LAST_EPOCH_FILE = SD_STATE_DIR + "/ha_discovery.last"
DISCOVERY_BOOTS_FILE      = SD_STATE_DIR + "/ha_discovery.boots"
DEVICE_INFO_HASH_FILE     = SD_STATE_DIR + "/device_info.sha"
DEVICE_INFO_LAST_FILE     = SD_STATE_DIR + "/device_info.last"
DEVICE_INFO_REFRESH_S     = 24 * 3600
SUN_FILE                  = SD_STATE_DIR + "/sun.json"   # stores LOCAL sunrise/sunset ISO + epoch + local day
SUN_LAST_FILE             = SD_STATE_DIR + "/sun.last"   # last successful LOCAL-day fetch epoch
TZ_FILE                   = SD_STATE_DIR + "/tz.json"    # cache from WorldTimeAPI

# Hardware handles
i2c = None
bme = None
rtc = None
max17048 = None
RTC_PRESENT = False
HAVE_BATTERY = False

# In-RAM caches
_SUN_CACHE = None  # {"sunrise_local_iso": str, "sunset_local_iso": str, "sunrise_local_ep": int, "sunset_local_ep": int, "fetched_local_ep": int, "tz_abbr": str, "utc_offset_sec": int, "local_day": "YYYY-MM-DD"}
_TZ_CACHE = None   # {"tz": str, "utc_offset_sec": int, "abbrev": str, "dst": bool}
_SD_SELFTEST_DONE = False

# Phase breadcrumbs
_PHASE = "boot"

# ---- NeoPixel (dim status) ----
try:
    pixel = neopixel.NeoPixel(board.NEOPIXEL, 1, brightness=0.02, auto_write=True)
    def pix(c):
        try: pixel[0] = c
        except Exception: pass
    def pix_off():
        try: pixel[0] = (0,0,0)
        except Exception: pass
except Exception:
    pixel = None
    def pix(c):
        return
    def pix_off():
        return

COL_BOOT  = (0, 0, 16)
COL_WIFI  = (16, 8, 0)
COL_OK    = (0, 16, 0)
COL_ERROR = (16, 0, 0)
pix(COL_BOOT)

# ---- Rolling in-memory log ----
LOG_BUFFER, _LOG_MAX = [], 500

# ---------- Time helpers ----------

def _now_epoch_utc():
    """RTC→epoch (seconds) for CP9.x without extra offsets."""
    try:
        if RTC_PRESENT and rtc:
            tm = rtc.datetime
            if tm and tm.tm_year >= 2020:
                return int(time.mktime(tm))
    except Exception:
        pass
    return None


def _format_iso_utc(y, mo, d, h, mi, se):
    return "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z".format(y, mo, d, h, mi, se)


def _now_iso_utc():
    try:
        if RTC_PRESENT and rtc:
            tm = rtc.datetime
            if tm and tm.tm_year >= 2020:
                return _format_iso_utc(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
    except Exception:
        pass
    return None

# ---------- SD helpers ----------

def _ensure_dir(path):
    try:
        if path not in ("/", ""):
            parts = path.split("/")
            cur = ""
            for p in parts:
                if not p:
                    continue
                cur += "/" + p
                try:
                    _os.mkdir(cur)
                except OSError:
                    pass
    except Exception:
        pass


def sd_mount():
    global SD_OK, _SD_SPI, _SD_CS, _SD_SDOBJ, _SD_VFS
    if not ENABLE_SD_LOGGING:
        return False
    if SD_OK:
        return True
    print("SD: begin mount")
    try:
        if _SD_SPI is None:
            _SD_SPI = busio.SPI(board.SCK, board.MOSI, board.MISO)
            t0 = time.monotonic()
            while not _SD_SPI.try_lock():
                if (time.monotonic() - t0) > 0.25:
                    print("SD: SPI lock timeout; continuing anyway")
                    break
                time.sleep(0.005)
            try:
                _SD_SPI.configure(baudrate=SD_SPI_BAUD, phase=0, polarity=0)
            except Exception as e:
                print("SD: spi.configure err:", e)
            try:
                _SD_SPI.unlock()
            except Exception:
                pass
        if _SD_CS is None:
            _SD_CS = DigitalInOut(SD_CS_PIN)
            _SD_CS.direction = Direction.OUTPUT
            _SD_CS.value = True
        if _SD_SDOBJ is None:
            _SD_SDOBJ = adafruit_sdcard.SDCard(_SD_SPI, _SD_CS)
        if _SD_VFS is None:
            _SD_VFS = storage.VfsFat(_SD_SDOBJ)
        storage.mount(_SD_VFS, SD_MOUNT)
        for d in (SD_LOG_DIR, SD_SAMPLE_DIR, SD_HEALTH_DIR, SD_CRASH_DIR, SD_STATE_DIR):
            _ensure_dir(d)
        SD_OK = True
        print("SD: mounted OK at /sd")
    except Exception as e:
        print("SD: mount exception:", e)
        _SD_SDOBJ = None
        _SD_VFS = None
        SD_OK = False
    return SD_OK


def sd_selftest(tag="boot"):
    try:
        if not SD_OK:
            print("SD selftest skipped; SD_OK is False")
            return
        try:
            print(" /sd ->", _os.listdir("/sd"))
        except Exception as e:
            print(" listdir(/sd) error:", e)
        print("SD selftest:", tag)
        for d in (SD_LOG_DIR, SD_SAMPLE_DIR, SD_HEALTH_DIR, SD_CRASH_DIR, SD_STATE_DIR):
            _ensure_dir(d)
        try:
            print(" /sd ->", _os.listdir("/sd"))
        except Exception as e:
            print(" listdir(/sd) error:", e)
        for d in (SD_LOG_DIR, SD_SAMPLE_DIR, SD_HEALTH_DIR, SD_CRASH_DIR, SD_STATE_DIR):
            try:
                print(" ", d, "->", _os.listdir(d))
            except Exception as e:
                print(" listdir(", d, ") error:", e)
        with open("/sd/selftest.txt", "a") as f:
            f.write(f"selftest {tag}\n")
        for d, name in ((SD_LOG_DIR,"_selftest.log"), (SD_SAMPLE_DIR,"_selftest.csv"), (SD_HEALTH_DIR,"_selftest.log"), (SD_CRASH_DIR,"_selftest.log"), (SD_STATE_DIR,"_selftest.txt")):
            try:
                with open(d + "/" + name, "a") as f:
                    f.write(f"{tag}\n")
            except Exception as e:
                print(" write sentinel error for", d, ":", e)
        print("SD selftest complete")
    except Exception as e:
        print("SD selftest fatal:", e)


def _sd_daily_path(base_dir, stem, ext):
    ts_iso = _now_iso_utc()
    if ts_iso:
        day = ts_iso[0:10]
    else:
        day = "mono_{:010d}".format(int(time.monotonic() // 86400))
    fname = f"{day}{stem}.{ext}"
    return base_dir + "/" + fname


def sd_log_lines(lines):
    if not SD_OK:
        return
    try:
        path = _sd_daily_path(SD_LOG_DIR, "", "log")
        with open(path, "a") as f:
            for line in lines:
                f.write(line + "\n")
    except Exception as e:
        print("SD: sd_log_lines error:", e)


def sd_log_sample(payload: dict):
    if not SD_OK:
        return
    try:
        ts = payload.get("rtc_iso") or _now_iso_utc() or "{:.3f}".format(time.monotonic())
        row = [
            ts,
            str(payload.get("temperature_F")),
            str(payload.get("dewpoint_F")),
            str(payload.get("humidity")),
            str(payload.get("p_hpa")),
            str(payload.get("battery_percent")),
            str(payload.get("battery_voltage")),
            str(payload.get("wifi_attempts")),
            str(payload.get("wifi_connect_ms")),
            str(payload.get("mqtt_ms")),
            str(payload.get("active_ms")),
            str(payload.get("sleep_s")),
            str(payload.get("fw_version")),
            str(payload.get("heartbeat")),
            str(payload.get("fail_count")),
            str(payload.get("phase")),
        ]
        path = _sd_daily_path(SD_SAMPLE_DIR, "_samples", "csv")
        newfile = False
        try:
            _os.stat(path)
        except OSError:
            newfile = True
        with open(path, "a") as f:
            if newfile:
                f.write("ts,tempF,dewF,rh,slp_hpa,batt_pct,batt_v,wifi_attempts,wifi_connect_ms,mqtt_ms,active_ms,sleep_s,fw,hb,fail_count,phase\n")
            f.write(",".join(row) + "\n")
    except Exception as e:
        print("SD: sd_log_sample error:", e)


def sd_health(msg):
    if not SD_OK:
        return
    try:
        ts = _now_iso_utc() or "{:.3f}".format(time.monotonic())
        line = f"[{ts}] {msg}"
        path = _sd_daily_path(SD_HEALTH_DIR, "_health", "log")
        with open(path, "a") as f:
            f.write(line + "\n")
    except Exception as e:
        print("SD: sd_health error:", e)


def sd_crash(exc: Exception):
    if not SD_OK:
        return
    try:
        ts = _now_iso_utc() or "{:.3f}".format(time.monotonic())
        path = _sd_daily_path(SD_CRASH_DIR, "_crash", "log")
        with open(path, "a") as f:
            f.write(f"[{ts}] CRASH: {repr(exc)}\n")
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=f)
    except Exception as e:
        print("SD: sd_crash error:", e)


def _epoch_from_yyyymmdd(day_str):
    try:
        yyyy, mm, dd = int(day_str[0:4]), int(day_str[5:7]), int(day_str[8:10])
        t = time.struct_time((yyyy, mm, dd, 0, 0, 0, -1, -1, -1))
        return int(time.mktime(t))
    except Exception:
        return None


def _prune_sd(now_ep: int):
    if not SD_OK or not now_ep:
        return
    try:
        def prune_dir(d):
            try:
                for name in _os.listdir(d):
                    day = None
                    if name.startswith("mono_"):
                        continue
                    if len(name) >= 10 and name[4] == '-' and name[7] == '-':
                        day = name[0:10]
                    if not day:
                        continue
                    day_ep = _epoch_from_yyyymmdd(day)
                    if day_ep is None:
                        continue
                    if (now_ep - day_ep) > LOG_RETENTION_S:
                        try:
                            _os.remove(d + "/" + name)
                        except Exception:
                            pass
            except Exception:
                pass
        for d in (SD_LOG_DIR, SD_SAMPLE_DIR, SD_HEALTH_DIR, SD_CRASH_DIR):
            prune_dir(d)
    except Exception:
        pass

# ---------- Logging ----------

def log_message(msg, mqtt=None):
    ts_mon = time.monotonic()
    ts_iso = _now_iso_utc()
    line = f"[{ts_mon:0.1f}s {ts_iso}]" if ts_iso else f"[{ts_mon:0.1f}s]"
    line += f" {msg}"
    print(line)
    LOG_BUFFER.append(line)
    if len(LOG_BUFFER) > _LOG_MAX:
        LOG_BUFFER.pop(0)
    try:
        if SD_OK:
            sd_log_lines([line])
    except Exception:
        pass


def debug(msg):
    if DEBUG:
        log_message("DBG: " + msg)

# ---- Load secrets ----

try:
    from secrets import secrets
except Exception as e:
    raise RuntimeError("WiFi/MQTT secrets are in secrets.py") from e

WIFI_SSID = getenv("CIRCUITPY_WIFI_SSID") or secrets.get("ssid")
WIFI_PASS = getenv("CIRCUITPY_WIFI_PASSWORD") or secrets.get("password")
MQTT_BROKER = secrets.get("mqtt_broker")
MQTT_PORT   = int(secrets.get("mqtt_port", 1883))
MQTT_USER   = secrets.get("mqtt_username") or None
MQTT_PASS   = secrets.get("mqtt_password") or None

# ---------- BME280 (forced mode) ----------

MODE_SLEEP, MODE_FORCED = 0x00, 0x01
OVERSCAN_X1 = 0x01
IIR_FILTER_DISABLE = 0x00

# ---------- Watchdog helpers ----------

class WatchdogMgr:
    def __init__(self):
        self.active = False

    def start(self, timeout_s):
        """Arm/resize WD in RESET mode and feed. No-ops if disabled."""
        if not ENABLE_WATCHDOG:
            self.active = False
            return
        try:
            WDT.timeout = int(timeout_s)
            WDT.mode = WatchDogMode.RESET
            WDT.feed()
            self.active = True
        except Exception:
            self.active = False

    def feed(self):
        if not self.active:
            return
        try:
            WDT.feed()
        except Exception:
            pass

    def park_raise(self):
        """Optionally switch to RAISE for short awake sections (only if active)."""
        if not self.active:
            return
        try:
            WDT.mode = WatchDogMode.RAISE
        except Exception:
            pass

    def set_for_sleep(self, sleep_s, safety_margin_s=10, min_timeout_s=30):
        """Before sleep: extend timeout and keep RESET mode (only if active)."""
        if not self.active:
            return
        try:
            t = max(int(math.ceil(sleep_s)) + safety_margin_s, min_timeout_s)
            WDT.timeout = t
            WDT.mode = WatchDogMode.RESET
            WDT.feed()
        except Exception:
            pass

    def disable_if_supported(self):
        """Hook for ports that expose a real disable/deinit."""
        if not self.active:
            return
        try:
            # WDT.deinit()  # enable if your port supports it
            self.active = False
        except Exception:
            pass

WD = WatchdogMgr()

def _wdt_sleep(seconds):
    if seconds <= 0:
        return
    end = time.monotonic() + float(seconds)
    while True:
        now = time.monotonic()
        if now >= end:
            break
        WD.feed()
        chunk = end - now
        if chunk > 1:
            chunk = 1
        time.sleep(chunk)

# ---------- I2C helpers ----------

def _bus_stuck():
    try:
        global i2c
        if i2c:
            try: i2c.deinit()
            except Exception: pass
        sda = DigitalInOut(board.SDA); sda.direction = Direction.INPUT; sda.pull = Pull.UP
        scl = DigitalInOut(board.SCL); scl.direction = Direction.INPUT; scl.pull = Pull.UP
        stuck = (not sda.value)
        sda.deinit(); scl.deinit()
        return stuck
    except Exception:
        return False


def _raw_i2c_new():
    return BusI2C(board.SCL, board.SDA, frequency=I2C_FREQ_HZ)


def _try_bme_soft_reset_raw(bus):
    try:
        for a in (0x76, 0x77):
            try:
                bus.writeto(a, bytes((0xE0, 0xB6)))
            except Exception:
                pass
        debug("BME soft reset issued")
    except Exception:
        pass


def recover_i2c_bus():
    debug("I2C recover: starting")
    try:
        try:
            if i2c:
                try: i2c.deinit()
                except Exception: pass
        except Exception:
            pass
        scl = DigitalInOut(board.SCL)
        sda = DigitalInOut(board.SDA)
        scl.direction = Direction.OUTPUT
        sda.direction = Direction.INPUT
        sda.pull = Pull.UP
        for _ in range(16):
            if sda.value:
                break
            scl.value = True;  time.sleep(0.00015)
            scl.value = False; time.sleep(0.00015)
        scl.value = True
        time.sleep(0.0002)
        scl.deinit(); sda.deinit()
        time.sleep(0.005)
        new_bus = _raw_i2c_new()
        debug("I2C recover: success")
        return new_bus
    except Exception as e:
        log_message(f"I2C recovery failed: {e}")
        try:
            return _raw_i2c_new()
        except Exception:
            return None


def ensure_hw_ready():
    global i2c, bme, rtc, max17048, RTC_PRESENT, HAVE_BATTERY
    if i2c and bme:
        return
    debug("HW init: starting")
    if _bus_stuck():
        debug("HW init: bus appears stuck, attempting recovery")
        i2c_new = recover_i2c_bus()
        if i2c_new is None:
            raise RuntimeError("I2C recover failed before init")
        i2c = i2c_new
    else:
        i2c = _raw_i2c_new()
    time.sleep(0.01)
    if SLEEP_MODE != 'none':
        _try_bme_soft_reset_raw(i2c)
        time.sleep(0.01)
    try:
        _bme = Adafruit_BME280_I2C(i2c)
        _bme.iir_filter = IIR_FILTER_DISABLE
        _bme.overscan_humidity = OVERSCAN_X1
        _bme.overscan_temperature = OVERSCAN_X1
        _bme.overscan_pressure = OVERSCAN_X1
        _bme.mode = MODE_SLEEP
        _ = _bme.sea_level_pressure
        bme = _bme
        debug("HW init: BME280 OK")
    except Exception as e:
        bme = None
        log_message(f"HW init: BME280 failed: {e}")
    try:
        _rtc = adafruit_ds3231.DS3231(i2c)
        rtc = _rtc
        RTC_PRESENT = True
        debug("HW init: DS3231 OK")
    except Exception as e:
        rtc = None
        RTC_PRESENT = False
        log_message(f"HW init: DS3231 not found: {e}")
    try:
        _max = adafruit_max1704x.MAX17048(i2c)
        max17048 = _max
        HAVE_BATTERY = True
        debug("HW init: MAX17048 OK")
    except Exception as e:
        max17048 = None
        HAVE_BATTERY = False
        log_message(f"HW init: MAX17048 not found: {e}")
    if not bme:
        debug("HW init: attempting one more I2C recovery + BME reprobe")
        i2c_new = recover_i2c_bus()
        if i2c_new:
            i2c = i2c_new
            try:
                _try_bme_soft_reset_raw(i2c)
                time.sleep(0.01)
                _bme = Adafruit_BME280_I2C(i2c)
                _bme.iir_filter = IIR_FILTER_DISABLE
                _bme.overscan_humidity = OVERSCAN_X1
                _bme.overscan_temperature = OVERSCAN_X1
                _bme.overscan_pressure = OVERSCAN_X1
                _bme.mode = MODE_SLEEP
                bme = _bme
                debug("HW init: BME280 OK after recovery")
            except Exception as e:
                log_message(f"HW init: BME280 reprobe failed: {e}")
    if not bme:
        raise RuntimeError("BME280 unavailable after init")
    debug("HW init: done")

# ---------- Met calcs ----------

def _sat_vapor_pressure_hpa(temp_c):
    return 6.112 * math.exp((17.62 * temp_c) / (243.12 + temp_c))


def sea_level_pressure_hpa(abs_press_hpa, alt_m, temp_c=None, rh_pct=None):
    try:
        if (temp_c is not None) and (rh_pct is not None) and (0.0 <= rh_pct <= 100.0):
            p = float(abs_press_hpa) * 100.0
            T = float(temp_c) + 273.15
            es = _sat_vapor_pressure_hpa(temp_c) * 100.0
            e = max(0.0, min(es, (rh_pct/100.0)*es))
            eps = 0.622
            denom = max(1.0, p - e)
            r = eps * e / denom
            Tv = T * (1.0 + 0.61 * (r/(1.0+r)))
            g = 9.80665
            R = 287.05
            p0 = p * math.exp((g * float(alt_m)) / (R * Tv))
            return max(80000.0, min(110000.0, p0)) / 100.0
    except Exception:
        pass
    return abs_press_hpa / ((1 - (float(alt_m) / 44330.0)) ** 5.255)


def dewpoint_c(temp_c, rh_pct):
    a, b = 17.62, 243.12
    rh = max(0.01, min(100.0, rh_pct))
    gamma = (a * temp_c) / (b + temp_c) + math.log(rh / 100.0)
    return (b * gamma) / (a - gamma)


def heat_index_f(temp_f, rh_pct):
    T, R = temp_f, rh_pct
    if T < 80 or R < 40:
        return T
    HI = (-42.379 + 2.04901523*T + 10.14333127*R - 0.22475541*T*R - 6.83783e-3*T*T - 5.481717e-2*R*R + 1.22874e-3*T*T*R + 8.5282e-4*T*R*R - 1.99e-6*T*T*R*R)
    if R < 13 and 80 <= T <= 112:
        HI -= ((13 - R)/4)*math.sqrt((17 - abs(T - 95))/17)
    elif R > 85 and 80 <= T <= 87:
        HI += 0.02*(R - 85)*(87 - T)
    return HI

# ---------- NVM helpers ----------
# 0..3: heartbeat (u32)
# 4..5: fail_count (u16)
# 6..7: cycles_since_rtc_sync (u16)

def _get_counters():
    try:
        hb = int.from_bytes(microcontroller.nvm[0:4], "little")
        fc = int.from_bytes(microcontroller.nvm[4:6], "little")
        cs = int.from_bytes(microcontroller.nvm[6:8], "little")
        return hb, fc, cs
    except Exception:
        return 0, 0, 0


def _set_counters(hb, fc, cs):
    try:
        microcontroller.nvm[0:4] = int(hb & 0xFFFFFFFF).to_bytes(4, "little")
        microcontroller.nvm[4:6] = int(fc & 0xFFFF).to_bytes(2, "little")
        microcontroller.nvm[6:8] = int(cs & 0xFFFF).to_bytes(2, "little")
    except Exception as e:
        log_message(f"NVM write counters err: {e}")


def _flush_heartbeat(hb=None):
    if hb is None:
        hb, fc, cs = _get_counters()
    else:
        _, fc, cs = _get_counters()
    _set_counters(hb, fc, cs)


def _inc_heartbeat():
    hb, fc, cs = _get_counters()
    hb = (hb + 1) & 0xFFFFFFFF
    if (hb % _HEARTBEAT_FLUSH_INTERVAL) == 0:
        _set_counters(hb, fc, cs)
    return hb


def _inc_fail_count():
    hb, fc, cs = _get_counters()
    fc = (fc + 1) & 0xFFFF
    _set_counters(hb, fc, cs)
    return fc


def _reset_fail_count():
    hb, _, cs = _get_counters()
    _set_counters(hb, 0, cs)


def _get_heartbeat_and_fails():
    hb, fc, _ = _get_counters()
    return hb, fc


def _get_cycles_since_sync():
    _, _, cs = _get_counters()
    return cs


def _set_cycles_since_sync(cs):
    hb, fc, _ = _get_counters()
    _set_counters(hb, fc, cs & 0xFFFF)


def _inc_cycles_since_sync():
    hb, fc, cs = _get_counters()
    cs = (cs + 1) & 0xFFFF
    _set_counters(hb, fc, cs)
    return cs

# ---------- Networking helpers ----------

def _http_session(pool):
    ctx = ssl.create_default_context()
    return adafruit_requests.Session(pool, ctx)

# ---------- TZ + Local time helpers ----------

def _tz_cache_valid():
    try:
        return _TZ_CACHE and isinstance(_TZ_CACHE.get("utc_offset_sec", None), int) and isinstance(_TZ_CACHE.get("tz", None), str)
    except Exception:
        return False


def _read_json_file(path):
    try:
        with open(path, "r") as f:
            return json.loads(f.read())
    except Exception:
        return None


def _write_json_file(path, obj):
    if not SD_OK:
        return
    try:
        with open(path, "w") as f:
            f.write(json.dumps(obj))
    except Exception:
        pass


def fetch_tz_if_needed(pool, force=False):
    global _TZ_CACHE
    if not force and _tz_cache_valid():
        return True
    # 1) Try WorldTimeAPI (with one retry)
    try:
        sess = _http_session(pool)
        url = f"https://worldtimeapi.org/api/timezone/{LOCAL_TZ}"
        debug("WorldTimeAPI fetch: " + LOCAL_TZ)
        last_exc = None
        for attempt in range(2):
            try:
                r = sess.get(url, timeout=12)
                data = r.json(); r.close()
                off = data.get("utc_offset", "+00:00")
                sign = -1 if off.startswith("-") else 1
                parts = off[1:].split(":")
                hh = int(parts[0]); mm = int(parts[1]); ss = int(parts[2]) if len(parts) > 2 else 0
                utc_off_sec = sign * (hh*3600 + mm*60 + ss)
                abbr = data.get("abbreviation", "")
                _TZ_CACHE = {"tz": LOCAL_TZ, "utc_offset_sec": utc_off_sec, "abbrev": abbr, "dst": bool(data.get("dst", False))}
                _write_json_file(TZ_FILE, _TZ_CACHE)
                sd_health(f"TZ updated {LOCAL_TZ} off={utc_off_sec} abbr={abbr}")
                return True
            except Exception as e:
                last_exc = e
                time.sleep(1.5)
        log_message(f"WorldTimeAPI fetch failed: {last_exc}")
    except Exception as e:
        log_message(f"WorldTimeAPI session error: {e}")

    # 2) Fallback: Use OWM timezone seconds if available
    try:
        if OW_API_KEY and OW_LAT and OW_LON:
            sess = _http_session(pool)
            url = ("https://api.openweathermap.org/data/2.5/weather?lat="
                   + str(OW_LAT) + "&lon=" + str(OW_LON) + "&appid=" + str(OW_API_KEY))
            r = sess.get(url, timeout=10)
            data = r.json(); r.close()
            utc_off_sec = int(data.get("timezone", 0))
            _TZ_CACHE = {"tz": LOCAL_TZ, "utc_offset_sec": utc_off_sec, "abbrev": "", "dst": None}
            _write_json_file(TZ_FILE, _TZ_CACHE)
            sd_health(f"TZ fallback via OWM offset={utc_off_sec}")
            return True
    except Exception as e:
        log_message(f"TZ fallback via OWM failed: {e}")

    # 3) Last resort: cached file
    cache = _read_json_file(TZ_FILE)
    if cache and isinstance(cache.get("utc_offset_sec", None), int):
        debug("Using cached TZ from disk")
        _TZ_CACHE = cache
        return True
    return False


def _local_from_utc_epoch(utc_ep):
    """Return (local_epoch, local_tuple, local_date_str 'YYYY-MM-DD') or (None, None, None)."""
    try:
        if utc_ep is None:
            return None, None, None
        off = int(_TZ_CACHE.get("utc_offset_sec", 0)) if _TZ_CACHE else 0
        loc = int(utc_ep + off)
        tt = time.localtime(loc)  # localtime() treats value as epoch seconds (no TZ awareness)
        date_str = "{:04}-{:02}-{:02}".format(tt.tm_year, tt.tm_mon, tt.tm_mday)
        return loc, tt, date_str
    except Exception:
        return None, None, None


def _tz_offset_str():
    """Return '+HH:MM' or '-HH:MM' from _TZ_CACHE; defaults to '+00:00'."""
    try:
        off = int(_TZ_CACHE.get("utc_offset_sec", 0)) if _TZ_CACHE else 0
        sign = "+" if off >= 0 else "-"
        off = abs(off)
        hh, rem = divmod(off, 3600)
        mm = rem // 60
        return f"{sign}{hh:02}:{mm:02}"
    except Exception:
        return "+00:00"


def _format_local_display(tt):
    """
    Build 'YYYY-MM-DD HH:MM:SS TZ (-05:00)' from a local time tuple and _TZ_CACHE.
    Falls back cleanly if abbr is missing.
    """
    try:
        date_part = f"{tt.tm_year:04}-{tt.tm_mon:02}-{tt.tm_mday:02} {tt.tm_hour:02}:{tt.tm_min:02}:{tt.tm_sec:02}"
        abbr = ""
        try:
            abbr = _TZ_CACHE.get("abbrev") or ""
        except Exception:
            pass
        off = _tz_offset_str()
        return f"{date_part} {abbr} ({off})" if abbr else f"{date_part} ({off})"
    except Exception:
        return None

# ---------- OpenWeather sunrise/sunset (daily @ 03:00 local) ----------

def _sun_cache_valid_for_today(local_date):
    try:
        return _SUN_CACHE and _SUN_CACHE.get("local_day") == local_date
    except Exception:
        return False


def _load_sun_from_disk():
    global _SUN_CACHE
    s = _read_json_file(SUN_FILE)
    if s and s.get("sunrise_local_ep") and s.get("sunset_local_ep"):
        _SUN_CACHE = s
        return True
    return False


def _save_sun_to_disk():
    if _SUN_CACHE:
        _write_json_file(SUN_FILE, _SUN_CACHE)
        try:
            _write_json_file(SUN_LAST_FILE, {"last": _SUN_CACHE.get("fetched_local_ep", 0)})
        except Exception:
            pass


def _need_sun_refresh(now_local_ep, now_local_date, force_if_missing=False):
    """True if cache not for 'today' and it's after the refresh hour,
       or immediately if force_if_missing=True."""
    if not NIGHT_MODE_ENABLED:
        return False
    try:
        if now_local_ep is None or now_local_date is None:
            return False
        # If cache is already for today, no refresh needed
        if _sun_cache_valid_for_today(now_local_date):
            return False
        if force_if_missing:
            return True
        # Otherwise, refresh only after local 03:00
        tt = time.localtime(now_local_ep)
        return tt.tm_hour >= OW_REFRESH_LOCAL_HOUR
    except Exception:
        return False


def refresh_sun_times_if_due(pool, now_utc_ep, force_if_missing=False):
    """Compute local time, decide whether to refresh, and update cache.
       Requires _TZ_CACHE to be valid. Returns True if sun-cache valid for today."""
    global _TZ_CACHE, _SUN_CACHE
    if not NIGHT_MODE_ENABLED:
        return False
    if not (OW_API_KEY and OW_LAT and OW_LON):
        return False
    loc_ep, loc_tt, loc_date = _local_from_utc_epoch(now_utc_ep)
    if loc_ep is None:
        return False
    if _sun_cache_valid_for_today(loc_date):
        return True
    if not _need_sun_refresh(loc_ep, loc_date, force_if_missing=force_if_missing):
        return _sun_cache_valid_for_today(loc_date)
    # Fetch from OWM
    try:
        debug("Fetching sunrise/sunset via OpenWeatherMap")
        sess = _http_session(pool)
        url = ("https://api.openweathermap.org/data/2.5/weather?lat="
               + str(OW_LAT) + "&lon=" + str(OW_LON) + "&appid=" + str(OW_API_KEY))
        r = sess.get(url, timeout=15)
        data = r.json(); r.close()
        # Opportunistic TZ set from OWM if needed
        try:
            if (not _tz_cache_valid()) and ("timezone" in data):
                _TZ_CACHE = {"tz": LOCAL_TZ, "utc_offset_sec": int(data.get("timezone", 0)), "abbrev": "", "dst": None}
                _write_json_file(TZ_FILE, _TZ_CACHE)
                sd_health("TZ opportunistically set from OWM")
        except Exception:
            pass
        sysd = data.get("sys", {})
        rise_utc = int(sysd.get("sunrise", 0)); set_utc = int(sysd.get("sunset", 0))
        if rise_utc <= 0 or set_utc <= 0:
            raise ValueError("OWM sunrise/sunset missing")
        rise_loc_ep, rise_tt, _ = _local_from_utc_epoch(rise_utc)
        set_loc_ep, set_tt, _  = _local_from_utc_epoch(set_utc)
        _SUN_CACHE = {
            "sunrise_utc": rise_utc,
            "sunset_utc": set_utc,
            "sunrise_local_ep": rise_loc_ep,
            "sunset_local_ep": set_loc_ep,
            "sunrise_local_iso": _format_local_display(rise_tt),
            "sunset_local_iso":  _format_local_display(set_tt),
            "fetched_local_ep": loc_ep,
            "tz_abbr": _TZ_CACHE.get("abbrev") if _TZ_CACHE else None,
            "utc_offset_sec": _TZ_CACHE.get("utc_offset_sec") if _TZ_CACHE else 0,
            "local_day": loc_date,
        }
        _save_sun_to_disk()
        log_message(f"Sun cache set: sunrise={rise_utc} sunset={set_utc} local_day={loc_date}")
        sd_health("Sun times refreshed from OpenWeatherMap")
        return True
    except Exception as e:
        log_message(f"OpenWeatherMap fetch failed: {e}")
        # Try disk cache (may be stale)
        return _load_sun_from_disk()


def is_night_now(now_utc_ep):
    if not NIGHT_MODE_ENABLED:
        return False
    try:
        loc_ep, _, loc_date = _local_from_utc_epoch(now_utc_ep)
        if not _sun_cache_valid_for_today(loc_date):
            # If cache is stale, be conservative: assume day (so device is more active)
            debug("Night eval: sun cache not for today — defaulting to day")
            return False
        s_r = int(_SUN_CACHE.get("sunrise_local_ep", 0))
        s_s = int(_SUN_CACHE.get("sunset_local_ep", 0))
        night = (loc_ep >= s_s) or (loc_ep < s_r)
        log_message(f"Night eval(local): now_ep={loc_ep} sunrise={s_r} sunset={s_s} night={night}")
        return night
    except Exception as e:
        log_message(f"Night eval error: {e}")
        return False

# ---------- Device inventory (retained) ----------

def _hex(b):
    try:
        return binascii.hexlify(b).decode().upper()
    except Exception:
        return None


def collect_device_info():
    info = {
        "client_id": CLIENT_ID,
        "topic_base": TOPIC_BASE,
        "platform": getattr(sys, "platform", None),
        "cp_version": getattr(sys, "version", None),
        "implementation": str(getattr(sys, "implementation", "")),
        "board_id": getattr(board, "board_id", None),
        "uid": None, "mac": None, "ip": None, "rssi": None,
        "reset_reason": str(getattr(microcontroller, "reset_reason", "")),
        "fw_version": FW_VERSION,
        "phase": _PHASE,
    }
    try:
        u = _os.uname()
        info["machine"] = getattr(u, "machine", None)
        info["release"] = getattr(u, "release", None)
        info["version_str"] = getattr(u, "version", None)
    except Exception:
        pass
    try: info["uid"] = _hex(microcontroller.cpu.uid)
    except Exception: pass
    try:
        info["mac"] = ":".join(f"{b:02X}" for b in wifi.radio.mac_address)
        info["ip"]  = str(wifi.radio.ipv4_address)
        try: info["rssi"] = int(getattr(wifi.radio.ap_info, "rssi", None))
        except Exception: pass
    except Exception:
        pass
    return info

# ----- Hash helpers -----

def _hash_bytes(h, b):
    for x in b:
        h ^= x
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def _hash_any(h, v):
    t = type(v)
    if v is None: return _hash_bytes(h, b"N")
    if t is bool: return _hash_bytes(h, b"T" if v else b"F")
    if t in (int, float, str): return _hash_bytes(h, ("S"+str(v)).encode("utf-8"))
    if isinstance(v, dict):
        h = _hash_bytes(h, b"D")
        for k in sorted(v.keys()):
            h = _hash_bytes(h, ("K"+str(k)).encode("utf-8"))
            h = _hash_any(h, v[k])
        return h
    if isinstance(v, (list, tuple)):
        h = _hash_bytes(h, b"L")
        for item in v:
            h = _hash_any(h, item)
        return h
    return _hash_bytes(h, ("X"+str(v)).encode("utf-8"))


def _hash_dict(d):
    h = 0x811C9DC5
    for k in sorted(d.keys()):
        h = _hash_bytes(h, ("K"+str(k)).encode("utf-8"))
        h = _hash_any(h, d[k])
    return h


def _read_text_file(path):
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception:
        return None


def _read_hex32(path):
    t = _read_text_file(path)
    if not t: return None
    try: return int(t, 16)
    except Exception: return None

# ---------- Retained device info ----------

def publish_device_info_retained(mqtt_client):
    try:
        info = collect_device_info()
        stable = {k: v for k, v in info.items() if k not in ("rssi", "ip", "phase")}
        new_h = _hash_dict(stable)
        # Back-compat: these were historically plain-text files.
        old_h = _read_hex32(DEVICE_INFO_HASH_FILE)
        now_ep = _now_epoch_utc()
        last_ep_txt = _read_text_file(DEVICE_INFO_LAST_FILE)
        last_ep = int(last_ep_txt) if last_ep_txt else 0
        should_publish = (old_h is None) or (new_h != old_h)
        if not should_publish and now_ep is not None:
            should_publish = (now_ep - last_ep) >= DEVICE_INFO_REFRESH_S
        if not should_publish:
            debug("device_info unchanged; skip retained publish")
            mqtt_client.publish(DEVICE_AVAIL_TOPIC, "online", qos=0, retain=True)
            return
        payload = json.dumps(info)
        mqtt_client.publish(DEVICE_INFO_TOPIC, payload, qos=0, retain=True)
        mqtt_client.publish(DEVICE_AVAIL_TOPIC, "online", qos=0, retain=True)
        # Write plain text so future boots can read them with existing helpers
        try:
            with open(DEVICE_INFO_HASH_FILE, "w") as f:
                f.write("{:08X}".format(new_h))
        except Exception:
            pass
        if now_ep is not None:
            try:
                with open(DEVICE_INFO_LAST_FILE, "w") as f:
                    f.write(str(int(now_ep)))
            except Exception:
                pass
        log_message("device_info retained published")
    except Exception as e:
        log_message(f"device_info publish failed: {e}")

# ---------- Build payload ----------

def build_payload(extra_fields=None):
    debug("Payload build: sampling BME")
    # Sample BME in forced mode
    t_c, rh, p_abs = sample_bme_forced()
    p_sl = round(sea_level_pressure_hpa(p_abs, SITE_ALT_M, temp_c=t_c, rh_pct=rh), 2)
    t_c = round(t_c, 2); t_f = round((t_c*9/5)+32, 2)
    rh = round(rh, 2)
    dp_c = round(dewpoint_c(t_c, rh), 2); dp_f = round((dp_c*9/5)+32, 2)
    hi_f = round(heat_index_f(t_f, rh), 2); hi_c = round((hi_f-32)*5/9, 2)

    batt_pct = batt_v = batt_low = None
    if HAVE_BATTERY and max17048:
        try:
            batt_pct = round(max17048.cell_percent, 1)
            batt_v   = round(max17048.cell_voltage, 3)
            batt_low = (batt_pct <= BATTERY_LOW_THRESHOLD)
        except Exception as e:
            log_message(f"Battery read failed: {e}")
    if batt_low is None: batt_low = False

    hb, fc = _get_heartbeat_and_fails()

    rtc_iso = None; rtc_ep = _now_epoch_utc()
    try:
        if RTC_PRESENT and rtc:
            tm = rtc.datetime
            rtc_iso = _format_iso_utc(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
    except Exception as e:
        log_message(f"RTC read failed: {e}")

    # Night mode eval (LOCAL time)
    night_flag = False
    local_date_str = None
    sunrise_iso = None
    sunset_iso = None
    if fetch_tz_ok:
        loc_ep, _, local_date_str = _local_from_utc_epoch(rtc_ep)
        if _SUN_CACHE:
            sunrise_iso = _SUN_CACHE.get("sunrise_local_iso")
            sunset_iso  = _SUN_CACHE.get("sunset_local_iso")
        night_flag = is_night_now(rtc_ep)

    cs = _get_cycles_since_sync()
    rtc_sync_age_s = int(cs * PUBLISH_PERIOD_SEC)
    rtc_recently_synced = bool(rtc_sync_age_s <= RTC_RESYNC_S)

    payload = {
        "temperature_C": t_c, "temperature_F": t_f,
        "dewpoint_C": dp_c, "dewpoint_F": dp_f,
        "heat_index_C": hi_c, "heat_index_F": hi_f,
        "p_hpa": p_sl, "humidity": rh,
        "battery_percent": batt_pct, "battery_voltage": batt_v, "battery_low": batt_low,
        "client_id": CLIENT_ID, "uptime_s": int(time.monotonic()),
        "heartbeat": hb, "fail_count": fc,
        "rtc_iso": rtc_iso, "rtc_epoch": rtc_ep,
        "rtc_sync_age_s": rtc_sync_age_s, "rtc_recently_synced": rtc_recently_synced,
        "last_error": str(LAST_ERROR or ""),
        "reset_reason": str(getattr(microcontroller, "reset_reason", "")),
        "fw_version": FW_VERSION,
        "phase": "sample",
        # Night-mode extras (LOCAL)
        "night_mode": bool(night_flag),
        "local_date": local_date_str,
        "local_sunrise_iso": sunrise_iso,
        "local_sunset_iso": sunset_iso,
    }
    if isinstance(extra_fields, dict):
        payload.update(extra_fields)
    debug("Payload build: done")
    return payload

# ---------- Wi-Fi ----------

def ensure_wifi(total_attempts=6):
    delays = [2, 3, 5, 8, 10, 10]
    attempts_used = 0
    try:
        pix(COL_WIFI)
        try: wifi.radio.hostname = CLIENT_ID
        except Exception: pass
        wifi.radio.enabled = False; _wdt_sleep(0.3); wifi.radio.enabled = True
    except Exception as e:
        log_message(f"Radio toggle failed: {e}")

    for i in range(min(total_attempts, len(delays))):
        attempts_used = i + 1
        WD.feed()
        try:
            log_message(f"Wi-Fi attempt {attempts_used}/{total_attempts}")
            t0 = time.monotonic()
            wifi.radio.connect(WIFI_SSID, WIFI_PASS)
            conn_ms = int((time.monotonic() - t0) * 1000)
            log_message(f"Wi-Fi OK: {wifi.radio.ipv4_address} in {conn_ms} ms")
            return True, attempts_used, conn_ms
        except Exception as e:
            log_message(f"Wi-Fi failed: {e}")
            if i in (2, 4):
                try:
                    wifi.radio.enabled = False; _wdt_sleep(0.5); wifi.radio.enabled = True
                except Exception as e2:
                    log_message(f"Radio re-toggle failed: {e2}")
            _wdt_sleep(delays[i])
    return False, attempts_used, None


def _new_mqtt_client(pool):
    mqtt = MQTT(
        broker=MQTT_BROKER, port=MQTT_PORT,
        username=MQTT_USER, password=MQTT_PASS,
        client_id=CLIENT_ID, socket_pool=pool,
        is_ssl=False, keep_alive=MQTT_KEEPALIVE,
        socket_timeout=MQTT_SOCKET_TIMEOUT, recv_timeout=MQTT_RECV_TIMEOUT,
    )
    try:
        mqtt.will_set(f"{TOPIC_BASE}/status", "offline", qos=QOS, retain=True)
    except Exception as e:
        log_message(f"Failed to set MQTT will: {e}")
    return mqtt

# ---------- HA Discovery ----------

def publish_ha_discovery(mqtt_client):
    if not ENABLE_HA_DISCOVERY:
        return

    device = {
        "identifiers": [DEVICE_ID],
        "name": "Feather ESP32S3 BME280",
        "manufacturer": "Adafruit",
        "model": "Feather ESP32-S3 + BME280 + MAX17048",
        "sw_version": "CircuitPython",
    }
    expire_after = PUBLISH_PERIOD_SEC * 2

    sensors = [
        {"oid":"temperature_c","name":"BME280 Temperature","unit":"°C","dclass":"temperature","sclass":"measurement","tpl":"{{ value_json.temperature_C }}"},
        {"oid":"temperature_f","name":"BME280 Temperature (F)","unit":"°F","dclass":"temperature","sclass":"measurement","tpl":"{{ value_json.temperature_F }}"},
        {"oid":"humidity","name":"BME280 Humidity","unit":"%","dclass":"humidity","sclass":"measurement","tpl":"{{ value_json.humidity }}"},
        {"oid":"pressure","name":"BME280 Pressure (SLP)","unit":"hPa","dclass":"pressure","sclass":"measurement","tpl":"{{ value_json.p_hpa }}"},
        {"oid":"dewpoint_c","name":"BME280 Dew Point","unit":"°C","dclass":"temperature","sclass":"measurement","tpl":"{{ value_json.dewpoint_C }}"},
        {"oid":"dewpoint_f","name":"BME280 Dew Point (F)","unit":"°F","dclass":"temperature","sclass":"measurement","tpl":"{{ value_json.dewpoint_F }}"},
        {"oid":"heat_index_c","name":"BME280 Heat Index","unit":"°C","dclass":"temperature","sclass":"measurement","tpl":"{{ value_json.heat_index_C }}"},
        {"oid":"heat_index_f","name":"BME280 Heat Index (F)","unit":"°F","dclass":"temperature","sclass":"measurement","tpl":"{{ value_json.heat_index_F }}"},
        {"oid":"heartbeat","name":"Feather Heartbeat","unit":None,"dclass":None,"sclass":"total_increasing","tpl":"{{ value_json.heartbeat }}"},
        {"oid":"fail_count","name":"Feather Fail Count","unit":None,"dclass":None,"sclass":"measurement","tpl":"{{ value_json.fail_count }}"},
        {"oid":"rtc_iso","name":"Feather RTC ISO","unit":None,"dclass":"timestamp","sclass":None,"tpl":"{{ value_json.rtc_iso }}"},
        {"oid":"rtc_sync_age_s","name":"Feather RTC Sync Age","unit":"s","dclass":None,"sclass":"measurement","tpl":"{{ value_json.rtc_sync_age_s }}"},
        {"oid":"last_error","name":"Feather Last Error","unit":None,"dclass":None,"sclass":None,"tpl":"{{ value_json.last_error }}"},
        # Telemetry additions
        {"oid":"wifi_connect_ms","name":"WiFi Connect ms","unit":"ms","dclass":None,"sclass":"measurement","tpl":"{{ value_json.wifi_connect_ms }}"},
        {"oid":"mqtt_ms","name":"MQTT ms","unit":"ms","dclass":None,"sclass":"measurement","tpl":"{{ value_json.mqtt_ms }}"},
        {"oid":"active_ms","name":"Active ms","unit":"ms","dclass":None,"sclass":"measurement","tpl":"{{ value_json.active_ms }}"},
        {"oid":"active_s","name":"Active s","unit":"s","dclass":None,"sclass":"measurement","tpl":"{{ ((value_json.active_ms | default(0)) / 1000) | round(0) }}"},
        {"oid":"sleep_s","name":"Sleep s","unit":"s","dclass":None,"sclass":"measurement","tpl":"{{ value_json.sleep_s }}"},
        {"oid":"wifi_attempts","name":"WiFi Attempts","unit":None,"dclass":None,"sclass":"measurement","tpl":"{{ value_json.wifi_attempts }}"},
        # Local time sensors
        {"oid":"local_date","name":"Local Date","unit":None,"dclass":None,"sclass":None,"tpl":"{{ value_json.local_date }}"},
        {"oid":"local_sunrise","name":"Local Sunrise","unit":None,"dclass":None,"sclass":None,"tpl":"{{ value_json.local_sunrise_iso | default('') }}"},
        {"oid":"local_sunset","name":"Local Sunset","unit":None,"dclass":None,"sclass":None,"tpl":"{{ value_json.local_sunset_iso | default('') }}"},
    ]

    pairs = []
    for s in sensors:
        uid = f"{DEVICE_ID}_{s['oid']}"
        cfg_topic = f"{DISCOVERY_PREFIX}/sensor/{uid}/config"
        cfg = {
            "name": s["name"],
            "unique_id": uid,
            "availability_topic": f"{TOPIC_BASE}/status",
            "device": device,
            "expire_after": expire_after,
            "state_topic": f"{TOPIC_BASE}/state",
            "value_template": s["tpl"],
        }
        if s["unit"] is not None:  cfg["unit_of_measurement"] = s["unit"]
        if s["dclass"] is not None: cfg["device_class"] = s["dclass"]
        if s["sclass"] is not None: cfg["state_class"] = s["sclass"]
        if s["oid"] in ("heartbeat","fail_count","rtc_iso","rtc_sync_age_s","last_error","wifi_attempts","local_date","local_sunrise","local_sunset"):
            cfg["entity_category"] = "diagnostic"
        pairs.append((cfg_topic, cfg))

    # Device info sensor (attributes)
    uid = f"{DEVICE_ID}_device_info"
    pairs.append((
        f"{DISCOVERY_PREFIX}/sensor/{uid}/config",
        {
            "name": "Feather Device Info",
            "unique_id": uid,
            "availability_topic": f"{TOPIC_BASE}/status",
            "device": device,
            "entity_category": "diagnostic",
            "state_topic": DEVICE_AVAIL_TOPIC,
            "json_attributes_topic": DEVICE_INFO_TOPIC,
            "expire_after": expire_after,
        }
    ))

    # Firmware version sensor
    uid = f"{DEVICE_ID}_fw_version"
    pairs.append((
        f"{DISCOVERY_PREFIX}/sensor/{uid}/config",
        {
            "name": "Feather FW Version",
            "unique_id": uid,
            "availability_topic": f"{TOPIC_BASE}/status",
            "device": device,
            "entity_category": "diagnostic",
            "state_topic": f"{TOPIC_BASE}/state",
            "value_template": "{{ value_json.fw_version }}",
            "expire_after": expire_after,
        }
    ))

    # Binary sensors
    if RTC_PRESENT:
        uid = f"{DEVICE_ID}_rtc_recently_synced"
        pairs.append((
            f"{DISCOVERY_PREFIX}/binary_sensor/{uid}/config",
            {
                "name": "Feather RTC Synced (Recent)",
                "unique_id": uid,
                "availability_topic": f"{TOPIC_BASE}/status",
                "device": device,
                "device_class": "connectivity",
                "expire_after": expire_after,
                "entity_category": "diagnostic",
                "payload_on": "true", "payload_off": "false",
                "state_topic": f"{TOPIC_BASE}/state",
                "value_template": "{{ 'true' if (value_json.rtc_recently_synced | default(false)) else 'false' }}",
            }
        ))

    uid = f"{DEVICE_ID}_modem_sleep"
    pairs.append((
        f"{DISCOVERY_PREFIX}/binary_sensor/{uid}/config",
        {
            "name": "Feather Modem Sleep",
            "unique_id": uid,
            "availability_topic": f"{TOPIC_BASE}/status",
            "device": device,
            "device_class": "power",
            "expire_after": expire_after,
            "entity_category": "diagnostic",
            "payload_on": "true", "payload_off": "false",
            "state_topic": f"{TOPIC_BASE}/state",
            "value_template": "{{ 'true' if (value_json.modem_sleep | default(false)) else 'false' }}",
        }
    ))

    uid = f"{DEVICE_ID}_night_mode"
    pairs.append((
        f"{DISCOVERY_PREFIX}/binary_sensor/{uid}/config",
        {
            "name": "Feather Night Mode",
            "unique_id": uid,
            "availability_topic": f"{TOPIC_BASE}/status",
            "device": device,
            "device_class": "power",
            "expire_after": expire_after,
            "entity_category": "diagnostic",
            "payload_on": "true", "payload_off": "false",
            "state_topic": f"{TOPIC_BASE}/state",
            "value_template": "{{ 'true' if (value_json.night_mode | default(false)) else 'false' }}",
        }
    ))

    # Hash + conditional publish (same strategy)
    pairs.sort(key=lambda p: p[0])
    h = 0x811C9DC5
    for topic, cfg in pairs:
        h = _hash_bytes(h, ("TOPIC:"+topic).encode("utf-8"))
        h = _hash_any(h, cfg)

    h_prev = None
    try:
        prev_obj = _read_json_file(DISCOVERY_HASH_FILE)
        if prev_obj and "h" in prev_obj:
            h_prev = int(prev_obj["h"], 16)
    except Exception:
        pass

    now_epoch = _now_epoch_utc()
    last_epoch = None
    try:
        last_obj = _read_json_file(DISCOVERY_LAST_EPOCH_FILE)
        if last_obj and "t" in last_obj:
            last_epoch = int(last_obj["t"])
    except Exception:
        last_epoch = None

    force_daily = False
    if now_epoch is not None and (last_epoch is None or (now_epoch - last_epoch) >= DAILY_DISCOVERY_REFRESH_S):
        force_daily = True

    changed = (h_prev is None) or (h_prev != h)

    if not changed and not force_daily:
        debug("HA discovery unchanged; skipping publish")
        return

    for topic, cfg in pairs:
        mqtt_client.publish(topic, json.dumps(cfg), retain=True, qos=1)

    _write_json_file(DISCOVERY_HASH_FILE, {"h": "{:08X}".format(h)})
    if now_epoch is not None:
        _write_json_file(DISCOVERY_LAST_EPOCH_FILE, {"t": now_epoch})

    reason = "hash changed" if changed else "daily refresh"
    log_message(f"HA discovery published ({reason})")

# ---------- Sensor sampling ----------

def sample_bme_forced(max_tries=3):
    if not bme:
        ensure_hw_ready()
    last_exc = None
    for attempt in range(1, max_tries+1):
        try:
            bme.mode = MODE_SLEEP
            time.sleep(0.002)
            bme.mode = MODE_FORCED
            time.sleep(0.05)
            t_c = float(bme.temperature)
            rh = float(bme.humidity)
            p_hpa_abs = float(bme.pressure)
            bme.mode = MODE_SLEEP
            debug(f"BME sample OK on attempt {attempt}")
            return t_c, rh, p_hpa_abs
        except Exception as e:
            last_exc = e
            log_message(f"BME sample attempt {attempt}/{max_tries} failed: {e}")
            try:
                ensure_hw_ready()
            except Exception as e2:
                log_message(f"I2C/sensor reinit failed: {e2}")
            time.sleep(0.05)
    raise RuntimeError(f"BME forced sample failed after {max_tries} tries: {last_exc}")

# ---------- RTC sync (NTP) ----------

def _rtc_looks_unset():
    if not RTC_PRESENT or not rtc:
        return False
    try:
        tm = rtc.datetime
        return (tm.tm_year < 2020)
    except Exception:
        return True


def _weekday_sakamoto(y, m, d):
    t = [0,3,2,5,0,3,5,1,4,6,2,4]
    if m < 3:
        y -= 1
    return (y + y//4 - y//100 + y//400 + t[m-1] + d) % 7


def sync_rtc_via_ntp_if_needed(pool, force=False):
    if not RTC_PRESENT:
        return False
    need = force or _rtc_looks_unset() or (_get_cycles_since_sync() >= RESYNC_CYCLES)
    if not need:
        return False
    try:
        debug("RTC NTP sync: start")
        ntp = adafruit_ntp.NTP(pool, server="time.google.com", tz_offset=0)
        tm = ntp.datetime
        w_sun0 = _weekday_sakamoto(tm.tm_year, tm.tm_mon, tm.tm_mday)
        w_mon0 = (w_sun0 - 1) % 7
        rtc.datetime = time.struct_time((tm.tm_year, tm.tm_mon, tm.tm_mday,
                                         tm.tm_hour, tm.tm_min, tm.tm_sec,
                                         w_mon0, -1, -1))
        _set_cycles_since_sync(0)
        log_message("RTC set via NTP")
        sd_health("RTC synced via NTP")
        _prune_sd(_now_epoch_utc())
        return True
    except Exception as e:
        log_message(f"NTP sync failed: {e}")
        return False

# ---------- MQTT connect/publish with retries ----------

def mqtt_publish_with_retries(payload, max_attempts=3):
    global LAST_ERROR
    LAST_ERROR = ""
    pool = socketpool.SocketPool(wifi.radio)
    last_err = None
    for attempt in range(1, max_attempts+1):
        WD.feed()
        try:
            mqtt = _new_mqtt_client(pool)
            _wdt_sleep(0.3)
            t0 = time.monotonic()
            mqtt.connect()
            payload["mqtt_ms"] = int((time.monotonic() - t0) * 1000)
            mqtt.publish(f"{TOPIC_BASE}/status", "online", retain=True, qos=QOS)
            publish_device_info_retained(mqtt)
            publish_ha_discovery(mqtt)
            mqtt.publish(f"{TOPIC_BASE}/state", json.dumps(payload), qos=QOS, retain=False)
            log_message("Publish complete", mqtt=mqtt)
            try: mqtt.disconnect()
            except Exception: pass
            return True
        except Exception as e:
            last_err = e
            LAST_ERROR = repr(e)
            log_message(f"MQTT attempt {attempt}/{max_attempts} failed: {e}")
            _wdt_sleep(2 * attempt)
    log_message(f"MQTT publish failed after {max_attempts} attempts: {last_err}")

    try:
        pool2 = socketpool.SocketPool(wifi.radio)
        mqtt2 = _new_mqtt_client(pool2)
        mqtt2.connect()
        mqtt2.publish(f"{TOPIC_BASE}/last_debug_log", "\n".join(LOG_BUFFER[-12:]), qos=0, retain=True)
        mqtt2.publish(f"{TOPIC_BASE}/last_error", repr(last_err) if last_err else "", qos=0, retain=True)
        mqtt2.disconnect()
    except Exception:
        pass

    return False


def publish_offline_status():
    try:
        pool = socketpool.SocketPool(wifi.radio)
        mqtt = _new_mqtt_client(pool)
        mqtt.connect()
        mqtt.publish(f"{TOPIC_BASE}/status", "offline", retain=True, qos=QOS)
        mqtt.publish(DEVICE_AVAIL_TOPIC, "offline", retain=True, qos=0)
        mqtt.disconnect()
        log_message("Published retained offline before long sleep")
    except Exception as e:
        log_message(f"Could not publish offline before long sleep: {e}")

# ---------- Single publish cycle ----------

def publish_once():
    global fetch_tz_ok
    cycle_t0 = time.monotonic()
    WD.start(WDT_TIMEOUT_S); WD.feed()

    # Mount SD (idempotent)
    print("BOOT: about to mount SD…")
    try:
        if not sd_mount():
            print("SD: mount failed (fail-open), continuing without SD")
    except Exception as _e:
        print("SD: init error:", _e)

    # Widen WDT during active cycle
    WD.start(60)

    wifi.radio.enabled = True
    ok_wifi, attempts, wifi_ms = ensure_wifi(total_attempts=6)
    extra_fields = {"wifi_attempts": attempts, "wifi_connect_ms": wifi_ms}
    if not ok_wifi:
        log_message("Wi-Fi unavailable after retries, skipping"); pix(COL_ERROR)
        WD.park_raise(); return False

    WD.feed(); _wdt_sleep(WIFI_SETTLE_SEC)

    # Hardware & RTC
    ensure_hw_ready()
    pool = socketpool.SocketPool(wifi.radio)
    sync_rtc_via_ntp_if_needed(pool, force=False)

    # TZ first (needed for all local-time logic)
    fetch_tz_ok = fetch_tz_if_needed(pool, force=False)

    # If after 03:00 local and cache not today → refresh sun times (decoupled from NTP)
    now_utc_ep = _now_epoch_utc()
    if fetch_tz_ok:
        # Fetch immediately if today's cache is missing (even before 03:00),
        # and on subsequent days let the 03:00 rule handle it.
        refresh_sun_times_if_due(pool, now_utc_ep, force_if_missing=True)

    # Night/day flags and planned sleep (computed with LOCAL times)
    night_flag = is_night_now(now_utc_ep) if fetch_tz_ok else False
    extra_fields.update({
        "night_mode": night_flag,
        "modem_sleep": (SLEEP_MODE == 'light'),
    })

    # Compute planned sleep now so HA sees it this publish
    batt_extra = 0
    try:
        if HAVE_BATTERY and max17048 and (max17048.cell_percent <= BATTERY_LOW_THRESHOLD):
            batt_extra = LOW_BATT_EXTRA_SLEEP
    except Exception:
        pass
    night_extra = 0
    try:
        if night_flag:
            night_extra = max(0, NIGHT_SLEEP_SEC - PUBLISH_PERIOD_SEC)
    except Exception:
        pass
    planned_sleep_s = int(PUBLISH_PERIOD_SEC + batt_extra + night_extra)
    extra_fields["sleep_s"] = planned_sleep_s

    # Build payload & include active_ms
    payload = build_payload(extra_fields)
    payload["active_ms"] = int((time.monotonic() - cycle_t0) * 1000)

    ok = mqtt_publish_with_retries(payload, max_attempts=3)

    # Mirror sample to SD
    try:
        sd_log_sample(payload)
    except Exception:
        pass

    # One-time SD selftest after first successful publish (DEBUG only)
    global _SD_SELFTEST_DONE
    if DEBUG and (not _SD_SELFTEST_DONE):
        try:
            sd_selftest("post-publish")
            _SD_SELFTEST_DONE = True
        except Exception as _e:
            print("SD: selftest post-publish error:", _e)

    if ok:
        hb = _inc_heartbeat(); _reset_fail_count()
        if (hb % _HEARTBEAT_FLUSH_INTERVAL) == 0:
            _flush_heartbeat(hb)
        cs = _inc_cycles_since_sync()
        log_message(f"Heartbeat {hb} (success); cycles_since_rtc_sync={cs}/{RESYNC_CYCLES}")
        pix(COL_OK)
        sd_health(f"publish ok; hb={hb}; active_ms={payload['active_ms']}")
    else:
        fails = _inc_fail_count()
        log_message(f"Consecutive failures: {fails}")
        pix(COL_ERROR)
        sd_health(f"publish FAIL; fails={fails}")

    # Park as RAISE only if WD is actually active
    WD.park_raise()
    return ok

# ---------- Main run loop + sleep policy ----------
try:
    # Initial SD mount & health; prune after we have an epoch
    print("BOOT: main init — SD mount & health")
    try:
        if sd_mount():
            sd_health(f"BOOT fw={FW_VERSION} reset_reason={getattr(microcontroller, 'reset_reason', '')}")
            _prune_sd(_now_epoch_utc())
        else:
            print("SD: not OK at boot (fail-open), continuing without SD")
    except Exception as e:
        print("SD: boot health/prune err:", e)

    while True:
        success = publish_once()

        if not success:
            hb, fails = _get_heartbeat_and_fails()
            if fails >= MAX_CONSEC_FAILS:
                log_message(f"Hit {fails} fails — long sleep {LONG_SLEEP_AFTER_FAILS}s then hard reset")
                sd_health(f"fail-throttle: sleeping {LONG_SLEEP_AFTER_FAILS}s before reset")
                try: publish_offline_status()
                except Exception: pass
                # Ensure WD won't bite during fail-throttle sleep
                WD.set_for_sleep(LONG_SLEEP_AFTER_FAILS, safety_margin_s=10, min_timeout_s=30)
                wake = alarm.time.TimeAlarm(monotonic_time=time.monotonic() + LONG_SLEEP_AFTER_FAILS)
                gc.collect(); pix_off()
                log_message("Entering DEEP sleep (failure throttle)")
                sd_health("enter deep sleep (fail throttle)")
                alarm.exit_and_deep_sleep_until_alarms(wake)

        # Sleep period selection
        extra = 0
        try:
            if HAVE_BATTERY and max17048 and (max17048.cell_percent <= BATTERY_LOW_THRESHOLD):
                extra = LOW_BATT_EXTRA_SLEEP
                log_message(f"Battery low; adding {LOW_BATT_EXTRA_SLEEP}s to sleep")
        except Exception:
            pass

        now_utc_ep = _now_epoch_utc()
        night_extra = 0
        try:
            if fetch_tz_ok and is_night_now(now_utc_ep):
                night_extra = max(0, NIGHT_SLEEP_SEC - PUBLISH_PERIOD_SEC)
        except Exception:
            pass

        sleep_period = PUBLISH_PERIOD_SEC + extra + night_extra

        try:
            sd_health(f"sleep plan s={sleep_period} night={'1' if night_extra>0 else '0'} batt_extra={'1' if extra>0 else '0'}")
        except Exception:
            pass

        # Ensure sleep-time WD timeout is long enough (no-op if WD disabled)
        WD.set_for_sleep(sleep_period, safety_margin_s=10, min_timeout_s=30)

        pix_off()
        log_message(f"Sleeping ({SLEEP_MODE}) for {sleep_period} s (FW {FW_VERSION})")

        try:
            sd_log_sample({
                "rtc_iso": _now_iso_utc(),
                "sleep_s": int(sleep_period),
                "fw_version": FW_VERSION,
                "phase": "sleep",
                "heartbeat": _get_counters()[0],
                "fail_count": _get_counters()[1],
            })
        except Exception:
            pass

        if SLEEP_MODE == 'deep':
            wake = alarm.time.TimeAlarm(monotonic_time=time.monotonic() + sleep_period)
            gc.collect(); sd_health("enter deep sleep (normal)")
            alarm.exit_and_deep_sleep_until_alarms(wake)
        elif SLEEP_MODE == 'light':
            try:
                wifi.radio.enabled = False
            except Exception:
                pass
            wake = alarm.time.TimeAlarm(monotonic_time=time.monotonic() + sleep_period)
            gc.collect(); sd_health("enter light sleep")
            alarm.light_sleep_until_alarms(wake)
            sd_health("wake from light sleep → soft reset")
            microcontroller.reset()
        else:
            time.sleep(sleep_period)
            microcontroller.reset()

except Exception as e:
    LAST_ERROR = str(e)
    log_message(f"TOP-LEVEL EXCEPTION: {LAST_ERROR}")
    try:
        sd_crash(e)
    except Exception:
        pass
    try:
        pool = socketpool.SocketPool(wifi.radio)
        mqtt = _new_mqtt_client(pool)
        mqtt.connect()
        mqtt.publish(f"{TOPIC_BASE}/status", "online", retain=True, qos=0)
        mini = {"client_id": CLIENT_ID, "last_error": LAST_ERROR, "fw_version": FW_VERSION, "phase": _PHASE}
        mqtt.publish(f"{TOPIC_BASE}/state", json.dumps(mini), qos=0, retain=False)
        mqtt.publish(f"{TOPIC_BASE}/last_error", LAST_ERROR, qos=0, retain=True)
        mqtt.disconnect()
    except Exception:
        pass
    try:
        for _ in range(2):
            pix((16,0,0)); time.sleep(0.1)
            pix((0,0,0));  time.sleep(0.1)
    except Exception:
        pass

    backoff = 60
    log_message(f"Backing off {backoff}s after crash")
    try:
        sd_health(f"crash backoff {backoff}s")
    except Exception:
        pass
    # Ensure WD won't bite during crash backoff sleep
    WD.set_for_sleep(backoff, safety_margin_s=10, min_timeout_s=30)
    wake = alarm.time.TimeAlarm(monotonic_time=time.monotonic() + backoff)
    gc.collect()
    alarm.exit_and_deep_sleep_until_alarms(wake)
