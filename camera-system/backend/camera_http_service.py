#!/usr/bin/env python3

import os
import threading
import subprocess
import shutil
from datetime import datetime

from flask import Flask, request, jsonify


# ============================================================
# SERVER CONFIGURATION
# ============================================================

SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("PORT", "8081"))

RECORD_DURATION = 15

VIDEO_WIDTH = 1280
VIDEO_HEIGHT = 720
VIDEO_FPS = 30

RECORDINGS_DIR = os.environ.get("RECORDINGS_DIR", "/opt/factory-camera/recordings")


# ============================================================
# CAMERA MAP
#
# Physical camera mapping confirmed:
#
# Zone 1       -> cam6
# Zone 2       -> cam1
# Zone 3       -> cam4
# Zone 4       -> cam5
#
# cam2 = Cooling Tower
# cam3 = Conveyor
# ============================================================

CAMERA_MAP = {
    1: "rtsp://127.0.0.1:8554/cam6",
    2: "rtsp://127.0.0.1:8554/cam1",
    3: "rtsp://127.0.0.1:8554/cam4",
    4: "rtsp://127.0.0.1:8554/cam5",
}


# ============================================================
# ESP32 ZONE REMAPPING
#
# Current physical wiring behavior:
#
# Person2 physically enters Zone 4
#        ↓
# ESP32 GPIO27 is triggered
#        ↓
# ESP32 reports switchyardZone = 1
#
# Therefore:
#
# ESP32 Zone 1 -> Physical Zone 4
#
# WARNING:
# If the real Zone 1 laser also triggers GPIO27,
# Python cannot distinguish it from Person2/Zone4.
# ============================================================

ZONE_REMAP = {
    1: 4,
}


app = Flask(__name__)

os.makedirs(RECORDINGS_DIR, exist_ok=True)


# ============================================================
# SIMULTANEOUS RECORDING STATE
# ============================================================

recording_lock = threading.Lock()

recordings = {
    zone: {
        "recording": False,
        "camera": CAMERA_MAP[zone],
        "output_file": None,
        "started_at": None
    }
    for zone in CAMERA_MAP
}


# ============================================================
# UTILITY
# ============================================================

def ffmpeg_available():
    return shutil.which("ffmpeg") is not None


# ============================================================
# RECORDING FUNCTION
# ============================================================

def record_intrusion(zone, camera_device):

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

    # ========================================================
    # MP4 OUTPUT
    # ========================================================

    output_file = os.path.join(
        RECORDINGS_DIR,
        f"zone{zone}_intrusion_{timestamp}.mp4"
    )

    with recording_lock:
        recordings[zone]["output_file"] = output_file
        recordings[zone]["started_at"] = datetime.now().isoformat()

    command = [
        "ffmpeg",

        "-hide_banner",
        "-loglevel", "warning",

        "-rtsp_transport", "tcp",
        "-i", camera_device,

        "-t", str(RECORD_DURATION),

        "-map", "0:v:0",
        "-c:v", "copy",

        # Optimize MP4 for browser/web playback
        "-movflags", "+faststart",

        "-y",
        output_file
    ]

    print()
    print("==============================================")
    print("Starting intrusion recording")
    print(f"Zone       : {zone}")
    print(f"Camera     : {camera_device}")
    print(f"Duration   : {RECORD_DURATION} seconds")
    print(f"Output     : {output_file}")
    print("Format     : MP4")
    print("Video      : H.264 stream copy")
    print("==============================================")

    try:

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode == 0:

            print()
            print("==============================================")
            print("Recording completed successfully.")
            print(f"Zone       : {zone}")
            print(f"Video saved: {output_file}")
            print("==============================================")

        else:

            print()
            print("==============================================")
            print("FFmpeg recording failed.")
            print(f"Zone       : {zone}")
            print(result.stderr)
            print("==============================================")

            try:
                if os.path.exists(output_file):
                    os.remove(output_file)
            except Exception:
                pass

    except Exception as e:

        print()
        print("==============================================")
        print("Recording exception")
        print(f"Zone       : {zone}")
        print(f"Error      : {e}")
        print("==============================================")


    finally:

        with recording_lock:

            recordings[zone]["recording"] = False
            recordings[zone]["output_file"] = None
            recordings[zone]["started_at"] = None

        print(f"[CAMERA] Zone {zone} recording state cleared.")


# ============================================================
# HEALTH
# ============================================================

@app.route("/health", methods=["GET"])
def health():

    return jsonify({
        "status": "ok",
        "service": "factory_camera_service"
    })


# ============================================================
# STATUS
# ============================================================

@app.route("/status", methods=["GET"])
def status():

    with recording_lock:

        active_recordings = {}

        for zone, info in recordings.items():

            if info["recording"]:

                active_recordings[str(zone)] = {
                    "recording": True,
                    "camera": info["camera"],
                    "output_file": info["output_file"],
                    "started_at": info["started_at"]
                }

        return jsonify({

            "service": "factory_camera_service",

            "recording": len(active_recordings) > 0,

            "active_recordings": active_recordings,

            "active_recording_count": len(active_recordings),

            "record_duration_seconds": RECORD_DURATION,

            "resolution": f"{VIDEO_WIDTH}x{VIDEO_HEIGHT}",

            "fps_requested": VIDEO_FPS,

            "camera_map": CAMERA_MAP,

            "zone_remap": ZONE_REMAP
        })


# ============================================================
# INTRUSION ENDPOINT
# ============================================================

@app.route("/intrusion", methods=["POST"])
def intrusion():

    data = request.get_json(silent=True)

    if not data:

        return jsonify({
            "success": False,
            "error": "JSON body required"
        }), 400


    # --------------------------------------------------------
    # Validate intrusion flag
    # --------------------------------------------------------

    if data.get("switchyardIntrusion") is not True:

        return jsonify({
            "success": False,
            "error": "switchyardIntrusion must be true"
        }), 400


    # --------------------------------------------------------
    # Get zone reported by ESP32
    # --------------------------------------------------------

    received_zone = data.get("switchyardZone")


    if received_zone not in (1, 2, 3, 4):

        return jsonify({
            "success": False,
            "error": "switchyardZone must be 1, 2, 3, or 4"
        }), 400


    # --------------------------------------------------------
    # Convert ESP32 zone to physical zone
    # --------------------------------------------------------

    zone = ZONE_REMAP.get(received_zone, received_zone)


    print()
    print("==============================================")
    print("[INTRUSION REQUEST]")
    print(f"ESP32 Zone     : {received_zone}")
    print(f"Physical Zone  : {zone}")
    print(f"Camera         : {CAMERA_MAP[zone]}")
    print("==============================================")


    # --------------------------------------------------------
    # Select camera
    # --------------------------------------------------------

    camera_device = CAMERA_MAP.get(zone)


    if camera_device is None:

        return jsonify({
            "success": False,
            "error": f"No camera configured for physical zone {zone}"
        }), 400


    # --------------------------------------------------------
    # Check FFmpeg
    # --------------------------------------------------------

    if not ffmpeg_available():

        return jsonify({
            "success": False,
            "error": "FFmpeg is not installed"
        }), 500


    # ========================================================
    # CHECK ONLY THIS PHYSICAL ZONE
    # ========================================================

    with recording_lock:

        if recordings[zone]["recording"]:

            return jsonify({

                "success": False,

                "error":
                    f"Zone {zone} recording is already running",

                "received_zone":
                    received_zone,

                "zone":
                    zone,

                "camera":
                    camera_device,

                "output_file":
                    recordings[zone]["output_file"]

            }), 409


        # Mark this physical zone as recording

        recordings[zone]["recording"] = True

        recordings[zone]["camera"] = camera_device


    # ========================================================
    # START RECORDING THREAD
    # ========================================================

    thread = threading.Thread(

        target=record_intrusion,

        args=(zone, camera_device),

        daemon=True,

        name=f"camera-zone-{zone}"

    )

    thread.start()


    # ========================================================
    # RESPONSE
    # ========================================================

    return jsonify({

        "success": True,

        "message":
            "Intrusion recording started",

        "received_zone":
            received_zone,

        "physical_zone":
            zone,

        "camera":
            camera_device,

        "duration_seconds":
            RECORD_DURATION

    }), 200


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":

    print("==============================================")
    print(" Factory Model Camera HTTP Service")
    print("==============================================")

    print(f"Listening : {SERVER_HOST}:{SERVER_PORT}")
    print(f"Storage   : {RECORDINGS_DIR}")
    print(f"Resolution: {VIDEO_WIDTH}x{VIDEO_HEIGHT}")
    print(f"FPS       : {VIDEO_FPS}")
    print(f"Duration  : {RECORD_DURATION} seconds")

    print()
    print("Camera Mapping:")

    for zone, device in CAMERA_MAP.items():

        print(f"  Physical Zone {zone} -> {device}")

    print()
    print("ESP32 Zone Remapping:")

    for received, physical in ZONE_REMAP.items():

        print(
            f"  ESP32 Zone {received} "
            f"-> Physical Zone {physical}"
        )

    print()
    print("Simultaneous recording: ENABLED")
    print("Output format: MP4")
    print("Video codec: H.264 (stream copy)")
    print("==============================================")


    app.run(
        host=SERVER_HOST,
        port=SERVER_PORT,
        threaded=True
)
