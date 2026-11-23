#!/usr/bin/env python3
import json, os, re, shutil, subprocess
from datetime import datetime
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import unquote

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8082"))
WEB_ROOT = Path(os.environ.get("WEB_ROOT", "/opt/factory-camera/web"))
RECORDINGS_DIR = Path(os.environ.get("RECORDINGS_DIR", "/opt/factory-camera/recordings"))
THUMBNAILS_DIR = Path(os.environ.get("THUMBNAILS_DIR", str(WEB_ROOT / "thumbnails")))

ZONE_PATTERN=re.compile(r"zone(\d+)_intrusion_(\d{8})_(\d{6})(?:_(\d{3}))?\.mp4$", re.I)
CAMERA_MAP={1:"cam6",2:"cam1",3:"cam4",4:"cam5"}

def valid(name):
    return Path(name).name == name and bool(ZONE_PATTERN.match(name))

def path_for(name):
    return RECORDINGS_DIR/name

def thumb_for(name):
    return THUMBNAILS_DIR/(name+".jpg")

def make_thumb(name):
    src=path_for(name); dst=thumb_for(name)
    if not src.exists(): return False
    if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime: return True
    THUMBNAILS_DIR.mkdir(parents=True,exist_ok=True)
    tmp=dst.with_suffix(".tmp.jpg")
    cmd=["ffmpeg","-hide_banner","-loglevel","error","-ss","1","-i",str(src),
         "-frames:v","1","-vf","scale=640:-1","-q:v","4","-y",str(tmp)]
    try:
        r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                         text=True,timeout=15)
        if r.returncode or not tmp.exists():
            tmp.unlink(missing_ok=True); return False
        tmp.replace(dst); return True
    except Exception as e:
        print(f"[THUMBNAIL] {name}: {e}",flush=True); return False

class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,directory=str(WEB_ROOT),**kwargs)

    def json(self,data,status=200):
        body=json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(body)))
        self.send_header("Cache-Control","no-cache")
        self.end_headers(); self.wfile.write(body)

    def file_target(self,prefix):
        name=unquote(self.path[len(prefix):])
        if not valid(name):
            self.send_error(403,"File not allowed"); return None,None
        p=path_for(name)
        if not p.is_file():
            self.send_error(404,"Recording not found"); return None,None
        return name,p

    def do_GET(self):
        if self.path=="/api/recordings": return self.recordings()
        if self.path=="/api/storage": return self.storage()
        if self.path.startswith("/video/"): return self.video("/video/")
        if self.path.startswith("/download/"): return self.download()
        if self.path.startswith("/thumbnail/"): return self.thumbnail()
        return super().do_GET()

    def recordings(self):
        out=[]
        for f in RECORDINGS_DIR.glob("*.mp4"):
            m=ZONE_PATTERN.match(f.name)
            if not m: continue
            try:
                dt=datetime.strptime(m.group(2)+m.group(3),"%Y%m%d%H%M%S")
                st=f.stat()
            except Exception: continue
            out.append({
                "filename":f.name,
                "zone":int(m.group(1)),
                "display_time":dt.strftime("%d-%b-%Y %H:%M:%S"),
                "timestamp":dt.timestamp(),
                "milliseconds":m.group(4) or "000",
                "size_mb":round(st.st_size/1048576,1),
                "duration_seconds":15,
                "camera":CAMERA_MAP.get(int(m.group(1)),"unknown"),
                "thumbnail":"/thumbnail/"+f.name,
                "video":"/video/"+f.name,
                "download":"/download/"+f.name
            })
        out.sort(key=lambda x:(x["timestamp"],x["milliseconds"]),reverse=True)
        zones={str(z):sum(1 for x in out if x["zone"]==z) for z in range(1,5)}
        self.json({"recordings":out,"total":len(out),"zones":zones})

    def storage(self):
        u=shutil.disk_usage(RECORDINGS_DIR)
        used=u.total-u.free
        self.json({"total_bytes":u.total,"used_bytes":used,"free_bytes":u.free,
                   "used_percent":round(used/u.total*100,1)})

    def video(self,prefix):
        name,p=self.file_target(prefix)
        if not p: return
        size=p.stat().st_size
        self.send_response(200); self.send_header("Content-Type","video/mp4")
        self.send_header("Content-Length",str(size)); self.send_header("Accept-Ranges","bytes")
        self.send_header("Cache-Control","no-cache"); self.end_headers()
        with open(p,"rb") as f:
            while True:
                b=f.read(1024*1024)
                if not b: break
                self.wfile.write(b)

    def download(self):
        name,p=self.file_target("/download/")
        if not p: return
        size=p.stat().st_size
        self.send_response(200); self.send_header("Content-Type","video/mp4")
        self.send_header("Content-Length",str(size))
        self.send_header("Content-Disposition",f'attachment; filename="{name}"')
        self.end_headers()
        with open(p,"rb") as f:
            while True:
                b=f.read(1024*1024)
                if not b: break
                self.wfile.write(b)

    def thumbnail(self):
        name,p=self.file_target("/thumbnail/")
        if not p: return
        dst=thumb_for(name)
        if not make_thumb(name):
            self.send_error(404,"Thumbnail unavailable"); return
        size=dst.stat().st_size
        self.send_response(200); self.send_header("Content-Type","image/jpeg")
        self.send_header("Content-Length",str(size)); self.send_header("Cache-Control","public,max-age=3600")
        self.end_headers()
        with open(dst,"rb") as f:
            while True:
                b=f.read(1024*1024)
                if not b: break
                self.wfile.write(b)

    def log_message(self,fmt,*args):
        print("[FactoryCameraWeb] "+fmt%args,flush=True)

if __name__=="__main__":
    WEB_ROOT.mkdir(parents=True,exist_ok=True)
    RECORDINGS_DIR.mkdir(parents=True,exist_ok=True)
    THUMBNAILS_DIR.mkdir(parents=True,exist_ok=True)
    print(f"Factory Camera Web UI running on http://{HOST}:{PORT}",flush=True)
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
