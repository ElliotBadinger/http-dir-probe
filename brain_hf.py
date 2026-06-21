#!/usr/bin/env python3
"""
brain_hf.py — Brain for Hugging Face Spaces deployment.

Changes from brain.py:
  - PORT 7860 (HF Space requirement)
  - GET / → health check so HF keeps container alive
  - CIDRs fetched from RIPEstat at startup (no file dependency)
  - found.jsonl persisted to HF Dataset on each new find

Set these as Space Secrets:
  HF_TOKEN   - your HF write token (for dataset push)
  HF_DATASET - "username/hunt-found" (dataset repo to persist finds)
"""
import base64, json, math, os, pathlib, queue, random, threading, time, urllib.request, urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

PORT       = int(os.environ.get("PORT", 7860))
OUT_DIR    = pathlib.Path("/tmp/hunt")
FOUND_FILE = OUT_DIR / "found.jsonl"
LOG_FILE   = OUT_DIR / "brain.log"

HF_TOKEN   = os.environ.get("HF_TOKEN", "")
HF_DATASET = os.environ.get("HF_DATASET", "")   # e.g. "alice/hunt-found"

OUT_DIR.mkdir(exist_ok=True)

# ── State ────────────────────────────────────────────────────────────────────
_lock      = threading.Lock()
_job_q: queue.Queue = queue.Queue()
_stats = {"dispatched": 0, "returned": 0, "creds": 0, "score": 0.0,
          "workers_seen": set()}

_PORT_BOOT = {8080: 0.85, 8888: 0.80, 8000: 0.50,
              3000: 0.10, 5000: 0.10, 7860: 0.05, 8501: 0.05, 11434: 0.03}
_arms: dict = {}

# ── Thompson bandit ───────────────────────────────────────────────────────────
def _arm_init(port):
    if port not in _arms:
        _arms[port] = {"alpha": _PORT_BOOT.get(port, 0.1) * 2, "beta": 2.0, "probes": 0}

def _arm_sample(port):
    a = _arms[port]
    return random.betavariate(a["alpha"] + 1, a["beta"] + 1)

def _arm_update(port, probes, score):
    _arm_init(port)
    a = _arms[port]
    a["probes"] += probes
    a["alpha"]  += score
    a["beta"]   += max(0.0, probes - score)

def port_order():
    with _lock:
        for p in _PORT_BOOT:
            _arm_init(p)
        return sorted(_PORT_BOOT.keys(), key=lambda p: _arm_sample(p), reverse=True)

# ── Logging ───────────────────────────────────────────────────────────────────
def _ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _log(msg):
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ── HF Dataset persistence ────────────────────────────────────────────────────
_hf_dirty = threading.Event()

def _push_to_dataset():
    """Push current found.jsonl to HF Dataset (background thread)."""
    while True:
        _hf_dirty.wait(timeout=60)
        _hf_dirty.clear()
        if not (HF_TOKEN and HF_DATASET and FOUND_FILE.exists()):
            continue
        try:
            content = FOUND_FILE.read_bytes()
            b64 = base64.b64encode(content).decode()
            repo_id = HF_DATASET
            # HF Hub raw file commit API
            url = f"https://huggingface.co/api/datasets/{repo_id}/raw/main/found.jsonl"
            req = urllib.request.Request(
                url,
                data=content,
                headers={
                    "Authorization": f"Bearer {HF_TOKEN}",
                    "Content-Type": "application/octet-stream",
                },
                method="PUT",
            )
            resp = urllib.request.urlopen(req, timeout=30)
            _log(f"HF Dataset push: {resp.status} ({len(content)} bytes)")
        except Exception as e:
            _log(f"HF Dataset push error: {e}")

def _save(rec):
    """Append record to found.jsonl (atomic) and trigger dataset push."""
    tmp = FOUND_FILE.with_suffix(".tmp")
    # Append: read existing + new line
    existing = FOUND_FILE.read_bytes() if FOUND_FILE.exists() else b""
    with tmp.open("wb") as f:
        f.write(existing)
        f.write((json.dumps(rec) + "\n").encode())
    tmp.rename(FOUND_FILE)
    _hf_dirty.set()

# ── CIDR loader ───────────────────────────────────────────────────────────────
def _fetch_cidrs_ripestat() -> list:
    cidrs = []
    for asn in [14061, 24940]:  # DigitalOcean, Hetzner
        url = f"https://stat.ripe.net/data/announced-prefixes/data.json?resource=AS{asn}"
        try:
            data = json.loads(urllib.request.urlopen(url, timeout=30).read())
            ipv4 = [p["prefix"] for p in data["data"]["prefixes"] if ":" not in p["prefix"]]
            cidrs.extend(ipv4)
            _log(f"AS{asn}: {len(ipv4)} IPv4 CIDRs")
        except Exception as e:
            _log(f"RIPEstat AS{asn} error: {e}")
    return cidrs

def _load_cidrs():
    cidrs = _fetch_cidrs_ripestat()
    random.shuffle(cidrs)
    for c in cidrs:
        _job_q.put(c)
    _log(f"Loaded {len(cidrs)} CIDRs into job queue")

# ── HTTP handler ──────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass

    def _send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def do_GET(self):
        p = urlparse(self.path)
        qs = parse_qs(p.query)

        if p.path in ("/", "/health"):
            # HF Space health check + UptimeRobot keep-alive target
            with _lock:
                remaining = _job_q.qsize()
            self._send_json(200, {"ok": True, "queue": remaining, "creds": _stats["creds"]})

        elif p.path == "/job":
            budget = int(qs.get("budget", ["50000"])[0])
            batch, ip_sum = [], 0
            with _lock:
                while ip_sum < budget:
                    try:
                        cidr = _job_q.get_nowait()
                        batch.append(cidr)
                        _stats["dispatched"] += 1
                        try:
                            import ipaddress as _ip
                            ip_sum += _ip.ip_network(cidr, strict=False).num_addresses
                        except Exception:
                            ip_sum += 256
                    except queue.Empty:
                        break
            self._send_json(200, {"cidrs": batch, "port_order": port_order(), "ip_count": ip_sum})

        elif p.path == "/stats":
            with _lock:
                s = dict(_stats)
                s["workers_seen"] = list(s["workers_seen"])
                s["queue_remaining"] = _job_q.qsize()
            self._send_json(200, s)

        elif p.path == "/arms":
            with _lock:
                self._send_json(200, dict(_arms))

        elif p.path == "/found":
            # Return current found.jsonl content
            try:
                content = FOUND_FILE.read_text() if FOUND_FILE.exists() else ""
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(content.encode())
            except Exception as e:
                self._send_json(500, {"error": str(e)})

        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        p = urlparse(self.path)
        body = self._read_body()

        if p.path == "/result":
            _save(body)
            with _lock:
                _stats["creds"] += 1
                _stats["score"] = round(_stats["score"] + body.get("R", 0.0), 4)
                _stats["returned"] += 1
                _stats["workers_seen"].add(body.get("worker", "?"))
            if body.get("R", 0) > 0:
                _log(f"HIT R={body['R']:.3f} tier={body.get('tier')} "
                     f"ip={body.get('ip')}:{body.get('port')}{body.get('path','')}")
            self._send_json(200, {"ok": True})

        elif p.path == "/arm":
            port = int(body.get("port", 0))
            if port:
                with _lock:
                    _arm_update(port, body.get("probes", 0), body.get("score", 0.0))
            self._send_json(200, {"ok": True})

        else:
            self._send_json(404, {"error": "not found"})

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    _load_cidrs()

    # HF Dataset persistence thread
    t = threading.Thread(target=_push_to_dataset, daemon=True)
    t.start()

    # Periodic stats
    def _stat_loop():
        while True:
            time.sleep(300)
            with _lock:
                _log(f"STATS dispatched={_stats['dispatched']} "
                     f"remaining={_job_q.qsize()} creds={_stats['creds']} "
                     f"ΣR={_stats['score']:.3f} workers={len(_stats['workers_seen'])}")
    threading.Thread(target=_stat_loop, daemon=True).start()

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    _log(f"brain listening on :{PORT}  queue={_job_q.qsize()}")
    server.serve_forever()

if __name__ == "__main__":
    main()
