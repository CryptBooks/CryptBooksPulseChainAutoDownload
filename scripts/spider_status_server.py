#!/usr/bin/env python3
"""
Tiny HTTP server that reads the spider checkpoint and serves status as JSON.
Runs on localhost:8777 — used by spider-status.html for live dashboard.
"""

import json, os, sys, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

CHECKPOINT = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "CryptBooksPortal", "Data", "SyncedData", "spider-checkpoint.json"
)
CHECKPOINT = os.path.normpath(CHECKPOINT)

# Also serve the HTML page
HTML_PAGE = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "CryptBooksPortal", "pages", "spider-status.html"
)
HTML_PAGE = os.path.normpath(HTML_PAGE)

PORT = 8777


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet

    def do_GET(self):
        if self.path == "/" or self.path == "/spider-status.html":
            self._serve_html()
        elif self.path == "/api/status":
            self._serve_status()
        else:
            self.send_error(404)

    def _serve_html(self):
        try:
            with open(HTML_PAGE, "r", encoding="utf-8") as f:
                body = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_error(404, "spider-status.html not found")

    def _serve_status(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        try:
            stat = os.stat(CHECKPOINT)
            with open(CHECKPOINT, "r", encoding="utf-8") as f:
                cp = json.load(f)

            processed = cp.get("processed", {})
            queue = cp.get("queue", [])
            edges = cp.get("edges", {})

            hubs = []
            no_data = []
            normal = []
            total_eoa_from_hubs = 0

            for addr, info in processed.items():
                if info.get("is_hub"):
                    hubs.append({"address": addr, "name": info.get("name", addr[:10]),
                                 "eoa_count": info.get("eoa_count", 0),
                                 "transfer_count": info.get("transfer_count", 0)})
                    total_eoa_from_hubs += info.get("eoa_count", 0)
                elif info.get("status") == "no_data":
                    no_data.append(addr)
                else:
                    normal.append({"address": addr, "name": info.get("name", addr[:10]),
                                   "eoa_count": info.get("eoa_count", 0),
                                   "transfer_count": info.get("transfer_count", 0)})

            # Edge stats
            total_tx_hashes = 0
            token_totals = {}
            for key, edge in edges.items():
                for sym, tdata in edge.get("tokens", {}).items():
                    hcount = len(tdata.get("tx_hashes", []))
                    total_tx_hashes += hcount
                    if sym not in token_totals:
                        token_totals[sym] = {"transfers": 0, "edges": 0}
                    token_totals[sym]["transfers"] += hcount
                    token_totals[sym]["edges"] += 1

            # Top tokens by transfer count
            top_tokens = sorted(token_totals.items(), key=lambda x: -x[1]["transfers"])[:20]

            # Sort hubs by eoa_count desc
            hubs.sort(key=lambda h: -h["eoa_count"])

            # Sort normal by transfer_count desc
            normal.sort(key=lambda n: -n["transfer_count"])

            result = {
                "checkpoint_file": CHECKPOINT,
                "checkpoint_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "checkpoint_size_mb": round(stat.st_size / 1024 / 1024, 1),
                "server_time": datetime.now().isoformat(),
                "summary": {
                    "total_processed": len(processed),
                    "total_queue": len(queue),
                    "total_wallets": len(processed) + len(queue),
                    "hubs": len(hubs),
                    "normal": len(normal),
                    "no_data": len(no_data),
                    "total_edges": len(edges),
                    "total_tx_hashes": total_tx_hashes,
                    "hub_eoas_avoided": total_eoa_from_hubs,
                    "pct_complete": round(len(processed) / max(len(processed) + len(queue), 1) * 100, 1),
                    "last_updated": cp.get("last_updated", "unknown"),
                },
                "top_hubs": hubs[:25],
                "top_wallets": normal[:25],
                "top_tokens": [{"symbol": s, "transfers": d["transfers"], "edges": d["edges"]} for s, d in top_tokens],
                "recent_no_data": no_data[-10:],
                "next_in_queue": [
                    (e[0] if isinstance(e, list) else e) for e in queue[:10]
                ],
            }

            self.wfile.write(json.dumps(result, indent=2).encode("utf-8"))

        except FileNotFoundError:
            self.wfile.write(json.dumps({"error": "Checkpoint file not found", "path": CHECKPOINT}).encode("utf-8"))
        except json.JSONDecodeError as e:
            self.wfile.write(json.dumps({"error": f"Checkpoint JSON parse error: {e}"}).encode("utf-8"))
        except Exception as e:
            self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))


if __name__ == "__main__":
    print(f"Spider Status Server")
    print(f"  Checkpoint: {CHECKPOINT}")
    print(f"  Dashboard:  http://localhost:{PORT}/")
    print(f"  API:        http://localhost:{PORT}/api/status")
    print()
    server = HTTPServer(("127.0.0.1", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()
