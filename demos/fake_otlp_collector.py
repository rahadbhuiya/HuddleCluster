"""
A minimal fake OTLP/HTTP collector — just prints every log record it
receives. Not a real collector (Jaeger/Tempo/etc. would parse and
store these), but enough to SEE that HuddleCluster's observability
export is actually sending real HTTP requests with real event data.

Usage: python fake_otlp_collector.py
Then run run_observability_demo.py in another terminal, pointed at
this collector's port (4318, printed on start).
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        for resource_log in body.get("resourceLogs", []):
            service = next(
                (a["value"]["stringValue"]
                 for a in resource_log.get("resource", {}).get("attributes", [])
                 if a["key"] == "service.name"),
                "unknown",
            )
            for scope_log in resource_log.get("scopeLogs", []):
                for rec in scope_log.get("logRecords", []):
                    attrs = {a["key"]: a["value"].get("stringValue")
                              for a in rec.get("attributes", [])}
                    print(f"  [{service}] {rec['severityText']:<8} "
                          f"{rec['body']['stringValue']:<20} {attrs}")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass   # silence default request logging, we print our own


if __name__ == "__main__":
    port = 4318
    server = HTTPServer(("127.0.0.1", port), Handler)
    print(f"Fake OTLP collector listening on http://127.0.0.1:{port}")
    print("Waiting for log exports... (Ctrl-C to stop)\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")