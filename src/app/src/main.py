from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class JobLakeAppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/", "/health"}:
            self.send_response(404)
            self.end_headers()
            return

        body = (
            "JobLake App\n"
            "status=ok\n"
            f"trino={os.getenv('TRINO_HOST', 'trino')}:{os.getenv('TRINO_PORT', '8080')}\n"
            f"agent={os.getenv('AGENT_URL', 'http://agent:8080/api/v1')}\n"
        ).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        print(f"{self.address_string()} - {format % args}")


def main() -> None:
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8501"))
    server = ThreadingHTTPServer((host, port), JobLakeAppHandler)
    print(f"JobLake app placeholder is listening on {host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
