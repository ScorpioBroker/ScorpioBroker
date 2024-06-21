import http.server
import socketserver
import logging
import json

class RequestHandler(http.server.BaseHTTPRequestHandler):
    last_request = None

    def _dump_request(self):
        # Store request details
        self.__class__.last_request = {
            'request_line': self.requestline,
            'command': self.command,
            'path': self.path,
            'version': self.request_version,
            'headers': {header: value for header, value in self.headers.items()},
            'body': None
        }

        # Log the request line
        logging.info(f"Request Line: {self.requestline}")
        logging.info(f"Command: {self.command}")
        logging.info(f"Path: {self.path}")
        logging.info(f"Version: {self.request_version}")

        # Log the headers
        logging.info("Headers:")
        for header, value in self.headers.items():
            logging.info(f"    {header}: {value}")

        # Log the body (if any)
        content_length = self.headers.get('Content-Length')
        if content_length:
            length = int(content_length)
            body = self.rfile.read(length)
            try:
                json_body = json.loads(body.decode('utf-8', errors='replace'))
                self.__class__.last_request['body'] = json_body
                logging.info(f"Body: {json.dumps(json_body, indent=4)}")
            except json.JSONDecodeError:
                self.__class__.last_request['body'] = "Invalid JSON"
                logging.warning("Body: Invalid JSON")
            logging.info(f"Body: {self.__class__.last_request['body']}")
        else:
            logging.info("Body: None")

    def do_GET(self):
        if self.path == '/last-request':
            # Return the last request received
            if self.__class__.last_request:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(self.__class__.last_request).encode('utf-8'))
            else:
                self.send_response(404)
                self.end_headers()
                # self.wfile.write(b"No requests received yet.")
        else:
            self._dump_request()
            self.send_response(200)
            self.end_headers()
            # self.wfile.write(b"GET request received")

    def do_POST(self):
        self._dump_request()
        self.send_response(200)
        self.end_headers()
        # self.wfile.write(b"POST request received")

    def do_PUT(self):
        self._dump_request()
        self.send_response(200)
        self.end_headers()
        # self.wfile.write(b"PUT request received")

    def do_DELETE(self):
        self._dump_request()
        self.send_response(200)
        self.end_headers()
        # self.wfile.write(b"DELETE request received")

    def do_PATCH(self):
        self._dump_request()
        self.send_response(200)
        self.end_headers()
        # self.wfile.write(b"PATCH request received")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    PORT = 8090
    with socketserver.TCPServer(("", PORT), RequestHandler) as httpd:
        logging.info(f"Serving on port {PORT}")
        httpd.serve_forever()
