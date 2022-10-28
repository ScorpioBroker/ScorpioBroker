#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, HTTPServer

import json

class S(BaseHTTPRequestHandler):
    receivedNotifications = {}
    
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        subId = self.path[1:]
        self._set_response()
        if subId == "deleteAll":
          print("delete all called")
          self.receivedNotifications.clear()
          self.wfile.write(json.dumps([]).encode("UTF-8"))
          return
        if subId in self.receivedNotifications:
          self.wfile.write(json.dumps(self.receivedNotifications[subId]).encode("UTF-8"))
        else:
          self.wfile.write(json.dumps([]).encode("UTF-8"))

    def do_POST(self):
        content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
        post_data = self.rfile.read(content_length) # <--- Gets the data itself
        print(str(post_data))
        notification = json.loads(post_data)
        subId = notification["subscriptionId"]
        if subId in self.receivedNotifications:
          notifications = self.receivedNotifications[subId]
        else:
          notifications = []
          self.receivedNotifications[subId] = notifications
        tmp = {}
        for h in self.headers:
          tmp[h] = self.headers[h]
        notifications.append({'headers': tmp, 'body': notification})
        self._set_response()
        self.wfile.write("".encode("UTF-8"))

def run(server_class=HTTPServer, handler_class=S, port=8080):

    server_address = ('', port)
    httpd = server_class(server_address, handler_class)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
