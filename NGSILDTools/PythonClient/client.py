import urllib
import json
from urllib.request import Request
from urllib.request import urlopen
import socket
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import threading

class NGSILDClient:
  def __init__(self, baseURL, notificationIp = None, notificationPort = 27150, addSuffix = True):
    #print("blaaa")
    if(baseURL[:-1] != '/'):
      self.baseURL = baseURL + "/"
    else:
      self.baseURL = baseURL
    #print("blub")
    if(addSuffix):
      self.baseURL = baseURL + "ngsi-ld/v1/"
    if(notificationIp):
      self.notificationIp = notificationIp
    else:
      self.notificationIp = self.getLocalhost()
    self.notificationEndpoint = "http://" + self.notificationIp + ":" + str(notificationPort) + "/notify/"
    #print("blib")
    self.subscriptionIds = []  
    self.server = MyServer(('', notificationPort), MyHandler)
    thread = threading.Thread(target = self.server.serve_forever)
    thread.start()
    #print("blob")
    return    
  def shutdown(self):
    for subId in self.subscriptionIds:
      self.unsubscribe(subId)
    self.server.shutdown()
  def getLocalhost(self):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP
  def createLocationEntry(self, createBody, coordinates):
    hashMapBecauseStupidPythonCantDoSwitch = { 1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPolygon"}
    depth = 1
    temp = coordinates[0]
    while type(temp) == list:
      depth += 1
      temp = temp[0]
    temp = {}
    temp['type'] = 'GeoProperty'
    temp['value'] = {'type': hashMapBecauseStupidPythonCantDoSwitch[depth], 'value': coordinates}
    createBody['location'] = temp
    
  def addAttribs(self, createBody, attribs, properties = True):
    if(not attribs):
      return
    for key, value in attribs.items():
      temp = {}
      if(properties):
        temp['type'] = 'Property'
      else:
        temp['type'] = 'Relationship'
      temp['value'] = value
      createBody[key] = temp
  def createEntity(self, entityId, entityType, properties=None, relationships=None, coordinates=None, description=None, atContext=None):
    createBody = {}
    createBody['id'] = entityId
    createBody['type'] = entityType
    url = self.baseURL + "entities/"
    if(coordinates):
      self.createLocationEntry(createBody, coordinates)
    if(description):
      createBody['description'] = description
    self.addAttribs(createBody, properties)
    self.addAttribs(createBody, relationships, False)
    headers = self.getHeaderForAtContext(atContext)
    self.doPost(url, headers, createBody)
  def getEntity(self, entityId, atContext=None):
    header = self.getHeaderForAtContext(atContext)
    url = self.baseURL + "entities/" + entityId
    return self.doGet(url, header)
  def query(self, ids=None, idPattern=None, entityType=None, attrs=None, q=None, georel=None, geometry=None, coordinates=None, geoproperty=None, atContext=None):
    if(not entityType and not attrs):
      raise ValueError("type or attrs is mandatory")
    if((georel or geometry or coordinates) and (not (georel and geometry and coordinates))):
      raise ValueError("geoqueries need all three components") 
    url = self.baseURL + "entities?"
    if(ids):
      url = url + "id=" + urllib.parse.quote(ids) + "&"
    if(idPattern):
      url = url + "idPattern=" + urllib.parse.quote(idPattern) + "&"
    if(entityType):
      url = url + "type=" + urllib.parse.quote(entityType) + "&"
    if(q):
      url = url + "q=" + urllib.parse.quote(q) + "&"
    if(georel):
      url = url + "georel=" + urllib.parse.quote(georel) + "&"
    if(geometry):
      url = url + "geometry=" + urllib.parse.quote(geometry) + "&"
    if(coordinates):
      url = url + "coordinates=" + urllib.parse.quote(coordinates) + "&"
    if(geoproperty):
      url = url + "geoproperty=" + urllib.parse.quote(geoproperty) + "&"  
    return self.doGet(url[:-1], self.getHeaderForAtContext(atContext))    
  def update(self, entityId, attribName, newValue, atContext):
    return
  def append(self, entityId, attribName, newValue, atContext, overwrite=True):
    return
  def delete(self, entityId, attribName = None, atContext = None):
    url = self.baseURL + "entities/" + urllib.parse.quote(entityId) + "/"
    header = self.getHeaderForAtContext(atContext)
    if(attribName):
      url = url + "/attrs/" + urllib.parse.quote(attribName)
    self.doDelete(url, header)
  def subscribe(self, notificationListener, entityType=None, entityId=None, idPattern=None, watchedAttribs=None, attribs=None, q=None, geoQuery=None, atContext=None):
    url = self.baseURL + "subscriptions/"
    subscription = {}
    subscription['type'] = 'Subscription'
    if(watchedAttribs):
      subscription['watchedAttributes'] = watchedAttribs
    if(q):
      subscription['q'] = q
    if(geoQuery):
      subscription['geoQ'] = geoQuery
    if(entityType):
      subscription['entities'] = self.getEntityInfos(entityType, entityId, idPattern)
    subscription['notification'] = self.getNotificationParams(attribs)
    subscriptionId = self.doPost(url, self.getHeaderForAtContext(atContext), subscription)
    self.setupNotificationHandling(notificationListener, subscriptionId)
    return subscriptionId
    
  def setupNotificationHandling(self, notificationListener, subscriptionId):
    self.subscriptionIds.append(subscriptionId)
    self.server.addListener(subscriptionId, notificationListener)
  def unsubscribe(self, subscriptionId):
    self.doDelete(self.baseURL + "subscriptions/" + subscriptionId, self.getHeaderForAtContext(None))
    self.subscriptionIds.remove(subscriptionId)
    self.server.removeListener(subscriptionId)
  def getEntityInfos(self, entityType, entityId, idPattern):
    result = {}
    if(entityId):
      result['id'] = entityId
    if(entityType):
      result['type'] = entityType
    if(idPattern):
      result['idPattern'] = idPattern
    return [result]
  def getNotificationParams(self, attribs):
    result = {}
    if(attribs):
      result['attributes'] = attribs
    result['format'] = "normalized"
    result['endpoint'] = {'accept': 'application/ld+json' , 'uri': self.notificationEndpoint}
    return result
  def getHeaderForAtContext(self, atContext):
    result = {}
    result['Accept'] = 'application/ld+json'
    result['Content-type'] = 'application/json'
    if(atContext):
      result['Link'] = '<' + atContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    return result
  def doDelete(self, url, headers):
    req = Request(url, headers=headers, method='DELETE')
    response = urlopen(req)
  def doGet(self, url, headers):
    req = Request(url, headers=headers)
    response = urlopen(req)
    result = response.read()
    return json.loads(result)
  def doPost(self, url, headers, body, method = "POST"):
    #print(str(body))
    req = Request(url, json.dumps(body).encode('utf-8'), headers, method=method)
    response = urlopen(req)
    result = response.read()
    return result.decode()
  def doPatch(self, url, headers, body):
    return self.doPost(url, headers, body, "PATCH")
  def doPut(self, url, headers, body):
    return self.doPost(url, headers, body, "PUT")

class MyServer(HTTPServer):
  def __init__(self, *args, **kvargs):
    super(MyServer,self).__init__(*args, **kvargs)
    self.clients = {}
  def notify(self, content):
    subId = content['subscriptionId']
    #print("notify incomming " + str(content))
    #print("subId " + str(subId))
    #print("clients " + str(self.clients))
    if(subId in self.clients):
      self.clients[subId](content)
  def addListener(self, subscriptionId, listener):
    self.clients[subscriptionId] = listener
  def removeListener(self, subscriptionId):
    del self.clients[subscriptionId]


class MyHandler(BaseHTTPRequestHandler):
  def __init__(self, *args, **kvargs):
    self.server = args[-1]
    super(MyHandler,self).__init__(*args, **kvargs)
    
  def do_POST(self):
    ##print(self.__dict__)
    length = int(self.headers.get('Content-Length'))
    postBody = self.rfile.read(length)
    content = json.loads(postBody)
    #print("post incomming " + str(content))
    self.server.notify(content)
    return
  def respond(self):
    self.send_response(200)
    self.send_header('Content-type', 'text/html')
    self.end_headers()
    #content = self.handle_http(200, 'text/html')
    #self.wfile.write("")
