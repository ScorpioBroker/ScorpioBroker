import urllib
import json
from urllib.request import Request
from urllib.request import urlopen
import socket
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import threading
import uuid

class NGSILDClient:
  def __init__(self, baseURL, notificationIp = None, notificationPort = 27150, addSuffix = True):
    """Create an instance of an NGSI-LD Client.

    Args:
        baseURL: The url of your broker, e.g. http://myscorpiohost.de:9090/
        notificationIp: <Optional> This is used for subscription handling. Your local ip will be used if none is provided.
        notificationPort: <Optional> <Optional> This is used for subscription handling. Default is 27150.
                    For multivalue a list can be provided. Unique datasetIds will be generated.
                    For manually setting the datasetId 2 entries have to be provided in a dict in the
                    multivalue list. datasetId and value. datasetId has to be a URI string. e.g.
                    [{'datasetId': 'urn:mydataset:123', 'value': 'testvalue1'}, 'testvalue2']
        addSuffix: <Optional> Defaults to True. Adds "ngsi-ld/v1/" as suffix to your baseURL.
    Returns:
        Instance of the NGSILDClient
    """

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
      self.notificationIp = self.__getLocalhost__()
    self.notificationEndpoint = "http://" + self.notificationIp + ":" + str(notificationPort) + "/notify/"
    #print("blib")
    self.subscriptionIds = []  
    self.server = MyServer(('', notificationPort), MyHandler)
    self.thread = threading.Thread(target = self.server.serve_forever)
    self.thread.start()
    #print("blob")
    return    
  def __getLocalhost__(self):
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
  def __createLocationEntry__(self, createBody, coordinates, locationName = 'location'):
    createBody[locationName] = self.__createLocationEntry__(coordinates)
  def __createLocationEntry__(self, coordinates):
    hashMapBecauseStupidPythonCantDoSwitch = { 1: "Point", 2: "LineString", 3: "Polygon", 4: "MultiPolygon"}
    depth = 1
    temp = coordinates[0]
    while type(temp) == list:
      depth += 1
      temp = temp[0]
    temp = {}
    temp['type'] = 'GeoProperty'
    temp['value'] = {'type': hashMapBecauseStupidPythonCantDoSwitch[depth], 'value': coordinates}
    return temp
  def __addAttribs__(self, createBody, attribs, attribType = "Property"):
    if(not attribs):
      return
    for key, value in attribs.items():
      if(type(value)==list):
        container = []
        for multivalue in value:
          temp = {}
          temp['type'] = attribType
          if(type(multivalue) == dict and "datasetId" in multivalue):
            datasetId = multivalue['datasetId']
            realValue = multivalue['value']
          else:
            realValue = multivalue
            datasetId = uuid.uuid1().urn
          if(attribType=="Property"):
            temp['value'] = multivalue
          elif(attribType=="Relationship"):
            temp['object'] = multivalue
          elif(attribType=="GeoProperty"):
            temp['value'] = __createLocationEntry__(multivalue)
          temp['datasetId'] = datasetId
          container.append(temp)
        createBody[key] = container
      else:
        temp = {}
        temp['type'] = attribType
        if(attribType=="Property"):
          temp['value'] = value
        elif(attribType=="Relationship"):
          temp['object'] = value
        elif(attribType=="GeoProperty"):
          temp['value'] = __createLocationEntry__(value)
        createBody[key] = temp
  def __setupNotificationHandling__(self, notificationListener, subscriptionId):
    self.subscriptionIds.append(subscriptionId)
    self.server.addListener(subscriptionId, notificationListener)
  def __getEntityInfos__(self, entityType, entityId, idPattern):
    result = {}
    if(entityId):
      result['id'] = entityId
    if(entityType):
      result['type'] = entityType
    if(idPattern):
      result['idPattern'] = idPattern
    return [result]
  def __getNotificationParams__(self, attribs):
    result = {}
    if(attribs):
      result['attributes'] = attribs
    result['format'] = "normalized"
    result['endpoint'] = {'accept': 'application/ld+json' , 'uri': self.notificationEndpoint}
    return result
  def __getHeaderForAtContext__(self, atContext):
    result = {}
    result['Accept'] = 'application/ld+json'
    result['Content-type'] = 'application/json'
    if(atContext):
      result['Link'] = '<' + atContext + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    return result
  def __doDelete__(self, url, headers):
    req = Request(url, headers=headers, method='DELETE')
    response = urlopen(req)
  def __doGet__(self, url, headers):
    req = Request(url, headers=headers)
    response = urlopen(req)
    result = response.read()
    return json.loads(result)
  def __doPost__(self, url, headers, body, method = "POST"):
    req = Request(url, json.dumps(body).encode('utf-8'), headers, method=method)
    response = urlopen(req)
    result = response.read()
    return result.decode()
  def __doPatch__(self, url, headers, body):
    return self.__doPost__(url, headers, body, "PATCH")
  def __doPut__(self, url, headers, body):
    return self.__doPost__(url, headers, body, "PUT")

  def shutdown(self):
    """Shuts the client down. Removes all existing subscriptions and kills the internal webserver
    """
    for subId in self.subscriptionIds:
      self.unsubscribe(subId)
    self.server.shutdown()
    self.thread.kill()
  def createEntity(self, entityId, entityType, properties=None, relationships=None, geoProperty=None, coordinates=None, description=None, atContext=None):
    """Creates an NGSI-LD Entity in the connected broker.

    Args:
        entityId: A URI string representing a system unique ID.
        entityType: A URI string representing the type of the entity.
        properties: <Optional> A dictionary of properties to be added. key=property name. value=property value.
                    For multivalue a list can be provided. Unique datasetIds will be generated.
                    For manually setting the datasetId 2 entries have to be provided in a dict in the
                    multivalue list. datasetId and value. datasetId has to be a URI string. e.g.
                    [{'datasetId': 'urn:mydataset:123', 'value': 'testvalue1'}, 'testvalue2']
        relationships: <Optional> A dictionary of relationships to be added. key=relationship name. value=relationship id.
                    value has to be a valid URI.
                    For multivalue a list can be provided. Unique datasetIds will be generated.
                    For manually setting the datasetId 2 entries have to be provided in a dict in the
                    multivalue list. datasetId and value. datasetId has to be a URI string. e.g.
                    [{'datasetId': 'urn:mydataset:123', 'value': 'testvalue1'}, 'testvalue2']
        geoProperty: <Optional> A dictionary of geo properties to be added. key=geo property name. value=geo property value.
                    value has to be similar to geo:json, so 
                    a list of (of a list)* of long and lat coordinates. depending on the type (point, line, polygon, etc.)
        coordinates: <Optional> The special location attribute of an entity.
                    value has to be similar to geo:json, so 
                    a list of (of a list)* of long and lat coordinates. depending on the type (point, line, polygon, etc.)
        description: <Optional> A human readable description string.
        atContext: <Optional> A link to a JSON-LD @context file
    Returns:
        None
    Raises:
        ValueError: If some conditions from the client are not met.
        HttpError: Raises NGSI-LD defined errors.
    """
    createBody = {}
    createBody['id'] = entityId
    createBody['type'] = entityType
    url = self.baseURL + "entities/"
    if(coordinates):
      self.__createLocationEntry__(createBody, coordinates)
    if(description):
      createBody['description'] = description
    self.__addAttribs__(createBody, properties)
    self.__addAttribs__(createBody, relationships, "Relationship")
    self.__addAttribs__(createBody, geoProperty, "GeoProperty")
    headers = self.__getHeaderForAtContext__(atContext)
    self.__doPost__(url, headers, createBody)
  def getEntity(self, entityId, atContext=None):
    """Retrieve a specific entity by it's ID.

    Args:
        entityId: A URI string representing a system unique ID.
         atContext: <Optional> A link to a JSON-LD @context file
    Returns:
        A dict containing the NGSI-LD stucture of an entity
    Raises:
        ValueError: If some conditions from the client are not met.
        HttpError: Raises NGSI-LD defined errors.
    """
    header = self.__getHeaderForAtContext__(atContext)
    url = self.baseURL + "entities/" + entityId
    return self.__doGet__(url, header)
  def query(self, ids=None, idPattern=None, entityType=None, attrs=None, q=None, georel=None, geometry=None, coordinates=None, geoproperty=None, atContext=None):
    """Query entities from the configured broker.
    As multiple combinations are possible all values are considered optional by the client. 
    However NGSI-LD restrictions still apply and you might get HttpErrors or ValueErrors when an invalid combination is provided
    Args:
        ids: A comma seperated string of IDs to be queried.
        idPattern: A regex string to match IDs against.
        entityType: A type to query for. Is mandatory unless attrs is provided.
        attrs: A comma seperated string of attribute names (property, relationship, geoproperty) to be queried for.
               Is mandatory unless entityType is provided.
        q: A string containing a NGSI-LD query working on attributes, e.g. vehicleId=='NCC1701'.
        georel: A geo relation used for a geoquery.
        geometry: A geometry type of a the provided coordinates.
        coordinates: coordinates to match against in the geoquery. 
        geoproperty: the geoproperty of an entity to match against. Defaults to the location parameter
        atContext: A link to a JSON-LD @context file.
    Returns:
        A list of dicts holding NGSI-LD entities which match against the query parameters.
    Raises:
        ValueError: If some conditions from the client are not met.
        HttpError: Raises NGSI-LD defined errors.
    """

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
    return self.__doGet__(url[:-1], self.__getHeaderForAtContext__(atContext))
  def update(self, entityId, properties=None, relationships=None, geoProperty=None, atContext=None):
    url = self.baseURL + "entities/" + entityId + "/attrs"
    body = {}
    self.__addAttribs__(body, properties, "Property")
    self.__addAttribs__(body, relationships, "Relationship")
    self.__addAttribs__(createBody, geoProperty, "GeoProperty")
    self.__doPatch__(url, self.__getHeaderForAtContext__(atContext), body)
    return
  def append(self, entityId, properties=None, relationships=None, geoProperty=None, atContext=None, noOverwrite=False):
    url = self.baseURL + "entities/" + entityId + "/attrs"
    if(noOverwrite):
      url += "?options=noOverwrite"
    body = {}
    self.__addAttribs__(body, properties, "Property")
    self.__addAttribs__(body, relationships, "Relationship")
    self.__addAttribs__(createBody, geoProperty, "GeoProperty")
    self.__doPost__(url, self.__getHeaderForAtContext__(atContext), body)
    return
  def delete(self, entityId, attribName = None, atContext = None):
    url = self.baseURL + "entities/" + urllib.parse.quote(entityId) + "/"
    header = self.__getHeaderForAtContext__(atContext)
    if(attribName):
      url = url + "/attrs/" + urllib.parse.quote(attribName)
    self.__doDelete__(url, header)
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
      subscription['entities'] = self.__getEntityInfos__(entityType, entityId, idPattern)
    subscription['notification'] = self.__getNotificationParams__(attribs)
    subscriptionId = self.__doPost__(url, self.__getHeaderForAtContext__(atContext), subscription)
    self.__setupNotificationHandling__(notificationListener, subscriptionId)
    return subscriptionId    
  def unsubscribe(self, subscriptionId):
    self.__doDelete__(self.baseURL + "subscriptions/" + subscriptionId, self.__getHeaderForAtContext__(None))
    self.subscriptionIds.remove(subscriptionId)
    self.server.removeListener(subscriptionId)

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
