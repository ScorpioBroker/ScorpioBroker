import urllib
import json
from urllib.request import Request
from urllib.request import urlopen

class NGSIClient:
  def __init__(self, baseURL, addSuffix = True):
    if(baseURL[:-1] != '/'):
      self.baseURL = baseURL + "/"
    else:
      self.baseURL = baseURL
    if(addSuffix):
      self.baseURL = baseURL + "ngsi-ld/v1/"
    return
    
  def addAttribs(self, createBody, attribs, properties = True):
    if(not attribs):
      return
    for key, value in attribs.values():
      temp = {}
      if(properties):
        temp['type'] = 'Property'
      else:
        temp['type'] = 'Relationship'
      temp['value'] = value
      createBody[key] = temp
  def createEntity(self, entityId, entityType, properties=None, relationships=None, location=None, locationType=None, description=None, atContext=None):
    createBody = {}
    createBody['id'] = entityId
    createBody['type'] = entityType
    if(location):
      self.createLocationEntry(location, locationType)
    if(description):
      createBody['description'] = description
    self.addAttribs(createBody, properties)
    self.addAttribs(createBody, relationships, False)
    
  def getEntity(self, entityId, atContext):
    header = self.getHeaderForAtContext(atContext)
    url = baseURL + "entities/entityId"
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
  def delete(self, entityId, attribName, atContext):
    url = self.baseURL + "entities/" + urllib.parse.quote(entityId) + "/"
    header = self.getHeaderForAtContext(atContext)
    if(attribName):
      url = url + "/attrs/" + urllib.parse.quote(attribName)
    self.doDelete(url, header)
  def subscribe(self):
    return
  def getHeaderForAtContext(self, atContext):
    result = {}
    result['Accept'] = 'application/ld+json'
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
    
bla = NGSIClient("http://localhost:9090/")
bla.query(entityType="https://uri.fiware.org/ns/data-models%23WasteContainer")