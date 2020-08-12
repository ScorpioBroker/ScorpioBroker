import urllib
import json
import time
import urllib.error
import urllib.request
import urllib.parse
import _thread
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import sys
import os



  
def importFile(file, ldHost, ldHeaders, isList):
  with open(file, 'r', encoding='utf-8') as filePointer:
    data = json.load(filePointer)
  forwardContent(getActualContent(data, False), isList)
def getActualContent(data, isRaw = True):
  global contentIndex
  if(isRaw):
    content = json.loads(data)
  else:
    content = data
  if(contentIndex != None):
    for subIndex in contentIndex.split("."):
      content = content[subIndex]
  return content
def pullFromSource(sourceHost, sourceHeaders, pollTime, ldHost, ldHeaders, isList):
  req = urllib.request.Request(sourceHost, headers=sourceHeaders)
  try:
    while True:
      try:
        response = urllib.request.urlopen(req)
        data = response.read().decode('utf-8')
        forwardContent(getActualContent(data), isList)
      except Exception as e:
        print(e)
        pass
      time.sleep(pollTime)
  except KeyboardInterrupt:
    pass
def startCallback(hostname, port):
  httpd = HTTPServer(('hostname', port), Server)
  try:
    httpd.serve_forever()
  except KeyboardInterrupt:
    pass
  httpd.server_close()
  return
def subscribeToRemoteHost(sourceConfig):
  if('subscribe' in sourceConfig):
    sleep(2000) #we wait a moment to let the callback server start
    body, headers = None
    subscribe = sourceConfig['subscribe']
    host = subscribe['host']
    method = subscribe['method']
    body = subscribe['body']
    headers = subscribe['headers']
    req = urllib.request.Request(host, data=body, headers=headers, method=method)
    response = urllib.request.urlopen(req)
  return
class GrowingList(list):
  def __setitem__(self, index, value):
    if index >= len(self):
      self.extend([None]*(index + 1 - len(self)))
    list.__setitem__(self, index, value)
    
class Server(BaseHTTPRequestHandler):
  def __init__(self, *args, **kvargs):
    super(Server,self).__init__(*args, **kvargs) 

  def do_POST(self):
    length = int(self.headers.get('Content-Length'))
    postBody = self.rfile.read(length)
    global isList
    forwardContent(getActualContent(postBody), isList)
    self.respond()
    return
  def respond(self):
    self.send_response(200)
    self.send_header('Content-type', 'text/html')
    self.end_headers()
def findInArray(key, contentList):
  findString = key.split(".*.", 1)[1]
  for entry in contentList:
    result = findItem(findString, entry)
    if(result[0] != None):
      if(type(result[0]) == list and len(result[0]) <= 0):
        continue
      return result[0]
  return None

def forwardContent(content, isList):
  #print('Is List ' + str(isList))
  if(isList and type(content)==list):
    for entry in content:
      forwardContent(entry, False)
    return
  global translate
  FALL_BACK_TERM = '_'
  if('type' in translate):
    typeId = translate['type']
    if typeId in content and str(content[typeId]) in translate:
      ngsiLdContent = parseContent(content, translate[typeid])
    elif FALL_BACK_TERM in translate:
      ngsiLdContent = parseContent(content, FALL_BACK_TERM)
    else:
      print('ignoring item ' + content)
  else:
    ngsiLdContent = parseContent(content, translate['*'])
  global ldHost
  global ldHeaders
  sendToBroker(ngsiLdContent, ldHost, ldHeaders)  
def findItem(key, temp):
  MY_VALUE_CHAR = "&&&&"
  MY_PREFIX_CHAR = "$$"
  prefix = None
  result = [None, None]
  for subs in key.split("."):
    if(subs[:2] == MY_PREFIX_CHAR):
      prefix = subs[2:]
    else:
      if(subs.isdigit() and type(temp)==list):
        tempindex = int(subs)
        if(len(temp) - 1 >= tempindex):
          temp = temp[tempindex]
        else:
          temp = None
          break
      elif(subs == "*" and type(temp)==list):
        temp = findInArray(key, temp)
        break
      else:
        if(subs in temp):
          temp = temp[subs]
        else:
          temp = None
          break
  result[0] = temp
  result[1] = prefix
  return result
def parseContent(content, translate):
  MY_VALUE_CHAR = "&&&&"
  MY_PREFIX_CHAR = "$$"
  result = {}
  failedItems = set()
  #print("Parsing content")
  #print(str(content))
  #print("Translate table")
  #print(str(translate))
  for value in translate:
    key = translate[value]
    temp = content
    setValue = False
    prefix = None
    if len(value) < 4 or value[-4:] != MY_VALUE_CHAR:
      #print("subsplit start")
      temp1 = findItem(key, temp)
      temp = temp1[0]
      prefix = temp1[1]
    if(temp == None):
      failedItems.add(value.split(".")[0])
      continue
    if prefix != None:
      temp = prefix + str(temp)
      key = prefix + str(key)
    tempresult = result;
    translationTarget = value.split(".")
    mylength = len(translationTarget)
    for i in range(0, mylength):
      subs = translationTarget[i]
      index = subs
      if (subs.isdigit()):
        index = int(subs)
      if(i == mylength -2 and translationTarget[i+1] == MY_VALUE_CHAR):
        tempresult[index] = key
        break
      if(i == mylength -1):
        tempresult[index] = temp
        break
      if not index in tempresult:
        if translationTarget[i+1].isdigit():
          tempresult[index] = GrowingList()
        else:
          tempresult[index] = {}
      tempresult = tempresult[index]
  for fail in failedItems:
    if (fail in result):
      if ('value' in result[fail] and result[fail]['value'] and not isinstance(result[fail]['value'], dict)):
        continue
      if(not ('value' in result[fail]) or not result[fail]['value']):
        del result[fail]
        continue
      if(not 'value' in result[fail]['value']):
        del result[fail]
  return result
def doNGSILDUpdate(ngsiLdContent):
  ngsiId = urllib.parse.quote(ngsiLdContent["id"].strip())
  del ngsiLdContent["id"]
  del ngsiLdContent["type"]
  if "name" in ngsiLdContent:
    del ngsiLdContent["name"]
  req = urllib.request.Request(ldHost + "/ngsi-ld/v1/entities/" + ngsiId + "/attrs", json.dumps(ngsiLdContent).encode('utf-8'), ldHeaders, method="POST")
  response = urllib.request.urlopen(req)
  return ngsiId
def doNGSILDCreate(ngsiLdContent):
  req = urllib.request.Request(ldHost + "/ngsi-ld/v1/entities/", json.dumps(ngsiLdContent).encode('utf-8'), ldHeaders)
  response = urllib.request.urlopen(req)
  return ngsiLdContent["id"]
def sendToBroker(ngsiLdContent, ldHost, ldHeaders):
  print("sending data")
  print(json.dumps(ngsiLdContent).encode('utf-8'))
  global idsAlreadyPosted
  if(ngsiLdContent['id'] in idsAlreadyPosted):
    doNGSILDUpdate(ngsiLdContent)
  else:
    try:
      entityId = doNGSILDCreate(ngsiLdContent)
    except urllib.error.HTTPError as e1:
        if(409 == e1.code):
          try:
            entityId = doNGSILDUpdate(ngsiLdContent)
          except Exception as e:
            print("ERROR1: " + str(e))
            print(e)
            return
        else:
          print("ERROR2: " + str(e1))
          print(e1)
          return
    except urllib.error.URLError as e2:
      print("ERROR3: " + str(e2))
      print(e2)
      return
    idsAlreadyPosted.append(entityId)
def startKafkaConsumer(bootstrap, topics, groupId, schema, schemaRegistry, generic):
  from confluent_kafka.avro import AvroConsumer
  from confluent_kafka.avro.serializer import SerializerError
  from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
  import datetime
  config = {
      'bootstrap.servers': bootstrap,
      'group.id': groupId}
  if schemaRegistry:
    config['schema.registry.url'] = schemaRegistry
  if schema:
    c = AvroConsumer(config, reader_value_schema=schema)
  else:
    c = AvroConsumer(config)
  sr = CachedSchemaRegistryClient({
      'url': schemaRegistry
  })
  c.subscribe(topics)
  while True:
    try:
      msg = c.poll(10)
      if msg is None:
        continue
      if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue
      valueSchema = sr.get_latest_schema(msg.topic() + '-value')
      if valueSchema[1].namespace != None and valueSchema[1].namespace != "":
        schemaName = valueSchema[1].namespace + ":"
      else:
        schemaName = "urn:"
      schemaName = schemaName + valueSchema[1].name
      if generic:
        ngsiLd = {}
        ngsiLd['id'] = "urn:midih:mole:" + str(msg.topic())
        ngsiLd['type'] = schemaName
        ngsiLd['@context'] = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        for key, value in msg.value().items():
          temp = {}
          temp['type'] = 'Property'
          temp['value'] = value
          temp['observedAt'] = datetime.datetime.fromtimestamp(msg.timestamp()[1]/1000).strftime('%Y-%m-%dT%H:%M:%SZ')
          myKey = key
          if myKey in ["name", "location"]:
            myKey = "my" + myKey
          ngsiLd[myKey] = temp
        global ldHost
        global ldHeaders
        sendToBroker(ngsiLd, ldHost, ldHeaders)
      else:
        value = msg.value()
        value['__schemaname'] = schemaName
        value['__key'] = msg.key()
        forwardContent(value, False)
    except Exception as e:
      print("ERROR: " + str(e))
      print(e)
      continue
  c.close()
def parseConfigAndGo(config):
  global ldHost
  global ldHeaders
  global translate
  global sendOut
  global isList
  sourceConfig = config['source']
  if 'translate' in config:
    translate = config["translate"]  
  sendOut = True;
  if('sendOut' in config):
    sendOut = config['sendOut']
  isList = False
  contentIndex = None
  if 'isList' in sourceConfig:
    isList = sourceConfig['isList']
  if 'contentIndex' in sourceConfig:
    contentIndex = sourceConfig['contentIndex']
  ldHeaders = config["toHeaders"]
  ldHost = os.getenv('NGSI_LD_TRANSLATOR_TO', config["to"])
  print("ld host " + ldHost)
  if(sourceConfig['type'] == 'subscribe'):
    port = sourceConfig["callback"]['port']
    hostname = sourceConfig["callback"]['hostname']
    _thread.start_new_thread(subscribeToRemoteHost, (sourceConfig,))
    startCallback(hostname, port)
  elif(sourceConfig['type'] == 'pull'):
    sourceHost = sourceConfig["from"]
    sourceHeaders = sourceConfig["fromHeaders"]
    pollTime = sourceConfig["pollTime"]
    pullFromSource(sourceHost, sourceHeaders, pollTime, ldHost, ldHeaders,isList)
  elif(sourceConfig['type'] == 'importFromFile'):
    file = sourceConfig["from"]
    importFile(file, ldHost, ldHeaders,isList)
  elif(sourceConfig['type'] == 'kafkaAvro'):
    bootstrap = os.getenv('NGSI_LD_TRANSLATOR_BOOTSTRAP', sourceConfig['bootstrap'])
    topics = os.getenv('NGSI_LD_TRANSLATOR_TOPICS', sourceConfig['topics'])
    topics = topics.split(',')
    groupId = os.getenv('NGSI_LD_TRANSLATOR_GROUP_ID', sourceConfig['groupId'])
    schema = None
    if 'schema' in sourceConfig:
      schema = sourceConfig['schema']
    schemaRegistry = None
    if 'schemaRegistry' in sourceConfig:
      schemaRegistry = sourceConfig['schemaRegistry']
    schema = os.getenv('NGSI_LD_TRANSLATOR_SCHEMA', schema)
    schemaRegistry = os.getenv('NGSI_LD_TRANSLATOR_SCHEMA_REGISTRY', schemaRegistry)
    
    generic = sourceConfig['generic']
    startKafkaConsumer(bootstrap, topics, groupId, schema, schemaRegistry, generic)

if (__name__ == "__main__"):
  configFile = sys.argv[1]
  with open(configFile) as json_file:
    config = json.load(json_file)
  idsAlreadyPosted = []
  parseConfigAndGo(config)