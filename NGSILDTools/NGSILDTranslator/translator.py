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
  return ngsiLdContent["id"]
def doNGSILDCreate(ngsiLdContent):
  req = urllib.request.Request(ldHost + "/ngsi-ld/v1/entities/", json.dumps(ngsiLdContent).encode('utf-8'), ldHeaders)
  response = urllib.request.urlopen(req)
  return ngsiLdContent["id"]
def sendToBroker(ngsiLdContent, ldHost, ldHeaders):
  #print("sending data")
  #print(json.dumps(ngsiLdContent).encode('utf-8'))
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
            print("ERROR: " + str(e))
            return
        else:
          print("ERROR: " + str(e1))
          return
    except urllib.error.URLError as e2:
      print("ERROR: " + str(e2))
      return
    idsAlreadyPosted.append(entityId)
if (__name__ == "__main__"):
  configFile = sys.argv[1]
  with open(configFile) as json_file:
    config = json.load(json_file)
  idsAlreadyPosted = []
  sourceConfig = config['source']
  translate = config["translate"]
  isList = False
  contentIndex = None
  if 'isList' in sourceConfig:
    isList = sourceConfig['isList']
  if 'contentIndex' in sourceConfig:
    contentIndex = sourceConfig['contentIndex']
  ldHost = config["to"]
  ldHeaders = config["toHeaders"]
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