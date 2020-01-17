import urllib
import json
import time
import urllib.request


class GrowingList(list):
	def __setitem__(self, index, value):
		if index >= len(self):
			self.extend([None]*(index + 1 - len(self)))
		list.__setitem__(self, index, value)
		
def parseContent(content, translate):
	#print(content)
	MY_VALUE_CHAR = "&&&&"
	MY_PREFIX_CHAR = "$$"
	result = {}
	for value in translate:
		key = translate[value]
		temp = content
		setValue = False
		prefix = None
		suffix = None
		if len(value) < 4 or value[-4:] != MY_VALUE_CHAR:
			for subs in key.split("."):
				if(subs[:2] == MY_PREFIX_CHAR):
					prefix = str(subs[2:])
				elif(subs[-2:] == MY_PREFIX_CHAR):
					suffix = str(subs[:-2])
				else:
					if(subs.isdigit()):
						temp = temp[int(subs)]
					else:
						temp = temp[subs]
		if prefix != None:
			temp = prefix + str(temp)
			key = prefix + str(key)
		if suffix != None:
			temp = str(temp) + suffix
			key = str(key) + suffix
		tempresult = result;
		translationTarget = value.split(".")
		mylength = len(translationTarget)
		#print (value)
		for i in range(0, mylength):
			subs = translationTarget[i]
			index = subs
			#print(subs)
			if (subs.isdigit()):
				index = int(subs)
			if(i == mylength -2 and translationTarget[i+1] == MY_VALUE_CHAR):
				#print("valuechar found")
				tempresult[index] = key
				break
			if(i == mylength -1):
				if temp == None:
					temp = "Not Available"
				tempresult[index] = temp
				break
			if not index in tempresult:
				if translationTarget[i+1].isdigit():
					tempresult[index] = GrowingList()
				else:
					tempresult[index] = {}
			tempresult = tempresult[index]
	return result
	

firstRun = True

with open(str(sys.argv[1])) as json_file:
		config = json.load(json_file)
		
print(config)
translate = config["translate"]
#print(json.dumps(translate))
requesthost = config["from"]
requestheaders = config["fromheaders"]
ldhost = config["to"]
ldheader = config["toheaders"]
polltime = config["polltime"]
while(True):
	try:
		req = urllib.request.Request(requesthost, None, requestheaders)
		response = urllib.request.urlopen(req)
		data = response.read().decode('utf-8')
		content = json.loads(data)['features']
		
		for entry in content:
				ngsiLdContent = parseContent(entry, translate)
				print('--------------------------')
				print(json.dumps(ngsiLdContent))
				print('-------------------------')
			#if firstRun:
				try:
					req = urllib.request.Request(ldhost + "/ngsi-ld/v1/entities/", json.dumps(ngsiLdContent).encode('utf-8'), ldheader)
					response = urllib.request.urlopen(req)
				except Exception as e:
					print(e)
					id = ngsiLdContent["id"].strip()
					del ngsiLdContent["id"]
					del ngsiLdContent["type"]
					if "name" in ngsiLdContent:
						del ngsiLdContent["name"]
					#print(ldhost + "/ngsi-ld/v1/entities/" + id + "/attrs")
					req = urllib.request.Request(ldhost + "/ngsi-ld/v1/entities/" + id + "/attrs", json.dumps(ngsiLdContent).encode('utf-8'), ldheader, method="PATCH")
					response = urllib.request.urlopen(req)
#					firstRun = False
#			else:
#				try:
#					id = ngsiLdContent["id"].strip()
#					del ngsiLdContent["id"]
#					del ngsiLdContent["type"]
#					if "name" in ngsiLdContent:
#						del ngsiLdContent["name"]
#					req = urllib.request.Request(ldhost + "/ngsi-ld/v1/entities/" + id + "/attrs", json.dumps(ngsiLdContent).encode('utf-8'), ldheader,method="PATCH")
#					response = urllib.request.urlopen(req)
#				except Exception as e:
#					#print(e)
#					pass
	except Exception as e:
		print(e)
		pass
	time.sleep(polltime)

