****************
Context Hosting
****************

NGSI-LD Brokers optionally offer the capability to store and serve @contexts to clients. The stored @contexts may be managed by clients directly, via the APIs specified. Clients can store custom user @contexts at the Broker, effectively using the Broker as a @context server.
Moreover, in order to optimize performance, NGSI-LD Brokers may automatically store and use the stored copies of common @contexts as a local cache, downloading them just once, thus avoiding fetching them over and over again at each NGSI-LD request. In order for the Broker to understand if a needed @context is already in the local storage or not, the Broker uses the URL, where the @context is originally hosted, as an identifier for it in the local storage.

There are following cases handles to manage @context server:

- **Cached** : To be auto cached when broker request from external url, cached context will not be served by the broker on demand, and cached context should be invalidate after a fixed time.

- **Hosted** : When user post @context, it need to be stored in database with a unique id.

- **ImplicitlyCreated** : When a subscription body contains an array of contexts and notification is to be given in the form of application/json then these context should be hosted in broker and marked as implicitly created. 

We can store @context to database and also download from other resource and store in the in-memory like cache memory. 
 
**POST API**

•	Resource URI  : **/jsonldContexts**

	Request body: JSON Object 
	
	Payload body in the request contains a JSON object that has a root node named @context, which represents a JSON-LD "local context".
	
	Add new table in existing database for store the context:
	 - Table name: context
	
	Column Name: id , body , kind , timestamp

	Response Body: 
	 - 201 Created: The HTTP response shall include a local URI to the added @context


**GET API**

•	Resource URI : **/jsonldContexts** 

	Response Body: 200 Ok URL[] (show the list of @contexts)

•	Resource URI : **/jsonldContexts?details=true**

	Response Body: 200 OK  {URL,id, more details} [show the list of @contexts with context details]
	
	Details: Boolean
	
	Whether a list of URLs or a more detailed list of JSON Objects is requested
	
	Kind: String
	
	Can be either *"Cached"*, *"Hosted"*, or *"ImplicitlyCreated"*.

**DELETE API**

•	Resource URI : **/jsonldContexts/{contextId}**

	Response Body: 204 No content
	
•	Resource URI : **/jsonldContexts/{contextId}?reload=true**

	Resource Body: 204 No context 


Example for Context Hosting
############################

1. **POST API**

• POST - **http://localhost:9090/ngsi-ld/v1/jsonldcontext**

Payload:
::
    {
        "@context": {
            "stringproperty": "http://testdom.com/stringproperty",
            "intproperty": "http://testdom.com/intproperty",
            "floatproperty": "http://testdom.com/floatproperty",
            "complexproperty": "http://testdom.com/complexproperty",
            "testrelationship": "http://testdom.com/testrelationship",
            "TestType": "http://testdom.com/TestType"
        }
    }


2. **GET API**

- Show the list of @contexts
 
	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontext**
		
Response:
::
	[
		"http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc"
	]



- Show the list of @contexts with context details

	GET  - **http://localhost:9090/ngsi-ld/v1/jsonldcontext?details=true**
 
Response:
::
	[
		{
			"id": "urn:9155d599-0db4-4fb0-91ba-4f478090b0fc",
			"body": {
				"@context": {
					"TestType": "http://testdom.com/TestType",
					"intproperty": "http://testdom.com/intproperty",
					"floatproperty": "http://testdom.com/floatproperty",
					"stringproperty": "http://testdom.com/stringproperty",
					"complexproperty": "http://testdom.com/complexproperty",
					"testrelationship": "http://testdom.com/testrelationship"
				}
			},
			"kind": "hosted",
			"timestmp": "2023-02-09T11:10:07.707324",
			"url": "http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc"
		}
	]



- Show the list of @contexts with kind

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontext?kind=hosted**

Response:
::
		[
			"http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc"
		]



- Show the @context with particular URI

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc**
 
Response:
::
	{
		"@context": {
			"TestType": "http://testdom.com/TestType",
			"intproperty": "http://testdom.com/intproperty",
			"floatproperty": "http://testdom.com/floatproperty",
			"stringproperty": "http://testdom.com/stringproperty",
			"complexproperty": "http://testdom.com/complexproperty",
			"testrelationship": "http://testdom.com/testrelationship"
		}
	}



- Show the @context with particular URI with context details

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc?details=true**

Response:
::
	{
		"id": "urn:9155d599-0db4-4fb0-91ba-4f478090b0fc",
		"body": {
			"@context": {
				"TestType": "http://testdom.com/TestType",
				"intproperty": "http://testdom.com/intproperty",
				"floatproperty": "http://testdom.com/floatproperty",
				"stringproperty": "http://testdom.com/stringproperty",
				"complexproperty": "http://testdom.com/complexproperty",
				"testrelationship": "http://testdom.com/testrelationship"
			}
		},
		"kind": "hosted",
		"timestmp": "2023-02-09T11:10:07.707324",
		"url": "http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc"
	}


3. **DELETE API**


• DELETE - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc**

Response:
::
	204 No content




• DELETE - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc?reload=true**
		
Response:
::
	204 No content
