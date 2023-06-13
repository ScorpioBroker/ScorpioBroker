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

Context Hosting Example
-------------------------

1. Create Operation
====================

In order to create an context, we can hit the endpoint POST **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontext** with the given payload.

Payload:

.. code-block:: JSON

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


2. Query Operation
===================

- **Show the list of @contexts**

To show the list of @contexts, you can send an HTTP GET request to - **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontexts** and we will get the list of contexts.
	
	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts**

Response:

.. code-block:: JSON

 [
 	"http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc"
 ]



- **Show the list of @contexts with context details**

To show the list of @contexts with context details, you can send an HTTP GET request to - **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontexts?details=true** and we will get the list of contexts with context details.

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts?details=true**

Response:

.. code-block:: JSON

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


- **Show the list of @contexts with kind**

To show the list of @contexts with kind either it can be *Cached*, *Hosted* or *ImplicitlyCreated*, you can send an HTTP GET request to - **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontexts?kind=(kind}** here we are getting contexts with **kind=hosted**, so we will get the list of contexts whose kind is equals to *hosted*.

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts?kind=hosted**

Response:

.. code-block:: JSON

 [
	 "http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc"
 ]


- **Show the @context with particular URI**

To show the @context with a particular URI, you can send an HTTP GET request to - **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontexts/{id}** and we will get the @context with particular URI.

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc**
 
Response:

.. code-block:: JSON

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



- **Show the @context with particular URI and context details**

To show the @context with a particular URI and context details, you can send an HTTP GET request to - **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontexts/{id}?details=true** and we will get the @context with particular URI and context details.

	GET - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc?details=true**

Response:

.. code-block:: JSON

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


3. DELETE Operation
====================

If we want to delete the @context, then we need to make DELETE request with the URL **http://<IP Address>:<port>/ngsi-ld/v1/jsonldcontexts/{id}**.

	DELETE - **http://localhost:9090/ngsi-ld/v1/jsonldcontexts/urn:9155d599-0db4-4fb0-91ba-4f478090b0fc**