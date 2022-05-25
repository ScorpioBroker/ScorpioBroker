***************
Operation flows
***************

Entity Create/Update/Append
###########################

.. figure:: figures/flow-1.png

The Figure is showing the operational flow of entity create/update/append in the Scorpio Broker system. Following are the marked steps interpretation:

1. An application calls the NGSI-LD compliant interface (exposed by service API gateway) to create/update/append an entity in the form of the HTTP POST request.

2. The request enters in service API gateway.

 2.1. The service API gateway discovers the actual serving micro-service endpoints (where the incoming requests need to be forwarded) from discovery & registry service.

 2.2. The service API gateway forwards the HTTP request to the Entity Manager micro-service.

3. From Entity Manager micro-service our request firstly goes in the database and internally calls an LDContext resolver service to resolve the payload with the given context sent along with the POST request. Once the payload is resolved with context, it now fetches the previously stored data/entities from the database and validates the requested entity against the existing stored entities based on EntityID.

- If the entity is already present (or with all the attributes and values that are requested to be modified), an error message (“already exists”) will be responded for the same and no further step will be executed.

- Else it will move for further processing.

4. The Entity Manager (EM) will do publish/store and send the response to the requester for the requested Entity creation operation given as follows:

 4.1. EM publishes in the Kafka under Topic “ENTITY”.

 4.2. EM publishes in the Kafka under Topic “Entity_Create/Update/Append” as well.

 4.3. Upon successful pub operation, EM will send the response back.

**Note**: “ENTITY” topic will save all changes of an entity done over a period of time by any of the create/update/append operations of an entity. However, “Entity_Create/Update/Append” Topic (specific to CREATE operation) will only store the data changes of entity create operation only. Having different topics per operation will avoid ambiguity situations among different consumers different requirements. E.g. the subscription manager may need to subscribe for the whole entity, a set of specific attributes, or might be some value change of certain attributes. So, managing all these requirements would be hard if a separate topic per operation is not maintained and would be very simplified to provide direct delta change in data for the given entity at any point in time if separate topics per operation are maintained. Therefore, putting all operations data in a single topic cannot offer the required decoupling, simplification, and flexibility to subscribe/manage at operations, data, or delta data level requirements. So that’s why creating separate topics per operation and one common topic for recording all changes (require to validate the whole entity changes for all operations over a period of time) of all operations to the given entity is the favorable design choice. The context for the given payload is being stored by the LDContext resolver service in the Kafka topic under the name AtContext.

5. Entity Manager also prepares for registration of the entity data model in the Context Source Registry. Following are the further functions it performs to achieve the same:

 5.1. So it prepares the csource registration payload from the entity payload and fills the necessary field (like id, endpoint as broker IP, location, etc.). Afterwards, Entity Topic sends this created context source payload to the Context Source Registry.

 5.2. Context Source Registry listens to this context source payload and then writes this created context source payload in the Registry Topic. So that Context Source Registry is able to know that some entity has registered.

6. When a message gets published to Kafka Topic, the consumers of that topic will get notified who has subscribed or listening to those topics. In this case, the consumers of “Entity” Topic upon receiving notification will do the following:

 6.1. Subscription Manager when getting a notification for the related entity from the Entity Topic, it will check for the notification validation for the current entity and checks if the notification needs to be sent accordingly.

 6.2. Subcription Manager sends this notification further to the "I_REGSUB" Topic that entity is being subscribed.
  

Entity Subscription
###################

.. figure:: figures/flow-2.png

The Figure is showing the operational flow of entity subscription in the Scorpio Broker system. Following are the marked steps interpretation:

1. An application calls the NGSI-LD compliant interface (exposed by service API gateway) to subscribe for an entity (or attribute) in the form of the HTTP POST request.

2. The request enters in service API gateway.

 2.1. The service API gateway discovers the actual serving micro-service endpoints (where the incoming requests need to be forwarded) from discovery & registry service.

 2.2. The service API gateway forwards the HTTP request to the Subscription Manager micro-service.

3. The Subscription Manager internally calls an LDContext resolver service to resolve the payload with the given context sent along with the POST request. The subscription manager fetches the previously stored data/entities from the Topic “ENTITY” and validates the requested entity against the existing stored values based on EntityID.

- If the data for the current request is already present, an error message will be responded for the same and no further step will be executed.

- Else it will move for further processing.

4. The Subscription Manager (SM) will publish/store and send the response to the requestor for the requested operation given as follows:

 4.1. Subscription Manager publish the subscription in the Kafka under Topic “I_REGSUB”

 4.2. Subscription Manager will start the notification functionality and will start/keep listening for related subscription on.

 4.2.1. Entity subscription request from “ENTITY” Topic same as step 3. 

 4.2.2. Upon successful subscription condition of subscription request, Subscription Manager will notify the subscribed entity to the given endpoint back.

 4.3. Upon successful sub operation, Subscription Manager will send the response back to the API gateway.

 4.4. API gateway sends back the response to the end-user/requestor.


Query
#####

.. figure:: figures/flow-3.png

The Figure is showing the operational flow of query in the Scorpio Broker system. Following are the marked steps interpretation:

1. An application calls the NGSI-LD compliant interface (exposed by service API gateway) to query for entities/an entity/attribute in the form of an HTTP GET request.

2. The request enters in service API gateway.

 2.1. The service API gateway discovers the actual serving micro-service endpoints (where the incoming requests need to be forwarded) from discovery & registry service.

 2.2. The service API gateway forwards the HTTP request to the Query Manager micro-service.

3. The Query Manager now fetches the previously stored data/entities directly from database.

4. Now, query response is being build after fetching the data from DB for the requested query and afterwards our query response gets published back in the Query Manager.

5. Query Manager gets the response for the query request.

 5.1. It sends the HTTP response back to the API gateway.

 5.2. API gateway sends back the response to the end-user/requestor.


Context Source Registration
###########################

.. figure:: figures/flow-4.png

The Figure is showing the operational flow of context source registration in the Scorpio Broker system. Following are the marked steps interpretation:

1. An application calls the NGSI-LD compliant interface (exposed by service API gateway) to csource registration for in the form of an HTTP POST request.

2. The request enters in service API gateway.

 2.1. The service API gateway discovers the actual serving micro-service endpoints (where the incoming requests need to be forwarded) from discovery & registry service.

 2.2. The service API gateway forwards the HTTP request to the Context Source Registry micro-service.

3. The Context Source Registry now fetches the previously stored data/entities from the Database.

- If the entry for the request csource is already present it exits the processing and informing the same to the requester. If it is not present, then it continues for further processing and sends back request to Context Source Registry micro-service.

- Now the Context Source Registry performs some basic validation to check if this is a valid request with the valid payload.

- After that it will move for further processing.

4. Context Source Registry will keep listening to the ENTITY topic and for any new entry.

5. When Context Source Registry gets a new entry, it prepares the response for context source registration request.

 5.1. Sends the Http response back to the API gateway.

 5.2. API gateway sends back the response to the end-user/requester.

6. Context Source Registry now writes this payload into the "REGISTRY" Topic.

7. When REGISTRY Topic get the request response of new entry. It sends context source registration response data further to Registry Subscription Manger for subcribing to our registered entity. 

**Note**: For Conext Source Update request only the payload will get changes and in step 3 upon validation for the existing entity it will not exit rather it will update the retrieved entity and write it back into the Kafka. The rest of the flow will remain mostly the same.



Context Source Subscription
###########################

.. figure:: figures/flow-5.png

The Figure is showing the operational flow of context source subscription in the Scorpio Broker system. Following are the marked steps interpretation:

1. An application calls the NGSI-LD compliant interface (exposed by service API gateway) to csource updates in the form of an HTTP POST request.

2. The request enters in service API gateway.

 2.1. The service API gateway discovers the actual serving micro-service endpoints (where the incoming requests need to be forwarded) from discovery & registry service.

 2.2. The service API gateway forwards the HTTP request to the Registry Subscription Manager micro-service.

3. The Registry Subscription Manager now fetches the previously stored subscription data from the Topic “REGISTRY”.

- If the entry for the request context source subscription is already present it exits the processing and informing the same to the requester.

- If it is not present, then it continues for further processing..

4. The Registry Subscription manager prepares the response for context source subscription request and

 4.1. sends the Http response back to the API gateway.

 4.2. API gateway sends back the response to the end-user/requester.

5. Registry Subscription Manager now writes this payload into the I_NOTIFY Topic.

6. When I_NOTIFY Topic get the request response of new entry. It sends context source subscription response data further to Subscription Manager.



History
#######

.. figure:: figures/flow-6.png

The Figure is showing the operational flow of history in the Scorpio Broker system. Following are the marked steps interpretation:

1.An application calls the NGSI-LD compliant interface (exposed by service API gateway) to the history manager in the form of an HTTP POST request.

2. The request enters in service API gateway.

 2.1 The service API gateway discovers the actual serving micro-service endpoints (where the incoming requests need to be forwarded) from discovery & registry service.

 2.2 The service API gateway forwards the HTTP request to the History Manager micro-service.

3. The history manager now executes the EVA algorithm approach on the received payload and push payload attributes to Kafka topic “TEMPORAL”.

**Note**: History Manager must walk through each attribute at the root level of the object (except @id and @type). Inside each attribute, it must walk through each instance (array element). Then, it sends the current object to the Kafka topic TEMPORALENTITY.

4. The history manager will keep listening to the “TEMPORAL” topic and for any new entry and performs the relative operation in the database.