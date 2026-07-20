# Annexes

# Annexes

* [Introduction](introduction.md) - The purpose of identifiers is to allow uniquely identifying NGSI-LD elements (Entities, Context Subscriptions or Context
* [Entity identifiers](entity-identifiers.md) - In order to enable the participation of NGSI-LD in linked data scenarios, all Entities are identified by URIs.
* [NGSI-LD namespace](ngsi-ld-namespace.md) - NGSI-LD defines a specific URN [9] namespace intended to help API users to design readable, clean and simple identifiers
* [Introduction](introduction-C-1.md) - This annex is informative and is intended to show in action the JSON-LD representation defined by NGSI-LD.
* [Entity Representation](entity-representation.md) - C.2.1 Property Graph Figure C.2.1-1 shows a diagram representing a property graph to be used for the examples discussed
* [Property Graph](property-graph.md) - Figure C.2.1-1 shows a diagram representing a property graph to be used for the examples discussed in this clause.
* [Vehicle Entity](vehicle-entity.md) - Normalized Representation The normalized representation is a lossless representation of an Entity, where every Property
* [Parking Entity](parking-entity.md) - Normalized Representation The normalized representation is a lossless representation of an Entity, where every Property
* [@context](at-context.md) - The disposition of the @context can be as an inline JSON object, as a dereferenceable URI or as a (multiple) combination
* [Context Source Registration](context-source-registration.md) - Below there is an example representation of a Context Source Registration.
* [Context Subscription](context-subscription.md) - Below there is an example of a Context Subscription.
* [HTTP REST API Examples](http-rest-api-examples.md) - C.5.1 Introduction This clause introduces some simple usage examples of the NGSI-LD API (HTTP REST binding).
* [Introduction](introduction-C-5-1.md) - This clause introduces some simple usage examples of the NGSI-LD API (HTTP REST binding).
* [Create Entity of Type Vehicle](create-entity-of-type-vehicle.md) - C.5.2.1 HTTP Request POST /ngsi-ld/v1/entities/ Content-Type: application/ld+json Content-Length: 556 C.5.2.2 HTTP Respo
* [HTTP Request](http-request.md) - POST /ngsi-ld/v1/entities/ Content-Type: application/ld+json Content-Length: 556
* [HTTP Response](http-response.md) - 201 Created Location: /ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A4567
* [Query Entities](query-entities.md) - C.5.3.1 Introduction EXAMPLE: Give back all the Entities of type "Vehicle" whose brandName attribute is not "Mercedes".
* [Introduction](introduction-C-5-3-1.md) - EXAMPLE: Give back all the Entities of type "Vehicle" whose brandName attribute is not "Mercedes".
* [HTTP Request](http-request-C-5-3-2.md) - GET /ngsi-ld/v1/entities/?type=Vehicle&q=brandName!="Mercedes"&format=simplified Accept: application/ld+json Link:; rel=
* [HTTP Response](http-response-C-5-3-3.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:B9211", "type": "Vehicle", "brandName": "Volvo",
* [Query Entities (Pagination)](query-entities-pagination.md) - C.5.4.1 Introduction EXAMPLE: Give back all the Entities of type "Vehicle".
* [Introduction](introduction-C-5-4-1.md) - EXAMPLE: Give back all the Entities of type "Vehicle".
* [HTTP Request](http-request-C-5-4-2.md) - GET /ngsi-ld/v1/entities/?type= Vehicle&format=simplified&limit=2 Accept: application/ld+json Link:; rel="http://www.w3.
* [HTTP Response](http-response-C-5-4-3.md) - 200 OK Content-Type: application/ld+json Link:; rel="next"; type="application/ld+json" [ { "id": "urn:ngsi-ld:Vehicle:B9
* [Temporal Query](temporal-query.md) - C.5.5.1 Introduction EXAMPLE 1: Give back the temporal evolution of the attribute speed of Entities of type "Vehicle" wh
* [Introduction](introduction-C-5-5-1.md) - EXAMPLE 1: Give back the temporal evolution of the attribute speed of Entities of type "Vehicle" whose brandName attribu
* [HTTP Request #1](http-request-1.md) - GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018- 08-01T12:
* [HTTP Response #1](http-response-1.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:B9211", "type": "Vehicle", "speed": [ { | | "spe
* [HTTP Request #2](http-request-2.md) - GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed,brandName&timerel=between&tim eAt=2018
* [HTTP Response #2](http-response-2.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:B9211", "type": "Vehicle", "speed": [ { | | "spe
* [Temporal Query (Simplified Representation)](temporal-query-simplified-representation.md) - C.5.6.1 Introduction EXAMPLE: Give back the temporal evolution of the speed attribute for Entities of type "Vehicle" who
* [Introduction](introduction-C-5-6-1.md) - EXAMPLE: Give back the temporal evolution of the speed attribute for Entities of type "Vehicle" whose brandName attribut
* [HTTP Request](http-request-C-5-6-2.md) - GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018- 08-01T12:
* [HTTP Response](http-response-C-5-6-3.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:B9211", "type": "Vehicle", "speed": { "type": "P
* [Retrieve Available Entity Types](retrieve-available-entity-types.md) - C.5.7.1 Introduction EXAMPLE: Give back all entity types for which entity instances are currently available in the NGSI-
* [Introduction](introduction-C-5-7-1.md) - EXAMPLE: Give back all entity types for which entity instances are currently available in the NGSI-LD system.
* [HTTP Request](http-request-C-5-7-2.md) - GET /ngsi-ld/v1/types Accept: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+j
* [HTTP Response](http-response-C-5-7-3.md) - 200 OK Content-Type: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" { "i
* [Retrieve Details of Available Entity Types](retrieve-details-of-available-entity-types.md) - C.5.8.1 Introduction EXAMPLE: Give back the details of all entity types for which entity instances are currently availab
* [Introduction](introduction-C-5-8-1.md) - EXAMPLE: Give back the details of all entity types for which entity instances are currently available in the NGSI-LD sys
* [HTTP Request](http-request-C-5-8-2.md) - GET /ngsi-ld/v1/types?details=true Accept: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="app
* [HTTP Response](http-response-C-5-8-3.md) - 200 OK Content-Type: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" [ {
* [Retrieve Available Entity Type Information](retrieve-available-entity-type-information.md) - C.5.9.1 Introduction EXAMPLE: Give back the details of entity type "Vehicle" (for which entity instances are currently a
* [Introduction](introduction-C-5-9-1.md) - EXAMPLE: Give back the details of entity type "Vehicle" (for which entity instances are currently available in the NGSI-
* [HTTP Request](http-request-C-5-9-2.md) - GET /ngsi-ld/v1/types/Vehicle [Alternative with FQN: GET /ngsi-ld/v1/attributes/http%3A%2F%2Fexample.org%2Fvehicle%2FVeh
* [HTTP Response](http-response-C-5-9-3.md) - 200 OK Content-Type: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" { "i
* [Retrieve Available Attributes](retrieve-available-attributes.md) - C.5.10.1 Introduction EXAMPLE: Give back all attribute names for which entity instances are currently available in the N
* [Introduction](introduction-C-5-10-1.md) - EXAMPLE: Give back all attribute names for which entity instances are currently available in the NGSI-LD system that hav
* [HTTP Request](http-request-C-5-10-2.md) - GET /ngsi-ld/v1/attributes Accept: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application
* [HTTP Response](http-response-C-5-10-3.md) - 200 OK Content-Type: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" { "i
* [Retrieve Details of Available Attributes](retrieve-details-of-available-attributes.md) - C.5.11.1 Introduction EXAMPLE: Give back the details of all attributes for which entity instances are currently availabl
* [Introduction](introduction-C-5-11-1.md) - EXAMPLE: Give back the details of all attributes for which entity instances are currently available in the NGSI-LD syste
* [HTTP Request](http-request-C-5-11-2.md) - GET /ngsi-ld/v1/attributes?details=true Accept: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type
* [HTTP Response](http-response-C-5-11-3.md) - 200 OK Content-Type: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" [ {
* [Retrieve Available Attribute Information](retrieve-available-attribute-information.md) - C.5.12.1 Introduction EXAMPLE: Give back the details of the attribute named brandName (for which entity instances with a
* [Introduction](introduction-C-5-12-1.md) - EXAMPLE: Give back the details of the attribute named brandName (for which entity instances with an attribute of this na
* [HTTP Request](http-request-C-5-12-2.md) - GET /ngsi-ld/v1/attributes/brandName [Alternative with FQN: GET /ngsi-ld/v1/attributes/http%3A%2F%2Fexample.org%2Fvehicl
* [HTTP Response](http-response-C-5-12-3.md) - 200 OK Content-Type: application/json Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" { "i
* [Query Entities (Natural Language Filtering)](query-entities-natural-language-filtering.md) - C.5.13.1 Introduction EXAMPLE: Give back all the Entities of type "Vehicle" where the marque attribute in British Englis
* [Introduction](introduction-C-5-13-1.md) - EXAMPLE: Give back all the Entities of type "Vehicle" where the marque attribute in British English is "Vauxhall Viva".
* [HTTP Request](http-request-C-5-13-2.md) - GET /ngsi-ld/v1/entities/?type=Vehicle&attrs=marque&q=marque[en-GB]== "Vauxhall Viva"&format =simplified&lang=de Accept:
* [HTTP Response](http-response-C-5-13-3.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:A4567", "type": "Vehicle", "marque": "Opel Karl"
* [Temporal Query (Aggregated Representation)](temporal-query-aggregated-representation.md) - C.5.14.1 Introduction EXAMPLE: Give back the maximum and average speed of Entities of type "Vehicle" whose brandName att
* [Introduction](introduction-C-5-14-1.md) - EXAMPLE: Give back the maximum and average speed of Entities of type "Vehicle" whose brandName attribute is not "Mercede
* [HTTP Request](http-request-C-5-14-2.md) - GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018- 08-01T12:
* [HTTP Response](http-response-C-5-14-3.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:B9211", "type": "Vehicle", "speed": { "type": "P
* [Scope Queries](scope-queries.md) - C.5.15.1 Introduction EXAMPLE: Give back all the Entities of type "OffStreetParking" that are within the Scope /Madrid/C
* [Introduction](introduction-C-5-15-1.md) - EXAMPLE: Give back all the Entities of type "OffStreetParking" that are within the Scope /Madrid/Centro or /Madrid/Corte
* [HTTP Request](http-request-C-5-15-2.md) - GET /ngsi-ld/v1/entities/?type=OffStreetParking&scopeQ="/Madrid/Centro,/Madrid/Cortes" Accept: application/ld+json Link:
* [HTTP Response](http-response-C-5-15-3.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:OffStreetParking:Downtown1", "type": "OffStreetParking",
* [Temporal Scope Queries](temporal-scope-queries.md) - C.5.16.1 Introduction EXAMPLE: Give back the speed of all the Entities of type "Vehicle" that have been within the Scope
* [Introduction](introduction-C-5-16-1.md) - EXAMPLE: Give back the speed of all the Entities of type "Vehicle" that have been within the Scope /Madrid/Centro betwee
* [HTTP Request](http-request-C-5-16-2.md) - GET /ngsi-ld/v1/temporal/entities/?type=Vehicle&attrs=speed,scope&timerel=between&timeAt=2018-08-01T12:00:00Z&endTimeAt=
* [HTTP Response](http-response-C-5-16-3.md) - 200 OK Content-Type: application/ld+json [ { "id": "urn:ngsi-ld:Vehicle:B9211", "type": "Vehicle", "scope": { "type": "P
* [Date Representation](date-representation.md) - In NGSI-LD, a TemporalProperty is represented only by its value, i.e.
* [@context utilization clarifications](at-context-utilization-clarifications.md) - When expanding or compacting JSON-LD terms, the JSON-LD @context to be used is always the one provided in the current AP
* [Link header utilization clarifications](link-header-utilization-clarifications.md) - The JSON-LD Specification [2] states clearly that only one HTTP Link header with the link relationship is required to ap
* [@context processing clarifications](at-context-processing-clarifications.md) - JSON-LD Specification [2] says that "If a term is redefined within a context, all previous rules associated with the pre
* [ValueType datatype utilization clarifications](valuetype-datatype-utilization-clarifications.md) - Using JSON-LD [2] syntax, typed values can be expressed using the JSON-LD @type keyword when defining a term, where @typ
* [Entity with digital signature for a Property](entity-with-digital-signature-for-a-property.md) - As specified in [35], the atomic piece of information that creators can digitally sign in an NGSI-LD ecosystem is each s
* [Introduction](introduction-D-1.md) - These algorithms are informative but NGSI-LD implementations should aim at either implementing them as they are describe
* [Algorithm for transforming an NGSI-LD Entity into a JSON-LD document (ALG1)](algorithm-for-transforming-an-ngsi-ld-entity-into-a-json-ld-document-alg1.md) - This algorithm takes as input an NGSI-LD graph which top level node is a particular Entity and returns as output a JSON-
* [Algorithm for transforming an NGSI-LD Property into JSON-LD (ALG1.1)](algorithm-for-transforming-an-ngsi-ld-property-into-json-ld-alg1-1.md) - Let Ps be the Property that has to be transformed.
* [Algorithm for transforming an NGSI-LD Relationship into JSON-LD (ALG1.2)](algorithm-for-transforming-an-ngsi-ld-relationship-into-json-ld-alg1-2.md) - Let Rs be the Relationship that has to be transformed.
* [Foreword](foreword.md) - These algorithms described below are informative, but NGSI-LD implementations should aim at either implementing them as
* [Introduction](introduction-G-1.md) - G.1.0 Foreword Since Internationalization is not core to context information management, any direct support within NGSI-
* [Foreword](foreword-G-1-0.md) - Since Internationalization is not core to context information management, any direct support within NGSI-LD systems is l
* [Associating an Entity with a Natural Language](associating-an-entity-with-a-natural-language.md) - Where a context Entity is associated with a single natural language, include a well-defined Property indicating the natu
* [Associating a Property with a Natural Language](associating-a-property-with-a-natural-language.md) - Where a Property of a context entity can be associated to one more natural language, include additional metadata as a su
* [Associating as equivalent entity](associating-as-equivalent-entity.md) - Where equivalent context entities in multiple natural languages exist, they may be associated with each other through th
* [Natural Language Collation Support](natural-language-collation-support.md) - G.2.0 Foreword All strings within an NGSI-LD system are defined and sorted as a sequence of Unicode characters.
* [Foreword](foreword-G-2-0.md) - All strings within an NGSI-LD system are defined and sorted as a sequence of Unicode characters.
* [Maintain collations as metadata](maintain-collations-as-metadata.md) - - Create a subscription on the attribute (e.g.
* [Route language sensitive queries via a proxy](route-language-sensitive-queries-via-a-proxy.md) - Create a simple forwarding proxy around the NGSI-LD system.
* [Localization of Dates, Currency formats, etc.](localization-of-dates-currency-formats-etc.md) - G.3.0 Foreword Context data entities are designed to be interoperable and therefore all dates are held as UTC dates, all
* [Foreword](foreword-G-3-0.md) - Context data entities are designed to be interoperable and therefore all dates are held as UTC dates, all currency amoun
* [Localizing Dates](localizing-dates.md) - For example, if a system needs to display DateTime data in Islamic Date format The following request on the proxy: GET /
* [Actuators and feedback to the consumer](actuators-and-feedback-to-the-consumer.md) - Actuators are things that can change their state (light on/off) or execute actions (move forward, detect face, etc.).
* [Architecture for actuation](architecture-for-actuation.md) - In this architecture, the application acts as Context Consumer, and the terms are used interchangeably.
* [Structure of Commands and additional Properties](structure-of-commands-and-additional-properties.md) - H.3.0 Introduction The NGSI-LD system has, in addition to the usual NGSI-LD Properties representing the actuator's statu
* [Introduction](introduction-H-3-0.md) - The NGSI-LD system has, in addition to the usual NGSI-LD Properties representing the actuator's status, a set of additio
* [Property for listing available commands](property-for-listing-available-commands.md) - The additional Property dedicated to the list of available commands is as follows: "commands": { "type": "Property", "va
* [Properties for command endpoints](properties-for-command-endpoints.md) - For each available command, a set of three endpoints is to be additionally created within the NGSI-LD system, by means o
* [Communication model](communication-model.md) - H.4.1 Possible communication models This convention can be leveraged by two different communication models: - Subscripti
* [Possible communication models](possible-communication-models.md) - This convention can be leveraged by two different communication models: - Subscription/notification, where both the appl
* [Subscription/notification model](subscription-notification-model.md) - For the interaction to work, the Context Adapter, acting as a proxy to the actuator, subscribes to all command propertie
* [Forwarding model](forwarding-model.md) - The forwarding model uses registrations and forwarding of requests.
* [Implementation of the subscription-based actuation workflow](implementation-of-the-subscription-based-actuation-workflow.md) - workflow The Fed4IoT project (https://fed4iot.org) leverages the NGSI-LD architecture and the subscription/notification
* [Implementation of the registration-based actuation workflow](implementation-of-the-registration-based-actuation-workflow.md) - NGSI-LD specification clause H.6: Implementation of the registration-based actuation workflow.
