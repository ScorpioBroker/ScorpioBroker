---
type: NGSI-LD Term
title: "Terms"
description: "For the purposes of the present document, the following terms apply: NOTE 1: The letters \"NGSI-LD\" were added to most terms to confirm that they are distinct from other terms of similar/same name in u"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-3.1
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "3.1"
---

For the purposes of the present document, the following terms apply:
NOTE 1: The letters "NGSI-LD" were added to most terms to confirm that they are distinct from other terms of
similar/same name in use in other organizations, however, in the present document the letters "NGSI-LD"
are generally omitted for brevity.
NOTE 2: The use of URI in the context of the present document also includes the use of International Resource
Identifiers (IRIs) as defined in IETF RFC 3987 [23], which extends the use of characters to Unicode
characters [22] beyond the ASCII character set, enabling the support of languages other than English.
NGSI-LD Attribute: reference to both an NGSI-LD Property and to an NGSI-LD Relationship
NGSI-LD Attribute Instance (in case of temporal representation of NGSI-LD Entities): reference to an NGSI-LD
Attribute, at a specific moment in time of its temporal evolution, usually identified by its instanceId
NGSI-LD Central Broker: NGSI-LD Context Broker that only uses a local storage when serving NGSI-LD
requests, without involving any external Context Sources
NGSI-LD Context Broker: architectural component that implements all the NGSI-LD interfaces


NGSI-LD Context Consumer: agent that uses the query and subscription functionality of NGSI-LD to retrieve context
information
NGSI-LD Context Producer: agent that uses the NGSI-LD context provision and/or registration functionality to
provide or announce the availability of its context information to an NGSI-LD Context Broker
NGSI-LD Context Registry: software functional element where Context Sources register the information that
they can provide
NOTE: It is used by Distribution Brokers and Federation Brokers to find the appropriate
Context Sources which can provide the information required for serving an NGSI-LD request.
NGSI-LD Context Source: source of context information which implements the NGSI-LD consumption and
subscription (and possibly provision) interfaces defined by the present document
NOTE: It is usually registered with an NGSI-LD Registry so that it can announce what kind of information it can
provide, when requested, to Context Consumers and Brokers.
NGSI-LD Context Source Registration: description of the information that can be provided by a Context
Source, which is used when registering the Context Source with the Context Registry
NGSI-LD Core API: core part of the NGSI-LD API that has to be implemented by all Brokers, including operations
for providing or managing Entities and Attributes, operations for consuming Entities and checking which Entity Types
and Attributes Entities are available in the system and operations for subscribing to Entities, receiving notifications and
managing subscriptions
NGSI-LD Distribution Broker: NGSI-LD Context Broker that uses both local context information and
registration information from an NGSI-LD Context Registry, to access matching context information from a set
of distributed Context Sources
NGSI-LD Element: any JSON element that is defined by the NGSI-LD API
NGSI-LD Entity: informational representative of something that is supposed to exist in the real world, physically or
conceptually
NOTE: In the NGSI-LD API, any instance of such an entity is uniquely identified by a URI, and characterized
by reference to one or more NGSI-LD Entity Type(s).
NGSI-LD Entity Map: mapping of NGSI-LD Entity ids to Context Source Registrations used in maintaining atomicity
of transactions performed by Distribution Brokers and Federation Brokers
NGSI-LD Entity Type: categorization of an NGSI-LD Entity as belonging to a class of similar entities, or sharing a set
of characteristic properties
NOTE: In the NGSI-LD API, an NGSI-LD Entity Type is uniquely identified by a URI.
EXAMPLE 1: "Vehicle" is an NGSI-LD Entity Type and is identified with a proper URI.
EXAMPLE 2: Bob's private car whose plate number is "ABCD1234" is an NGSI-LD Entity whose NGSI-LD
Entity Type name is "Vehicle".
EXAMPLE 3: Alice's motorhome has a unique URI as id, but can be assigned multiple NGSI-LD Entity types,
e.g. "Vehicle" and "Home".
NGSI-LD External Linked Entity: Linked Entity that is identified through a dereferenceable URI which does
not exist within the current NGSI-LD system
NOTE: It can exist within another NGSI-LD system or within a non-NGSI-LD system.
EXAMPLE: An NGSI-LD Entity, whose Entity Type name is "Book", can be externally linked, through the
"wasWrittenBy" relationship, to a resource identified by the URI
"http://dbpedia.org/resource/Mark_Twain".
NGSI-LD Federation Broker: Distribution Broker that federates information from multiple underlying
NGSI-LD Context Brokers and across domains


NGSI-LD GeoProperty: subclass of NGSI-LD Property which is a description instance which associates a main
characteristic, i.e. an NGSI-LD Value, to either an NGSI-LD Entity, an NGSI-LD Relationship or another NGSI-LD
Property, that uses the special hasValue property to define its target value and holds a geographic location in GeoJSON
format
NGSI-LD Internal Linked Entity: Linked Entity that exists within the current NGSI-LD system
EXAMPLE: An NGSI-LD Entity, whose Entity Type name is "Vehicle", can be internally linked, through
the "isParkedAt" relationship, to another NGSI-LD Entity, of Type name "Parking",
identified by the URI "urn:ngsi-ld:Parking:Downtown1".
NGSI-LD JSONLDContext API: part of the NGSI-LD API that is used to host, serve and manage JSON-LD
@contexts
NGSI-LD JsonProperty: subclass of NGSI-LD Property which is a description instance which associates a raw JSON
literal value as a defined main characteristic to an NGSI-LD Entity, an NGSI-LD Relationship or another NGSI-LD
Property and that uses the special hasJson (a subproperty of hasValue) property to define its target value
NOTE: The target value contains data which is not available for interpretation.
NGSI-LD LanguageProperty: subclass of NGSI-LD Property which is a description instance which associates a set of
strings in different natural languages as a defined main characteristic, i.e. an NGSI-LD Map, to an NGSI-LD Entity, an
NGSI-LD Relationship or another NGSI-LD Property and that uses the special hasLanguageMap (a subproperty of
hasValue) property to define its target value
NGSI-LD Linked Entity: NGSI-LD Entity referenced from another NGSI-LD Entity (the linking NGSI-LD Entity) via
an NGSI-LD Relationship
NGSI-LD Linking Entity: NGSI-LD Entity which is the subject of a Relationship to another NGSI-LD Entity (the
linked NGSI-LD Entity) or an external resource (identified by a URI)
NGSI-LD ListProperty: description instance which associates an ordered array of main characteristics, i.e. NGSI-LD
Values, to either an NGSI-LD Entity, an NGSI-LD Relationship or another NGSI-LD Property and that uses the special
hasValueList property to define its target value
NGSI-LD ListRelationship: description of an ordered array of directed links between a subject which is either an
NGSI-LD Entity, an NGSI-LD Property or another NGSI-LD Relationship on one hand, and a series of objects, which
are NGSI-LD Entities, on the other hand, and which uses the special hasObjectList property to define its target objects
EXAMPLE: "A bus route services the following bus stops" can be represented by an NGSI-LD
ListRelationship whose name is "route" which holds an array of directed links towards a series of
NGSI-LD Entities of type (Type name) "BusStop".
NGSI-LD Map: JSON-LD language map in the form of key-value pairs holding the string representation of a main
characteristic in a series of natural languages
EXAMPLE: "Bob's vehicle is currently parked on a street which is known as 'Grand Place' in French and 'Grote
Markt' in Dutch" can be represented by an NGSI-LD LanguageProperty whose name is
"street" which holds an NGSI-LD Map of two key-value pairs containing both the French
("fr") and Dutch ("nl") exonyms of the street name.
NGSI-LD Null: "urn:ngsi-ld:null" or {"@none": "urn:ngsi-ld:null"} used as an encoding for
null values
NGSI-LD Property: description instance which associates a main characteristic, i.e. an NGSI-LD Value, to either an
NGSI-LD Entity, an NGSI-LD Relationship or another NGSI-LD Property and that uses the special hasValue property
to define its target value
EXAMPLE: "Bob's vehicle's speed is 40 km/h" can be represented by an NGSI-LD Property, whose name is
"speed", and which characterizes an NGSI-LD Entity, whose NGSI-LD Type is "Vehicle".
Such a name can be expanded to a fully qualified name in the form of a URI, for instance
"http://example.org/Vehicle" or "http://example.org/speed".
NGSI-LD Query: collection of criteria used to select a sub-set of NGSI-LD Elements, matching the criteria


NGSI-LD Registry API: part of the NGSI-LD API that is implemented by the Context Registry, including
operations for registering Context Sources and managing Context Source Registrations (CSRs),
operations for retrieving and discovering CSRs, and operations for subscribing to CSRs and receiving notifications
NGSI-LD Relationship: description of a directed link between a subject which is either an NGSI-LD Entity, an
NGSI-LD Property or another NGSI-LD Relationship on one hand, and an object, or unordered array of objects, each of
which is an NGSI-LD Entity, on the other hand, and which uses the special hasObject property to define its target
object
EXAMPLE 1: An NGSI-LD Entity of type "Vehicle" can be the subject of an NGSI-LD Relationship whose
object is an NGSI-LD Entity of type "Parking".
EXAMPLE 2: An NGSI-LD Entity of type "Vehicle" can be the subject of an NGSI-LD Relationship whose
object is an array of NGSI-LD Entities of type "Passenger".
NGSI-LD Scope: hierarchical structuring of Entities that enables restricting query results and notifications
NGSI-LD Snapshot: set of NGSI-LD Entities, retrieved through one or more NGSI-LD Queries, which are locally
persisted, providing a relatively consistent view of the situation when the queries were executed, and on which Context
Consumers can execute further local NGSI-LD Operations
NGSI-LD Temporal API: part of the NGSI-LD API pertaining to the Temporal Evolution of Entities,
including operations for providing and managing the Temporal Evolution of Entities and Attributes, and
operations for consuming the Temporal Evolution of Entities
NGSI-LD Temporal Evolution of an Entity: sequence of values attributed to an NGSI-LD Entity over time, i.e.
its history or future predictions
NGSI-LD Tenant: user or group of users that utilize a single instance of a system implementing the NGSI-LD API
(NGSI-LD Context Source or NGSI-LD Broker) in isolation from other users or groups of users of the same
instance, so that any information related to one Tenant (e.g. Entities, Subscriptions, Context Source
Registrations) are only visible to users of the same Tenant, but not to users of a different Tenant
NGSI-LD Value: JSON value (i.e. a string, a number, true or false, an object, an array), or JSON-LD typed value (i.e. a
string as the lexical form of the value together with a type, defined by an XSD base type or more generally an IRI), or
JSON-LD structured value (i.e. a set, a list, a language-tagged string)
EXAMPLE: Bob's private car 'speed' NGSI-LD Value is the number 100 (kilometres per hour).
NGSI-LD VocabProperty: subclass of NGSI-LD Property which is a description instance which associates a string
value which can be coerced to a URI as a defined main characteristic to an NGSI-LD Entity, an NGSI-LD Relationship
or another NGSI-LD Property and that uses the special hasVocab (a subproperty of hasValue) property to define its
target value
EXAMPLE: "Bob's car is a non-commercial vehicle" can be represented by an NGSI-LD VocabProperty
whose name is "category" which holds the string value "non-commercial". If the
associated JSON-LD context defines the term "non-commercial" as
"http://example.com/non-commercial", then the returned value shall be expanded
using JSON-LD type coercion into the URI the "http://example.com/non
commercial".

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 3.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Terms
