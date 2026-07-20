---
type: NGSI-LD Clause
title: "Simplified Representation"
description: "The NGSI-LD specification defines an abbreviated, lossy representation of Entities, which allows consuming only entity data (the target object of each Relationship or the value of each Property) corre"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.4
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.4"
---

The NGSI-LD specification defines an abbreviated, lossy representation of Entities, which allows consuming only entity data (the target object of each Relationship or the value of each Property) corresponding to the Properties or Relationships whose subject is the Entity itself i.e. the own Attributes of the Entity. The simplified representation of Entities shall be supported by implementations and can be selected by Context Consumers through specific request parameters. An example of this representation can be found in annex C, clause C.2.2. The simplified representation of an Entity shall be a JSON-LD object containing the following members: Mandatory

- "id" whose value shall be a URI that identifies the Entity.
- "type" whose value shall be equal to the Entity Type name or an unordered JSON array with multiple Entity Type names in case of an Entity that has multiple Entity Types.


Optional

- "@context", a JSON-LD @context as described in clause 4.4.
- For each Property a member whose key is the Property name (a term) and whose value is the Property Value.
- In the multi-attribute case (see clause 4.5.5), the simplified representation of a Property (or any of its subtypes) changes. Each Property consists of a key-value pair, the key being the Property name (a term) and the value being a JSON Object, which contains a single Attribute with a key called "dataset", and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId, where the value corresponds to the simplified representation of the property value. The default datasetId (where present) is represented by the JSON-LD keyword "@none".
EXAMPLE 1: "name": "David Robert Jones"
EXAMPLE 2:
"name": {
"dataset": {
"@none": "David Robert Jones",
"urn:ngsi-ld:datasetId:001": "David Bowie",
"urn:ngsi-ld:datasetId:002": "Ziggy Stardust"

} }

- For each GeoProperty, a member whose key is the Property name (a term) and whose value is the Property Value.
- For each LanguageProperty, a member whose key is the Property name (a term) and whose value is a JSON Object containing a single Attribute with a key called "languageMap" where the value shall correspond to a LanguageProperty languageMap.
- For each JsonProperty, a member whose key is the Property name (a term) and whose value is a JSON Object containing a single Attribute with a key called "json" where the value shall correspond to the raw JSON data that cannot be held in JSON-LD format. Within the example below, the id attribute is never expanded to @id and therefore does not have to be defined as a URI, and the value attribute is never expanded to ngsi-ld:hasValue.
EXAMPLE 3: "location": {"type": "Point", "coordinates": [13.3986, 52.5547]}
EXAMPLE 4: "says": {"languageMap": {"en": "yes", "fr": "oui"}
EXAMPLE 5:
"parkingTickets": {
"json": {
"id": "85a6cc52-0589-45f9",
"value": "Overstay 60 minutes"

} }

- For each VocabProperty, a member whose key is the Property name (a term) and whose value is a JSON Object containing a single Attribute with a key called "vocab" where the value shall correspond to a VocabProperty vocab.
- For each Relationship a term whose key is the Relationship name (a term) and whose value is the Relationship's Object (represented as a URI or array of URIs).

EXAMPLE 6: "gender": {"vocab": "Male"} EXAMPLE 7: "providedBy": "urn:ngsi-ld:Device:31415"


EXAMPLE 8:
"devices": [
"urn:ngsi-ld:Device:14142",
"urn:ngsi-ld:Device:13562",
"urn:ngsi-ld:Device:37309"

]

- In the inline Linked Entity retrieval case (see clause 4.5.23.2), the simplified representation of a Relationship changes. Any Relationship which targets an Entity stored locally or includes an objectType Attribute is returned as a JSON object holding key-value pairs corresponding to the data from the Relationship's object URI in simplified format.
EXAMPLE 9:
"providedBy": {
"id": "urn:ngsi-ld:Device:31415",
"type": "Device",
"brandName": "Anemometer",
"manufacturerName": "Cyberdyne Systems",
"windSpeed": 60

}

EXAMPLE 10: "devices": [ {

"id": "urn:ngsi-ld:Device:14142",
"type": "Device",
"brandName": "Anemometer",
"manufacturerName": "Cyberdyne Systems",
"windSpeed": 60

}, {

"id": "urn:ngsi-ld:Device:13562",
"type": "Device",
"brandName": "Hygromometer",
"manufacturerName": "Acme Corporation",
"humidity": 64

}, {

"id": "urn:ngsi-ld:Device:37309",
"type": "Device",
"brandName": "Barometer",
"manufacturerName": "Trask Industries",
"pressure": 760

} ]

- In the flattened Linked Entity retrieval case (see clause 4.5.23.3), the simplified representation of a Relationship changes to return multiple Entities. Any Relationships which target Entities stored locally or include an objectType Attribute are returned as part of the array as an additional JSON object holding key-value pairs corresponding to the data from the Relationship's object URI in simplified format.

EXAMPLE 11: [

{

…etc "providedBy": "urn:ngsi-ld:Device:31415" }, {

"id": "urn:ngsi-ld:Device:31415",


"type": "Device",
"brandName": "Anemometer",
"manufacturerName": "Cyberdyne Systems",
"windSpeed": 60

}

]
EXAMPLE 12:
[

{

… etc
"providedBy": [
"urn:ngsi-ld:Device:14142",
"urn:ngsi-ld:Device:13562",
"urn:ngsi-ld:Device:37309"

] }, {

"id": "urn:ngsi-ld:Device:14142",
"type": "Device",
"brandName": "Anemometer",
"manufacturerName": "Cyberdyne Systems",
"windSpeed": 60

}, {

"id": "urn:ngsi-ld:Device:13562",
"type": "Device",
"brandName": "Hygromometer",
"manufacturerName": "Acme Corporation",
"humidity": 64

}, {

"id": "urn:ngsi-ld:Device:37309",
"type": "Device",
"brandName": "Barometer",
"manufacturerName": "Trask Industries",
"pressure": 760

} ]

- In the multi-attribute case (see clause 4.5.5), the simplified representation of a Relationship changes. Each Relationship consists of a key-value pair, the key being the Relationship name (a term) and the value being a JSON Object containing a single Attribute with a key called "dataset" and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId, where the value corresponds to the object of the Relationship. The default datasetId (where present) is represented by the JSON-LD keyword "@none".
EXAMPLE 13:
"providedBy": {
"dataset": {
"@none": "urn:ngsi-ld:Device:31415",
"urn:ngsi-ld:datasetId:001": "urn:ngsi-ld:Device:27182",
"urn:ngsi-ld:datasetId:002": "urn:ngsi-ld:Device:14142"

} }

- For each ListProperty a member whose key is the Property name (a term) and whose value is an ordered array holding the Property Values.


EXAMPLE 14: "periods": ["First", "Second", "Third", "Fourth"]

- In the multi-attribute case (see clause 4.5.5), the simplified representation of a ListProperty changes. Each ListProperty consists of a key-value pair, the key being the Property name (a term) and the value being a JSON Object containing a single Attribute with a key called "dataset" and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId, where the value corresponds to the simplified representation of the ListProperty valueList. The default datasetId (where present) is represented by the JSON-LD keyword "@none".
EXAMPLE 15:
"periods": {
"dataset": {
"@none": ["First", "Second", "Third", "Fourth"],
"urn:ngsi-ld:datasetId:001": ["1st", "2nd", "3rd", "4th"],
"urn:ngsi-ld:datasetId:002": ["Primary", "Secondary", "Tertiary",
"Quaternary"]
}

}

- For each ListRelationship a term whose key is the Relationship name (a term) and whose value is an ordered array holding the ListRelationship's Objects (represented as URIs).
EXAMPLE 16:
"route":[
"urn:ngsi-ld:BusStop:0101",
"urn:ngsi-ld:BusStop:0102",
"urn:ngsi-ld:BusStop:9912"

]

- In the inline Linked Entity retrieval case (see clause 4.5.23.2), the simplified representation of a ListRelationship changes. Any ListRelationship which targets Entities stored locally or includes an objectType Attribute is returned as an ordered array of JSON objects holding key-value pairs corresponding to the data from the ListRelationship's target objectList URIs in simplified format.

EXAMPLE 17: "route": [ {

"id": "urn:ngsi-ld: BusStop:0101", "type": "BusStop", "stopName": "High Street", "location": {"type": Point, coordinates [54.112,0.334]}} }, {

"id": "urn:ngsi-ld: BusStop:0102", "type": "BusStop", "stopName": "Station Road", "location": {"type": Point, coordinates [54.101,0.302]}} }, {

"id": "urn:ngsi-ld: BusStop:9912",
"type": "BusStop",
"stopName": "Mornington Cresent",
"location": {"type": Point, coordinates [54.142,0.332]}

} ]

- In the flattened Linked Entity retrieval case (see clause 4.5.23.3), the simplified representation of a ListRelationship changes to return multiple Entities. Any ListRelationships which target Entities stored locally or include an objectType Attribute are returned as additional JSON objects holding key-value pairs corresponding to the data from the ListRelationship's target objectList URIs in simplified format.


EXAMPLE 18: [

{

…etc
"route": [
"urn:ngsi-ld:BusStop:0101",
"urn:ngsi-ld:BusStop:0102",
"urn:ngsi-ld:BusStop:9912"

] }, {

"id": "urn:ngsi-ld: BusStop:0101"
"type": "BusStop",
"stopName": "High Street",
"location: {"type": "Point", "coordinates": [54.112,0.334]}}

{

"id": "urn:ngsi-ld: BusStop:0102", "type": "BusStop", "stopName": "Station Road", "location": {"type": "Point", "coordinates": [54.101,0.302]}} }, {

"id": "urn:ngsi-ld:BusStop:9912"
"type": "BusStop",
"stopName": "Mornington Cresent",
"location: {"type": "Point", "coordinates": [54.142,0.332]}

} ]

- In the multi-attribute case (see clause 4.5.5), the simplified representation of a ListRelationship changes. Each ListRelationship consists of a key-value pair, the key being the Relationship name (a term) and the value being a JSON Object containing a single Attribute with a key called "dataset" and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId, where the value corresponds to the array of objects of the relationship list. The default datasetId (where present) is represented by the JSON-LD keyword "@none".
EXAMPLE 19:
"route": {
"dataset": {
"@none": [
"urn:ngsi-ld:BusStop:0101",
"urn:ngsi-ld:BusStop:0102",
"urn:ngsi-ld:BusStop:9912"
],
"urn:ngsi-ld:datasetId:001": [
"urn:ngsi-ld:BusStop:0101",
"urn:ngsi-ld:BusStop:0102",
"urn:ngsi-ld:BusStop:0022"
],
"urn:ngsi-ld:datasetId:002": [
"urn:ngsi-ld:BusStop:0101",
"urn:ngsi-ld:BusStop:0102",
"urn:ngsi-ld:BusStop:0022",
"urn:ngsi-ld:BusStop:9912"

] } }

NOTE: When the simplified GeoJSON representation is selected, the layout of the Entities changes, see clause 4.5.17 for details.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.17](/framework/data-representation/simplified-geojson-representation-of-entities.md)
* [Clause 4.5.23.2](/framework/data-representation/inline-linked-entity-representation.md)
* [Clause 4.5.23.3](/framework/data-representation/flattened-linked-entity-representation.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Simplified Representation
