---
type: NGSI-LD Clause
title: "Simplified temporal representation of an Entity"
description: "The NGSI-LD specification defines an alternative, abbreviated temporal representation of Temporal Evolution of Entities, which allows consuming temporal Entity data in a more straightforward manner."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.9
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.9"
---

The NGSI-LD specification defines an alternative, abbreviated temporal representation of Temporal Evolution of Entities, which allows consuming temporal Entity data in a more straightforward manner. The simplified temporal representation of Entities shall be supported by implementations and can be selected by Context Consumers through specific request parameters. An example can be found in annex C, clause C.5.6. The simplified temporal representation of an Entity shall be a JSON-LD object containing the following members: Mandatory

- "id" whose value shall be a URI that identifies the Entity.
- "type" whose value shall be equal to the Entity Type name or an unordered JSON array with multiple Entity Type names in case of an Entity that has multiple Entity Types.


Optional

- "@context", a JSON-LD @context as described in clause 4.4.

- For each Property, a member whose key is the Property name (a term), the member value shall be a JSON-LD object labelled with the type "Property". Such JSON-LD object shall only contain a member whose key shall be "values". The value of the referred "values" member shall be a JSON-LD Array that shall contain as many array elements as Property instances (i.e. data points of the concerned Property) being represented. Each array element shall be another Array containing exactly two array elements: the first element shall be a Property value and the second element shall correspond to the associated Temporal Property (for instance observedAt).

EXAMPLE 1:

"name": { "type": "Property", "values": [ [

"Joe Bloggs", "2022-08-09T18:25:02Z" ], [

"Bill Smith", "2022-08-10T18:25:02Z"

] ] }

- For each GeoProperty, a member whose key is the Property name (a term), the member value shall be a JSON-LD object labelled with the type "GeoProperty". Such JSON-LD object shall only contain a member whose key shall be "values". The value of the referred "values" member shall be a JSON-LD Array that shall contain as many array elements as GeoProperty instances (i.e. data points of the concerned GeoProperty) being represented. Each array element shall be another Array containing exactly two array elements: the first element shall be a GeoProperty value and the second element shall correspond to the associated Temporal Property (for instance observedAt).
- For each LanguageProperty, a member whose key is the Property name (a term), the member value shall be a JSON-LD object labelled with the type "LanguageProperty". Such JSON-LD object shall only contain a member whose key shall be "languageMaps". The value of the referred "languageMaps" member shall be a JSON-LD Array that shall contain as many array elements as LanguageProperty instances (i.e. data points of the concerned LanguageProperty) being represented. Each array element shall be another Array containing exactly two array elements: the first element shall be a JSON Object containing a single Attribute with a key called "languageMap" where the value shall correspond to a LanguageProperty languageMap and the second element shall correspond to the associated Temporal Property (for instance observedAt).
EXAMPLE 2:
"says": {
"type": "LanguageProperty",
"languageMaps": [
[

{"languageMap": {"en": "yes", "fr": "oui"}}, "2022-08-09T18:25:02Z" ], [

{"languageMap": {"en": "no", "fr": "non"}}, "2022-08-10T18:25:02Z"

] ] }


- For each ListProperty, a member whose key is the Property name (a term), the member value shall be a JSON-LD object labelled with the type "ListProperty". Such JSON-LD object shall only contain a member whose key shall be "valueLists". The value of the referred "valueLists" member shall be a JSON-LD Array that shall contain as many array elements as ListProperty instances (i.e. data points of the concerned ListProperty) being represented. Each array element shall be another array containing exactly two elements: the first element shall be an ordered array of Property values, and the second element shall correspond to the associated Temporal Property (for instance observedAt).
EXAMPLE 3:
"period": {
"type": "ListProperty",
"valueLists": [
[

["First", "Second", "Third", "Fourth"], "2022-08-09T18:25:02Z" ], [

["1st", "2nd", "3rd", "4th"], "2022-08-10T18:25:02Z"

] ] }

- For each JsonProperty, a member whose key is the Property name (a term), the member value shall be a JSON-LD object labelled with the type "JsonProperty". Such JSON-LD object shall only contain a member whose key shall be "jsons". The value of the referred "jsons" member shall be a JSON-LD Array that shall contain as many array elements as JsonProperty instances (i.e. data points of the concerned JsonProperty) being represented. Each array element shall be another Array containing exactly two array elements: the first element shall be a JSON Object containing a single Attribute with a key called "json", where the value shall correspond to raw JSON data that cannot be held in JSON-LD format and the second element shall correspond to the associated Temporal Property (for instance observedAt).
EXAMPLE 4:
"parkingTickets": {
"type": "JsonProperty",
"jsons": [
[

{

"json": [ {

"id": "85a6cc52-0589-45f9", "value": "Overstay 60 minutes"

} ], }, "2022-08-09T18:25:02Z" ], [

{

"json": [ {

"id": "85a6cc52-0589-45f9", "value": "Overstay 60 minutes" }, {

"id": "x5c56s-0589-45f9", "value": "Overstay 45 minutes"

}

]
},
"2022-08-10T18:25:02Z"


] ] }

- For each VocabProperty, a member whose key is the Property name (a term), the member value shall be a JSON-LD object labelled with the type "VocabProperty". Such JSON-LD object shall only contain a member whose key shall be "vocabs". The value of the referred "vocabs" member shall be a JSON-LD Array that shall contain as many array elements as VocabProperty instances (i.e. data points of the concerned VocabProperty) being represented. Each array element shall be another Array containing exactly two array elements: the first element shall be a JSON Object containing a single Attribute with a key called "vocab", where the value shall correspond to a VocabProperty vocab and the second element shall correspond to the associated Temporal Property (for instance observedAt).
EXAMPLE 5:
"gender": {
"type": "VocabProperty",
"vocabs": [
[

{"vocab": "Male"}, "2022-08-09T18:25:02Z" ], [

{"vocab": "Female"}, "2022-08-10T18:25:02Z"

] ] }

- For each Relationship, a term whose key is the Relationship name (a term). The member value shall be a JSON-LD object labelled with the type "Relationship". Such JSON-LD object shall only contain a member whose key shall be "objects". The value of the referred "objects" member shall be a JSON LD Array that shall contain as many array elements as Relationship instances (i.e. data points of the concerned Relationship) being represented. Each array element shall be another array containing exactly two elements: the first element shall be a Relationship object (a URI or array of URIs) and the second element shall correspond to the associated Temporal Property (for instance observedAt).
EXAMPLE 6:
"spouse": {
"type": "Relationship",
"objects": [
[

"urn:ngsi-ld:Person:123455", "2022-08-09T18:25:02Z" ], [

"urn:ngsi-ld:Person:999999", "2022-08-10T18:25:02Z"

] ]

}
EXAMPLE 7:
"activeDevices": {
"type": "Relationship",
"objects": [
[

["urn:ngsi-ld:Device:14142", "urn:ngsi-ld:Device:13562"], "2022-08-09T18:25:02Z" ],


[

["urn:ngsi-ld:Device:14142", "urn:ngsi-ld:Device:13562", "urn:ngsi ld:Device:37309"], "2022-08-10T18:25:02Z"

] ] }

- For each ListRelationship, a term whose key is the Relationship name (a term), the member value shall be a JSON-LD object labelled with the type "ListRelationship". Such JSON-LD object shall only contain a member whose key shall be "objectLists". The value of the referred "objectLists" member shall be a JSON-LD Array that shall contain as many array elements as ListRelationship instances (i.e. data points of the concerned ListRelationship) being represented. Each array element shall be another array containing exactly two elements: the first element shall be an ordered array of Relationship objects (URIs) and the second element shall correspond to the associated Temporal Property (for instance observedAt).
EXAMPLE 7:
"membersPresent": {
"type": "ListRelationship",
"objectLists": [
[

["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"], "2022-08-09T18:25:02Z" ], [

["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Eve", "urn:ngsi ld:Person:Mallory"], "2022-08-10T18:25:02Z"

] ] }

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Simplified temporal representation of an Entity
