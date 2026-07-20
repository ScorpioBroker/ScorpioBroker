---
type: Reference
title: "NGSI-LD namespace"
description: "NGSI-LD defines a specific URN [9] namespace intended to help API users to design readable, clean and simple identifiers."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-A.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "A.3"
---

NGSI-LD defines a specific URN [9] namespace intended to help API users to design readable, clean and simple
identifiers. As it is based on URNs, the usage of this identification approach is not recommended when dereferenceable
URIs are needed (fully-fledged linked data scenarios).
The referred namespace is defined as follows (to be registered with IANA):
- namespace identifier: NID = "ngsi-ld"
- namespace specific string: NSS = EntityTypeName ":" EntityIdentificationString EntityTypeName shall be an Entity Type name which can be expanded to a URI as per the @context.
EntityIdentificationString shall be a string that allows uniquely identifying the subject Entity in combination with the
other items being part of the NSS.
EXAMPLE: urn:ngsi-ld:Person:28976543.
It is recommended that applications use this URN namespace when applicable.
In general, the URN specification defines namespace equivalence in a case-insensitive manner, however it is assumed
that context-broker implementations shall always use lowercase letters in namespaces where they have a choice in case,
unless there is a strong reason otherwise. Restricting the namespace prefix to lower case urn:ngsi-ld: can improve
caching and retrieval, since this ensures since alphabetic characters within the namespace specific string are always
consistent.


Annex B (normative):

Core NGSI-LD @context definition

Below is the definition of the Core NGSI-LD @context which shall be supported by implementations.

Such definition has been tested using [i.7].

{

"@context": { "@version": 1.1, "@protected": true, "ngsi-ld": "https://uri.etsi.org/ngsi-ld/", "geojson": "https://purl.org/geojson/vocab#", "id": "@id", "type": "@type", "Attribute": "ngsi-ld:Attribute", "AttributeList": "ngsi-ld:AttributeList", "ContextSourceIdentity": "ngsi-ld:ContextSourceIdentity", "ContextSourceNotification": "ngsi-ld:ContextSourceNotification", "ContextSourceRegistration": "ngsi-ld:ContextSourceRegistration", "Date": "ngsi-ld:Date", "DateTime": "ngsi-ld:DateTime", "EntityType": "ngsi-ld:EntityType", "EntityTypeInfo": "ngsi-ld:EntityTypeInfo", "EntityTypeList": "ngsi-ld:EntityTypeList", "ExecutionResultDetails": "ngsi-ld:ExecutionResultDetails", "Feature": "geojson:Feature", "FeatureCollection": "geojson:FeatureCollection", "GeoProperty": "ngsi-ld:GeoProperty", "GeometryCollection": "geojson:GeometryCollection", "JsonProperty": "ngsi-ld:JsonProperty", "LanguageProperty": "ngsi-ld:LanguageProperty", "LineString": "geojson:LineString", "ListProperty": "ngsi-ld:ListProperty", "ListRelationship": "ngsi-ld:ListRelationship", "MultiLineString": "geojson:MultiLineString", "MultiPoint": "geojson:MultiPoint", "MultiPolygon": "geojson:MultiPolygon", "Notification": "ngsi-ld:Notification", "Point": "geojson:Point", "Polygon": "geojson:Polygon", "Property": "ngsi-ld:Property", "Relationship": "ngsi-ld:Relationship", "Snapshot": "ngsi-ld:Snapshot", "SnapshotNotification": "ngsi-ld:SnapshotNotification", "Subscription": "ngsi-ld:Subscription", "TemporalProperty": "ngsi-ld:TemporalProperty", "Time": "ngsi-ld:Time", "VocabProperty": "ngsi-ld:VocabProperty", "accept": "ngsi-ld:accept", "aggrParams": "ngsi-ld:aggrParams", "aggrMethods": "ngsi-ld:aggrMethods", "aggrPeriodDuration": "ngsi-ld:aggrPeriodDuration", "attributeCount": "attributeCount", "attributeDetails": "attributeDetails", "attributeList": { "@id": "ngsi-ld:attributeList", "@type": "@vocab" }, "attributeName": { "@id": "ngsi-ld:attributeName", "@type": "@vocab" }, "attributeNames": { "@id": "ngsi-ld:attributeNames", "@type": "@vocab" }, "attributeTypes": { "@id": "ngsi-ld:attributeTypes", "@type": "@vocab" }, "attributes": { "@id": "ngsi-ld:attributes", "@type": "@vocab" },


"attrs": "ngsi-ld:attrs",
"avg": {
"@id": "ngsi-ld:avg",
"@container": "@list"
},
"bbox": {
"@container": "@list",
"@id": "geojson:bbox"
},
"cacheDuration": "ngsi-ld:cacheDuration",
"collation": "ngsi-ld:collation",
"containedBy": "ngsi-ld:isContainedBy",
"contextSourceAlias": "ngsi-ld:contextSourceAlias",
"contextSourceExtras": {
"@id": "ngsi-ld:contextSourceExtras",
"@type": "@json"
},
"contextSourceInfo": "ngsi-ld:contextSourceInfo",
"contextSourceTimeAt": {
"@id": "ngsi-ld:contextSourceTimeAt",
"@type": "DateTime"
},
"contextSourceUptime": "ngsi-ld:contextSourceUptime",
"cooldown": "ngsi-ld:cooldown",
"coordinates": {
"@container": "@list",
"@id": "geojson:coordinates"
},
"createdAt": {
"@id": "ngsi-ld:createdAt",
"@type": "DateTime"
},
"csf": "ngsi-ld:csf",
"data": "ngsi-ld:data",
"dataset": {
"@id": "ngsi-ld:hasDataset",
"@container": "@index"
},
"datasetId": {
"@id": "ngsi-ld:datasetId",
"@type": "@id"
},
"deletedAt": {
"@id": "ngsi-ld:deletedAt",
"@type": "DateTime"
},
"description": "http://purl.org/dc/terms/description",
"detail": "ngsi-ld:detail",
"distinctCount": {
"@id": "ngsi-ld:distinctCount",
"@container": "@list"
},
"endAt": {
"@id": "ngsi-ld:endAt",
"@type": "DateTime"
},
"endTimeAt": {
"@id": "ngsi-ld:endTimeAt",
"@type": "DateTime"
},
"endpoint": "ngsi-ld:endpoint",
"entities": "ngsi-ld:entities",
"entity": "ngsi-ld:entity",
"entityCount": "ngsi-ld:entityCount",
"entityId": {
"@id": "ngsi-ld:entityId",
"@type": "@id"
},
"entityList": {
"@id": "ngsi-ld:entityList",
"@container": "@list"
},
"entityMap": "ngsi-ld:hasEntityMap",
"error": "ngsi-ld:error",
"errors": "ngsi-ld:errors",
"expandValues": "ngsi-ld:expandValues",
"expiresAt": {
"@id": "ngsi-ld:expiresAt",


"@type": "DateTime"
},
"features": {
"@container": "@set",
"@id": "geojson:features"
},
"format": "ngsi-ld:format",
"geoQ": "ngsi-ld:geoQ",
"geometry": "geojson:geometry",
"geoproperty": "ngsi-ld:geoproperty",
"georel": "ngsi-ld:georel",
"idPattern": "ngsi-ld:idPattern",
"information": "ngsi-ld:information",
"instanceId": {
"@id": "ngsi-ld:instanceId",
"@type": "@id"
},
"isActive": "ngsi-ld:isActive",
"join": "ngsi-ld:join",
"joinLevel": "ngsi-ld:hasJoinLevel",
"json": {
"@id": "ngsi-ld:hasJSON", "@type": "@json"
},
"jsonKeys": "ngsi-ld:jsonKeys",
"jsons": {
"@id": "ngsi-ld:jsons",
"@container": "@list"
},
"key": "ngsi-ld:hasKey",
"lang": "ngsi-ld:lang",
"languageMap": {
"@id": "ngsi-ld:hasLanguageMap",
"@container": "@language"
},
"languageMaps": {
"@id": "ngsi-ld:hasLanguageMaps",
"@container": "@list"
},
"langString": "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
"lastFailure": {
"@id": "ngsi-ld:lastFailure",
"@type": "DateTime"
},
"lastNotification": {
"@id": "ngsi-ld:lastNotification",
"@type": "DateTime"
},
"lastSuccess": {
"@id": "ngsi-ld:lastSuccess",
"@type": "DateTime"
},
"lastUsedAt": {
"@id": "ngsi-ld:lastUsedAt",
"@type": "DateTime"
},
"linkedMaps": "ngsi-ld:linkedMaps",
"localOnly": "ngsi-ld:localOnly",
"location": "ngsi-ld:location",
"management": "ngsi-ld:management",
"managementInterval": "ngsi-ld:managementInterval",
"max": {
"@id": "ngsi-ld:max",
"@container": "@list"
},
"min": {
"@id": "ngsi-ld:min",
"@container": "@list"
},
"mode": "ngsi-ld:mode",
"modifiedAt": {
"@id": "ngsi-ld:modifiedAt",
"@type": "DateTime"
},
"ngsildproof": {
"@id": "ngsi-ld:ngsildproof",
"@context": [{
"entityIdSealed": "ngsi-ld:entityIdSealed",
"entityTypeSealed": {

"@id": "ngsi-ld:entityTypeSealed", "@type": "@vocab"

}
},
"https://w3id.org/security/data-integrity/v2"
]
},
"notification": "ngsi-ld:notification",
"notificationTrigger": "ngsi-ld:notificationTrigger",
"notifiedAt": {
"@id": "ngsi-ld:notifiedAt",
"@type": "DateTime"
},
"notifierInfo": "ngsi-ld:notifierInfo",
"notUpdated": "ngsi-ld:notUpdated",
"object": {
"@id": "ngsi-ld:hasObject",
"@type": "@id"
},
"objectList": {
"@id": "ngsi-ld:hasObjectList",
"@container": "@list"
},
"objectLists": {
"@id": "ngsi-ld:hasObjectLists",
"@container": "@list"
},
"objects": {
"@id": "ngsi-ld:hasObjects",
"@container": "@list"
},
"objectType": {
"@id": "ngsi-ld:hasObjectType",
"@type": "@vocab"
},
"observationInterval": "ngsi-ld:observationInterval",
"observationSpace": "ngsi-ld:observationSpace",
"observedAt": {
"@id": "ngsi-ld:observedAt",
"@type": "DateTime"
},
"omit": "ngsi-ld:omit",
"operations": "ngsi-ld:operations",
"operationSpace": "ngsi-ld:operationSpace",
"orderBy": {
"@container": "@list",
"@id": "ngsi-ld:orderBy"
},
"ordering": "ngsi-ld:ordering",
"pick": "ngsi-ld:pick",
"previousJson": {
"@id": "ngsi-ld:hasPreviousJson",
"@type": "@json"
},
"previousLanguageMap": {
"@id": "ngsi-ld:hasPreviousLanguageMap",
"@container": "@language"
},
"previousObject": {
"@id": "ngsi-ld:hasPreviousObject",
"@type": "@id"
},
"previousObjectList": {
"@id": "ngsi-ld:hasPreviousObjectList",
"@container": "@list"
},
"previousValue": "ngsi-ld:hasPreviousValue",
"previousValueList": {
"@id": "ngsi-ld:hasPreviousValueList",
"@container": "@list"
},
"previousVocab": {
"@id": "ngsi-ld:hasPreviousVocab",
"@type": "@vocab"
},
"problemDetails": {
"@id": "ngsi-ld:problemDetails",
"@type": "@json"

| | "@type": "@vocab" | |
| --- | --- | --- |
| }, | | |
| "problemDetails": { | | |


},
"properties": "geojson:properties",
"propertyNames": {
"@id": "ngsi-ld:propertyNames",
"@type": "@vocab"
},
"q": "ngsi-ld:q",
"reason": "ngsi-ld:reason",
"receiverInfo": "ngsi-ld:receiverInfo",
"refreshRate": "ngsi-ld:refreshRate",
"registrationId": "ngsi-ld:registrationId",
"registrationName": "ngsi-ld:registrationName",
"relationshipNames": {
"@id": "ngsi-ld:relationshipNames",
"@type": "@vocab"
},
"resultStatus": "ngsi-ld:resultStatus",
"scope": "ngsi-ld:scope",
"scopeQ": "ngsi-ld:scopeQ",
"showChanges": "ngsi-ld:showChanges",
"snapshotId": "ngsi-ld:snapshotId",
"snapshotLifetime": "ngsi-ld:snapshotLifetime",
"snapshotPriority": "ngsi-ld:snapshotPriority",
"snapshotQueries": {
"@id": "ngsi-ld:snapshotQueries",
"@container": "@list"
},
"snapshotQueriesDetails": {
"@id": "ngsi-ld:snapshotQueriesDetails",
"@container": "@list"
},
"snapshotStatus": "ngsi-ld:snapshotStatus",
"snapshotTemporalQueries": {
"@id": "ngsi-ld:snapshotTemporalQueries",
"@container": "@list"
},
"snapshotTemporalQueriesDetails": {
"@id": "ngsi-ld:snapshotTemporalQueriesDetails",
"@container": "@list"
},
"startAt": {
"@id": "ngsi-ld:startAt",
"@type": "DateTime"
},
"status": "ngsi-ld:status",
"stddev": {
"@id": "ngsi-ld:stddev",
"@container": "@list"
},
"subscriptionId": {
"@id": "ngsi-ld:subscriptionId",
"@type": "@id"
},
"subscriptionName": "ngsi-ld:subscriptionName",
"success": {
"@id": "ngsi-ld:success",
"@type": "@id"
},
"sum": {
"@id": "ngsi-ld:sum",
"@container": "@list"
},
"sumsq": {
"@id": "ngsi-ld:sumsq",
"@container": "@list"
},
"sysAttrs": "ngsi-ld:sysAttrs",
"temporalQ": "ngsi-ld:temporalQ",
"tenant": {
"@id": "ngsi-ld:tenant",
"@type": "@id"
},
"throttling": "ngsi-ld:throttling",
"timeAt": {
"@id": "ngsi-ld:timeAt",
"@type": "DateTime"
},
"timeInterval": "ngsi-ld:timeInterval",


"timeout": "ngsi-ld:timeout",
"timeproperty": "ngsi-ld:timeproperty",
"timerel": "ngsi-ld:timerel",
"timesFailed": "ngsi-ld:timesFailed",
"timesSent": "ngsi-ld:timesSent",
"title": "http://purl.org/dc/terms/title",
"totalCount": {
"@id": "ngsi-ld:totalCount",
"@container": "@list"
},
"triggerReason": "ngsi-ld:triggerReason",
"typeList": {
"@id": "ngsi-ld:typeList",
"@type": "@vocab"
},
"typeName": {
"@id": "ngsi-ld:typeName",
"@type": "@vocab"
},
"typeNames": {
"@id": "ngsi-ld:typeNames",
"@type": "@vocab"
},
"unchanged": "ngsi-ld:unchanged",
"unitCode": "ngsi-ld:unitCode",
"updated": "ngsi-ld:updated",
"uri": "ngsi-ld:uri",
"value": "ngsi-ld:hasValue",
"valueList": {
"@id": "ngsi-ld:hasValueList",
"@container": "@list"
},
"valueLists": {
"@id": "ngsi-ld:hasValueLists",
"@container": "@list"
},
"values": {
"@id": "ngsi-ld:hasValues",
"@container": "@list"
},
"valueType": {
"@id": "ngsi-ld:hasValueType",
"@type": "@vocab"
},
"vocab": {
"@id": "ngsi-ld:hasVocab",
"@type": "@vocab"
},
"vocabs": {
"@id": "ngsi-ld:hasVocabs",
"@container": "@list"
},
"watchedAttributes": {
"@id": "ngsi-ld:watchedAttributes",
"@type": "@vocab"
},
"@vocab": "https://uri.etsi.org/ngsi-ld/default-context/"

} }

NOTE: Implementers can take advantage of prefixed terms, i.e. in the form ngsi-ld:term, to provide a terser

representation of the Core @context.


Annex C (informative):

Examples of using the API

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause A.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD namespace
