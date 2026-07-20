---
type: NGSI-LD Data Type
title: "Query"
description: "This datatype represents the information that is required in order to convey a query when a \"Query Entities\" operation or a \"Query Temporal Evolution of Entities\" operation is to be performed (as per"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.23
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.23"
---

This datatype represents the information that is required in order to convey a query when a "Query Entities" operation or a "Query Temporal Evolution of Entities" operation is to be performed (as per clauses 5.7.2 and 5.7.4, respectively). The supported JSON members shall follow the requirements provided in Table 5.2.23-1.

Table 5.2.23-1: Query data type definition Name Data Type Restrictions Cardinality Description type String It shall be equal to "Query"

1 JSON-LD @type

| | entities | | EntitySelector[] | See data type definition in | | 0..1 | | Entity IDs, id pattern and Entity | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | clause 5.2.33. Empty array | | | | types that shall be matched by | | |
| | | | | (0 length) is not allowed | | | | Entities in order to be retrieved. | | |
| | q | | String | A valid query string as per | | 0..1 | | Query that shall be matched by | | |
| | | | | clause 4.9 | | | | Entities in order to be retrieved. | | |
| | geoQ | | GeoQuery | See data type definition in | | 0..1 | | Geoquery that shall be matched | | |
| | | | | clause 5.2.13 | | | | by Entities in order be retrieved. | | |
| | scopeQ | | String | See clause 4.19 | | 0..1 | | Scope query. | | |
| | temporalQ | | TemporalQuery | See data type definition in | | 0..1 | | Temporal Query to be present | | |
| | | | | clause 5.2.21 | | | | only for "Query Temporal | | |

0..1 Temporal Query to be present only for "Query Temporal Evolution of Entities" operation (clause 5.7.4).

0..1 A synonym for a combination of the pick and q members. Deprecated

attrs String[] Attribute name as short hand strings or URIs. Empty array (0 length) is not allowed

List of Attributes that shall be matched by Entities in order to be retrieved. If not present all Attributes will be retrieved.

| | omit | | String[] | Entity member ("id", | | 0..1 | | When defined, the specified | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | "type", "scope" or a | | | | Entity members are removed | | |
| | | | | projected Attribute name) as | | | | from each Entity within the | | |
| | | | | a valid attribute projection | | | | payload. | | |

language string as per clause 4.21. Empty array (0 length) is not allowed


Name Data Type Restrictions Cardinality Description pick String[] Entity member ("id", "type", "scope" or a projected Attribute name) as a valid attribute projection language string as per clause 4.21. Empty array (0 length) is not allowed

0..1 When defined, every Entity within the payload body is reduced down to only contain the specified Entity members.

aggrParams AggregationParams See data type definition in clause 5.2.44.

0..1 Aggregation parameters required for supporting aggregation methods in to be present only for "Query Temporal Evolution of Entities" operation (clause 5.7.4). Only applicable if "aggregatedValues" is present in the format or options parameter.

csf String A valid query string as per clause 4.9

0..1 Context source filter that shall be matched by Context Source Registrations describing Context Sources to be used for retrieving Entities.

containedBy String[] Comma separated list of URIs. Empty array (0 length) is not allowed

0..1 List of entity ids which have previously been encountered whilst retrieving the Entity Graph. Only applicable if joinLevel is present. Only applicable for the "Retrieve Entity" (clause 5.7.1) and "Query Entities" operations (clause 5.7.2).

datasetId String[] Valid URIs, "@none" for including the default Attribute instances.

0..1 Specifies the datasetIds of the Attribute instances to be selected for each matched Attribute as per clause 4.5.5.

expandValues String Comma separated list of attribute names

0..1 Values of the identified attributes should be expanded against the supplied @context using JSON-LD type coercion prior to executing the query. entityMap Boolean 0..1 If true, the location of the EntityMap used in the operation is returned in the response.

entityMapLifeti me

String String representing a duration in ISO 8601 [17] format

0..1 Suggested duration for which the requester wants the EntityMap to exist. The actual expiresAt time of the EntityMap shall be set by the Context Broker or Context Source, possibly overriding the requested duration. Only applicable if entityMap is set to true.

jsonKeys String Comma separate list of attribute names

0..1 Values of the identified attributes are to be considered uninterpretable as JSON-LD and should not be expanded against the supplied @context using JSON-LD type coercion prior to executing the query.


Name Data Type Restrictions Cardinality Description join String It shall be one of: "flat", "inline", "@none"

0..1 String representing the type of Linked Entity retrieval to apply. By default, it will be "@none". joinLevel Number Positive Integer 0..1 Depth of Linked Entity retrieval to apply. Default is 1. Only applicable if join parameter is "flat", or "inline".

lang String A natural language filter in the form of a IETF RFC 5646 [28] language code

0..1 Language filter to be applied to the query (clause 4.15).

ordering OrderingParams See data type definition in clause 5.2.43

0..1 When defined, the Entities returned in the payload shall be ordered according to the members defined. This only applies if the operation is limited to the local scope.

splitEntities Boolean default decided by implementation; it should be configurable. The parameter does not apply in case the parameter local is true or the query applies to a Snapshot

0..1 If true it is assumed that single Entities are distributed between different Context Brokers and/or Context Sources and this has to be taken into account when applying any kind of filters (q, geoQ, scopeQ, Attributes etc.). If false it is expected that Context Broker and/or Context Source always have complete Entities, which allows applying filters locally.

# Related

* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.2.13](/api-operations/data-types/geoquery.md)
* [Clause 5.2.21](/api-operations/data-types/temporalquery.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.23](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query
