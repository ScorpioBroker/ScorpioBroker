---
type: NGSI-LD Clause
title: "Foreword"
description: "The NGSI-LD specification defines an alternative temporal representation of Entities, called aggregated temporal representation, which allows consuming temporal Entity data after applying an aggregati"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.19.0
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.19.0"
---

The NGSI-LD specification defines an alternative temporal representation of Entities, called aggregated temporal
representation, which allows consuming temporal Entity data after applying an aggregation function on the values of the
Attribute instances. The aggregated temporal representation of Entities shall be supported by implementations
supporting the temporal representation of Entities and can be selected by Context Consumers through specific
request parameters. An example can be found in annex C, clause C.5.14.
The aggregation function is applied according to the following principles:
- An aggregation method specifies the function used to aggregate the values (e.g. sum, mean, etc.). A Context Consumer can ask for many aggregation methods in one request.
- The duration of an aggregation period specifies the duration of each period to be used when applying the aggregation function on the values of a Temporal Entity. The aggregated temporal representation of an Entity shall include the following:
- A JSON-LD object containing the following members: - id, type and @context as described in clause 4.5.1. - For each Property a member whose key is the Property name (a term). The member value shall be a JSON-LD object labelled with the type "Property". Such JSON-LD object shall contain one member per aggregation method requested by the Context Consumer. Each member uses the aggregation method name as a key. The value of each member shall be a JSON-LD Array that shall contain as many array elements as there are periods in the time range of the query. Each array element shall be another Array containing exactly three array elements in the following order: 1) the value obtained after applying the aggregation method over the period; 2) the start DateTime of the corresponding period; 3) the end DateTime of the corresponding period. - For each Relationship a term whose key is the Relationship name (a term). The member value shall be a JSON-LD object labelled with the type "Relationship". Such JSON-LD object shall contain one member per aggregation method requested by the Context Consumer. Each member uses the aggregation method name as a key. The value of each member shall be a JSON-LD Array that shall contain as many array elements as there are periods in the time range of the query. Each array element shall be another array containing exactly three array elements in the following order: 1) the value obtained after applying the aggregation method over the period; 2) the start DateTime of the corresponding period; 3) the end DateTime of the corresponding period. An example of this aggregated temporal representation can be found in annex C, clause C.5.14.

# Related

* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.19.0](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Foreword
