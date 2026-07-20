---
type: NGSI-LD Clause
title: "Aggregated temporal representation of an Entity"
description: "4.5.19.0 Foreword The NGSI-LD specification defines an alternative temporal representation of Entities, called aggregated temporal representation, which allows consuming temporal Entity data after app"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.19
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.19"
---

4.5.19.0 Foreword

The NGSI-LD specification defines an alternative temporal representation of Entities, called aggregated temporal
representation, which allows consuming temporal Entity data after applying an aggregation function on the values of the
Attribute instances. The aggregated temporal representation of Entities shall be supported by implementations
supporting the temporal representation of Entities and can be selected by Context Consumers through specific
request parameters. An example can be found in annex C, clause C.5.14.
The aggregation function is applied according to the following principles:
- An aggregation method specifies the function used to aggregate the values (e.g. sum, mean, etc.). A Context Consumer can ask for many aggregation methods in one request.
- The duration of an aggregation period specifies the duration of each period to be used when applying the aggregation function on the values of a Temporal Entity. The aggregated temporal representation of an Entity shall include the following:
- A JSON-LD object containing the following members: - id, type and @context as described in clause 4.5.1. - For each Property a member whose key is the Property name (a term). The member value shall be a JSON-LD object labelled with the type "Property". Such JSON-LD object shall contain one member per aggregation method requested by the Context Consumer. Each member uses the aggregation method name as a key. The value of each member shall be a JSON-LD Array that shall contain as many array elements as there are periods in the time range of the query. Each array element shall be another Array containing exactly three array elements in the following order: 1) the value obtained after applying the aggregation method over the period; 2) the start DateTime of the corresponding period; 3) the end DateTime of the corresponding period. - For each Relationship a term whose key is the Relationship name (a term). The member value shall be a JSON-LD object labelled with the type "Relationship". Such JSON-LD object shall contain one member per aggregation method requested by the Context Consumer. Each member uses the aggregation method name as a key. The value of each member shall be a JSON-LD Array that shall contain as many array elements as there are periods in the time range of the query. Each array element shall be another array containing exactly three array elements in the following order: 1) the value obtained after applying the aggregation method over the period; 2) the start DateTime of the corresponding period; 3) the end DateTime of the corresponding period. An example of this aggregated temporal representation can be found in annex C, clause C.5.14.

4.5.19.1 Supported behaviours for aggregation functions

In order to support such aggregation functions, two parameters are defined:

- aggrMethods, to express the aggregation methods to apply.

- aggrPeriodDuration to express the duration of the period to consider in each step of the aggregation. The duration is expressed using the ISO 8601 [17] Duration Representation and in particular using the following format and conventions:
- The duration shall be a string in the format P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W, where [n] is replaced by the value for each of the date and time elements that follow the [n], P is the duration designator and T is the time designator. For example, "P3Y6M4DT12H30M5S" represents a duration of "three years, six months, four days, twelve hours, thirty minutes, and five seconds".

- Date and time elements including their designator may be omitted if their value is zero.

- Lower-order elements may be omitted for reduced precision.

- A duration of 0 second (e.g. expressed as "PT0S" or "P0D") is valid and is interpreted as a duration spanning the whole time range specified by the temporal query.

- Alternative representations based on combined date and time representations are not allowed. The values supported by the aggrMethods parameter are the following:

- aggrMethods = "avg" / "distinctCount" / "max" / "min" / "stddev" / "sum" / "sumsq" / "totalCount" The semantics of the different aggregation methods defined above is as follows, and shall be supported by compliant implementations:
Table 4.5.19.1-1: Semantics of aggregation methods for Properties on JSON native data types
Aggregation
Method

JSON String JSON Number JSON Object JSON Array JSON Boolean (see note)

| avg | N/A | | Calculate the | N/A | | | Calculate the | | | | Calculate the | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | average of the | | average number of | | | | | | average of the | | |
| | | | values inside the | | | the sizes of the | | | | | values inside the | | |
| | | | period | | | arrays inside the | | | | | period | | |

period

| distinctCount | | | Calculate the count of distinct values inside the period | | | | | | | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| max | Calculate the last | | Calculate the | N/A | | | Calculate the | | | | Calculate the | | |
| | value in | | maximum value | | | maximum size of | | | | | maximum value | | |
| | lexicographical | | inside the period | | | the arrays inside | | | | | inside the period | | |
| | order inside the | | | | | | the period | | | | | | |

period

| min | Calculate the first | | Calculate the | N/A | | | Calculate the | | | | Calculate the | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | value in | | minimum value | | | minimum size of | | | | | minimum value | | |
| | lexicographical | | inside the period | | | the arrays inside | | | | | inside the period | | |
| | order inside the | | | | | | the period | | | | | | |

period

N/A N/A Calculate the standard deviation of the values inside the period

stddev N/A Calculate the standard deviation of the values inside the period

| sum | N/A | | Calculate the | N/A | Calculate the sum | | | | | Calculate the sum | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | sum of the values | | of the sizes of the | | | | | of the values inside | | | |
| | | | inside the period | | | arrays inside the | | | | | the period | | |

period


Aggregation Method

JSON String JSON Number JSON Object JSON Array JSON Boolean (see note)

sumsq N/A Calculate the sum of the square of the values inside the period

N/A N/A Calculate the sum of the square of the values inside the period

totalCount Calculate the number of times the value has been updated inside the period NOTE: For the purpose of aggregation, true is considered as a value of 1, false is considered as a value of 0.

Table 4.5.19.1-2: Semantics of aggregation methods for Properties on other supported data types Aggregation Method DateTime Date Time URI avg N/A N/A Calculate the average time inside the period (e.g. to apply on an event that occurs at non fixed times, like the time a car enters a given parking)

N/A

distinctCount Calculate the count of distinct values inside the period max Calculate the maximum value inside the period

Calculate the maximum value inside the period

Calculate the maximum value inside the period

N/A

min Calculate the minimum value inside the period

Calculate the minimum value inside the period

Calculate the minimum value inside the period

N/A

stddev N/A N/A N/A N/A sum N/A N/A N/A N/A sumsq N/A N/A N/A N/A totalCount Calculate the number of times the value has been updated inside the period

Table 4.5.19.1-3: Semantics of aggregation methods for Relationships Aggregation Method Relationship avg N/A distinctCount Calculate the count of distinct relationships targets inside the period max N/A min N/A stddev N/A sum N/A sumsq N/A totalCount Calculate the number of times the relationship has been updated inside the period

# Related

* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.19](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Aggregated temporal representation of an Entity
