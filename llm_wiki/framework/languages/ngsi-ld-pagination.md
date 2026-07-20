---
type: NGSI-LD Clause
title: "NGSI-LD Pagination"
description: "NGSI-LD operations can potentially return a result set including a large number of NGSI-LD Elements, so that pagination of query results shall be supported by compliant implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.12
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.12"
---

NGSI-LD operations can potentially return a result set including a large number of NGSI-LD Elements, so that
pagination of query results shall be supported by compliant implementations.
The list of operations that incur this behaviour is as follows:
- Query Entities (clause 5.7.2).
- Query Subscriptions (clause 5.8.4).
- Query Context Source Registrations (clause 5.10.2).
- Query Context Source Registration Subscriptions (clause 5.11.5).
- Query Temporal Evolution of Entities (clause 5.7.4). Nonetheless, the NGSI-LD API is agnostic about specific pagination mechanisms and only defines the behaviour that shall be observed by NGSI-LD Systems. For each operation above, NGSI-LD Systems shall:
- provide a mechanism to iterate through the NGSI-LD Elements of a result set without exhausting NGSI-LD Client or Broker resources;
- provide a mechanism to flag NGSI-LD Clients when there are remaining NGSI-LD Elements to be traversed as part of a result set;
- allow NGSI-LD Clients specifying a limit (page size), as a parameter of API operations, to the number of NGSI-LD Elements (at a maximum) retrieved by the implementation for each pagination iteration;
- define a default limit (default page size) to the number of NGSI-LD Elements retrieved per pagination iteration;
- allow NGSI-LD Clients iterating forwards and backwards through a result set. NGSI-LD implementations should:
- avoid Denial of Service attacks or other potential security risks, by defining a hard limit to the size of generated response payload body while paginating. For instance, certain queries can be rejected by issuing an error of type TooManyResults.


NGSI-LD implementations may:

- warn NGSI-LD Clients when result sets become invalid due to dynamic changes in NGSI-LD Elements (additions, deletions) occurred while iterating over pages. The concrete realization of the features described above might depend on each API binding. Nonetheless, NGSI-LD Systems shall implement pagination features as mandated by the present clause, for any API binding.

# Related

* [Clause 5.10.2](/api-operations/discovery/query-context-source-registrations.md)
* [Clause 5.11.5](/api-operations/csource-subscription/query-context-source-registration-subscriptions.md)
* [Clause 5.7.2](/api-operations/consumption/query-entities.md)
* [Clause 5.7.4](/api-operations/consumption/query-temporal-evolution-of-entities.md)
* [Clause 5.8.4](/api-operations/subscription/query-subscriptions.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Pagination
