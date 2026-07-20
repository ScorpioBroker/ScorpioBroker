---
type: NGSI-LD Clause
title: "Counting the Number of Results"
description: "Given that NGSI-LD Query operations can potentially return a result set including a large number of NGSI-LD Elements and that pagination of query results shall be supported (see clause 4.12), complian"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.13
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.13"
---

Given that NGSI-LD Query operations can potentially return a result set including a large number of NGSI-LD
Elements and that pagination of query results shall be supported (see clause 4.12), compliant implementations shall also
support a mechanism for relaying to the client the number of expected resulting elements, when a query is executed.
A specific field (e.g. a custom header in the response in case of HTTP binding, see clause 6.3.13) shall be returned
within the response of a query, whenever this is requested by the client.
Mechanisms for limiting the number of returned NGSI-LD Elements are independent of the counting mechanism, so
that, potentially, a client can issue a query that limits to zero the number of desired results but asks for the count to be
present.
This is useful for client-side planning and fine-tuning of subsequent queries and their parameters.

# Related

* [Clause 4.12](/framework/languages/ngsi-ld-pagination.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Counting the Number of Results
