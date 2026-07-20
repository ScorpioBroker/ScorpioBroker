---
type: NGSI-LD Data Type
title: "ExecutionResultDetails"
description: "This type represents the details of the result of execution a request, e.g."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.42
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.42"
---

This type represents the details of the result of execution a request, e.g. a Query.
The supported JSON members shall follow the requirements provided in Table 5.2.42-1.


Table 5.2.42-1: ExecutionResultDetails data type definition Name Data Type Restriction Cardinality Description problemDetails @JSON ProblemDetails (see IETF RFC 7807 [10])

0..1 Provides more details regarding the result status, especially when reporting an error.

resultStatus String It shall be one of: "success", "failure" or "empty"

1 Describes the status of the result. "failure", if an error is reported, "success" in case of a non-empty result and "empty" otherwise.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.42](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — ExecutionResultDetails
