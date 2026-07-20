---
type: NGSI-LD Clause
title: "Operation execution and generic error handling"
description: "When executing an operation if an unexpected error happens and the operation cannot be completed, implementations shall raise an error of type InternalError."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.6"
---

When executing an operation if an unexpected error happens and the operation cannot be completed, implementations
shall raise an error of type InternalError. This includes, as well, situations such as database timeouts, etc.
If the NGSI-LD endpoint is not capable of executing the requested operation, an error of type OperationNotSupported
shall be raised. This may happen in a distributed architecture where a Context Broker might not be able to store
Entities (only to forward queries to Context Sources), and as a result, certain operations such as "Create Entity"
might not be supported.
When a query operation is so complex that cannot be resolved by an NGSI-LD system, implementations shall raise an
error of type TooComplexQuery.
When a query operation is producing so many results that can potentially exhaust client or server resources, or it can be
just impractical to be managed, implementations shall raise an error of type TooManyResults. The threshold conditions
used as criteria to raise such error is up to each implementation.
When a remote JSON-LD @context referenced by an incoming request is not available, implementations shall raise an
error of type LdContextNotAvailable. If the remote JSON-LD @context is invalid, implementations shall raise an error
of type BadRequestData.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Operation execution and generic error handling
