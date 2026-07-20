---
type: NGSI-LD Clause
title: "Error types"
description: "Table 5.5.2-1 details a list of error types defined by NGSI-LD."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.2"
---

Table 5.5.2-1 details a list of error types defined by NGSI-LD. The particular conditions under which error type shall be raised are defined when describing each operation supported by the API.


Table 5.5.2-1: Error types in NGSI-LD
Error Type Description
https://uri.etsi.org/ngsi-ld/errors/AlreadyExists The referred element already exists.
https://uri.etsi.org/ngsi-ld/errors/BadRequestData The request includes input data which does not meet the
requirements of the operation.
https://uri.etsi.org/ngsi-ld/errors/Conflict The operation conflicts with the current state of the system.
https://uri.etsi.org/ngsi-ld/errors/InternalError There has been an error during the operation execution.
https://uri.etsi.org/ngsi-ld/errors/InvalidRequest The request associated to the operation is syntactically
invalid or includes wrong content.
https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable A remote JSON-LD @context referenced in a request cannot
be retrieved by the NGSI-LD Broker and expansion or
compaction cannot be performed.
https://uri.etsi.org/ngsi-ld/errors/NoMultiTenantSupport The NGSI-LD API implementation does not support multiple
tenants.
https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant The addressed tenant does not exist.
https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported The operation is not supported.
https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound The referred resource has not been found.
https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery The query associated to the operation is too complex and
cannot be resolved.
https://uri.etsi.org/ngsi-ld/errors/TooManyResults The query associated to the operation is producing so many
results that can exhaust client or server resources. It should
be made more restrictive.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Error types
