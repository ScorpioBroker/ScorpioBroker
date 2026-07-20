---
type: NGSI-LD Clause
title: "Error response payload body"
description: "When reporting errors back to clients, NGSI-LD implementations shall generate a JSON object in accordance with IETF RFC 7807 [10], section 3.1, including, at least the following terms: - type: Error t"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.3"
---

When reporting errors back to clients, NGSI-LD implementations shall generate a JSON object in accordance with IETF RFC 7807 [10], section 3.1, including, at least the following terms:

- type: Error type as per clause 5.5.2.

- title: Error title which shall be a short string summarizing the error.

- detail: A detailed message that should convey enough information about the error. Even though IETF RFC 7807 [10] defines a specific MIME type for error payloads, NGSI-LD implementations shall use the standard JSON MIME type ("application/json") when reporting errors, so that old clients or existing tools are not broken.

EXAMPLE:

{

"type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
"title": "Resource not found.",
"detail": "urn:ngsi-ld:Device:widget001 was not found",
"status": 404,
"instance": "urn:ngsi-ld:Device:widget001"

}

# Related

* [Clause 5.5.2](/api-operations/common-behaviours/error-types.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Error response payload body
