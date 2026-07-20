---
type: NGSI-LD Data Type
title: "KeyValuePair"
description: "This datatype represents the optional information that is required when contacting an endpoint for notifications."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.22
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.22"
---

This datatype represents the optional information that is required when contacting an endpoint for notifications.
The supported members shall follow the indications provided in Table 5.2.22-1. They are intended to represent a
key/value pair.
Example optional information includes additional HTTP Headers such as:

- The HTTP Authentication Header.

- The HTTP Prefer Header (IETF RFC 7240 [26]) used when notifying the GeoJSON Endpoint.

| | Name | Data Type | | Restrictions | Cardinality | | | Description | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | key | String | Binding-dependent | | 1 The key of the key/value pair. | | | | | |
| | value | String | Binding-dependent | | 1 The value of the key/value pair. | | | | | |

Table 5.2.22-1: KeyValuePair data type definition

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.22](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — KeyValuePair
