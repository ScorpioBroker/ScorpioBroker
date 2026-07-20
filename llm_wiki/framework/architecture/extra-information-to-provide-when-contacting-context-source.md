---
type: NGSI-LD Clause
title: "Extra information to provide when contacting Context Source"
description: "If the optional array (of KeyValuePair type, as defined by clause 5.2.22) contextSourceInfo of the CSourceRegistration is present, it contains, whatever extra information the Context Broker shall conv"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.5
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.5"
---

If the optional array (of KeyValuePair type, as defined by clause 5.2.22) contextSourceInfo of the CSourceRegistration
is present, it contains, whatever extra information the Context Broker shall convey when contacting the Context
Source. This can be information the Context Broker needs to successfully communicate with the Context
Source (e.g. Authorization material), or for the Context Source to correctly interpret the received content
(e.g. the Link URL to fetch an @context). The method for conveying this information is binding-specific, e.g. using
headers in the case of HTTP.
Instead of providing the actual value, the special value "urn:ngsi-ld:request" can be used to indicate that the
respective value is to be taken from the request that triggered the given request, if present.
EXAMPLE: If the key value pair "user": "urn:ngsi-ld:request" is part of contextSourceInfo of
the CSourceRegistration, the Context Broker checks if "user" was conveyed in the
triggering request. If this is the case, e.g. "user": "abcd", "user": "abcd" is also
conveyed when contacting the Context Source.
As Tenant information, if applicable, is directly specified in the CSourceRegistration, it shall not be part of
contextSourceInfo. Binding-specific information that is used for setting up the connection or is specific for an
interaction, e.g. Content-length in HTTP, cannot be overridden by contextSourceInfo. If present, such information shall
be ignored.

# Related

* [Clause 5.2.22](/api-operations/data-types/keyvaluepair.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Extra information to provide when contacting Context Source
