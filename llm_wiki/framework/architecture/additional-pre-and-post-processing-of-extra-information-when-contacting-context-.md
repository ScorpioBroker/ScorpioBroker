---
type: NGSI-LD Clause
title: "Additional pre- and post-processing of extra information when contacting Context Source"
description: "The following key-values have a specific well-defined meaning when defined as elements within the optional array contextSourceInfo of the CSourceRegistration."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.6
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.6"
---

The following key-values have a specific well-defined meaning when defined as elements within the optional array contextSourceInfo of the CSourceRegistration.

If the key "accept" is defined:

- the value shall be a MIME type acceptable to the Context Broker (one of: "application/json", "application/ld+json").
- the response from the distributed endpoint shall be returned in this defined format and if necessary, the Context Broker shall be responsible for converting this to the desired content type when aggregating responses to the initial request.


If the key "contentType" is defined:

- the value shall be a MIME type acceptable to the Context Broker (one of: "application/json", "application/ld+json").
- the Context Broker shall provide the request and the associated @context as required by the MIME type when distributing the request to the context source endpoint, regardless of how it was provided in the initial request. If the key "jsonldContext" is defined:
- the value shall correspond to a URL reference as defined by the JSON-LD specification [2], section 3.1.
- the Context Broker shall apply a compaction operation as defined by the JSON-LD specification [2], section 4.1.5 over both payload and query parameters using the JSON-LD Context supplied in the value of the "jsonldContext" key-value pair, prior to distributing the request to the context source endpoint and forwarding with this JSON-LD context using an appropriate binding. Additionally, if a payload is defined in the initial request to the Context Broker, the "Content-Type" of the forwarded request shall be "application/json" and the Context Broker shall remove any @context members from the payload prior to distributing the request to the context source endpoint. If the key "ngsildConformance" is defined:
- The value shall define in the form major.minor, for example 1.5.
- The Context Broker shall apply a backwards compatibility operation over the payload (as defined by clause 4.3.6.8) prior to distributing the request to the context source endpoint such that the forwarded payload conforms to the specified version of the NGSI-LD specification.

# Related

* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Additional pre- and post-processing of extra information when contacting Context Source
