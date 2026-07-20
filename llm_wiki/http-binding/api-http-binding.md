---
type: NGSI-LD Clause
title: "API HTTP Binding"
description: "6.1 Introduction This clause defines the resources and operations of the NGSI-LD API."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6"
---

6.1 Introduction

This clause defines the resources and operations of the NGSI-LD API. The NGSI-LD API is structured in terms of HTTP [3], [4] verbs, request and response payload bodies. A non-normative OAS specification [i.12] of the referred HTTP binding can be found at [i.14].

6.2 Global Definitions and Resource Structure

All resource URIs of this API shall have the following root:

- {apiRoot}/{apiName}/{apiVersion}/ NOTE 1: The apiRoot discovery process is out of the scope of the present document.

NOTE 2: The apiRoot for Context Source related aspects and the apiRoot for general Entity-related aspects can be different, e.g. the Context Source related aspects can be implemented by a Context Registry as shown for the distributed and federated architectures (see clause 4.3), whereas the Entity related aspects would be implemented by a Context Broker. NOTE 3: The apiRoot for Context Source related aspects and the apiRoot for general Entity-related aspects can be different than the apiRoot for temporal aspects, e.g. the temporal aspects can be implemented by an NGSI-LD subsystem specialized in historical data. The apiRoot includes the scheme ("http" or "https"), host and optional port, and an optional prefix string. The API shall support HTTP over TLS (also known as HTTPS - see IETF RFC 2818 [18]). TLS version 1.2 as defined by IETF RFC 5246 [19] shall be supported. HTTP without TLS is not recommended. The apiName shall be set to "ngsi-ld" and the apiVersion shall be set to "v1" for the present document. All resource URIs are defined relative to the above root URI. The structure of the resources under the root URI is shown in Figure 6.2-1 and methods defined on them are shown in Table 6.2-1. An OpenAPI companion specification is available, see [37].


Figure 6.2-1: Resource URI structure of the NGSI-LD API

Table 6.2-1: Resources and HTTP methods defined on them Resource Name Resource URI HTTP Method Meaning Clauses

| | | POST | Entity creation | 5.6.1; 6.4.3.1 |
| --- | --- | --- | --- | --- |
| Entity List | /entities/ | GET | Query entities | 5.7.2; 6.4.3.2 |
| | | DELETE | Purge entities | 5.6.21; 6.4.3.3 |

Entity by id /entities/{entityId}

GET Entity retrieval by id 5.7.1; 6.5.3.1
DELETE Entity deletion by id 5.6.6; 6.5.3.2
PATCH Entity merge by id 5.6.17; 6.5.3.4
PUT Entity replacement by id 5.6.18; 6.5.3.3

POST Append Attributes to Entity 5.6.3; 6.6.3.1 PATCH Update Attributes of an Entity 5.6.2; 6.6.3.2

Attribute List /entities/{entityId}/attrs/

Attribute by id /entities/{entityId}/attrs/{attrId}

PATCH Partial Attribute update 5.6.4; 6.7.3.1
DELETE Attribute deletion 5.6.5; 6.7.3.2
PUT Attribute replacement 5.6.19; 6.7.3.3
POST Create Subscription 5.8.1; 6.10.3.1
GET Retrieve list of Subscriptions 5.8.4; 6.10.3.2

Subscriptions List /subscriptions/

Subscription by Id /subscriptions/{subscriptionId}

GET Subscription retrieval by id 5.8.3; 6.11.3.1 PATCH Subscription update by id 5.8.2; 6.11.3.2 DELETE Subscription deletion by id 5.8.5; 6.11.3.3 Entity Types /types/ GET Retrieve available entity types

# Related

* [Clause 4.3](/framework/architecture/ngsi-ld-architectural-considerations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — API HTTP Binding
