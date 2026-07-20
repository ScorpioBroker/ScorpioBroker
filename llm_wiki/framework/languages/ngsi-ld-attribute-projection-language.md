---
type: NGSI-LD Clause
title: "NGSI-LD Attribute Projection Language"
description: "The NGSI-LD Attribute Projection Language shall be supported by implementations for projection parameters (e.g."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.21
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.21"
---

The NGSI-LD Attribute Projection Language shall be supported by implementations for projection parameters (e.g. pick and omit). Its aim is to specify the Attributes to be retrieved within an Entity (or associated Linked Entities when Linked Entity Retrieval is used). Projected Attributes are specified as a disjunction of elements, where each element can either directly be an Attribute name, or in the case of Linked Entity Retrieval, a Relationship name followed by an Attribute name within the Linked Entity. Since a disjunction of Attributes is a list, either a comma or a pipe character can be used as alternative representations of the or operator. In the following, ABNF grammar for NGSI-LD Attribute Projection Language is given.

orOp = %x7C / %x2C ; | , ProjectionTerm = AttrName *1(LinkedEntityTerm) *(orOp ProjectionTerm) LinkedEntityTerm = %x7B ProjectionTerm %x7D ; {ProjectionTerm}

See clause 4.9 for the definition of AttrName. EXAMPLE 1: ?pick=temperature EXAMPLE 2: ?pick=temperature,humidity EXAMPLE 3: ?pick=observation{temperature,humidity}

# Related

* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.21](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Attribute Projection Language
