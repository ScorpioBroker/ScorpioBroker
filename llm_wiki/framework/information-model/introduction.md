---
type: NGSI-LD Clause
title: "Introduction"
description: "The NGSI-LD Information Model prescribes the structure of context information that shall be supported by an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.2.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.2.1"
---

The NGSI-LD Information Model prescribes the structure of context information that shall be supported by an
NGSI-LD system. It specifies the data representation mechanisms that shall be used by the NGSI-LD API itself. In
addition, it specifies the structure of the Context Information Management vocabularies to be used in conjunction with
the API.
The NGSI-LD Information Model is defined at two levels (see Figure 4.2.1-1): the foundation classes which correspond
to the Core Meta-model and the Cross-Domain Ontology. The former amounts to a formal specification of the "property
graph" model [i.6]. The latter is a set of generic, transversal classes which are aimed at avoiding conflicting or
redundant definitions of the same classes in each of the domain-specific ontologies. Below these two levels, domain
specific ontologies or vocabularies can be devised. For instance, the SAREF Ontology ETSI TS 103 264 [i.4] can be
mapped to the NGSI-LD Information Model, so that smart home applications will benefit from this Context Information
Management API specification.
The version of the cross-domain model proposed by the present document is a minimal one, aimed at defining the
classes used in this release of the API specification. It has been extended by other work items like ETSI
GS CIM 006 [i.8], with classes defining extra concepts such as mobile vs. stationary entities, instantaneous vs. static
properties, etc.

Figure 4.2.1-1: Overview of the NGSI-LD Information Model Structure

Core MetaModel

Cross-Domain Ontology

Domain-Specific Ontologies

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.2.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
