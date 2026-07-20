---
type: NGSI-LD Operation
title: "Add @context"
description: "5.13.2.1 Description With this operation, a client can ask the broker to store the full content of a specific @context, by giving it to the broker."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.13.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.13.2"
---

5.13.2.1 Description

With this operation, a client can ask the broker to store the full content of a specific @context, by giving it to the broker.

5.13.2.2 Use case diagram

A client can add an @context to be stored within an NGSI-LD system as shown in Figure 5.13.2.2-1.


Figure 5.13.2.2-1: Add @context use case

5.13.2.3 Input data

A JSON object that has a top-level field named @context, i.e. a JSON object representing a JSON-LD "local context". As specified in the JSON-LD specification [2], all extra information located outside of the @context subtree in the referenced object shall be discarded.

5.13.2.4 Behaviour

A new entry is created in the local storage and a locally unique identifier (URI) is generated for it. The JSON object representing the client-supplied @context and the current UTC time are stored alongside the locally unique identifier. That identifier shall be given back as a result in the output data. The entry is flagged as being of kind "Hosted". The behaviour described in clause 5.5.4 about JSON and JSON-LD validation shall be applied in case of invalid @context.

5.13.2.5 Output data

A locally unique URI identifying the @context in the broker's internal storage.

# Related

* [HTTP: Resource: jsonldContexts/](/http-binding/resource-jsonldcontexts.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.13.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Add @context
