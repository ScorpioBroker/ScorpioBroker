---
type: Reference
title: "Entity with digital signature for a Property"
description: "As specified in [35], the atomic piece of information that creators can digitally sign in an NGSI-LD ecosystem is each single Attribute of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.11
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.11"
---

As specified in [35], the atomic piece of information that creators can digitally sign in an NGSI-LD ecosystem is each single Attribute of an Entity. In the following example, an Entity of type "Store" with two Properties, "address" and "location" is presented. The "address" Property is digitally signed. The signature is created using one Ed25519

instantiation of the Edwards-Curve Digital Signature Algorithm (EdDSA).The used crypto suite is "eddsa-rdfc-2022".


EXAMPLE: Entity of type "Store" with two Properties. The "address" Property is digitally signed.

{

"id": "urn:ngsi-ld:Store:002",
"type": "Store",
"address": {
"type": "Property",
"value": {
"streetAddress": ["Tiger Street 4", "al"],
"addressRegion": "Metropolis",
"addressLocality": "Cat City",
"postalCode": "42420"
}
"ngsildproof": {
"type": "Property",
"entityIdSealed": "urn:ngsi-ld:Store:002",
"entityTypeSealed": "Store",
"value": {
"type": "DataIntegrityProof",
"created": "2025-01-27T21:02:24Z",
"verificationMethod": "https://example.edu/issuers/565049#z6MkwXG2WjeQnN ... Hc6SaVWoT",
"cryptosuite": "eddsa-rdfc-2022",
"proofPurpose": "assertionMethod",
"proofValue": "z3XrH3diVCqpVHXkE7WbnictqyQCkJBGTx ... NRTzmuoWU1Y2FyqGfSV9eS"

}

}
},
"location": {
"type": "GeoProperty",
"value": {
"type": "Point",
"coordinates": [57.5522, -20.3484]
}
},
"@context": "https://uri.etsi.org/ngsi-ld/primer/store-context.jsonld"

}

Annex D (informative): Transformation Algorithms

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity with digital signature for a Property
