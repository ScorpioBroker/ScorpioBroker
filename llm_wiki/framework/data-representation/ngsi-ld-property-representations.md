---
type: NGSI-LD Clause
title: "NGSI-LD Property Representations"
description: "4.5.2.1 Introduction An NGSI-LD Property, its value and sub-attributes can be represented in two equally valid lossless formats."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.2"
---

4.5.2.1 Introduction

An NGSI-LD Property, its value and sub-attributes can be represented in two equally valid lossless formats. The
normalized representation is a JSON-LD document that is complete with respect to mandatory members. The concise
representation is a terser alternative, which makes various implicit assumptions against the payloads and removes
redundancy from them.
Both normalized and concise representation of Properties shall be supported by implementations and can be selected by
Context Consumers through specific request parameters. An example of this representation can be found in annex
C, clause C.2.2.

4.5.2.2 Normalized NGSI-LD Property

An NGSI-LD Property in normalized representation shall be represented by a member whose key is the Property name (a term) and whose value is a JSON-LD object (or JSON-LD array with such JSON-LD objects, if there are multiple instances with the same Property name, as described in clause 4.5.5), which includes the following members: Mandatory

- "type": the fixed value "Property".
- "value": the Property Value (see definition of NGSI-LD Value in clause 3.1). The datatype URI can be placed in the optional "valueType" member. An NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "value" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the Property, as well as in notifications and in temporal evolutions (for encoding a deleted Property). It should be noted that in the JSON-LD serialization, a number data type does not allow for leading zeros, and octal and hexadecimal formats are not used. In the context broker's internal representation, a number is equivalent to either an integer or floating-point number, depending on if the number has a non-zero fractional part. The degree of precision of a floating-point number held within a context broker will depend upon the implementation, and insignificant digits may be lost during the deserialization and serialization process.


Optional

- "datasetId": a URI as mandated by clause 4.5.5.
- "expiresAt": a string as mandated by clause 4.22.
- "ngsildproof": a Property with the non-reified subproperties "entityIdSealed" and "entityTypeSealed" as specified in [35]. The value of its "value" element shall be an object containing the W3C ® Data integrity "proof" structure [35]. See clause C.11 for an example.
- "observedAt": a string as mandated by clause 4.8.
- "unitCode": a string representing the measurement unit corresponding to the Property value. It shall be encoded using the UNECE/CEFACT Common Codes for Units of Measurement [15].
- "valueType": a string value which shall be type coerced into a datatype URI. It is a non-reified alternative to the use of a native JSON-LD @type for the Property value. When it is a JSON primitive, it should align this with the RDF datatype [34].
- For each of the Properties this Property is associated with, a member whose key (a term) is the Property name and value is the result of serializing a Property (or any of its subclasses) in normalized representation (see clause 4.5.2.2).
- For each of the Relationships this Property is associated with, a member whose key (a term) is the Relationship name and value is the result of serializing a Relationship in normalized representation (see clause 4.5.3.2). System Generated
- "createdAt": a string as mandated by clause 4.8.
- "deletedAt": a string as mandated by clause 4.8.
- "instanceId": a URI uniquely identifying a Property instance, as mandated by clause 4.5.7.
- "modifiedAt": a string as mandated by clause 4.8. Output Only
- "previousValue": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous Property Value, before the triggering change. The representation is the same as that of "value". Furthermore, an NGSI-LD Property in the normalized representation shall never include the following members: Prohibited
- "entity": shall never be present, as it is used during inline Linked Entity retrieval to define the target Entity of a Relationship's object.
- "entityIdSealed" and "entityTypeSealed": shall never be present, unless the Property name is "ngsildproof".
- "entityList": shall never be present, as it is used during inline Linked Entity retrieval to define the target Entities of a ListRelationship's object.
- "json" and "previousJson": shall never be present, as they define a JsonProperty value.
- "languageMap" and "previousLanguageMap": shall never be present, as they define a LanguageProperty value.
- "object" and "previousObject": shall never be present, as they define a Relationship's object URI.


- "objectList" and "previousObjectList": shall never be present, as they define an ordered array of Relationship object URIs.
- "valueList" and "previousValueList": shall never be present, as they define an ordered array of Property values.
- "vocab" and "previousVocab": shall never be present, as they define a VocabProperty value.

4.5.2.3 Concise NGSI-LD Property

An NGSI-LD Property without sub-attributes shall be represented in a concise but lossless representation by a
member whose key is the Property name (a term) and whose value is the Property Value (see definition of NGSI-LD
Value in clause 3.1). In this case the concise representation is equivalent to simplified representation (see clause 4.5.4).
If the provided value is a JSON object and contains a "type" field, the whole Attribute shall be treated as a
normalized representation (see clause 4.5.2.2).
An NGSI-LD Property which includes sub-attributes shall be represented in a concise but lossless representation by a
member whose key is the Property name (a term) and whose value is a JSON-LD object (or JSON-LD array with such
JSON-LD objects if there are multiple instances with the same Property name as described in clause 4.5.5) which
includes the following members:
Mandatory
- "value": the Property Value (see definition of NGSI-LD Value in clause 3.1). The datatype URI can be placed in the optional "valueType" member.

An NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "value" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the Property, as well as in notifications and in temporal evolutions (for encoding a deleted Property). It should be noted that in the JSON-LD serialization, a number data type does not allow for leading zeros, and octal and hexadecimal formats are not used. In the context broker's internal representation, a number is equivalent to either an integer or floating-point number, depending on if the number has a non-zero fractional part. The degree of precision of a floating-point number held within a context broker will depend upon the implementation, and insignificant digits may be lost during the deserialization and serialization process. Optional

- "datasetId": a URI as mandated by clause 4.5.5.
- "expiresAt": a string as mandated by clause 4.22.
- "ngsildproof": a Property with the non-reified subproperties "entityIdSealed" and "entityTypeSealed" as specified in [35]. The value of its "value" element shall be an object containing the W3C® Data integrity "proof" structure [35]. See clause C.11 for an example.
- "observedAt": a string as mandated by clause 4.8.
- "type": If missing, "Property" can be inferred by the presence of the "value" attribute. An exception to this inference rule occurs for geospatial Property Values, where the "GeoProperty" sub-type shall be inferred instead, if the Property Value resolves to a supported GeoJSON geometry (see clause 4.7).
- "unitCode": a string representing the measurement unit corresponding to the Property value. It shall be encoded using the UNECE/CEFACT Common Codes for Units of Measurement [15].
- "valueType": a string value which shall be type coerced into a datatype URI. It is a non-reified alternative to the use of a native JSON-LD @type for the Property value. When it is a JSON primitive, it should align this with the RDF datatype [34].
- For each of the Properties this Property is associated with, a member whose key (a term) is the Property name and value is the result of serializing a Property (or any of its subclasses) in concise representation (see clause 4.5.2.3).


- For each of the Relationships this Property is associated with, a member whose key (a term) is the Relationship name and value is the result of serializing a Relationship (or any of its subclasses) in concise representation (see clause 4.5.3.3). System Generated
- "createdAt": a string as mandated by clause 4.8.
- "deletedAt": a string as mandated by clause 4.8.
- "instanceId": a URI uniquely identifying a Property instance as mandated by clause 4.5.7.
- "modifiedAt": a string as mandated by clause 4.8. Output Only
- "previousValue": only provided if the showChanges option is explicitly requested. It represents the previous Property Value, before the triggering change. The representation is the same as that of "value". Furthermore, an NGSI-LD Property in the concise representation shall never include the following members: Prohibited
- "entity": shall never be present, as it is used during inline Linked Entity retrieval to define the target Entity of a Relationship's object.
- "entityIdSealed" and "entityTypeSealed" shall never be present, unless the Property name is "ngsildproof".
- "entityList": shall never be present, as it is used during inline Linked Entity retrieval to define the target Entities of a ListRelationship's object.
- "languageMap" and "previousLanguageMap": shall never be present, as they define a LanguageProperty value.
- "json" and "previousJson": shall never be present, as they define a JsonProperty value.
- "object" and "previousObject": shall never be present, as they define a Relationship's object URI.
- "objectList" and "previousObjectList": shall never be present, as they define an ordered array of Relationship object URIs.
- "vocab" and "previousVocab": shall never be present, as they define a VocabProperty value.
- "valueList" and "previousValueList": shall never be present, as they define an ordered array of Property values.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.5.0](/framework/data-representation/introduction.md)
* [Clause 4.5.2.2](/framework/data-representation/normalized-ngsi-ld-property.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
* [Clause 4.5.3.2](/framework/data-representation/normalized-ngsi-ld-relationship.md)
* [Clause 4.5.3.3](/framework/data-representation/concise-ngsi-ld-relationship.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Property Representations
