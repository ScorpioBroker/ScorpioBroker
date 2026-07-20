---
type: NGSI-LD Clause
title: "NGSI-LD Data Representation"
description: "4.5.0 Introduction All NGSI-LD elements are represented in JSON-LD [2]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5"
---

4.5.0 Introduction

All NGSI-LD elements are represented in JSON-LD [2]. For the use with the API, the compacted JSON-LD
representation is used, i.e. short terms are used, which are expanded by the component implementing the NGSI-LD API
using a JSON-LD @context, typically provided as part of the request. As described in clause 4.4, the NGSI-LD Core
@context is always considered to be part of the @context to be used.
The use of JSON-LD for NGSI-LD elements has some implications for the use of null values, as JSON-LD interprets
setting elements to null as elements to be removed when performing JSON-LD expansion. Thus, null cannot be used as
a value in NGSI-LD.
To nevertheless allow deletions as part of NGSI-LD operations that update NGSI-LD data, which is typically handled
by setting the respective JSON key to null (e.g. as in IETF RFC 7396 [16]), the URI "urn:ngsi-ld:null" is used
as a replacement for null in all places where URI strings are valid JSON values. If an array is required, an array with
one single NGSI-LD Null is used. For languageMap, the JSON object {"@none": "urn:ngsi-ld:null"} is to
be used as explained in clause 4.5.18. These encodings of null are referred to as NGSI-LD Null.
For representing deleted elements in notifications and in the temporal representation, the URI "urn:ngsi
ld:null" is used as a Property value or Relationship object and the JSON object {"@none": "urn:ngsi
ld:null"} for the "languageMap" of a Language Property, respectively.
As null cannot be used as a value in JSON-LD, there is still the possibility of using a JSON null literal represented as
{"@type": "@json", "@value": null} in JSON-LD instead. JSON literals are not to be expanded in JSON
LD and thus the respective element is not removed during JSON-LD expansion.

4.5.1 NGSI-LD Entity Representation

An NGSI-LD Entity shall be represented by an object encoded using JSON-LD [2]. The rules described below state the
encoding that shall be supported by implementations. Annex D provides a computational description of this process in
terms of an algorithm.
The JSON-LD object contains the following members:
Mandatory
- "id" whose value shall be a URI that identifies the Entity.
- "type" whose value shall be equal to the Entity Type name or an unordered JSON array with multiple Entity Type names in case of an Entity that has multiple Entity Types.

Optional - "expiresAt": a string as mandated by clause 4.22.


- "scope" whose value shall be a Scope as defined in clause 4.18 or an unordered JSON array with multiple Scopes in case of an Entity that has multiple Scopes.
- "@context" a JSON-LD @context as described in clause 4.4.
- One member for each Property as per the rules stated in clause 4.5.2. In case of multiple Property instances with the same Property name as described in clause 4.5.5, all instances are provided as an unordered JSON array, and datasetId (see clause 4.5.5) shall be used to distinguish them.
- One member for each Relationship as per the rules stated in clause 4.5.3. In case of multiple Relationship instances with the same Relationship name as described in clause 4.5.5, all instances are provided as an unordered JSON array, and datasetId (see clause 4.5.5) shall be used to distinguish them.

NOTE 1: In the following, the term Attribute is used when referring in the text to both a Property and a Relationship (see definition of NGSI-LD Attribute in clause 3.1). NOTE 2: When GeoJSON representation is selected, the layout of the Entities changes, see clause 4.5.16 for details. Terms defined in the Core Context as non-reified Properties (such as "datasetId", "instanceId", etc.) shall not be used as Attribute names. Attributes shall not contain any embedded @context, as described in clause 5.5.7.

4.5.2 NGSI-LD Property Representations

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

4.5.3 NGSI-LD Relationship Representations

4.5.3.1 Introduction

An NGSI-LD Relationship, its value and sub-attributes can be represented in two equally valid lossless formats. The
normalized representation is a JSON-LD document that is complete with respect to mandatory members. The concise
representation is a terser alternative, which makes various implicit assumptions against the payloads and removes
redundancy from them.
Both normalized and concise representation of Relationships shall be supported by implementations and can be selected
by Context Consumers through specific request parameters. An example of this representation can be found in
annex C, clause C.2.2.


4.5.3.2 Normalized NGSI-LD Relationship

An NGSI-LD Relationship in normalized representation shall be represented by a member whose key is the Relationship name (a term) and whose value is a JSON-LD object (or JSON-LD array with such JSON-LD objects, if there are multiple instances with the same Relationship name, as described in clause 4.5.5) with the following terms: Mandatory

- "object": the Relationship's object represented by a URI or array of URIs. An NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "object" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the Relationship, as well as in notifications and in temporal evolutions (for encoding a deleted Relationship).
- "type": the fixed value "Relationship". Optional
- "datasetId": a URI as mandated by clause 4.5.5.
- "expiresAt": a string as mandated by clause 4.22.
- "ngsildproof": a Property, as mandated by clause 4.5.2, with the non-reified subproperties "entityIdSealed" and "entityTypeSealed" as specified in [35]. The value of its "value" element shall be an object containing the W3C® Data integrity "proof" structure [35]. See clause C.11 for an example.
- "objectType": a string as mandated by clause 4.5.23.
- "observedAt": a string as mandated by clause 4.8.
- For each Relationship this Relationship is associated with, a member whose key is the Relationship name (a term) and whose value is the result of serializing a Relationship (or any of its subclasses) as per the rules of representation of a Relationship in normalized representation (see clause 4.5.3.2).
- For each Property this Relationship is associated with, a member whose key is the Property name (a term) and whose value is the result of serializing a Property (or any of its subclasses) as per the rules of representation of a Property in normalized representation (see clause 4.5.2.2). System Generated
- "createdAt": a string as mandated by clause 4.8.
- "deletedAt": a string as mandated by clause 4.8.
- "instanceId": a URI uniquely identifying a Relationship instance as mandated by clause 4.5.8.
- "modifiedAt": a string as mandated by clause 4.8. Output Only
- "entity": only provided in case of Linked Entity retrieval, and only if the inline join option is explicitly requested (see clause 4.5.23.2), where it used to define the target Linked Entity of a Relationship's object in normalized representation.
- "previousObject": only provided if the showChanges option is explicitly requested. It represents the previous Relationship "object", before the triggering change. The representation is the same as that of "object". Furthermore, an NGSI-LD Relationship in the normalized representation shall never include the following members: Prohibited
- "entityIdSealed" and "entityTypeSealed" shall never be present.


- "entityList" shall never be present, as it is used during inline Linked Entity retrieval to define the target Entities of a ListRelationship's object.
- "languageMap" and "previousLanguageMap": shall never be present, as they define a LanguageProperty value.
- "json" and "previousJson": shall never be present, as they define a JsonProperty value.
- "objectList" and "previousObjectList": shall never be present, as they define an ordered array of Relationship object URIs.
- "unitCode": shall never be present, as Relationships are unitless.
- "value" and "previousValue": shall never be present, as they define a Property value.
- "valueList" and "previousValueList": shall never be present, as they define an ordered array of Property values.
- "vocab" and "previousVocab": shall never be present, as they define a VocabProperty value.

4.5.3.3 Concise NGSI-LD Relationship

An NGSI-LD Relationship in shall be represented in a concise but lossless representation by a member whose key is the Relationship name (a term) and whose value is a JSON-LD object (or JSON-LD array with such JSON-LD objects if there are multiple instances with the same Relationship name as described in clause 4.5.5) with the following terms: Mandatory

- "object": the Relationship's object represented by a URI or array of URIs. An NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "object" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the Relationship, as well as in notifications and in temporal evolutions (for encoding a deleted Relationship). Optional
- "datasetId": a URI as mandated by clause 4.5.5.
- "expiresAt": a string as mandated by clause 4.22.
- "ngsildproof": a Property, as mandated by clause 4.5.2, with the non-reified subproperties "entityIdSealed" and "entityTypeSealed" as specified in [35]. The value of its "value" element shall be an object containing the W3C® Data integrity "proof" structure [35]. See clause C.11 for an example.
- "observedAt": a string as mandated by clause 4.8.
- "objectType": a string as mandated by clause 4.5.23.
- "type": If missing, "Relationship" can be inferred by the presence of the "object" attribute.
- For each Relationship this Relationship is associated with, a member whose key is the Relationship name (a term) and whose value is the result of serializing a Relationship (or any of its subclasses) as per the rules of representation of a Relationship in concise representation. (see clause 4.5.3.3).
- For each Property this Relationship is associated with, a member whose key is the Property name (a term) and whose value is the result of serializing a Property (or any of its subclasses) as per the rules of representation of a Property in concise representation. (see clause 4.5.2.3). System Generated
- "createdAt": a string as mandated by clause 4.8.


- "deletedAt": a string as mandated by clause 4.8.
- "instanceId": a URI uniquely identifying a Relationship instance as mandated by clause 4.5.8.
- "modifiedAt": a string as mandated by clause 4.8. Output Only
- "entity": only provided in case of Linked Entity retrieval, and only if the inline join option is explicitly requested (see clause 4.5.23.2), where it used to define the target Linked Entity of a Relationship's object in concise representation.
- "previousObject": only provided if the showChanges option is explicitly requested. It represents the previous Relationship "object", before the triggering change. The representation is the same as that of "object". Furthermore, an NGSI-LD Relationship in the concise representation shall never include the following members: Prohibited
- "entityIdSealed" and "entityTypeSealed" shall never be present.
- "entityList": shall never be present, as it is used during inline Linked Entity retrieval (see clause 4.5.23.2) to define the target Entities of a ListRelationship's object.
- "languageMap" and "previousLanguageMap": shall never be present, as they define a LanguageProperty value.
- "json" and "previousJson": shall never be present, as they define a JsonProperty value.
- "objectList" and "previousObjectList": shall never be present, as they define an ordered array of Relationship object URIs.
- "unitCode": shall never be present, as Relationships are unitless.
- "valueList" and "previousValueList": shall never be present, as they define an ordered array of Property values.
- "value" and "previousValue": shall never be present, as they define a Property value.
- "vocab" and "previousVocab": shall never be present, as they define a VocabProperty value. Notwithstanding the definition above, during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12), an NGSI-LD Relationship with a value of NGSI-LD Null and without a datasetId should be represented in a concise representation by a member whose key is the Relationship name (a term) and whose value is "urn:ngsi ld:null".

4.5.4 Simplified Representation

The NGSI-LD specification defines an abbreviated, lossy representation of Entities, which allows consuming only entity data (the target object of each Relationship or the value of each Property) corresponding to the Properties or Relationships whose subject is the Entity itself i.e. the own Attributes of the Entity. The simplified representation of Entities shall be supported by implementations and can be selected by Context Consumers through specific request parameters. An example of this representation can be found in annex C, clause C.2.2. The simplified representation of an Entity shall be a JSON-LD object containing the following members: Mandatory

- "id" whose value shall be a URI that identifies the Entity.
- "type" whose value shall be equal to the Entity Type name or an unordered JSON array with multiple Entity Type names in case of an Entity that has multiple Entity Types.


Optional

- "@context", a JSON-LD @context as described in clause 4.4.
- For each Property a member whose key is the Property name (a term) and whose value is the Property Value.
- In the multi-attribute case (see clause 4.5.5), the simplified representation of a Property (or any of its subtypes) changes. Each Property consists of a key-value pair, the key being the Property name (a term) and the value being a JSON Object, which contains a single Attribute with a key called "dataset", and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId, where the value corresponds to the simplified representation of the property value. The default datasetId (where present) is represented by the JSON-LD keyword "@none".
EXAMPLE 1: "name": "David Robert Jones"
EXAMPLE 2:
"name": {
"dataset": {
"@none": "David Robert Jones",
"urn:ngsi-ld:datasetId:001": "David Bowie",
"urn:ngsi-ld:datasetId:002": "Ziggy Stardust"

} }

- For each GeoProperty, a member whose key is the Property name (a term) and whose value is the Property Value.
- For each LanguageProperty, a member whose key is the Property name (a term) and whose value is a JSON Object containing a single Attribute with a key called "languageMap" where the value shall correspond to a LanguageProperty languageMap.
- For each JsonProperty, a member whose key is the Property name (a term) and whose va

… (truncated; see full clause in the PDF).

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.0](/framework/data-representation/introduction.md)
* [Clause 4.5.16](/framework/data-representation/geojson-representation-of-entities.md)
* [Clause 4.5.18](/framework/data-representation/ngsi-ld-languageproperty-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Data Representation
