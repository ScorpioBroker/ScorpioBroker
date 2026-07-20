# Data Representation

# Data Representation

* [NGSI-LD Data Representation](ngsi-ld-data-representation.md) - 4.5.0 Introduction All NGSI-LD elements are represented in JSON-LD [2].
* [Introduction](introduction.md) - All NGSI-LD elements are represented in JSON-LD [2].
* [NGSI-LD Entity Representation](ngsi-ld-entity-representation.md) - An NGSI-LD Entity shall be represented by an object encoded using JSON-LD [2].
* [NGSI-LD Property Representations](ngsi-ld-property-representations.md) - 4.5.2.1 Introduction An NGSI-LD Property, its value and sub-attributes can be represented in two equally valid lossless
* [Introduction](introduction-4-5-2-1.md) - An NGSI-LD Property, its value and sub-attributes can be represented in two equally valid lossless formats.
* [Normalized NGSI-LD Property](normalized-ngsi-ld-property.md) - An NGSI-LD Property in normalized representation shall be represented by a member whose key is the Property name (a term
* [Concise NGSI-LD Property](concise-ngsi-ld-property.md) - An NGSI-LD Property without sub-attributes shall be represented in a concise but lossless representation by a member who
* [NGSI-LD Relationship Representations](ngsi-ld-relationship-representations.md) - 4.5.3.1 Introduction An NGSI-LD Relationship, its value and sub-attributes can be represented in two equally valid lossl
* [Introduction](introduction-4-5-3-1.md) - An NGSI-LD Relationship, its value and sub-attributes can be represented in two equally valid lossless formats.
* [Normalized NGSI-LD Relationship](normalized-ngsi-ld-relationship.md) - An NGSI-LD Relationship in normalized representation shall be represented by a member whose key is the Relationship name
* [Concise NGSI-LD Relationship](concise-ngsi-ld-relationship.md) - An NGSI-LD Relationship in shall be represented in a concise but lossless representation by a member whose key is the Re
* [Simplified Representation](simplified-representation.md) - The NGSI-LD specification defines an abbreviated, lossy representation of Entities, which allows consuming only entity d
* [Multi-Attribute Support](multi-attribute-support.md) - 4.5.5.1 Introduction For each Entity, there can be Attributes that simultaneously have more than one instance.
* [Introduction](introduction-4-5-5-1.md) - For each Entity, there can be Attributes that simultaneously have more than one instance.
* [Processing of Conflicting Transient Entities](processing-of-conflicting-transient-entities.md) - In case of conflicting information when an Entity is received from a registered Context Source and marked with an expire
* [Processing of Conflicting Attributes](processing-of-conflicting-attributes.md) - In case of conflicting information for an Attribute, where a datasetId is duplicated, but there are differences in the o
* [Temporal Representation of an Entity](temporal-representation-of-an-entity.md) - The temporal representation of an Entity is the way to represent the Temporal Evolution of an Entity: the Entity shall b
* [Temporal Representation of a Property](temporal-representation-of-a-property.md) - The temporal evolution of a Property (for instance, its historical evolution or future predictions) is composed of the s
* [Temporal Representation of a Relationship](temporal-representation-of-a-relationship.md) - The temporal evolution of a Relationship (for instance, its historical evolution or future predictions) is composed of t
* [Simplified temporal representation of an Entity](simplified-temporal-representation-of-an-entity.md) - The NGSI-LD specification defines an alternative, abbreviated temporal representation of Temporal Evolution of Entities,
* [Entity Type List Representation](entity-type-list-representation.md) - The entity type list representation is used to consume information about entity types.
* [Detailed Entity Type List Representation](detailed-entity-type-list-representation.md) - The detailed entity type list representation is used to consume detailed information about entity types including the na
* [Entity Type Information Representation](entity-type-information-representation.md) - The entity type information representation is used to consume detailed information about an entity type.
* [Attribute List Representation](attribute-list-representation.md) - The attribute list representation is used to consume information about attributes.
* [Detailed Attribute List Representation](detailed-attribute-list-representation.md) - The detailed attribute list representation is used to consume detailed information about attributes including the names
* [Attribute Information Representation](attribute-information-representation.md) - The attribute information representation is used to consume detailed information about an attribute.
* [GeoJSON Representation of Entities](geojson-representation-of-entities.md) - 4.5.16.0 Foreword The NGSI-LD specification defines an alternative representation of Entities, to make NGSI-LD responses
* [Foreword](foreword.md) - The NGSI-LD specification defines an alternative representation of Entities, to make NGSI-LD responses compatible with G
* [Top-level "geometry" field selection algorithm](top-level-geometry-field-selection-algorithm.md) - A parameter of the request (named geometryProperty) may be used to indicate the name of the GeoProperty to be selected.
* [GeoJSON Representation of an individual Entity](geojson-representation-of-an-individual-entity.md) - The GeoJSON representation of a spatially bounded Entity is defined as a single GeoJSON Feature object including the fol
* [GeoJSON Representation of Multiple Entities](geojson-representation-of-multiple-entities.md) - The GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection obje
* [Simplified GeoJSON Representation of Entities](simplified-geojson-representation-of-entities.md) - 4.5.17.0 Foreword When both simplified (see clause 4.5.4) and GeoJSON representation is requested, the following simplif
* [Foreword](foreword-4-5-17-0.md) - When both simplified (see clause 4.5.4) and GeoJSON representation is requested, the following simplified GeoJSON repres
* [Simplified GeoJSON Representation of an individual Entity](simplified-geojson-representation-of-an-individual-entity.md) - The simplified GeoJSON representation of a spatially bounded Entity is defined as a single GeoJSON Feature object as fol
* [Simplified GeoJSON Representation of multiple Entities](simplified-geojson-representation-of-multiple-entities.md) - The simplified GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureColl
* [NGSI-LD LanguageProperty Representations](ngsi-ld-languageproperty-representations.md) - 4.5.18.1 Introduction NGSI-LD defines a specialized type of Property named LanguageProperty, defined by the NGSI-LD @con
* [Introduction](introduction-4-5-18-1.md) - NGSI-LD defines a specialized type of Property named LanguageProperty, defined by the NGSI-LD @context described by the
* [Normalized NGSI-LD LanguageProperty](normalized-ngsi-ld-languageproperty.md) - When its value is "concise", a concise lossless representation of Entities shall be provided as defined by clause 4.5.1.
* [Concise NGSI-LD LanguageProperty](concise-ngsi-ld-languageproperty.md) - Broker will return data in the most concise lossless representation possible, for example removing all Attribute type me
* [Aggregated temporal representation of an Entity](aggregated-temporal-representation-of-an-entity.md) - 4.5.19.0 Foreword The NGSI-LD specification defines an alternative temporal representation of Entities, called aggregate
* [Foreword](foreword-4-5-19-0.md) - The NGSI-LD specification defines an alternative temporal representation of Entities, called aggregated temporal represe
* [Supported behaviours for aggregation functions](supported-behaviours-for-aggregation-functions.md) - In order to support such aggregation functions, two parameters are defined: - aggrMethods, to express the aggregation me
* [NGSI-LD VocabProperty Representations](ngsi-ld-vocabproperty-representations.md) - 4.5.20.1 Introduction NGSI-LD defines a specialized type of Property named VocabProperty, defined by the NGSI-LD @contex
* [Introduction](introduction-4-5-20-1.md) - NGSI-LD defines a specialized type of Property named VocabProperty, defined by the NGSI-LD @context described by the pre
* [Normalized NGSI-LD VocabProperty](normalized-ngsi-ld-vocabproperty.md) - An NGSI-LD VocabProperty shall be represented in normalized representation by a member whose key is the Property name (a
* [Concise NGSI-LD VocabProperty](concise-ngsi-ld-vocabproperty.md) - An NGSI-LD VocabProperty shall be represented in concise but lossless representation by a member whose key is the Proper
* [NGSI-LD ListProperty Representations](ngsi-ld-listproperty-representations.md) - 4.5.21.1 Introduction NGSI-LD defines a specialized type of Property named ListProperty, defined by the NGSI-LD @context
* [Introduction](introduction-4-5-21-1.md) - NGSI-LD defines a specialized type of Property named ListProperty, defined by the NGSI-LD @context described by the pres
* [Normalized NGSI-LD ListProperty](normalized-ngsi-ld-listproperty.md) - An NGSI-LD ListProperty shall be represented in normalized representation by a member whose key is the Property name (a
* [Concise NGSI-LD ListProperty](concise-ngsi-ld-listproperty.md) - An NGSI-LD ListProperty shall be represented in concise but lossless representation by a member whose key is the ListPro
* [NGSI-LD ListRelationship Representations](ngsi-ld-listrelationship-representations.md) - 4.5.22.1 Introduction NGSI-LD defines a specialized type of Relationship named ListRelationship, defined by the NGSI-LD
* [Introduction](introduction-4-5-22-1.md) - NGSI-LD defines a specialized type of Relationship named ListRelationship, defined by the NGSI-LD @context described by
* [Normalized NGSI-LD ListRelationship](normalized-ngsi-ld-listrelationship.md) - An NGSI-LD ListRelationship shall be represented in normalized representation by a member whose key is the Relationship
* [Concise NGSI-LD ListRelationship](concise-ngsi-ld-listrelationship.md) - An NGSI-LD ListRelationship shall be represented in concise but lossless representation by a member whose key is the Rel
* [NGSI-LD Linked Entity Retrieval](ngsi-ld-linked-entity-retrieval.md) - 4.5.23.1 Introduction Since Entities are uniquely identifiable by a URI, it is possible to traverse across the Entity gr
* [Introduction](introduction-4-5-23-1.md) - Since Entities are uniquely identifiable by a URI, it is possible to traverse across the Entity graph directly from a Li
* [Inline Linked Entity Representation](inline-linked-entity-representation.md) - With the inline representation, the Context Broker response shall only consist of Linking Entities - either a single Lin
* [Flattened Linked Entity Representation](flattened-linked-entity-representation.md) - With the flattened representation, the Context Broker response shall always consist of an array of Entities.
* [NGSI-LD JsonProperty Representations](ngsi-ld-jsonproperty-representations.md) - 4.5.24.1 Introduction NGSI-LD defines a specialized type of Property named JsonProperty, defined by the NGSI-LD @context
* [Introduction](introduction-4-5-24-1.md) - NGSI-LD defines a specialized type of Property named JsonProperty, defined by the NGSI-LD @context described by the pres
* [Normalized NGSI-LD JsonProperty](normalized-ngsi-ld-jsonproperty.md) - An NGSI-LD JsonProperty shall be represented in normalized representation by a member whose key is the Property name (a
* [Concise NGSI-LD JsonProperty](concise-ngsi-ld-jsonproperty.md) - An NGSI-LD JsonProperty shall be represented in concise but lossless representation by a member whose key is the Propert
* [NGSI-LD EntityMap Representation](ngsi-ld-entitymap-representation.md) - The EntityMap representation is used by Context Brokers to ensure unity when querying across distributed operations.
