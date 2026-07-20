---
type: NGSI-LD Clause
title: "NGSI-LD Query Language"
description: "The NGSI-LD Query Language shall be supported by implementations."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.9
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.9"
---

The NGSI-LD Query Language shall be supported by implementations. It is intended to:

- filter out Entities by Attribute Values (target is the value member of a Property, see Table 5.2.5-1, or the object member of a Relationship, see Table 5.2.6-1);
- filter out Context Sources by the values of properties that describe them, defined when Context Sources are registered (target is the name of a Context Source Property member of the CSourceRegistration, see Table 5.2.9-1).
- filter out Snapshots by the values of the Snapshot data type members, i.e. when filtering Snapshots, only the names of the members defined for Snapshot in Table 5.2.43-1 are allowed values for AttrName. In this clause, three string parameters are defined in order to fully specify an NGSI-LD Query:
- q, to express the desired query;
- expandValues, to define the list of attributes whose values should be expanded against the supplied @context using JSON-LD type coercion prior to executing the query in the Context Broker. Optional
- jsonKeys, to define the list of attributes whose values uninterpretable as JSON-LD and should not be expanded against the supplied @context using JSON-LD type coercion prior to executing the query in the Context Broker. Optional In case of HTTP binding, whenever the string acting as a filter is part of the HTTP binding's URI, then it shall be URI-encoded (percent-encoded, as described in IETF RFC 3986 [5]). The grammar that encodes the syntax of the q parameter, expressed in ABNF format [12], is the NGSI-LD Query Language. It is described below (it has been validated using https://github.com/ietf-tools/bap), and it shall be supported by implementations:
Query = (QueryTerm / QueryTermAssoc) *(LogicalOp (QueryTerm / QueryTermAssoc))
QueryTermAssoc = %x28 QueryTerm *(LogicalOp QueryTerm) %x29 ; (QueryTerm)
QueryTerm = Attribute
QueryTerm =/ Attribute Operator ComparableValue
QueryTerm =/ Attribute equal CompEqualityValue
QueryTerm =/ Attribute unequal CompEqualityValue
QueryTerm =/ Attribute patternOp RegExp
QueryTerm =/ Attribute notPatternOp RegExp
Attribute = LinkedEntityRelation
LinkedEntityRelation = AttrName %x7B LinkedEntityPath %x7D ; AttrName{LinkedEntityPath}
LinkedEntityRelation =/ ValuePath
LinkedEntityPath = *1(EntityType 1*(%x2C EntityType) %x3A) AttrName %x7B LinkedEntityPath %x7D


;*1(EntityType 1*(,EntityType):)AttrName{LinkedEntityPath}

LinkedEntityPath =/ ValuePath
ValuePath = DottedPath *1(%x5B DottedPath %x5D) ; DottedPath *1([DottedPath])
DottedPath = AttrName *(%x2E AttrName) ; AttrName *(.AttrName)
Operator = equal / unequal / greaterEq / greater / lessEq / less
ComparableValue = Number / quotedStr / dateTime / date / time
OtherValue = false / true
Value = ComparableValue / OtherValue
Range = ComparableValue dots ComparableValue
ValueList = Value 1*(%x2C Value) ; Value 1*(, Value)
CompEqualityValue = OtherValue / ValueList / Range / URI
equal = %x3D %x3D ; ==
unequal = %x21 %x3D ; !=
greater = %x3E ; >
greaterEq = %x3E %x3D ; >=
less = %x3C ; <
lessEq = %x3C %x3D ; <=
patternOp = %x7E %x3D ; ~=
notPatternOp = %x21 %x7E %x3D ; !~=
dots = %x2E %x2E ; ..
TermChar = unicodeNumber / unicodeLetter
TermChar =/ %x5F ; _
AttrName = unicodeLetter *TermChar
EntityType = unicodeLetter *TermChar
quotedStr = String ; "*char"
andOp = %x3B ; ;
orOp = %x7C ; |
LogicalOp = andOp / orOp

- unicodeNumber is any Unicode character that has Number as a Category [22]. With Unicode-capable regular expression (RegEx) parsers, such a character may be matched by \p{N}.

- unicodeLetter is any Unicode character that has Letter as a Category [22]. With Unicode-capable regular expression (RegEx) parsers, such a character may be matched by \p{L}.

- Number shall be a number as mandated by the JSON Specification, following the ABNF Grammar, production

rule named number, section 6 of IETF RFC 8259 [6].

- String shall be a text string as mandated by the JSON Specification, following the ABNF Grammar, production rule named String, section 7 of IETF RFC 8259 [6].

- char shall be a character as mandated by the JSON Specification, ABNF Grammar, production rule named

char, section 7 of IETF RFC 8259 [6].

- false shall be conformant with the JSON ABNF Grammar, production rule named false, section 3 of IETF

RFC 8259 [6]. It is intended to represent the Boolean value corresponding to false.

- true shall be conformant with the JSON ABNF Grammar, production rule named true, section 3 of IETF

RFC 8259 [6]. It is intended to represent the Boolean value corresponding to true.

- RegExp shall be a regular expression as mandated by IEEE 1003.2™ [11].

- dateTime shall be a DateTime value as mandated by clause 4.6.3.

- time shall be a Time value as mandated by clause 4.6.3.

- date shall be a Date value as mandated by clause 4.6.3.

- URI shall be a URI as mandated by IETF RFC 3986 [5] or an IRI as mandated by IETF RFC 3987 [23],

appendix A, production rule named URI.

A Query Term (production rule QueryTerm) defines a predicate which serves as a matching condition for Entities. The

constituent parts of a Query Term are:

- an attribute path (production rule named Attribute).

- an optional pair composed by an operator (production rule named Operator) and a value (production rule

named Value).


The attribute path (production rule Attribute) is a simple name AttrName, optionally followed by a dot-separated list
of more AttrName (see later example 8), optionally followed by one trailing list of more dot-separated AttrNames
enclosed in one pair of square brackets (see later example 9). The attribute path is always a composition of short hand
names and not a fully qualified ones, because, when the query language is used, an @context properly defining all the
terms (as per clause 5.5.7) shall be issued.
EXAMPLE 0: ?q=temperature (checks for the existence of the attribute temperature).
EXAMPLE 1: ?q=temperature==20.
EXAMPLE 2: ?q=brandName!="Mercedes".
EXAMPLE 3: ?q=isParked=="urn:ngsi-ld:OffStreetParking:Downtown1".
EXAMPLE 4: A query encoded as an HTTP Query String. Note that this is HTTP binding specific, to be used via
GET method, as defined in clause 6.4.3.2. The NGSI-LD query language string is conveyed by
means of parameter q.
?q=speed>50;brandName!="Mercedes". Also note that (as stated above) URI-encoding
(percent-encoding) is required if the query string contains reserved characters (see IETF
RFC 3986 [5] and IETF RFC 3987 [23], for the exact list of them).
EXAMPLE 5: ?q=isMonitoredBy (to query Entities that have the Attribute isMonitoredBy).
Query Terms may be combined through logical operators that shall be supported by implementations as follows:
- The production rule andOp defines a logical AND operator conveying that the requested entities are those which meet at the same time the conditions posed by all the Query Terms affected by such an operator.
- The production rule orOp defines a logical OR operator conveying that the requested entities are those which meet any of the conditions posed by the Query Terms affected by such an operator.
- When evaluating logical conditions, and in the absence of specific Query Term associations (see below), the logical AND operator shall take precedence over the logical OR operator. Association of Query Terms shall be supported by implementations as per the grammar included by the present clause (production rule named QueryTermAssoc). An association of Query Terms is composed of the combination of different Query Terms linked by logical operators (AND, OR) and delimited by parenthesis. The evaluation of an association of Query Terms shall always take precedence over individual, non-associated Query Terms.
- Every name in the list shall be expanded to a URI (Fully Qualified Name) as mandated by clause 5.5.7.
- The first name shall refer to a Property or Relationship (top level element) whose subject shall be a matching Entity. Strictly speaking, and as per the JSON-LD representation rules, such (fully qualified) name shall be equal to the (fully qualified) name of the concerned Property or Relationship.
- Each other name (if present) represents a (sub)Property or (sub)Relationship, starting with the top-level element as subject and continuing through the graph traversal. The element addressed by the last name in the list is defined as the target element. If only one name is present in the attribute path, then the target element is the top level one.

EXAMPLE 6: ?q=((speed>50|rpm>3000);brandName=="Mercedes") EXAMPLE 7: ?q=(temperature>=20;temperature<=25)|capacity<=10 The following example 8 shows the syntax of an attribute path that is defined by the production rule Attribute, as a dot separated list of names. Such a list is intended to address a Property or Relationship included by the matching entities subjacent graph, in accordance with the following rules: EXAMPLE 8: ?q=temperature.observedAt>=2017-12-24T12:00:00Z If the target element is a Property, the target value is defined as the Value associated to such Property. If a Property has multiple instances (identified by its respective datasetId), and no datasetId is explicitly addressed, the target value shall be any Value of such instances.


If the target element is a LanguageProperty, and no target language is specified, the target value is defined as a value
from any of the key-value pairs held within the languageMap associated to such LanguageProperty.
If the target element is a ListProperty, the target value is defined as the valueList array associated to such a
ListProperty.
If the target element is a LanguageProperty and a target language is specified, the target value is defined as the Value
associated to the matching key-value pair held within the languageMap associated to such LanguageProperty where the
key matches the target language.
If the target element is a VocabProperty, the target value shall be expanded according to the @context.
If the target element is a Relationship, the target object is defined as the object associated (represented as a URI or
array of URIs) to such Relationship. If a Relationship has multiple instances (identified by its respective datasetId), and
no datasetId is explicitly addressed, the target object shall be any object of such instances.
If the target element is a ListRelationship, the target object is defined as the array of objects associated (represented as
URIs) to such ListRelationship.
When a Query Term only defines an attribute path (production rule named Attribute), the matching Entities shall be
those which define the target element (Property or a Relationship), regardless of any target value or object.
Lastly, implementations shall support queries involving specific data subitems belonging to a Property Value (seed
target value) represented by a JSON object structure (compound value). For that purpose, an attribute path may
additionally contain a trailing path (enclosed in a single pair of square brackets that signal that the overall path is now
entering the compound value) composed of a dot-concatenated list of JSON member names, and intended to address a
specific data subitem (member) within the seed target value. When such a trailing path is present, implementations
shall interpret and evaluate it (against the seed target value) as a MemberExpression of ECMA 262 [21], in dot notation,
as clarified therein at section Property Accessors). If the evaluation of such MemberExpression does not result in a
defined value, the target element shall be considered as non-existent for the purpose of query resolution.
EXAMPLE 9: ?q=address[city]=="Berlin". The trailing path is [city]. It is used to refer to a
particular subitem within the value of the address Property, which is a complex JSON object
representing a postal address. Refer to the following NGSI-LD Entity:
{
"id": "urn:ngsi-ld:placedescription:123",
"type": "PlaceDescription",
"address": {
"type": "Property",
"value": {
"city": "Berlin",
"street": "Ulrich Strasse"

} } }

EXAMPLE 10: ?q=sensor.rawdata[airquality.particulate]==40. The trailing path is [airquality.particulate]. The particulate Property of the compound JSON object is targeted. Refer to the following NGSI-LD Entity: {

"id": "urn:ngsi-ld:particulatemeasurement:345",
"type": "ParticulateMeasurement",
"sensor": {
"type": "Property",
"value": 40,
"rawdata": {
"type": "Property",
"value": {
"airquality": {
"particulate": 40,
"PM20": 85

}


} } } }

EXAMPLE 11: ?q=parkingTickets[value]=="Overstay 60 minutes"&jsonKeys=parkingTickets. The trailing path is parkingTickets. The parkingTickets Property of the JSON object is targeted, but the target value raw is JSON, and is not expanded to ngsi-ld:hasValue using the core @context. Refer to the following NGSI-LD Entity: {

"id": "urn:ngsi-ld:Car:6152s",
"type": "Car",
"parkingTickets": {
"type": "JsonProperty",
"json": {
"id": "85a6cc52-0589-45f9",
"value": "Overstay 60 minutes"

} } }

EXAMPLE 12: ?q=gender==Male&expandValues=gender. The trailing path is gender. The gender Property of JSON object is targeted, but the target value is first expanded to a URI using the supplied @context. Refer to the following NGSI-LD Entity: {

"id": "urn:ngsi-ld:Person:678",
"type": "Person",
"gender": {
"type": "VocabProperty",
"vocab": "Male",
}
},
@context": {
"Male": "http://example.org/Male",

} }

The filter can also apply to a Property or Relationship of an NGSI-LD Entity targeted by a (recursively) followed
Relationship, for example as part of a linked entity retrieval (clause 4.5.23).
EXAMPLE 13: ?q=sensor{humidity}==40. The trailing path is sensor{humidity}. The query targets
entities with a sensor Relationship and makes a sub-query on matching target objects which have
the matching humidity Attribute. Refer to the following NGSI-LD Entities:
{
"id": "urn:ngsi-ld:WeatherStation:123",
"type": "WeatherStation",
"sensor": {
"type": "Relationship",
"objectType": "Device",
"object": "urn:ngsi-ld:Device:345"

} } {

"id": "urn:ngsi-ld:Device:345",
"type": "Device",
"humidity": {
"type": "Property",


"value": 40

} }

As not knowing the Entity Type targeted by a Relationship could make the query significantly more expensive, a hint
for the required Entity Type can be provided, so only such NGSI-LD Entities need to be considered.
EXAMPLE 14: ?q=sensor{Device:humidity}==40. The trailing path is
sensor{Device:humidity}. The query targets entities with a sensor.entityType =
"Device" within a Relationship and then makes a sub-query on matching target objects which
have the matching humidity Attribute. The entityType hint results in a faster lookup. Refer to the
following NGSI-LD Entities.
{
"id": "urn:ngsi-ld:WeatherStation:123",
"type": "WeatherStation",
"sensor": {
"type": "Relationship",
"objectType": "Device",
"object": "urn:ngsi-ld:Device:345"

} } {

"id": "urn:ngsi-ld:Device:345",
"type": "Device",
"humidity": {
"type": "Property",
"value": 40

} }

If the target element corresponds to a Relationship or ListRelationship, the combination of such target element with any
operator different than equal or unequal shall result in not matching.
A Query Term value shall be any of the following (depending on the operator used):
- A literal value (string, number, date, etc.) (production rule named Value).
- A range of values (production rule named Range), specified as a minimum and a maximum value.
- A regular expression (production rule named RegExp).
- A URI (production rule named URI).
- A comma-separated list of literal values (production rule named ValueList). When comparing dates or times, the order relation considered shall be a temporal one. When it comes to comparing text strings, implementations:
- shall follow the recommendations defined by IETF RFC 8259 [6], section 8.3.
- should support the Unicode Collation Algorithm (UCA), as defined by [13]. URI comparison should be performed so that the number of false negatives is minimized, as recommended by IETF RFC 3986 [5], section 6. The semantics of the different logical operators used by Query Terms are described as follows and shall be supported by compliant implementations:
- Existence (only attribute is specified). A matching entity shall contain the target element.


- Equal operator (production rule named equal). A matching Entity shall contain the target element and meet any of the following conditions: - The Query Term value, e.g. color=="red": - Is identical or equivalent to the target value (e.g. matches "red"). - Is included in the target value, if the latter is an array (e.g. matches ["blue","red","green"]). - If the Query Term value is a list of values (production rule named ValueList), e.g. color=="black", "red": - The target value is identical or equivalent to any of the list values (e.g. matches "red"). - The target value includes any of the Query Term values, if the target value is an array (e.g. matches ["red","blue"]). - If the Query Term value is a range (production rule named Range), e.g. temperature==10..20: - The target value is in the interval between the minimum and maximum of the range (both included) (e.g. matches 15). - The Query Term value target element corresponds to a LanguageProperty and a natural language is specified e.g. color[en]=="red": - a match is found as the value of the key-value pair corresponding to the specified natural language of the languageMap (e.g. matches {"fr": "rouge", "en": "red","de": "rot"} but not {"fr": "red", "en": "black","de": "blue"}). - a match is found as a single element from the array of values of the key-value pair corresponding to the specified natural language of the languageMap (e.g. matches {"fr": ["chat", "rouge"], "en": ["red", "cat], "de": ["rote", "Katze"]} but not {"fr": ["chat", "rouge"], "en" : ["coal", "black"],"de": ["blaue", "Engel"]}). - The Query Term value target element corresponds to a LanguageProperty and no natural language is specified e.g. color[*]=="red": - any match is found in the values of the key-value pairs of the languageMap (e.g. matches {"fr": "rouge", "en": "red", "de": "rote"}. - a match is found as a single element of the array of values of the key-value pairs of the languageMap (e.g. matches {"fr": "chat", "rouge"], "en": ["red", "cat"], "de": ["rote", "Katze"]}). - The Query Term value is a URI and the target element corresponds to a Relationship, a ListRelationship or a VocabProperty, e.g. color=="http://example/red": - Is identical to the target value (e.g. matches "http://example.com/red"). - Is included in the target value, if the latter is an array (e.g. matches ["http://example.com/blue"," http://example.com/red"," http://example.com/green"]). - If the Query Term value target element corresponds to a Relationship, a ListRelationship or a VocabProperty and is a list of URIs (production rule named ValueList), e.g. color==" http://example/black","http://example/red": - The target value is identical to any of the list values (e.g. matches "http://example.com/red"). - The target value includes any of the Query Term values, if the target value is an array (e.g. matches ["http://example.com/red", "http://example.com/blue"]).


- If there is no equality between the target value data type and the Query Term value data type, then it shall be considered as not matching.

- Unequal operator (production rule named unequal). A matching entity shall contain the target element and meet any of the following conditions: - The Query Term value, e.g. color!="red": - Is neither identical nor equivalent to the target value (e.g. matches "black"). - Is not included in the target value, if the latter is an array (e.g. matches ["blue","black","green"], but not ["blue","red","green"]). - If the Query Term value is a list of values (production rule named ValueList), e.g. color!= "black", "red": - The target value is neither identical nor equivalent to any of the list values (e.g. matches "blue"). - The target value does not include any of the list values, if the target value is an array (e.g. matches ["blue","yellow","green"], but not ["blue","red","green"]). - If the Query Term value is a range (production rule named Range), e.g. temperature!=10..20: - The target value is not in the interval between the minimum and the maximum (both included) (e.g. matches 9). - The Query Term value target element corresponds to a LanguageProperty and a natural language is specified e.g. color[en]!="red": - No matching value is found as the value of the specified language key of a languageMap where a language filter is specified. (e.g. matches {"fr": "noir", "en": "black","de": "schwarz"} but not {"fr": "rouge", "en" : "red","de": "rot"}). - No matching value is found as a single element from the array of values of the key-value pair corresponding to the specified natural language of the languageMap (e.g. matches {"fr": ["chat", "rouge"], "en": ["coal", "black"], "de": ["blaue", "Engel"]} but not {"fr": ["rouge", "noir"], "en" : ["red", "black"],"de": ["rot", "schwarz"]}). - The Query Term value target element corresponds to a LanguageProperty and no language filter is specified e.g. color[*]!="red": - No matching value is found in any of the values of the key-value pairs of a languageMap (e.g. matches {"fr": "noir", "en": "black","de": "schwarz"}, but not {"fr": "rouge", "en": "red","de": "rot"}). - No matching value is found as a single element from the array of values of the key-value pair corresponding to the specified natural language of the languageMap (e.g. matches {"fr": ["chat", "rouge"], "en": ["coal", "black"], "de": ["blaue", "Engel"]} but not {"fr": ["rouge", "noir"], "en": ["red", "black"],"de": ["rot", "schwartz"]}). - The Query Term value is a URI and the target element corresponds to a Relationship, a ListRelationship or a VocabProperty, e.g. color!="http://example.com/red": - Is not identical to the target value (e.g. matches "http://example.com/black"). - Is not included in the target value, if the latter is an array (e.g. matches ["http://example.com/blue", "http://example.com/black", "http://example.com/green"], but not ["http://example.com/blue", "http://example.com/red", "http://example.com/green"]).


- If the Query Term value target element corresponds to a Relationship, a ListRelationship or a
VocabProperty and is a list of URIs e.g. color!="http://example.com/black", "
http://example.com/red":
- The target value is not identical to any of the list values (e.g. matches
"http://example.com/blue").
- The target value does not include any of the list values, if the target value is an array (e.g. matches
["http://example.com/blue", "http://example.com/yellow",
"http://example.com/green"], but not ["http://example.com/blue",
"http://example.com/red", "http://example.com/green"]).
- If the data type of the target value and the data type of the Query Term value are different, then they shall
be considered unequal.
- Greater than operator (production rule named greater). For an entity to match, it shall contain the target element and the target value has to be strictly greater than the Query Term value: - If there is no equality between the target value data type and the Query Term value data type then it shall be considered as not matching.
- Less than operator (production rule named less). For an entity to match, it shall contain the target element and the target value shall be strictly less than the value: - If there is no equality between the target value data type and the Query Term value data type then it shall be considered as not matching.
- Greater or equal than (production rule named greaterEq). A matching entity shall meet any of the Greater than or the Equal conditions for single values.
- Less or equal than (production rule named lessEq). A matching entity shall meet any of the Less than or the Equal conditions for single values.
- Match pattern (production rule named patternOp). A matching entity shall contain the target element and the target value shall be in the L(R) of the regular pattern specified by the Query Term: - If the target value data type is different than String then it shall be considered as not matching.
- Do not match pattern (production rule named notPatternOp). A matching entity shall contain the target element and the target value shall not be in the L(R) of the regular pattern specified by the Query Term: - If the target value data type is different than String then it shall be considered as not matching.

# Related

* [Query Entities](/api-operations/consumption/query-entities.md)
* [Geoquery Language](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Query Language
