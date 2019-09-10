-- TODO: dateTime / date / time

\pset pager 0
\set ECHO queries

/*
Query = (QueryTerm / QueryTermAssoc) *(logicalOp (QueryTerm / QueryTermAssoc))
QueryTermAssoc = %x28 QueryTerm *(logicalOp QueryTerm) %x29 ; (QueryTerm)
QueryTerm = Attribute
QueryTerm = Attribute Operator ComparableValue
QueryTerm =/ Attribute equal CompEqualityValue
QueryTerm =/ Attribute unequal CompEqualityValue
QueryTerm =/ Attribute patternOp RegExp
QueryTerm =/ Attribute notPatternOp RegExp
Attribute = attrName / compoundAttrName / attrPathName
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
attrNameChar =/ DIGIT / ALPHA
attrNameChar =/ %x5F ; _
attrName = 1*attrNameChar
attrPathName = attrName *(%x2E attrName) ; attrName *(. attrName)
compoundAttrName = attrName *(%x5B (attrName) %x5D) ; . attrName *([ attrName ])
quotedStr = String ; '*char'
andOp = %x3B ; ;
orOp = %x7C ; |
logicalOp = andOp / orOp
*/

--tests
\x
select id, 
  data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as number_a,
  data#>>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as number_b, 
  jsonb_typeof(data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as number_c,
  data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as string_a, 
  data#>>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as string_b, 
  jsonb_typeof(data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as string_c, 
  data#>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as other_a,     
  data#>>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as other_b, 
  jsonb_typeof(data#>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as other_c, 
  data#>'{http://example.org/trueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as true_a,     
  data#>>'{http://example.org/trueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as true_b, 
  jsonb_typeof(data#>'{http://example.org/trueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as true_c, 
  data#>'{http://example.org/falseExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as false_a,
  data#>>'{http://example.org/falseExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as false_b,
  jsonb_typeof(data#>'{http://example.org/falseExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as false_c, 
  data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as uri_a,     
  data#>>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as uri_b,
  jsonb_typeof(data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as uri_c,

  data#>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as datetime_a, 
  data#>>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as datetime_b, 
  jsonb_typeof(data#>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as datetime_c, 

  data#>'{http://example.org/observedAtDateTimeExample,0,https://uri.etsi.org/ngsi-ld/observedAt,0,@value}' as datetimewithobservedat_a, 
  data#>>'{http://example.org/observedAtDateTimeExample,0,https://uri.etsi.org/ngsi-ld/observedAt,0,@value}' as datetimewithobservedat_b, 
  jsonb_typeof(data#>'{http://example.org/observedAtDateTimeExample,0,https://uri.etsi.org/ngsi-ld/observedAt,0,@value}') as datetimewithobservedat_c, 

  data#>'{http://example.org/dateExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as date_a, 
  data#>>'{http://example.org/dateExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as date_b, 
  jsonb_typeof(data#>'{http://example.org/dateExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as date_c, 

  data#>'{http://example.org/timeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as time_a, 
  data#>>'{http://example.org/timeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as time_b, 
  jsonb_typeof(data#>'{http://example.org/timeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') as time_c, 

  data#>'{http://example.org/objectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}' as object_a,     
  data#>>'{http://example.org/objectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}' as object_b,
  jsonb_typeof(data#>'{http://example.org/objectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') as object_c,

  data#>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}' as multilevelobject_a,     
  data#>>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}' as multilevelobject_b,
  jsonb_typeof(data#>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') as multilevelobject_c,

  data#>'{http://example.org/relationshipExample,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' as relationship_a,
  data#>>'{http://example.org/relationshipExample,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' as relationship_b,
  jsonb_typeof(data#>'{http://example.org/relationshipExample,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}') as relationship_c,

  data#>'{http://example.org/arrayExample1,0,https://uri.etsi.org/ngsi-ld/hasValue}' as array1_a,     
  data#>>'{http://example.org/arrayExample1,0,https://uri.etsi.org/ngsi-ld/hasValue}' as array1_b,
  jsonb_typeof(data#>'{http://example.org/arrayExample1,0,https://uri.etsi.org/ngsi-ld/hasValue}') as array1_c,

  data#>'{http://example.org/arrayExample2,0,https://uri.etsi.org/ngsi-ld/hasValue}' as array2_a,     
  data#>>'{http://example.org/arrayExample2,0,https://uri.etsi.org/ngsi-ld/hasValue}' as array2_b,
  jsonb_typeof(data#>'{http://example.org/arrayExample2,0,https://uri.etsi.org/ngsi-ld/hasValue}') as array2_c,

  data#>'{http://example.org/topLevelExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as toplevel_a,
  data#>>'{http://example.org/topLevelExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as toplevel_b,
  jsonb_typeof(data#>'{http://example.org/topLevelExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')  as toplevel_c,
  
  data#>'{http://example.org/topLevelExample,0,http://example.org/subPropertyExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as subprop_a,
  data#>>'{http://example.org/topLevelExample,0,http://example.org/subPropertyExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as subprop_b,
  jsonb_typeof(data#>'{http://example.org/topLevelExample,0,http://example.org/subPropertyExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')  as subprop_c

from entity 
 where id = 'urn:ngsi-ld:Test:all_datatypes' ;
\x

-- common validation: 

-- If the target element is a Property, the target value is defined as the Value associated to such Property. If a Property has
-- multiple instances (identified by its respective datasetId), and no datasetId is explicitly addressed, the target value shall
-- be any Value of such instances.

-- If the target element is a Relationship, the target object is defined as the object associated (represented as a URI) to
-- such Relationship. If a Relationship has multiple instances (identified by its respective datasetId), and no datasetId is
-- explicitly addressed, the target object shall be any object of such instances

-- If the target element corresponds to a Relationship, the combination of such target element with any operator different
-- than equal or unequal shall result in not matching.


-- This dynamic function approach DOES NOT WORK! Solution: UNION ALL or OR (see below)
-- CREATE OR REPLACE FUNCTION f_get_attr_field(text) RETURNS text
--     AS 'select case when $1 in (''Property'', ''GeoProperty'') then ''value'' 
--                   when $1 = ''Relationship'' then ''object''
--                   else null end '
--     LANGUAGE SQL
--     IMMUTABLE
--     RETURNS NULL ON NULL INPUT;

-- q=attr1==urn:ngsi-ld:test
-- select data
--   from entity   
--   where data#>>'{attr1,' || 'test' || '}' = 'urn:ngsi-ld:test';   !!! DID NOT WORK! jsonb operators do not support concatenation in the path
--   where data#>>'{attr1,' || f_get_attr_field(data#>>'{attr1,type}') || '}' = 'urn:ngsi-ld:test'; !!! DID NOT WORK! 

\echo Testing ngsi-ld types
select id, 
      data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr1_value, 
      data#>>'{http://example.org/attr2,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr2_value,
      data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' as attr1_object, 
      data#>>'{http://example.org/attr2,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' as attr2_object
  from entity
  where id like 'urn:ngsi-ld:Test:entity%';

\echo q=attr1==urn:ngsi-ld:test
select data
  from entity   
  where (data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
         data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = 'urn:ngsi-ld:test') OR 
        (data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Relationship"]}]}' and
         data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' = 'urn:ngsi-ld:test');
\echo solution using union, same result
select data
  from entity   
  where data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = 'urn:ngsi-ld:test'
union 
select data
  from entity   
  where data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Relationship"]}]}' and
        data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' = 'urn:ngsi-ld:test';

\echo q=attr1!=urn:ngsi-ld:testx
select data
  from entity   
  where data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' <> 'urn:ngsi-ld:testx'
union 
select data
  from entity   
  where data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Relationship"]}]}' and 
        data#>>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' <> 'urn:ngsi-ld:testx';

\echo q=attr2!=urn:ngsi-ld:testx
select data
  from entity   
  where data@>'{"http://example.org/attr2":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>>'{http://example.org/attr2,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' <> 'urn:ngsi-ld:testx'
union 
select data
  from entity   
  where data@>'{"http://example.org/attr2":[{"@type":["https://uri.etsi.org/ngsi-ld/Relationship"]}]}' and 
        data#>>'{http://example.org/attr2,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' <> 'urn:ngsi-ld:testx';

\echo q=attr1>7
select data
  from entity 
  where data@>'{"http://example.org/attr1":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/attr1,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' > '7'::jsonb;
-- as the operator is neither equal nor unequal, "union all + extra sql for object" is not required here

-- attrPathName
\echo q=topLevelExample.subPropertyExample>4
select id, data#>'{http://example.org/topLevelExample}'
  from entity   
  where data@>'{"http://example.org/topLevelExample":[{"http://example.org/subPropertyExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}]}' and 
        data#>'{http://example.org/topLevelExample,0,http://example.org/subPropertyExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' > '4'::jsonb;


-- compoundAttrName
\echo q=objectExample[postalCode]==42000
/*
Lastly, implementations shall support queries involving specific data subitems belonging to a Property Value (seed
target value) represented by a JSON object structure (complex value). For that purpose, an attribute path may contain a
trailing path (production rule named compoundAttrName) composed of a concatenated list of JSON member names,
each one enclosed in between square brackets, and intended to address a specific data subitem (member) within the seed
target value. When such a trailing path is present, implementations shall interpret and evaluate it (against the seed
target value) as a MemberExpression of Ecma 262 [21]. If the evaluation of such MemberExpression does not result in a
defined value, the target element shall be considered as non-existent for the purpose of query resolution.
EXAMPLE 9: address[addressLocality]== "Berlin". The trailing path is [addressLocality] and is used to refer to a
particular subitem within a Postal Address.

data#>'{http://example.org/objectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}'
  {
    "https://example.org/postalCode": [
      {
        "@value": "42000"
      }
    ],
    "https://example.org/addressRegion": [
      {
        "@value": "Metropolis"
      }
    ],
    ...
  }
*/
select data
  from entity 
  where data@>'{"http://example.org/objectExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/objectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,https://example.org/postalCode,0,@value}' = '42000'::jsonb;

\echo q=multiLevelObjectExample[streetAddress][streetName]=="Main Street"
/*
data#>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0}'
  {
    "https://example.org/streetAddress": [
      {
        "https://example.org/streetName": [
          {
            "@value": "Main Street"
          }
        ],
        "https://example.org/houseNumber": [
          {
            "@value": 65
          }
        ]
      }
    ]
  }
*/
select data
  from entity 
  where data@>'{"http://example.org/multiLevelObjectExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,https://example.org/streetAddress,0,https://example.org/streetName,0,@value}' = '"Main Street"'::jsonb;


-- case 1
-- QueryTerm = Attribute Operator ComparableValue

-- NOTE: we cannot use "#>>" (as text) for  basic operations, because great/less operators will not work with number types.
--       possible solution would be to typecast. is it worthy? (please consider the URI problem below)
-- example: data#>>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' > '99'  -- DOES NOT WORK!

\echo case 1.1
\echo Attribute = attrName
\echo Operator = equal / unequal / greater / greaterEq / less / lessEq 

\echo case 1.1.1
\echo ComparableValue = Number
\echo q=numberExample==100
select id, data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/numberExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = '100'::jsonb;
        --data@>'{"http://example.org/numberExample":[{"https://uri.etsi.org/ngsi-ld/hasValue":[{"@value":100}]}]}';

\echo q=numberExample>99        
select id, data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/numberExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' > '99'::jsonb;

\echo case 1.1.2
\echo ComparableValue = quotedStr
\echo q=stringExample=="Mercedes"       
select id, data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/stringExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = '"Mercedes"'::jsonb;
\echo q=stringExample!="Mercedes"
select id, data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/stringExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' <> '"Mercedes"'::jsonb;

-- spec, page 35: When comparing dates or times, the order relation considered shall be a temporal one.
\echo case 1.1.3
\echo ComparableValue = dateTime
\echo q=dateTimeExample>=2018-12-04T12:00:00.00000Z
select id, data#>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/dateTimeExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@type}' = 'https://uri.etsi.org/ngsi-ld/DateTime' and 
        --data@>'{"http://example.org/dateTimeExample":[{"https://uri.etsi.org/ngsi-ld/hasValue":[{"@type":"https://uri.etsi.org/ngsi-ld/DateTime"}]}]}' and
        --data#>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' >= '"2018-12-04T12:00:00.00001Z"'::jsonb;  -- wrong result, must cast
        (data#>>'{http://example.org/dateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::timestamp >= '2018-12-04T12:00:00.00000Z'::timestamp;

-- !!! createdAt/modifiedAt/observedAt do not have the "hasValue" element!
\echo q=observedAtDateTimeExample.observedAt==2018-12-04T12:00:00Z
select id, data#>'{http://example.org/observedAtDateTimeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/observedAtDateTimeExample":[{"https://uri.etsi.org/ngsi-ld/observedAt":[{"@type":"https://uri.etsi.org/ngsi-ld/DateTime"}]}]}' and
        -- data#>>'{http://example.org/observedAtDateTimeExample,0,https://uri.etsi.org/ngsi-ld/observedAt,0,@type}' = 'https://uri.etsi.org/ngsi-ld/DateTime' and
        (data#>>'{http://example.org/observedAtDateTimeExample,0,https://uri.etsi.org/ngsi-ld/observedAt,0,@value}')::timestamp >= '2018-12-04T12:00:00Z'::timestamp;

\echo case 1.1.4
\echo ComparableValue = date
\echo q=dateExample>=2018-12-04
select id, data#>'{http://example.org/dateExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/dateExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>>'{http://example.org/dateExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@type}' = 'https://uri.etsi.org/ngsi-ld/Date' and 
        (data#>>'{http://example.org/dateExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::date >= '2018-12-04'::date;

\echo case 1.1.5
\echo ComparableValue = time
\echo q=timeExample>=12:00:00.00000Z
select id, data#>'{http://example.org/timeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/timeExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>>'{http://example.org/timeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@type}' = 'https://uri.etsi.org/ngsi-ld/Time' and 
        (data#>>'{http://example.org/timeExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::time >= '12:00:00.00000Z'::time;


\echo case 2
\echo QueryTerm = Attribute equal CompEqualityValue
\echo QueryTerm = Attribute unequal CompEqualityValue

\echo case 2.1
\echo CompEqualityValue = OtherValue

\echo case 2.1.1
\echo OtherValue = false
\echo q=falseValue==false
select id, data#>'{http://example.org/falseExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/falseExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/falseExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = 'false'::jsonb;

\echo case 2.1.2
\echo OtherValue = true
\echo q=trueValue==true
select id, data#>'{http://example.org/trueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/trueExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/trueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = 'true'::jsonb;

\echo case 2.2
\echo CompEqualityValue = ValueList
\echo ValueList = Value 1*(%x2C Value)                                   ; Value 1*(, Value)
\echo Value = ComparableValue / OtherValue
\echo q=stringExample=="Mercedes",false,true
select id, data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/stringExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' in ('"Mercedes"'::jsonb, '100'::jsonb, 'false'::jsonb, 'true'::jsonb);
-- equal =  "in"
-- unequal =  "not in"

\echo case 2.2.1
\echo ComparableValue = Number
\echo q=numberExample==100,101
select id, data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/numberExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' in ('100'::jsonb, '101'::jsonb);

\echo case 2.2.2
\echo ComparableValue = quotedStr
\echo q=stringExample=="Mercedes","BMW"
select id, data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/stringExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' in ('"Mercedes"'::jsonb, '"BMW"'::jsonb);

\echo case 2.2.3
\echo OtherValue = false
\echo OtherValue = true
\echo q=otherValueExample==false,true
select id, data#>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/otherValueExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' in ('false'::jsonb, 'true'::jsonb);

\echo case 2.3
\echo CompEqualityValue = Range
\echo Range = ComparableValue dots ComparableValue
\echo dots = %x2E %x2E                                                    ; ..

-- equal =  "between"
-- unequal =  "not between"

\echo case 2.3.1
\echo ComparableValue = Number
\echo q=numberExample==99..102
select id, data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/numberExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' between '99'::jsonb and '102'::jsonb;

\echo case 2.3.2
\echo ComparableValue = quotedStr
\echo q=stringExample=="BMW".."Volkswagen"
select id, data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/stringExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' between '"BMW"'::jsonb and '"Volkswagen"'::jsonb;


\echo case 2.4
\echo CompEqualityValue = URI
\echo q=uriExample==http://www.example.com
\echo q=uriExample==urn:ngsi-ld:relationshipExample  (relationship object)

-- we need to detect beforehand whether the value is an URI or not, and then
--   either use the #>> operator or insert double quotes to the value (adopted solution)
select id, data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/uriExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        -- two alternatives:
        data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = '"http://www.example.com"'::jsonb;
        --data#>>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = 'http://www.example.com'
union all
select id, data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/uriExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Relationship"]}]}' and 
        -- two alternatives:
        data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' = '"http://www.example.com"'::jsonb;
        --data#>>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' = 'http://www.example.com';


\echo case 3
\echo QueryTerm = Attribute patternOp RegExp
\echo q=stringExample~=Mer.*
\echo q=uriExample~=http.*
\echo q=numberExample~=1.*
\echo q=otherValueExample~=tr.*
-- spec defines: "If the target value data type is different than String then it shall be considered as not matching" for patternOp.
-- always use "#>>" operator

select id, data#>>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and
        data@>'{"http://example.org/stringExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        jsonb_typeof(data#>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') = 'string' and 
        data#>>'{http://example.org/stringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ~ 'Mer.*';  

select id, data#>>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and
        data@>'{"http://example.org/uriExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        jsonb_typeof(data#>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') = 'string' and 
        data#>>'{http://example.org/uriExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ~ 'http.*';  

select id, data#>>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/numberExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        jsonb_typeof(data#>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') = 'string' and 
        data#>>'{http://example.org/numberExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ~ '1.*';

select id, data#>>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and 
        data@>'{"http://example.org/otherValueExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
        jsonb_typeof(data#>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') = 'string' and 
        data#>>'{http://example.org/otherValueExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ~ 'tr.*';

\echo mixing object and patternOp
\echo q=multiLevelObjectExample[streetAddress][streetName]~="Main.*"
select id, data from entity 
  where id = 'urn:ngsi-ld:Test:all_datatypes' and
        data@>'{"http://example.org/multiLevelObjectExample":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and         
        data#>>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,https://example.org/streetAddress,0,https://example.org/streetName,0,@value}' ~ 'Main.*' and 
        jsonb_typeof(data#>'{http://example.org/multiLevelObjectExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,https://example.org/streetAddress,0,https://example.org/streetName,0,@value}') = 'string';  
