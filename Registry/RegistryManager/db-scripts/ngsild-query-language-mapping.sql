\pset pager 0
-- \set ECHO queries

/*

Section 5.10

General rule 1) If present, the entity specification in the query consisting of a combination of entity type and 
entity id/entity id pattern matches an EntityInfo specified in a RegistrationInfo of the information property in a 
context source registration. ***If there is no EntityInfo specified in the RegistrationInfo, the entity specification 
is considered matching.*** 

General rule 2) If present, at least one Attribute name specified in the query matches one Property or Relationship in 
the RegistrationInfo element of the information property in a context source registration. ***If no Properties or Relationships 
are specified in the RegistrationInfo, the Attribute names are considered matching.***


Section 5.12
An Entity specification consisting of Entity Types, Entity identifiers and id pattern matches an EntityInfo element 
if one of the specified Entity Types matches the entity type in the EntityInfo element and one of the following conditions holds 
(only in case an id or idPattern is specified in the query [included after a discussion with Martin in Feb. 2019]):
a) The EntityInfo contains neither an id nor an idPattern.
b) One of the specified entity identifiers matches the id in the EntityInfo.
c) At least one of the specified entity identifiers matches the idPattern in the EntityInfo.
d) The specified id pattern matches the id in the EntityInfo.
e) Both a specified id pattern and an idPattern in the Entity Info are present (since in the general case it is not easily feasible to determine if there can be identifiers matching both patterns).

Attribute names match the combination of Properties and Relationships if one of the following conditions hold:
f) No Attribute names have been specified (as this means all Attributes are requested).
g) The combination of Properties and Relationships is empty (as this means only Entities have been registered and the Context Sources may have matching Property or Relationship instances).
h) If at least one of the specified attribute names matches a Property or Relationship specified in the RegistrationInfo.
*/

\echo ****** Filters by TYPE + (id/idPattern)

\echo *** Case 1)
\echo type=http://example.org/vehicle/Vehicle
\echo General rule 1

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (c.has_registrationinfo_with_attrs_only) OR
       (ci.entity_type = 'http://example.org/vehicle/Vehicle');

\echo type=http://example.org/vehicle/Vehicle,http://example.org/bus/Bus
SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (c.has_registrationinfo_with_attrs_only) OR
       (ci.entity_type IN ('http://example.org/vehicle/Vehicle', 'http://example.org/bus/Bus'));


\echo *** Case 2)
\echo type=http://example.org/vehicle/Vehicle&id=urn:ngsi-ld:Vehicle:A456
\echo General rule 1
\echo a) The EntityInfo contains neither an id nor an idPattern
\echo b) One of the specified entity identifiers matches the id in the EntityInfo.
\echo c) At least one of the specified entity identifiers matches the idPattern in the EntityInfo.

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (c.has_registrationinfo_with_attrs_only) OR
       (
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         ci.entity_id IS NULL AND 
         ci.entity_idpattern IS NULL) OR
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         (ci.entity_id = 'urn:ngsi-ld:Vehicle:A456' OR
          'urn:ngsi-ld:Vehicle:A456' ~ ci.entity_idPattern)
        )
       );

\echo type=http://example.org/vehicle/Vehicle&id=urn:ngsi-ld:Vehicle:A789
SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (c.has_registrationinfo_with_attrs_only) OR
       (
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         ci.entity_id IS NULL AND 
         ci.entity_idpattern IS NULL) OR
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         (ci.entity_id = 'urn:ngsi-ld:Vehicle:A789' OR
         'urn:ngsi-ld:Vehicle:A789' ~ ci.entity_idPattern)
        )
       );

\echo type=http://example.org/vehicle/Vehicle&id=urn:ngsi-ld:Vehicle:A456,urn:ngsi-ld:Vehicle:A789
SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (c.has_registrationinfo_with_attrs_only) OR
       (
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         ci.entity_id IS NULL AND 
         ci.entity_idpattern IS NULL) OR
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         (ci.entity_id IN ('urn:ngsi-ld:Vehicle:A456', 'urn:ngsi-ld:Vehicle:A789') OR
                            ('urn:ngsi-ld:Vehicle:A456' ~ ci.entity_idPattern OR 
                             'urn:ngsi-ld:Vehicle:A789' ~ ci.entity_idPattern)
         )
        )
       );


\echo *** Case 3)
\echo type=http://example.org/vehicle/Vehicle&idPattern=urn:ngsi-ld:Vehi.*
\echo General rule 1
\echo a) The EntityInfo contains neither an id nor an idPattern
\echo d) The specified id pattern matches the id in the EntityInfo.
\echo e) Both a specified id pattern and an idPattern in the Entity Info are present (since in the general case it is not easily feasible to determine if there can be identifiers matching both patterns).

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (c.has_registrationinfo_with_attrs_only) OR
       (
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         ci.entity_id IS NULL AND 
         ci.entity_idpattern IS NULL) OR
        (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
         (ci.entity_id ~ 'urn:ngsi-ld:Vehi.*' OR 
         ci.entity_idpattern ~ 'urn:ngsi-ld:Vehi.*')
        )
       );

\echo *** Case 4)
\echo type=http://example.org/vehicle/Vehicle&id=urn:ngsi-ld:Vehicle:A456&idPattern=urn:ngsi-ld:Vehi.*

\echo on this case, idPattern must be ignored. spec says id takes precedence over idPattern. 
\echo thus, this case is the same as case 2.


\echo ****** Filters by ATTRIBUTE


\echo *** Case 5)
\echo attrs=http://example.org/vehicle/brandName
\echo General rule 2
\echo h) If at least one of the specified attribute names matches a Property or Relationshiop in the RegistrationInfo

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE (ci.property_id = 'http://example.org/vehicle/brandName' OR
       ci.relationship_id = 'http://example.org/vehicle/brandName');

/* *** We had an internal discussion and decided to not match RegistrationInfos with EntityInfo only: 
SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE (c.has_registrationinfo_with_entityinfo_only) OR 
      (ci.property_id = 'http://example.org/vehicle/brandName' OR
       ci.relationship_id = 'http://example.org/vehicle/brandName');*/

\echo attrs=http://example.org/vehicle/brandName,http://example.org/common/isParked
SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE (ci.property_id IN ('http://example.org/vehicle/brandName', 'http://example.org/common/isParked') OR
       ci.relationship_id IN ('http://example.org/vehicle/brandName', 'http://example.org/common/isParked'));


\echo ****** Filters by TYPE + (id/idPattern) + ATTRIBUTE

\echo *** Case 6)
\echo type=http://example.org/vehicle/Vehicle&attrs=http://example.org/vehicle/brandName
\echo General rule 1
\echo General rule 2
\echo g) The combination of Properties and Relationships is empty
\echo h) If at least one of the specified attribute names matches a Property or Relationshiop in the RegistrationInfo

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (ci.entity_type = 'http://example.org/vehicle/Vehicle') 

       AND

       (
        NOT EXISTS (SELECT 1 FROM csourceinformation ci2 
                     WHERE ci2.group_id = ci.group_id AND 
                     (ci2.property_id IS NOT NULL OR ci2.relationship_id IS NOT NULL)) 
        OR 
        EXISTS (SELECT 1 FROM csourceinformation ci3 
                     WHERE ci3.group_id = ci.group_id AND 
                            (ci3.property_id = 'http://example.org/vehicle/brandName' OR
                            ci3.relationship_id = 'http://example.org/vehicle/brandName'))
       );


\echo *** Case 7)
\echo type=http://example.org/vehicle/Vehicle&id=urn:ngsi-ld:Vehicle:A456&attrs=http://example.org/vehicle/brandName
\echo General rule 1
\echo General rule 2
\echo a) The EntityInfo contains neither an id nor an idPattern
\echo b) One of the specified entity identifiers matches the id in the EntityInfo.
\echo c) At least one of the specified entity identifiers matches the idPattern in the EntityInfo.
\echo g) The combination of Properties and Relationships is empty
\echo h) If at least one of the specified attribute names matches a Property or Relationshiop in the RegistrationInfo

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (
         (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
          ci.entity_id IS NULL AND 
          ci.entity_idpattern IS NULL) OR
         (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
          (ci.entity_id = 'urn:ngsi-ld:Vehicle:A456' OR
           'urn:ngsi-ld:Vehicle:A456' ~ ci.entity_idPattern)
         )
       )

       AND

       (
        NOT EXISTS (SELECT 1 FROM csourceinformation ci2 
                     WHERE ci2.group_id = ci.group_id AND 
                     (ci2.property_id IS NOT NULL OR ci2.relationship_id IS NOT NULL)) 
        OR 
        EXISTS (SELECT 1 FROM csourceinformation ci3 
                     WHERE ci3.group_id = ci.group_id AND 
                            (ci3.property_id = 'http://example.org/vehicle/brandName' OR
                            ci3.relationship_id = 'http://example.org/vehicle/brandName'))
       );


\echo *** Case 8)
\echo type=http://example.org/vehicle/Vehicle&idPattern=urn:ngsi-ld:Vehi.*&attrs=http://example.org/vehicle/brandName
\echo General rule 1
\echo General rule 2
\echo a) The EntityInfo contains neither an id nor an idPattern
\echo d) The specified id pattern matches the id in the EntityInfo.
\echo e) Both a specified id pattern and an idPattern in the Entity Info are present (since in the general case it is not easily feasible to determine if there can be identifiers matching both patterns).
\echo g) The combination of Properties and Relationships is empty
\echo h) If at least one of the specified attribute names matches a Property or Relationshiop in the RegistrationInfo

SELECT DISTINCT c.id FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE  (
         (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
          ci.entity_id IS NULL AND 
          ci.entity_idpattern IS NULL) OR
         (ci.entity_type = 'http://example.org/vehicle/Vehicle' AND
          (ci.entity_id ~ 'urn:ngsi-ld:Vehi.*' OR 
          ci.entity_idpattern ~ 'urn:ngsi-ld:Vehi.*')
         )
       )

       AND

       (
        NOT EXISTS (SELECT 1 FROM csourceinformation ci2 
                     WHERE ci2.group_id = ci.group_id AND 
                     (ci2.property_id IS NOT NULL OR ci2.relationship_id IS NOT NULL)) 
        OR 
        EXISTS (SELECT 1 FROM csourceinformation ci3 
                     WHERE ci3.group_id = ci.group_id AND 
                            (ci3.property_id = 'http://example.org/vehicle/brandName' OR
                            ci3.relationship_id = 'http://example.org/vehicle/brandName'))
       );
