begin;

truncate temporalentity cascade; 

/* 
If the NGSI-LD endpoint already knows about this Temporal Representation of an Entity, 
because there is an existing Temporal Representation of an Entity whose id (URI) is equivalent, 
then all the Attribute instances included by the Temporal Representation shall be added to the 
existing Entity as mandated by clause 5.6.12.

The Attribute (considering term expansion rules as mandated by clause 5.5.7) instance(s) shall be added to the target Entity. 
For the avoidance of doubt, if no previous Attribute instances existed, then a new Attribute instance collection shall be 
created and added to the Entity.
*/

-- DML to create "vehicle-context-expanded.jsonld"

insert into temporalentity (id, type, createdat, modifiedat) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/Vehicle',
'2018-08-01T12:03:00Z',
'2018-08-01T12:03:00Z');

-- static without instanceId
insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/brandName',
'
    {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "Volvo"
        }
      ]
    }
  ');

-- static with instanceId
insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/color',
'
   {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:91afea8c-23d9-4d6d-9a35-798c28a9db79"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-06-01T12:03:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "Red"
        }
      ]
    }
  ');  

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/speed',
'
     {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:be664aaf-a7af-4a99-bebc-e89528238abf"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-06-01T12:03:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 120
        }
      ]
    }
');

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/speed',
'
   {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:d3ac28df-977f-4151-a432-dc088f7400d7"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:05:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 80
        }
      ]
    }
');

-- this instance has sub-properties!
insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/speed',
'
      {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:70ac695b-52a3-4dde-8d29-d2d5a2b662f7"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:07:00Z"
        }
      ],
      "http://example.org/vehicle/speedAccuracy": [
        {
          "https://uri.etsi.org/ngsi-ld/createdAt": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-08-01T12:03:00Z"
            }
          ],
          "https://uri.etsi.org/ngsi-ld/instanceId": [
            {
              "@id": "urn:ngsi-ld:c6db2da4-c9a3-41da-83c1-a9e05c7ebc9c"
            }
          ],
          "https://uri.etsi.org/ngsi-ld/modifiedAt": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-08-01T12:03:00Z"
            }
          ],
          "https://uri.etsi.org/ngsi-ld/observedAt": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-08-01T12:07:01Z"
            }
          ],
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Property"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "@value": 5
            }
          ]
        },
        {
          "https://uri.etsi.org/ngsi-ld/createdAt": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-08-01T12:03:00Z"
            }
          ],
          "https://uri.etsi.org/ngsi-ld/instanceId": [
            {
              "@id": "urn:ngsi-ld:a6158a85-95e8-4cb2-aadd-bf4b79250884"
            }
          ],
          "https://uri.etsi.org/ngsi-ld/modifiedAt": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-08-01T12:03:00Z"
            }
          ],
          "https://uri.etsi.org/ngsi-ld/observedAt": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-08-01T12:07:02Z"
            }
          ],
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Property"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "@value": 7
            }
          ]
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 100
        }
      ]
    }
');

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'https://uri.etsi.org/ngsi-ld/location',
'    {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:cfade4cb-7c71-4135-b69c-24ab83e2afae"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-07-01T12:03:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "{ \"type\":\"Point\", \"coordinates\":[ -8.5, 41.2 ] }"
        }
      ]
    }'
);

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'https://uri.etsi.org/ngsi-ld/location',
'       {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:37192165-11d9-48fa-9952-b6aab55e5046"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-07-01T12:05:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "{ \"type\":\"Point\", \"coordinates\":[ -8.5, 42.2 ] }"
        }
      ]
    }
');

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'https://uri.etsi.org/ngsi-ld/location',
'
     {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:0f978241-7d03-4fa9-96e4-094c2c467395"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:07:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "{ \"type\":\"Point\", \"coordinates\":[ -8.5, 43.2 ] }"
        }
      ]
    }
');

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/temperature',
'
   {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "http://example.org/vehicle/testedAt": [
        {
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Property"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2018-12-04T12:00:00Z"
            }
          ]
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 10
        }
      ]
    }
');


insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/temperature',
'
   {
      "https://uri.etsi.org/ngsi-ld/createdAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/modifiedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T12:03:00Z"
        }
      ],
      "http://example.org/vehicle/testedAt": [
        {
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Property"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
              "@value": "2019-12-04T12:00:00Z"
            }
          ]
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 15
        }
      ]
    }
');

\echo PATCH instance operation
\echo PATCH http://192.168.56.101:9090/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Vehicle:B9211/attrs/speed/urn:ngsi-ld:95bea784-4539-4212-a7ba-fb4973870b19

insert into temporalentityattrinstance (temporalentity_id, attributeid, data) values (
'urn:ngsi-ld:Vehicle:B9211',
'http://example.org/vehicle/speed',
'
    {
      "https://uri.etsi.org/ngsi-ld/instanceId": [
        {
          "@id": "urn:ngsi-ld:d3ac28df-977f-4151-a432-dc088f7400d7"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2018-08-01T17:45:00Z"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 82
        }
      ]
    }
') ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data;

commit;

begin;

\echo DELETE http://192.168.56.101:9090/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Vehicle:B9211/attrs/speed
delete from temporalentityattrinstance where temporalentity_id = 'urn:ngsi-ld:Vehicle:B9211' and attributeid = 'http://example.org/vehicle/speed';

rollback; -- just testing

