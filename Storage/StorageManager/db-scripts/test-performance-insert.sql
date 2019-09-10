\pset pager 0
\timing



\echo Cleaning benchmark data...
-- delete from entity where id like 'urn:ngsi-ld:Vehicle:AP%';
truncate entity;

\echo Inserting data...
INSERT INTO entity (id, data)
  SELECT 
'urn:ngsi-ld:Vehicle:AP' || i,
('{
    "@id": "urn:ngsi-ld:Vehicle:AP' || i || '",
    "@type": [
      "http://example.org/vehicle/Vehicle"
    ],
    "http://example.org/vehicle/brandName": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "Mercedes"
          }
        ]
      }
    ],
    "http://example.org/common/isParked": [
      {
        "https://uri.etsi.org/ngsi-ld/hasObject": [
          {
            "@id": "urn:ngsi-ld:OffStreetParking:Downtown' || i* trunc(100*random()) || '"
          }
        ],
        "https://uri.etsi.org/ngsi-ld/observedAt": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
            "@value": "2017-07-29T12:00:04Z"
          }
        ],
        "http://example.org/common/providedBy": [
          {
            "https://uri.etsi.org/ngsi-ld/hasObject": [
              {
                "@id": "urn:ngsi-ld:Person:Bob' || i* trunc(1000*random()) || '"
              }
            ],
            "@type": [
              "https://uri.etsi.org/ngsi-ld/Relationship"
            ]
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Relationship"
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue":[
          {
            "@value":
              "{ \"type\":\"Point\", \"coordinates\":[ -' || round( (10 * random())::numeric , 1) || ', ' || round( (10 * random())::numeric , 1) || ' ] }"
          }
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/createdAt": [
      {
        "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
        "@value": "' || to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"
      }
    ],
    "https://uri.etsi.org/ngsi-ld/modifiedAt": [
      {
        "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
        "@value": "' || to_char(current_timestamp  + interval '2 hours', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"
      }
    ],
    "http://example.org/vehicle/speed": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": ' || trunc(100*random()) || '
          }
        ]
      }
    ]
  }')::jsonb
  FROM generate_series(1, 99994) i;


\echo Insert four specific records... (distinct speed and location)
INSERT INTO entity (id, data)
  SELECT 
'urn:ngsi-ld:Vehicle:AP' || i,
('{
    "@id": "urn:ngsi-ld:Vehicle:AP' || i || '",
    "@type": [
      "http://example.org/vehicle/Vehicle"
    ],
    "http://example.org/vehicle/brandName": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "Mercedes"
          }
        ]
      }
    ],
    "http://example.org/common/isParked": [
      {
        "https://uri.etsi.org/ngsi-ld/hasObject": [
          {
            "@id": "urn:ngsi-ld:OffStreetParking:Downtown' || i* trunc(1000*random()) || '"
          }
        ],
        "https://uri.etsi.org/ngsi-ld/observedAt": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
            "@value": "2017-07-29T12:00:04Z"
          }
        ],
        "http://example.org/common/providedBy": [
          {
            "https://uri.etsi.org/ngsi-ld/hasObject": [
              {
                "@id": "urn:ngsi-ld:Person:Bob' || i* trunc(1000*random()) || '"
              }
            ],
            "@type": [
              "https://uri.etsi.org/ngsi-ld/Relationship"
            ]
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Relationship"
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue":[
          {
            "@value":
              "{ \"type\":\"Point\", \"coordinates\":[ -' || round( random()::numeric , 1) || ', ' || round( random()::numeric , 1) || ' ] }"
          }
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/createdAt": [
      {
        "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
        "@value": "' || to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/modifiedAt": [
      {
        "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
        "@value": "' || to_char(current_timestamp  + interval '2 hours', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"
      }
    ],
    "http://example.org/vehicle/speed": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": ' || trunc(1000*random()) || '
          }
        ]
      }
    ]
  }')::jsonb
  FROM generate_series(99995, 99999) i;

\echo Inserting data...
INSERT INTO entity (id, data)
  SELECT 
'urn:ngsi-ld:Vehicle:AP' || i,
('{
    "@id": "urn:ngsi-ld:Vehicle:AP' || i || '",
    "@type": [
      "http://example.org/vehicle/Vehicle"
    ],
    "http://example.org/vehicle/brandName": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "Mercedes"
          }
        ]
      }
    ],
    "http://example.org/common/isParked": [
      {
        "https://uri.etsi.org/ngsi-ld/hasObject": [
          {
            "@id": "urn:ngsi-ld:OffStreetParking:Downtown' || i* trunc(100*random()) || '"
          }
        ],
        "https://uri.etsi.org/ngsi-ld/observedAt": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
            "@value": "2017-07-29T12:00:04Z"
          }
        ],
        "http://example.org/common/providedBy": [
          {
            "https://uri.etsi.org/ngsi-ld/hasObject": [
              {
                "@id": "urn:ngsi-ld:Person:Bob' || i* trunc(1000*random()) || '"
              }
            ],
            "@type": [
              "https://uri.etsi.org/ngsi-ld/Relationship"
            ]
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Relationship"
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue":[
          {
            "@value":
              "{ \"type\":\"Point\", \"coordinates\":[ -' || round( (10 * random())::numeric , 1) || ', ' || round( (10 * random())::numeric , 1) || ' ] }"
          }
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/createdAt": [
      {
        "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
        "@value": "' || to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"
      }
    ],
    "https://uri.etsi.org/ngsi-ld/modifiedAt": [
      {
        "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
        "@value": "' || to_char(current_timestamp  + interval '2 hours', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"
      }
    ],
    "http://example.org/vehicle/speed": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": ' || trunc(100*random()) || '
          }
        ]
      }
    ]
  }')::jsonb
  FROM generate_series(100000, 999999) i;
