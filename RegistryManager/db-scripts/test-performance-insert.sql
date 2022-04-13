\pset pager 0
\timing

\echo Cleaning benchmark data...
-- delete from csource where id like 'urn:ngsi-ld:ContextSourceRegistration:AP%';
truncate csource cascade;

\echo Inserting data...
-- source: csource-expanded.jsonld
insert into csource (id, data) 
select 
'urn:ngsi-ld:ContextSourceRegistration:AP' || i, 
(
'
{
  "@id": "urn:ngsi-ld:ContextSourceRegistration:AP' || i || '",
  "https://uri.etsi.org/ngsi-ld/name": [
    {
      "@value": "NameExample' || i* trunc(100*random()) || '"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/description": [
    {
      "@value": "DescriptionExample"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://my.csource.org:1026"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/expires": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
      "@value": "2030-11-29T14:53:15"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/information": [
    {
      "https://uri.etsi.org/ngsi-ld/entities": [
        {
          "@id": "urn:ngsi-ld:Vehicle:AP' || trunc(10000*random()) || '",
          "@type": [
            "http://example.org/vehicle/Vehicle"
          ]
        }
      ],
      "https://uri.etsi.org/ngsi-ld/properties": [
        {
          "@id": "https://json-ld.org/playground/brandName"
        },
        {
          "@id": "https://json-ld.org/playground/speed"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/relationships": [
        {
          "@id": "https://json-ld.org/playground/isParked"
        }
      ]
    },
    {
      "https://uri.etsi.org/ngsi-ld/entities": [
        {
          "https://uri.etsi.org/ngsi-ld/idPattern": [
            {
              "@value": ".*downtown$"
            }
          ],
          "@type": [
            "http://example.org/parking/OffStreetParking"
          ]
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@value": "{ \"type\": \"Polygon\", \"coordinates\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }"
    }
  ],
  "@type": [
    "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
  ]
}
'
)::jsonb
  FROM generate_series(1, 999995) i;

insert into csource (id, data) 
select 
'urn:ngsi-ld:ContextSourceRegistration:AP' || i, 
(
'
{
  "@id": "urn:ngsi-ld:ContextSourceRegistration:AP' || i || '",
  "https://uri.etsi.org/ngsi-ld/name": [
    {
      "@value": "NameExample' || i* trunc(100*random()) || '"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/description": [
    {
      "@value": "DescriptionExample"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://my.csource.org:1026"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/expires": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
      "@value": "2030-11-29T14:53:15"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/information": [
    {
      "https://uri.etsi.org/ngsi-ld/entities": [
        {
          "@id": "urn:ngsi-ld:Drone:AP' || trunc(10000*random()) || '",
          "@type": [
            "http://example.org/drone/Drone"
          ]
        }
      ],
      "https://uri.etsi.org/ngsi-ld/properties": [
        {
          "@id": "https://json-ld.org/playground/modelName"
        },
        {
          "@id": "https://json-ld.org/playground/speed"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/relationships": [
        {
          "@id": "https://json-ld.org/playground/isFlying"
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@value": "{ \"type\": \"Polygon\", \"coordinates\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }"
    }
  ],
  "@type": [
    "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
  ]
}
'
)::jsonb
  FROM generate_series(999996, 999999) i;

insert into csource (id, data) 
select 
'urn:ngsi-ld:ContextSourceRegistration:AP' || i, 
(
'
{
  "@id": "urn:ngsi-ld:ContextSourceRegistration:AP' || i || '",
  "https://uri.etsi.org/ngsi-ld/name": [
    {
      "@value": "NameExample' || i* trunc(100*random()) || '"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/description": [
    {
      "@value": "DescriptionExample"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://my.csource.org:1026"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/expires": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
      "@value": "2030-11-29T14:53:15"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/information": [
    {
      "https://uri.etsi.org/ngsi-ld/entities": [
        {
          "@type": [
            "http://example.org/bus/Bus"
          ]
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@value": "{ \"type\": \"Polygon\", \"coordinates\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }"
    }
  ],
  "@type": [
    "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
  ]
}
'
)::jsonb
  FROM generate_series(1000000, 1000999) i;