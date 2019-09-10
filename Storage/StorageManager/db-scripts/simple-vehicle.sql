insert into Entity (id, data) values ('urn:ngsi-ld:Vehicle:V1-Heidelberg', 
'
{
    "@id": "urn:ngsi-ld:Vehicle:V1-Heidelberg",
    "@type": [
      "http://example.org/vehicle/Vehicle"
    ],
    "http://example.org/vehicle/speed": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": 90
          }
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "{ \"type\":\"Point\", \"coordinates\":[ 8.684783577919006, 49.406131991436396 ] }"
        }
      ]
    }
  ]
}
'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Vehicle:V2-Worms', 
'
{
    "@id": "urn:ngsi-ld:Vehicle:V2-Worms",
    "@type": [
      "http://example.org/vehicle/Vehicle"
    ],
    "http://example.org/vehicle/speed": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": 100
          }
        ]
      }
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "{ \"type\":\"Point\", \"coordinates\":[ 8.367462158203125, 49.62761437887251 ] }"
        }
      ]
    }
  ]
  }
'
);