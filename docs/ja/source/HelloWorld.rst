****************
Hello World の例
****************

一般的に言えば、次のようなペイロードで HTTP POST リクエストを *http://localhost:9090/ngsi-ld/v1/entities/* に送信する
ことで、Scorpio Broker の Hello World プログラムのようなエンティティを作成できます。

.. code-block:: JSON

 curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/json' -d @-
 {
    "id": "urn:ngsi-ld:testunit:123",
    "type": "AirQualityObserved",
    "dateObserved": {
        "type": "Property",
        "value": {
            "@type": "DateTime",
            "@value": "2018-08-07T12:00:00Z"
        }
    },
    "NO2": {
        "type": "Property",
        "value": 22,
        "unitCode": "GP",
        "accuracy": {
            "type": "Property",
            "value": 0.95
        }
    },
    "refPointOfInterest": {
        "type": "Relationship",
        "object": "urn:ngsi-ld:PointOfInterest:RZ:MainSquare"
    },
    "@context": [
        "https://schema.lab.fiware.org/ld/context",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ]
 }

与えられた例では、@context がペイロードにあるため、Content-Type header を application/ld+json に設定する必要があります。

エンティティを受信するには、HTTP GET を送信します。

 **http://localhost:9090/ngsi-ld/v1/entities/<entityId>**
