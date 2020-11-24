***************************
MQTT ノーティフィケーション
***************************

MQTT は pub/sub ベースのメッセージバスであり、トピックを処理します。詳細については、https://mqtt.org/ をご覧ください。
NGSI-LD を使用すると、MQTT を介してノーティフィケーションを受信できます。HTTP 経由で受信したサブスクリプションは、
サブスクリプションの "notification.endpoint.uri" メンバーで MQTT エンドポイントを指定し、MQTT ノーティフィケーション
バインディングは NGSI-LD 実装でサポートされます。このサブスクリプションに関連するノーティフィケーションは、MQTT
プロトコル経由で送信されます。

MQTT エンドポイント URI の構文は **mqtt[s]://[<username>][:<password>]@<host>[:<port>]/<topic>[/<subtopic>]** であり、
MQTT エンドポイントを URI として表すための既存の規則に従います。

ユーザー名とパスワードは、エンドポイント URI の一部としてオプションで指定できます。ポートが明示的に指定されていない
場合、デフォルトの MQTT ポートは MQTT over TCP の場合は **1883** 、MQTTS の場合は **8883** です。MQTT プロトコルの場合、
現在サポートされているバージョンは **MQTTv3.1.1** と **MQTTv5.0** の2つです。

.. figure:: ../../en/source/figures/MQTT.jpg

MQTT を介したScorpio Broker のノーティフィケーションのフロー:-

1. TOPIC をサブスクライブします。

2. ノーティフィケーションを送信するための連絡先として MQTT サーバーの URI を使用して、NGSI-LD サブスクリプションを作成します。

3. URI から抽出されたトピックにノーティフィケーションを公開します。

4. MQTT サーバーから MQTT サブスクライバーにノーティフィケーションを送信します。

MQTT ブローカーを開始するには、以下の手順に従います:-

1. MQTT ブローカー (Mosquitto) をインストールします。

2. Chrome 拡張機能 MQTTlens を追加します。

3. MQTT ブローカー接続を作成します。

4. トピックをサブスクライブします。

オペレーション
##############

1. エンティティの作成
*********************

エンティティを作成するには、指定されたペイロードでエンドポイント **http://<IP Address>:<port>/ngsi-ld/v1/entities/** を
ヒットします。

.. code-block:: JSON

 {
 "id":"urn:ngsi-ld:Vehicle:A135",
 "type":"Vehicle",
 "brandName":{
   "type":"Property",
   "value":"Mercedes"
 },
 "speed":[{
   "type":"Property",
   "value": 55,
   "datasetId": "urn:ngsi-ld:Property:speedometerA4567-speed",
   "source":{
     "type":"Property",
     "value": "Speedometer"
   }
 },
  {
   "type":"Property",
   "value": 11,
    "datasetId": "urn:ngsi-ld:Property:gpsA4567-speed",
   "source":{
     "type":"Property",
     "value": "GPS"
   }
   },
   {
   "type":"Property",
   "value": 10,
   "source":{
     "type":"Property",
     "value": "CAMERA"
   }
 }]
 }

2. サブスクリプション
*********************

エンティティにサブスクライブするには、指定されたペイロードでエンドポイント
**http://<IP Address>:<port>/ ngsi-ld/v1/subscriptions/** にアクセスします。

.. code-block:: JSON

 {
   "id": "urn:ngsi-ld:Subscription:16",
   "type": "Subscription",
   "entities": [{
          "id": "urn:ngsi-ld:Vehicle:A135",
          "type": "Vehicle"
        }],
  "watchedAttributes": ["brandName"],
        "q":"brandName!=Mercedes",
  "notification": {
   "attributes": ["brandName"],
   "format": "keyValues",
   "endpoint": {
    "uri": "mqtt://localhost:1883/notify",
    "accept": "application/json",
    "notifierinfo": {
      "version" : "mqtt5.0",
      "qos" : 0
     }
   }
  }
 }

3. ノーティフィケーション
*************************

属性の値を更新し、**http://<IP Address>:<port>/ngsi-ld/v1/entities/entityId/attrs** で PATCH リクエストを行う場合、

.. code-block:: JSON

 {
   "brandName":{
       "type":"Property",
       "value":"BMW"
  }
 }

次に、ノーティフィケーションを受け取ります。

.. code-block:: JSON

 {
  "metadata": {
  "link": "https://json-ld.org/contexts/person.jsonld",
  "contentType": "application/json"
 },
 "body": {
  "id": "ngsildbroker:notification:-7550927064189664633",
  "type": "Notification",
  "data": [{
   "id": "urn:ngsi-ld:Vehicle:A135",
   "type": "Vehicle",
   "brandName": {
    "type": "Property",
    "createdAt": "2020-07-29T07:19:33.872000Z",
    "value": "BMW",
    "modifiedAt": "2020-07-29T07:51:21.183000Z"
   }
  }],
  "notifiedAt": "2020-07-29T07:51:22.300000Z",
  "subscriptionId": "urn:ngsi-ld:Subscription:16"
  }
 }    
