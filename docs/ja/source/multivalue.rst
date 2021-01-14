**********************************
複数値属性 (Multi-value Attribute)
**********************************

複数値属性 (Multi-value Attribute) は、エンティティが複数のインスタンスを持つ属性を同時に持つことができる機能です。
プロパティの場合、たとえば、異なる品質特性を持つ独立したセンサー測定に基づいて、プロパティ値を提供するソースが一度に
複数存在する場合があります。

例: 車の現在の速度を提供するスピードメーターと GPS を使用するか、温度計または赤外線カメラを使用して両方とも体温を
提供します。

.. figure:: ../../en/source/figures/multivalueDiagram.png

リレーションシップの場合、機能しないリレーションシップ (Non-functional Relationships) が存在する可能性があります。
たとえば、部屋 (Room) の場合、さまざまな人々によってそこに置かれ、時間とともに動的に変化する、現在部屋にあるあらゆる
種類のオブジェクトに対して、複数の "含む" リレーションシップ (multiple "contains" Relationships) が存在する場合が
あります。このような複数属性を明示的に管理できるようにするために、オプションの datasetId プロパティが使用されます。
これはデータ型 URI です。

CRUD 操作
---------

属性の作成、更新、追加、または削除時に datasetId が指定された場合、同じ datasetId を持つインスタンスのみが影響を受け、
別の datasetId を持つインスタンスまたは datasetId のないインスタンスは変更されません。datasetId が指定されていない
場合は、デフォルトの属性インスタンスと見なされます。このデフォルトの datasetId を明示的に指定する必要はありませんが、
存在しない場合でも、このデフォルトの datasetId がリクエストに存在するかのように扱われます。したがって、datasetId
を指定せずに属性を作成、更新、追加、または削除すると、デフォルトのプロパティインスタンスにのみ影響します。

注:- リクエストまたはレスポンスでは、特定の属性名を持つ属性のデフォルトの属性インスタンスは1つだけです。

エンティティ情報をリクエストするときに、一致する属性のインスタンスが複数ある場合、これらは単一の Attribute 要素
ではなく、それぞれ属性の配列として返されます。デフォルトの属性インスタンスの datasetId がレスポンスに明示的に含まれる
ことはありません。datadataId が重複しているが、他の属性データに違いがある属性の情報が競合する場合は、最新の observedAt
DateTime がある場合はそれを使用し、それ以外の場合は最新の modifiedAt DateTime を使用するものが提供されます。

1. 作成操作 (Create Operation)
==============================

複数値属性を持つエンティティを作成するために 、指定されたペイロードでエンドポイント
**http://<IP Address>:<port>/ngsi-ld/v1/entities/** にアクセスできます。

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

2. 更新操作 (Update Operation)
==============================

- **datasetId に基づいて属性インスタンス値を更新します**

ボディで datasetId を送信し、**http://<IP Address>:<port>/ngsi-ld/v1/entities/entityId/attrs/attrsId** で
PATCH リクエストを行うことで、特定のインスタンスの値を更新できます。

.. code-block:: JSON

 {
      "value":"27",
      "datasetId":"urn:ngsi-ld:Property:speedometerA4567-speed"
 }
  

- **属性名に基づいてデフォルトの属性インスタンス値を更新します**

ペイロードの更新された値のみを使用 して **http://<IP Address>:<port>/ngsi-ld/v1/entities/entityId/attrs/attrsId** で
PATCH リクエストを行うことにより、デフォルトインスタンスの値を更新できます。

.. code-block:: JSON

 {
   "value":"27"
 }

3. 削除操作 (Delete Operation)
==============================

- **デフォルトの属性インスタンスを削除します**

デフォルトの属性インスタンスを削除するには、URL
**http://<IP Address>:<port>/ngsi-ld/v1/entities/entityId/attrs/attrsId** を使用して DELETE リクエストを行います。
これにより、属性のデフォルトインスタンスが削除されます。

- **datasetId を持つ属性インスタンスを削除します**

特定の属性インスタンスを削除するには、URL
**http://<IP Address>:<port>/ngsi-ld/v1/entities/entityId/attrs/attrsId?datasetId={{datasetId}}** を使用して DELETE
リクエストを行います。ここで、datasetId は削除する必要のあるインスタンスのID です。

- **指定された属性名を持つすべての属性インスタンスを削除します**

指定された属性名を持つすべての属性インスタンスを削除する場合は、URL
**http://<IP Address>:<port>/ngsi-ld/v1/entities/entityId/attrs/attrsId?deleteAll=true** を使用して DELETE リクエストを
行う必要があります。

4. クエリ操作 (Query Operation)
===============================

エンティティの詳細を取得するには、**http://<IP Address>:<port>/ngsi-ld/v1/entities/** を使用して GET リクエストを
行うと、必要な属性のすべてのインスタンスが取得されます。
