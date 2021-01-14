******************
イントロダクション
******************

このウォークスルーでは、読者が NGSI-LD 全般、特に Scorpio Broker に精通し、その過程を楽しんでいただけるよう、実践的な
アプローチを採用しています:)。

ウォークスルーは NGSI-LD 仕様に基づいており、ここ
[https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf] にあります。-> まもなく
gs_CIM009v010301p.pdf になります... NGSI-LD の実装に関する注意事項もご覧ください。--> 利用可能になったら NGSI-LD
に慣れるために、NGSI-LD 入門書 [https://www.etsi.org/deliver/etsi_gr/CIM/001_099/008/01.01.01_60/gr_CIM008v010101p.pdf]
もご覧ください。これは開発者を対象としています。

メイン セクションはコンテキスト管理についてです。コンテキスト管理の基本的なコンテキスト ブローカー機能 (車の温度などの
エンティティに関する情報) について説明します。コンテキスト ソース管理 (エンティティ自体に関する情報ではなく、
分散システム セットアップで情報を提供できるソースに関する情報) も、このドキュメントの一部として説明しています。

開始する前に、NGSI-LD モデルの基礎となる理論的概念を理解することをお勧めします。エンティティ、プロパティ、
リレーションシップなど。これに関する FIWARE ドキュメント、たとえばこの公開プレゼンテーションをご覧ください。
[... 適切なプレゼンテーションを見つける]

チュートリアル用の ScorpioBroker の起動
#######################################

ブローカーを起動するには、docker-compose を使用することをお勧めします。Scorpio のgithub リポジトリから docker-compose
ファイルを取得します。
::

	curl https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development/docker-compose-aaio.yml 


そして、次のコマンドでコンテナを起動します
::

	sudo docker-compose -f docker-compose-aaio.yml up

Docker なしでブローカーを起動することもできます。詳細な手順については、
readme https://github.com/ScorpioBroker/ScorpioBroker/blob/development/README.md を参照してください。

ブローカーへのコマンドの発行
############################

ブローカーにリクエストを発行するには、curl コマンドラインツールを使用できます。curl を選択したのは、ほどんどの
GNU/Linux システムで利用可能で、このドキュメントに簡単にコピーして貼り付けることができる例を含めるのが簡単だからです。
もちろん、これを使用することは必須ではありません。代わりに任意の REST クライアントツール (RESTClient など)
を使用できます。実際には、アプリケーションの REST クライアント部分を実装するプログラミング言語ライブラリを使用して
ScorpioBroker と対話することになります。

このドキュメントのすべての curl の例の基本的なパターンは次のとおりです:

POST の場合:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers]' -d @- <<EOF
[payload]
EOF
PUT の場合:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers] -X PUT -d @- <<EOF
[payload]
EOF
PATCH の場合:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers] -X PATCH -d @- <<EOF
[payload]
EOF
GET の場合:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers]
DELETE の場合:
curl localhost:9090/ngsi-ld/v1/<ngsi-ld-resource-path> -s -S [headers] -X DELETE
[headers] に関しては次のものを含めます:

Accept header では、レスポンスを受信するペイロード形式を指定します。JSON または JSON-LD を明示的に指定する必要が
あります。curl ... -H 'Accept: application/json' ... または curl ... -H 'Accept: application/ld-json'
これは、JSON-LD @context を link header で受信するか、レスポンスのボディで受信するかによって異なります
(JSON-LD と @context の使用については、次のセクションで説明します)

リクエスト (たとえば POST, PUT または PATCH) でペイロード使用する場合は、形式 (JSON または JSON-LD) を
指定するためにContext-Type HTTP header を指定する必要があります。
curl ... -H 'Content-Type: application/json' ... または -H 'Content-Type: application/ld+json'

JSON-LD @context がリクエストボディの一部として提供されていない場合は、link header として提供する必要があります (例:
curl ... -H 'Link: <https://uri.etsi.org/ngsi-ld/primer/store-context.jsonld>;
rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json" ここで、@context は最初の URI
から取得可能である必要があります。つまり、この例では: https://uri.etsi.org/ngsi-ld/primer/store-context.jsonld

いくつかの追加のコメント:

ほとんどの場合、複数行のシェルコマンドを使用して curl への入力を提供し、EOF を使用して複数行のブロック
(ヒアドキュメント) の開始と終了をマークします。場合によっては (GET および DELETE)、ペイロードが使用されないため、
-d @- を省略します。

例では、ブローカーがポート 9090 でリッスンしていると想定しています。別のポートを使用している場合は、curl
コマンドラインでこれを調整してください。

レスポンスで JSON をきれいに出力するために、msjon.tool で Python を使用できます
(チュートリアルとともに例ではこのスタイルを使用しています):

(curl ... | python -mjson.tool) <<EOF
...
EOF

以下を使用して、curl がシステムにインストールされていることを確認します:
::

	which curl


3 センテンスの NGSI-LD データ
############################

NGSI-LD は JSON-LD に基づいています。トップレベルのエントリは NGSI-LD エンティティです。エンティティはプロパティと
リレーションシップを持つことができ、プロパティとリレーションシップ自体もプロパティとリレーションシップ (メタ情報)
を持つことができます。JSON-LD ドキュメントのすべてのキーは URIs である必要がありますが、それを短縮する方法があります。

@context
########

NGSI-LD は JSON-LD に基づいて構築されています。JSON-LD に由来するものとして、拡張された完全な URIs と URL
の圧縮された短い形式の間で 'translate' (変換) するために使用される必須の @context エントリの概念があります。例:
"Property": "https://uri.etsi.org/ngsi-ld/Property"。@context エントリは、JSON 配列の URL を介してリンクすることも
できます。これを混ぜることもできるので、これはまったく問題ありません。
::

	{
		"@context": [{
			"myshortname": "urn:mylongname"
		},
		"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
		]
	}

NGSI-LDには、https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld で利用できるコアコンテキストがあります。
使用されているすべての @context エントリの完全なエントリを常に提供することを強くお勧めしますが、Scorpio およびその他の
NGSI-LD Broker は、欠落しているエントリにコアコンテキストを挿入します。

application/json および application/ld+json
###########################################

2つの異なる方法でデータを提供および受信できます。application/json と application/ld+json の主な違いは、必須の @context
エントリを提供または受信する場所です。Accept header または Content-type header を application/ld+json に設定すると、
@context エントリがルートレベルのエントリとして JSON ドキュメントに埋め込まれます。application/json に設定されている
場合は、このように header entry Link のリンクに @context を指定する必要があります。
Link: <https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

コンテキスト管理
################

@context の使用法を示すために、このチュートリアルのほとんどの例は、ペイロードのボディに @context エントリを持つ
application/ld+json として実行されます。このセクションの最後では、コンテキスト管理操作で Scorpio Broker
を使用してアプリケーション (コンテキストプロデューサーとコンシューマーの両方) を作成するための基本的な知識を習得します。

******************
エンティティの作成
******************

新たなスタートを想定すると、空の ScorpioBroker があります。まず、``house2:smartrooms:room1`` を作成します。
エンティティの作成時に、温度が23℃であり、``smartcity:houses:house2`` の一部であると仮定しましょう。
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
		{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		 }
	   },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  },
	  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

エンティティの ID とType を定義する id フィールドと type フィールドとは別に、ペイロードには一連の属性が含まれています。
ご覧のとおり、属性には2つのタイプがあります。プロパティとリレーションシップです。プロパティは、属性の値を直接提供
します。さらに、UN/CEFACT の測定単位の共通コードで説明されている単位コードを使用して値をより適切に説明するために
使用できるオプションのパラメータ unitCode があります。UnitCodes は、プロデューサーによって提供される追加のメタデータと
見なす必要があります。それらは制限的ではありません。値フィールドの検証はありません。

リレーションシップは常に、リレーションシップのオブジェクトとしてエンコードされた別のエンティティを指します。これらは、
さまざまなエンティティ間のリレーションシップを説明するために使用されます。プロパティとリレーションシップはそれ自体が
リレーションシップを持つことができ、メタ情報の表現を可能にします。ご覧のとおり、この情報を受け取ったセンサーを説明する
エンティティを指す温度プロパティへのリレーションシップも追加しました。

このリクエストを受信すると、Scorpioは内部データベースにエンティティを作成し、サブスクリプションの処理や履歴エントリの
作成など、作成に必要な追加の処理をすべて処理します。リクエストが検証されると、Scorpio は 201 Created HTTP
コードでレスポンスします。

次に、同様の方法で ``house2:smartrooms:room2`` を作成しましょう。
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  },
	  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

このセットアップを完了するために、id ``smartcity:houses:house2`` で家 (house) を説明するエンティティを作成しています。
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
		"id": "smartcity:houses:house2",
		"type": "House",
		"hasRoom": [{
			"type": "Relationship",
			"object": "house2:smartrooms:room1",
			"datasetId": "somethingunique1"
		},
		{
			"type": "Relationship",
			"object": "house2:smartrooms:room2",
			"datasetId": "somethingunique2"
		}],
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"entrance": {
			"type": "GeoProperty",
			"value": {
				"type": "Point",
				"coordinates": [-8.50000005, 41.2]
			}
		},
		"@context": [{"House": "urn:mytypes:house", "hasRoom": "myuniqueuri:hasRoom"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

もちろん、これを別の方法でモデル化することもできますが、このシナリオでは、家 (houses) と部屋 (rooms) の
リレーションシップを、複数のリレーションシップとして hasRoom エントリでモデル化します。エントリを一意に識別するために、
datasetId があります。これは、この特定のリレーションシップを更新するときにも使用されます。"default" インスタンスと
見なされる datasetId がない場合、リレーションシップごとに最大で1つのリレーションシップ インスタンスが存在できます。
プロパティの場合、マルチプロパティも同じように表されます。さらに、ここでは GeoProperty の3番目のタイプの属性を
使用しています。GeoProperty 値は GeoJSON 値であり、経度と緯度を使用してさまざまな形状とフォームを記述できます。
ここでは、家の輪郭を説明するエントリの場所と、入り口のドアを指す入り口を追加します。

ご覧のとおり、'entrance' の @context エントリは提供されておらず、'location' とは異なり、コアコンテキストの一部では
ありません。これにより、Scorpio はコアコンテキストで定義されたデフォルトのプレフィックスを使用してエントリを保存します。
この場合の結果は "https://uri.etsi.org/ngsi-ld/default-context/entrance" になります。

属性値の JSON データ型 (つまり、数値、文字列、ブール値など) に対応する単純な値とは別に、複雑な構造またはカスタム
メタデータを使用できます。

**************************
エンティティのクエリと受信
**************************

コンシューマー アプリケーションの役割を果たし、Scorpio に保存されているコンテキスト情報にアクセスしたいと考えています。
NGSI-LD には、エンティティを取得する2つの方法があります。GET /ngsi-ld/v1/entities/{id} リクエストを使用して、
特定のエンティティを受け取ることができます。別の方法は、NGSI-LD クエリ言語を使用して特定のエンティティ
セットをクエリすることです。

この例で家 (house) を取得したい場合は、次のような GET リクエストを実行します。
::

	curl localhost:9090/ngsi-ld/v1/entities/smartcity%3Ahouses%3Ahouse2 -s -S -H 'Accept: application/ld+json' 

ここで URL エンコードに注意してください。つまり、':' は %3A に置き換えられます。一貫性を保つために、常に URL
をエンコードする必要があります。

このリクエストでは独自の @context を提供しなかったため、レスポンスではコアコンテキストの一部のみが置き換えられます。
::

	{
		"id": "smartcity:houses:house2",
		"type": "urn:mytypes:house",
		"myuniqueuri:hasRoom": [{
			"type": "Relationship",
			"object": "house2:smartrooms:room1",
			"datasetId": "somethingunique1"
		},
		{
			"type": "Relationship",
			"object": "house2:smartrooms:room2",
			"datasetId": "somethingunique2"
		}],
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"entrance": {
			"type": "GeoProperty",
			"value": {
				"type": "Point",
				"coordinates": [-8.50000005, 41.2]
			}
		}
		"@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}

ご覧のとおり、エントランス (entrance) はコアコンテキストで指定されたデフォルトのコンテキストからプレフィックスが
付けられているため、適切に圧縮されています。

独自の @context ファイルをウェブサーバーでホストしていると仮定すると、'Link' header を介して提供できます。
便宜上、この例ではペーストビン (pastebin) を使用しています。コンテキストは次のようになります。
::

	{
		"@context": [{
			"House": "urn:mytypes:house",
			"hasRoom": "myuniqueuri:hasRoom",
			"Room": "urn:mytypes:room",
			"temperature": "myuniqueuri:temperature",
			"isPartOf": "myuniqueuri:isPartOf"
		}, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}


この呼び出しを繰り返し、次のように 'Link' を介して @context を提供します。
::

	curl localhost:9090/ngsi-ld/v1/entities/smartcity%3Ahouses%3Ahouse2 -s -S -H 'Accept: application/ld+json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 

リプライは次のようになります。
::

	{
		"id": "smartcity:houses:house2",
		"type": "House",
		"hasRoom": [{
			"type": "Relationship",
			"object": "house2:smartrooms:room1",
			"datasetId": "somethingunique1"
		},
		{
			"type": "Relationship",
			"object": "house2:smartrooms:room2",
			"datasetId": "somethingunique2"
		}],
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"entrance": {
			"type": "GeoProperty",
			"value": {
				"type": "Point",
				"coordinates": [-8.50000005, 41.2]
			}
		},
		"@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	
コアコンテキストは独自の @context で提供するため、結果には追加されません。ここからは、カスタム @context を使用して、
すべてのリクエストで短い名前を使用できるようにします。

attrs パラメータを使用して、指定された単一の属性を持つエンティティをリクエストすることもできます。たとえば、場所
(location) のみを取得するには、次のようにします:
::

	curl localhost:9090/ngsi-ld/v1/entities/smartcity%3Ahouses%3Ahouse2/?attrs=location -s -S -H 'Accept: application/ld+json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 

レスポンス:
::

	{
		"id": "smartcity:houses:house2",
		"type": "House",
		"location": {
			"type": "GeoProperty",
			"value": {
				"type": "Polygon",
				"coordinates": [[[-8.5, 41.2], [-8.5000001, 41.2], [-8.5000001, 41.2000001], [-8.5, 41.2000001], [-8.5, 41.2]]]
			}
		},
		"@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}

クエリ
#####

情報を取得する2番目の方法は、NGSI-LD クエリです。 この例では、最初に別の家 (house) に属する新しい部屋 (room)
を追加します。
::

	curl localhost:9090/ngsi-ld/v1/entities -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "house99:smartrooms:room42",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house99:sensor36"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house99"
	  },
	  "@context": [{"Room": "urn:mytypes:room", "temperature": "myuniqueuri:temperature", "isPartOf": "myuniqueuri:isPartOf"},"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]
	}
	EOF

Scorpio にあるすべての部屋 (rooms) を取得したいとします。これを行うには、次のような GET リクエストを実行します。
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

このリクエストには Accept header application/json があることに注意してください。つまり、@context へのリンクは
link header で返されます。結果は、
::

	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	  
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property"
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	},
	{
	  "id": "house99:smartrooms:room42",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house99:sensor36"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house99"
	  }
	}
	]

フィルタリング
#############

NGSI-LD は、クエリ結果 およびサブスクリプションのノーティフィケーションからエンティティをフィルタリングするための
多くの方法を提供します。私たちは ``smartcity:houses:house2`` にのみ興味があるので、Relatioship isPartOf で 'q'
フィルターを使用しています。(URL エンコーディングで ``smartcity:houses:house2`` は %22smartcity%3Ahouses%3Ahouse2%22
になります)
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room\&q=isPartOf==%22smartcity%3Ahouses%3Ahouse2%22 -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

結果は次のようになります。
::
	
	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	  
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property"
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	}
	]

同じ結果を得る別の方法は、idPattern パラメータを使用することです。これにより、正規表現を使用できます。この場合、部屋
(rooms) の IDs を構造化したため、これが可能です。
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room\&idPattern=house2%3Asmartrooms%3Aroom.%2A -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
	(house2%3Asmartrooms%3Aroom.%2A == house2:smartrooms:room.*)

属性を制限
##########

さらに、温度 (temperature) のみを与えるように結果を制限したいと思います。これは、attrs パラメータを使用して実行
されます。Attrs はコンマ区切りのリストを取ります。私たちの場合、エントリは1つだけなので、次のようになります。
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room&q=isPartOf==%22smartcity%3Ahouses%3Ahouse2%22\&attrs=temperature -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

::

	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": {
		"value": 23,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  }
	  
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": {
		"value": 21,
		"unitCode": "CEL",
		"type": "Property"
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor4711"
		}
	  }
	}
	]

KeyValues の結果
################

ここで、実際には温度 (temperature) の値のみに関心があり、メタ情報は気にしないため、リクエストのペイロードをさらに
制限したいとします。これは、keyValues オプションを使用して実行できます。KeyValues は、最上位の属性とそれぞれの値
またはオブジェクトのみを提供するエンティティの圧縮バージョンを返します。
::

	curl localhost:9090/ngsi-ld/v1/entities/?type=Room\&q=isPartOf==%22smartcity%3Ahouses%3Ahouse2%22\&attrs=temperature\&options=keyValues -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'

レスポンス:
::

	[
	{
	  "id": "house2:smartrooms:room1",
	  "type": "Room",
	  "temperature": 23
	},
	{
	  "id": "house2:smartrooms:room2",
	  "type": "Room",
	  "temperature": 21
	}
	]

****************************************
エンティティの更新とエンティティへの追加
****************************************

NGSI-LD を使用すると、エンティティを更新 (現在のエントリを上書き) するだけでなく、新しい属性を追加することもできます。
さらに、もちろん特定の属性を更新することもできます。``house2:smartrooms:room1`` の温度 (temperature)
のコンテキストプロデューサーの役割を果たして、5つのシナリオをカバーします。

1. エンティティ全体を更新して、新しい値をプッシュします。
2. 部屋 (room) からの湿度 (humidity) を提供する新しいプロパティを追加します。
3. 温度 (temperature) の値を部分的に更新します。
4. 新しい複数値エントリを温度 (temperature) に追加してケルビン度で情報を提供します。
5. 温度 (temperature) と華氏 (Fahrenheit) の特定の複数値エントリを更新します。

エンティティの更新
##################

基本的に、2つの例外を除いて、エンティティのすべての部分を更新できます。type と id は不変です。NGSI-LD の更新により、
既存のエントリが上書きされます。これは、現在存在する属性を含まないペイロードでエンティティを更新すると、
そのエンティティが削除されることを意味します。room1 を更新するには、次のような HTTP POST を実行します。
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1 -s -S -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"temperature": {
		"value": 25,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	  },
	  "isPartOf": {
		"type": "Relationship",
		"object": "smartcity:houses:house2"
	  }
	}
	EOF
	
これは1つの値を更新するためのペイロードが少し多いため、誤って何かを削除するリスクがあります。
エンティティの大部分を本当に更新する場合にのみ、このエンティティの更新をお勧めします。

属性の部分更新
##############

単一の属性の更新を処理するために、NGSI-LDは部分的な更新を提供します。これは、/entities/<entityId>/attrs/<attributeName>
の POST によって行われます。温度 (temperature) を更新するために、次のような POST を実行します。
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1/attrs/temperature -s -S -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"value": 26,
		"unitCode": "CEL",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
	}
	EOF
	
属性を追加
##########

エンティティに新しい属性を追加するには、ペイロードとして新しい属性を使用して /entities/<entityId>/attrs/ で HTTP PATCH
コマンドを実行します。デフォルトで NGSI-LD に追加すると、既存のエントリが上書きされます。これが望ましくない場合は、
/entities/<entityId>/attrs?options=noOverwrite のように URL に noOverwrite を使用してオプション パラメータを
追加できます。ここで、room1 の湿度 (humidity) のエントリを追加する場合は、次のように HTTP PATCH を実行します。
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1/attrs -s -S -X PATCH -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"humidity": {
		"value": 34,
		"unitCode": "PER",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor2222"
		}
	  }
	}
	

複数値の属性を追加
##################

NGSI-LD では、新しい複数値のエントリを追加することもできます。これを行うには、一意の datesetId を追加します。
datasetId が追加で指定されている場合、指定された datasetId のエントリにのみ影響します。華氏 (Fahrenheit) の温度
(temperature) を追加して、このような PATCH 呼び出しを行います。
::

	curl localhost:9090/ngsi-ld/v1/entities/house2%3Asmartrooms%3Aroom1/attrs/temperature -s -S -H 'Content-Type: application/json' -H 'Link: https://pastebin.com/raw/Mgxv2ykn' -d @- <<EOF
	{
		"value": 78,8,
		"unitCode": "FAH",
		"type": "Property",
		"providedBy": {
			"type": "Relationship",
			"object": "smartbuilding:house2:sensor0815"
		}
		"datasetId": "urn:fahrenheitentry:0815"
	}
	EOF

******************
サブスクリプション
******************

NGSI-LD は、エンティティに関するノーティフィケーションを受け取ることができるサブスクリプション インターフェイスを
定義します。サブスクリプションは on change サブスクリプションです。これは、サブスクリプションの結果としてエンティティの
初期状態に関するノーティフィケーションを受け取らないことを意味します。現時点でのサブスクリプションは、一致する
エンティティが作成、更新、または追加されたときにノーティフィケーションを発行します。エンティティが削除されても
ノーティフィケーションは届きません。

エンティティへのサブスクライブ
##############################

部屋 (rooms) の温度 (temperature) を取得するために、/ngsi-ld/v1/subscriptions エンドポイントに POST できる基本的な
サブスクリプションを作成します。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:1",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
	  }],
	  "notification": {
		"endpoint": {
			"uri": "http://ptsv2.com/t/30xad-1596541146/post",
			"accept": "application/json"
		}
	  },
	  "@context": ["https://pastebin.com/raw/Mgxv2ykn"]
	}
	EOF

ご覧のとおり、エンティティは配列であり、サブスクリプションの複数の一致基準を定義できます。必要に応じて、id または
idPattern (正規表現) でサブスクライブできます。ただし、エンティティのエントリでは常にタイプが必須です。

ノーティフィケーション エンドポイント
#####################################

NGSI-LD は現在、サブスクリプション用に2種類のエンドポイントをサポートしています。HTTP(S) および MQTT(S)。
サブスクリプションのノーティフィケーション エントリでは、URI と受け入れ MIME タイプを使用してエンドポイントを
定義できます。ご覧のとおり、HTTP エンドポイントを使用しています。

ノーティフィケーション エンドポイントのテスト
############################################

この例では、Post Test Server V2 (http://ptsv2.com/) を使用しています。これは、この例では認証のないパブリック
サービスです。したがって、データには注意してください。また、このサービスはテストとデバッグを目的としており、
それ以上のものではありません。いいね！彼らは私たちに開発のための良いツールを与えてくれます。通常は、この例をそのまま
使用できます。ただし、何らかの理由でエンドポイントが削除された場合は、ptsv2.com にアクセスして "New Random Toilet"
をクリックし、エンドポイントをそこに提供されている POST URL に置き換えてください。

ノーティフィケーション
######################

すべての部屋に温度変化があると仮定すると、変化ごとに1つずつ、3つの独立したノーティフィケーションが届きます。
::

	{
		"id": "ngsildbroker:notification:-5983263741316604694",
		"type": "Notification",
		"data": [
			{
				"id": "house2:smartrooms:room1",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T12:55:05.276000Z",
				"modifiedAt": "2020-08-07T13:53:56.781000Z",
				"myuniqueuri:isPartOf": {
					"type": "Relationship",
					"createdAt": "2020-08-04T12:55:05.276000Z",
					"object": "smartcity:houses:house2",
					"modifiedAt": "2020-08-04T12:55:05.276000Z"
				},
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T12:55:05.276000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T12:55:05.276000Z",
						"object": "smartbuilding:house2:sensor0815",
						"modifiedAt": "2020-08-04T12:55:05.276000Z"
					},
					"value": 22.0,
					"modifiedAt": "2020-08-04T12:55:05.276000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T13:53:57.640000Z",
		"subscriptionId": "urn:subscription:1"
	}

::

	{
		"id": "ngsildbroker:notification:-6853258236957905295",
		"type": "Notification",
		"data": [
			{
				"id": "house2:smartrooms:room2",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T11:17:28.641000Z",
				"modifiedAt": "2020-08-07T14:00:11.681000Z",
				"myuniqueuri:isPartOf": {
					"type": "Relationship",
					"createdAt": "2020-08-04T11:17:28.641000Z",
					"object": "smartcity:houses:house2",
					"modifiedAt": "2020-08-04T11:17:28.641000Z"
				},
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T11:17:28.641000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T11:17:28.641000Z",
						"object": "smartbuilding:house2:sensor4711",
						"modifiedAt": "2020-08-04T11:17:28.641000Z"
					},
					"value": 23.0,
					"modifiedAt": "2020-08-04T11:17:28.641000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T14:00:12.475000Z",
		"subscriptionId": "urn:subscription:1"
	}
	
::

	{
		"id": "ngsildbroker:notification:-7761059438747425848",
		"type": "Notification",
		"data": [{
				"id": "house99:smartrooms:room42",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T13:19:17.512000Z",
				"modifiedAt": "2020-08-07T14:00:19.100000Z",
				"myuniqueuri:isPartOf": {
					"type": "Relationship",
					"createdAt": "2020-08-04T13:19:17.512000Z",
					"object": "smartcity:houses:house99",
					"modifiedAt": "2020-08-04T13:19:17.512000Z"
				},
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T13:19:17.512000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T13:19:17.512000Z",
						"object": "smartbuilding:house99:sensor36",
						"modifiedAt": "2020-08-04T13:19:17.512000Z"
					},
					"value": 24.0,
					"modifiedAt": "2020-08-04T13:19:17.512000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T14:00:19.897000Z",
		"subscriptionId": "urn:subscription:1"
	}

ご覧のとおり、サブスクリプションで定義したタイプに一致する完全なエンティティを常に取得しています。

属性のサブスクライブ
####################

セットアップで同じ結果を得る別の方法は、サブスクリプションで watchedAttributes パラメータを使用することです。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:2",
	  "type": "Subscription",
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
		},
	  "@context": "https://pastebin.com/raw/Mgxv2ykn"
	}
	EOF


これはこの例では機能しますが、温度 (temperature) 属性が変更されるたびにノーティフィケーションを受け取ります。
したがって、実際のシナリオでは、おそらく私たちが望んでいたよりもはるかに多いでしょう。有効なサブスクリプションには、
少なくとも entities パラメータ (配列に有効なエントリがある) または watchedAttributes パラメータが必要です。ただし、
両方を組み合わせることもできます。したがって、"Room" の "temperature" が変化するたびにノーティフィケーションを
受け取りたい場合は、このようにサブスクライブします。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:3",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
	  }],
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

これで、クエリと非常によく似たノーティフィケーションで取得したいものをさらに制限できます。

IdPattern
#########

今は ``smartcity:houses:house99`` から "Room" も取得しますが、関心があるのは ``smartcity:houses:house2`` だけなので、
idPattern パラメータを使用して結果を制限します。私たちの場合、これは名前の構造 (namestructure) のために可能です。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:4",
	  "type": "Subscription",
	  "entities": [{
			"idPattern" : "house2:smartrooms:room.*",
			"type": "Room"
		}],
	  "watchedAttributes": ["temperature"],
	  "notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
	  },
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF
 

Q フィルター
############

クエリと同様に、q フィルタを使用して、isPartOf リレーションシップを介してこれを実現することもできます。ここで、
ボディには URL エンコードはありません。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:5",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "q": "isPartOf==smartcity.houses.house2",
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

属性を減らす
############

ノーティフィケーションでまだ完全なエンティティを取得しているので、属性の数を減らしたいと思います。これは、
ノーティフィケーション エントリの属性パラメータによって行われます。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:6",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "q": "isPartOf==smartcity.houses.house2",
	  "watchedAttributes": ["temperature"],
	  "notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			},
			"attributes": ["temperature"]
	  },
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

ご覧のとおり、温度 (temperature) が変化したときにのみ温度 (temperature) を取得するようになりました。
::

	{
		"id": "ngsildbroker:notification:-7761059438747425848",
		"type": "Notification",
		"data": [
			{
				"id": "house2:smartrooms:room1",
				"type": "urn:mytypes:room",
				"createdAt": "2020-08-04T13:19:17.512000Z",
				"modifiedAt": "2020-08-07T14:30:12.100000Z",
				"myuniqueuri:temperature": {
					"type": "Property",
					"createdAt": "2020-08-04T13:19:17.512000Z",
					"providedBy": {
						"type": "Relationship",
						"createdAt": "2020-08-04T13:19:17.512000Z",
						"object": "smartbuilding:house99:sensor36",
						"modifiedAt": "2020-08-04T13:19:17.512000Z"
					},
					"value": 24.0,
					"modifiedAt": "2020-08-04T13:19:17.512000Z"
				}
			}
		],
		"notifiedAt": "2020-08-07T14:00:19.897000Z",
		"subscriptionId": "urn:subscription:6"
	}
	
属性と watchedAttributes パラメータは非常に異なる場合があります。どの家で温度 (temperature) が変化するか知りたい場合は、
このようにサブスクライブします。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:7",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "watchedAttributes": ["temperature"],
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			},
			"attributes": ["isPartOf"]
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

GeoQ フィルター
###############

追加のフィルターは geoQ パラメータであり、地理クエリを定義できます。たとえば、このようにサブスクライブするポイントに
近いすべてのハウス (house) について情報を提供したい場合、次のようにサブスクライブします。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:8",
	  "type": "Subscription",
	  "entities": [{
			"type": "House"
		}],
	  "geoQ": {
	  "georel": {
		"near;maxDistance==2000",
		"geometry": "Point",
		"coordinates": [-8.50000005, 41.20000005]
	  },
		"notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json"
			},
			"attributes": ["isPartOf"]
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

追加のエンドポイント パラメータ
###############################

ノーティフィケーション エントリには、2つの追加のオプション エントリがあります。ReceiverInfo および notifierInfo です。
これらは両方とも、単純なキーバリューのセットの配列です。実際には、これらは Scorpio のノーティフィケーション機能
(notifierInfo) の設定と、ノーティフィケーションごとに送信する追加のヘッダー (receiverInfo) を表します。notifierInfo
は現在、MQTT にのみ使用されています。たとえば、oauth トークンを渡したい場合は、次のようなサブスクリプションを
実行します。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:9",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
	  "notification": {
			"endpoint": {
				"uri": "http://ptsv2.com/t/30xad-1596541146/post",
				"accept": "application/json",
				"receiverInfo": [{"Authorization": "Bearer sdckqk3123ykasd723knsws"}]
			}		
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

MQTT エンドポイント
###################

実行中の MQTT バスが利用可能な場合は、MQTT のトピックへのノーティフィケーションを受け取ることもできます。ただし、
MQTT バスのセットアップとトピックの作成は、NGSI-LD ブローカーの責任の範囲外です。MQTT バスアドレスは、MQTT の URI
表記を介して提供する必要があります。
mqtt[s]://[<username>:<password>@]<mqtt_host_name>:[<mqtt_port>]/<topicname>[[/<subtopic>]...]。
したがって、サブスクリプションは通常次のようになります。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:10",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
		"notification": {
			"endpoint": {
				"uri": "mqtt://localhost:1883/notifytopic",
				"accept": "application/json"
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

MQTT パラメータ
###############

MQTT には、構成する必要のあるクライアント設定がいくつかあります。提供しない場合は、ここにいくつかの妥当なデフォルトが
ありますが、クライアントを完全に構成する方がよいことを確認してください。これらのパラメータは、エンドポイントの
notifierInfo エントリを介して提供されます。現在サポートされているのは、"MQTT-Version" の可能な値として "mqtt3.1.1"
または "mqtt5.0" で、デフォルトは "mqtt5.0" です。"MQTT-QoS" の可能な値は、 0, 1, 2 で、デフォルトは 1 です。これを
"MQTT-Version" を 3.1.1 および "MQTT-QoS" を 2 に変更し、次のようにサブスクライブします。
::

	curl localhost:9090/ngsi-ld/v1/subscriptions -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	{
	  "id": "urn:subscription:11",
	  "type": "Subscription",
	  "entities": [{
			"type": "Room"
		}],
		"notification": {
			"endpoint": {
				"uri": "mqtt://localhost:1883/notifytopic",
				"accept": "application/json",
				"notifierInfo": [{"MQTT-Version": "mqtt3.1.1"},{"MQTT-QoS": 2}]
			}
		},
	  "@context": [ "https://pastebin.com/raw/Mgxv2ykn" ]
	}
	EOF

MQTT ノーティフィケーション
###########################

MQTT にヘッダーがないため、HTTP コールバックのノーティフィケーションの形式が少し変更されています。メタデータとボディ
エントリで構成されます。メタデータは、HTTP ヘッダーを介して通常配信されるものを保持し、ボディには通常の
ノーティフィケーション ペイロードが含まれます。
::

	{
		"metadata": {
			"Content-Type": "application/json"
			"somekey": "somevalue"
		},
		"body":
				{
					"id": "ngsildbroker:notification:-5983263741316604694",
					"type": "Notification",
					"data": [
						{
							"id": "house2:smartrooms:room1",
							"type": "urn:mytypes:room",
							"createdAt": "2020-08-04T12:55:05.276000Z",
							"modifiedAt": "2020-08-07T13:53:56.781000Z",
							"myuniqueuri:isPartOf": {
								"type": "Relationship",
								"createdAt": "2020-08-04T12:55:05.276000Z",
								"object": "smartcity:houses:house2",
								"modifiedAt": "2020-08-04T12:55:05.276000Z"
							},
							"myuniqueuri:temperature": {
								"type": "Property",
								"createdAt": "2020-08-04T12:55:05.276000Z",
								"providedBy": {
									"type": "Relationship",
									"createdAt": "2020-08-04T12:55:05.276000Z",
									"object": "smartbuilding:house2:sensor0815",
									"modifiedAt": "2020-08-04T12:55:05.276000Z"
								},
								"value": 22.0,
								"modifiedAt": "2020-08-04T12:55:05.276000Z"
							}
						}
					],
					"notifiedAt": "2020-08-07T13:53:57.640000Z",
					"subscriptionId": "urn:subscription:1"
				}
	}
	
********************
Batch オペレーション
********************

NGSI-LD は、4つのバッチ操作に対して4つのエンドポイントを定義します。エンティティの作成、アップデート、アップサート、
または削除 (creations, updates, upserts, deletes) のバッチを作成できます。作成、更新、アップサートは基本的に、
対応する単一エンティティ操作の配列です。house99 の room をいくつか作成したい場合は、次のようなエンティティを作成します。
::

	curl localhost:9090/ngsi-ld/v1/entityOperations/create -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	[{
			"id": "house99:smartrooms:room1",
			"type": "Room",
			
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room2",
			"type": "Room",
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room3",
			"type": "Room",
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room4",
			"type": "Room",
			"temperature": {
				"value": 21,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor20041113"
				}
			},
			"isPartOf": {
				"type": "Relationship",
				"object": "smartcity:houses:house99"
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		}
	]
	EOF

ここで、温度 (temperature) エントリを1つだけ追加したので、このようにすべての部屋 (room) の温度 (temperature) を
アップサート (upsert) します。
::

	curl localhost:9090/ngsi-ld/v1/entityOperations/upsert -s -S -H 'Content-Type: application/ld+json' -d @- <<EOF
	[{
			"id": "house99:smartrooms:room1",
			"type": "Room",
			"temperature": {
				"value": 22,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor19970309"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room2",
			"type": "Room",
			"temperature": {
				"value": 23,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor19960913"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room3",
			"type": "Room",
			"temperature": {
				"value": 21,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor19931109"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		},
		{
			"id": "house99:smartrooms:room4",
			"type": "Room",
			"temperature": {
				"value": 22,
				"unitCode": "CEL",
				"type": "Property",
				"providedBy": {
					"type": "Relationship",
					"object": "smartbuilding:house99:sensor20041113"
				}
			},
			"@context": "https://pastebin.com/raw/Mgxv2ykn"

		}
	]
	EOF

最後になりましたので、バッチ削除でクリーンアップしましょう。バッチ削除は、削除するエンティティ IDs の配列です。
::

	curl localhost:9090/ngsi-ld/v1/entityOperations/delete -s -S -H 'Content-Type: application/json' -d @- <<EOF
	[
		"house99:smartrooms:room1",
		"house99:smartrooms:room2",
		"house99:smartrooms:room3",
		"house99:smartrooms:room4"
	]
	EOF

***********************
コンテキストレジストリ
***********************

コンテキストプロデューサー (Context Producers) によって使用される作成、追加、更新インターフェイスの隣に、
コンテキストソース (Context Source) である NGSI-LD の別の概念があります。コンテキストソースは、NGSI-LD のクエリと
サブスクリプションインターフェイスを提供するソースです。すべての意図と目的において、NGSI-LD ブローカーはそれ自体が
NGSI-LD コンテキストソースです。これにより、分散セットアップが必要な場合に多くの柔軟性が得られます。これらの
コンテキストソースを検出するために、コンテキストレジストリが使用され、コンテキストソースが Scorpio にレジストレーション
されます。別の家 (house) に関する情報を提供する外部コンテキストソースがあると仮定して、次のようにシステムに
レジストレーションします。
::

	{
	  "id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3458",
	  "type": "ContextSourceRegistration",
	  "information": [
		{
		  "entities": [
			{
			  "type": "Room"
			}
		  ]
		}
	  ],
	  "endpoint": "http://my.csource.org:1234",
	  "location": { "type": "Polygon", "coordinates": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] },
	  "@context": "https://pastebin.com/raw/Mgxv2ykn"
	}

これで、Scorpioは、クエリとサブスクリプションで、一致するレジストレーションを持つレジストレーション済みの
コンテキストソースを考慮に入れます。また、通常のクエリまたはサブスクリプションと非常によく似た、
コンテキストレジストリエントリを個別にクエリまたはサブスクライブし、コンテキストソースと個別に対話することもできます。
ここで、このようなタイプの部屋 (room) を提供するすべてのレジストレーションをクエリすると、
::

	curl localhost:9090/ngsi-ld/v1/csourceRegistrations/?type=Room -s -S -H 'Accept: application/json' -H 'Link: <https://pastebin.com/raw/Mgxv2ykn>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 

オリジナルのレジストレーションと、タイプルーム (type Room) にレジストレーションされているすべてのものが返されます。

通常のクエリとサブスクリプションでのコンテキストレジストリの使用
################################################################

コンテキストレジストリエントリには、通常のクエリまたはサブスクリプションが Scorpio に到着したときに考慮される複数の
エントリを含めることができます。ご覧のとおり、サブスクリプションにあるものと同様のエンティティエントリがあります。
これは最初に考慮すべきことです。タイプ (type) をレジストレーションすると、Scorpio はそのタイプに一致する
リクエストのみを転送します。同様に、場所 (location) は、地理クエリ部分を含むクエリを転送するかどうかを決定するために
使用されます。やりすぎてはいけませんが、レジストレーションで詳細を提供すればするほど、システムはリクエストの転送先の
コンテキストソースをより効率的に判断できるようになります。以下に、より多くのプロパティが設定された例を示します。
::

	{
	  "id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3459",
	  "type": "ContextSourceRegistration",
	  "name": "NameExample",
	  "description": "DescriptionExample",
	  "information": [
		{
		  "entities": [
			{
			  "type": "Vehicle"
			}
		  ],
		  "properties": [
			"brandName",
			"speed"
		  ],
		  "relationships": [
			"isParked"
		  ]
		},
		{
		  "entities": [
			{
			  "idPattern": ".*downtown$",
			  "type": "OffStreetParking"
			}
		  ]
		}
	  ],
	  "endpoint": "http://my.csource.org:1026",
	  "location": "{ \"type\": \"Polygon\", \"coordinates\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }"
	}

情報部分には2つのエントリがあります。最初に、そのソースによって提供される2つのプロパティと1つのリレーションシップを
説明する2つの追加エントリがあることがわかります。つまり、属性フィルターなしで type Vehicle をリクエストするクエリは
すべてこのソースに転送され、属性フィルターがある場合は、レジストレーションされたプロパティまたはリレーションシップが
一致する場合にのみ転送されます。2番目のエントリは、このソースが "downtown" で終わるエンティティ ID を持つ
type OffStreetParking のエンティティを提供できることを意味します。
