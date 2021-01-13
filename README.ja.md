# <img src="./img/ScorpioLogo.svg" width="140" align="middle"> Scorpio NGSI-LD Broker

[![FIWARE Core](https://nexus.lab.fiware.org/static/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![License: BSD-4-Clause](https://img.shields.io/badge/license-BSD%204%20Clause-blue.svg)](https://spdx.org/licenses/BSD-4-Clause.html)
[![Docker](https://img.shields.io/docker/pulls/scorpiobroker/scorpio.svg)](https://hub.docker.com/r/scorpiobroker/scorpio/)
[![fiware](https://nexus.lab.fiware.org/repository/raw/public/badges/stackoverflow/fiware.svg)](https://stackoverflow.com/questions/tagged/fiware)
[![NGSI LD](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf)
<br>
[![Documentation badge](https://img.shields.io/readthedocs/scorpio.svg)](https://scorpio.readthedocs.io/en/latest/?badge=latest)
![Status](https://nexus.lab.fiware.org/static/badges/statuses/full.svg)
![Travis-CI](https://travis-ci.org/ScorpioBroker/ScorpioBroker.svg?branch=master)

Scorpio は、NEC Laboratories Europe と NEC Technologies India によって開発された NGSI-LD 準拠のコンテキストブローカー
です。クロスカッティングコンテキスト情報管理 ([ETSI ISG CIM](https://www.etsi.org/committee/cim)) に関する ETSI Industry
Specification Group (ETSI ISG) によって指定された完全な
[NGSI-LD API](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf) を実装します。

NGSI-LD API は、コンテキスト情報の管理、アクセス、およびディスカバリーを可能にします。コンテキスト情報は、エンティティ
(建物など) とそのプロパティ (住所や地理的位置など) およびリレーションシップ (所有者など) で構成されます。したがって、
Scorpio を使用すると、アプリケーションとサービスはコンテキスト情報をリクエストできます。つまり、必要なもの、必要な時期、
必要な方法などです。

NGSI-LD API の機能は次のとおりです:

-   コンテキスト情報を作成、更新、追加、および削除します。
-   フィルタリング、地理的スコープ、ページングなどのコンテキスト情報をクエリします。
-   コンテキスト情報の変更をサブスクライブし、非同期ノーティフィケーションを受信します。
-   コンテキスト情報のソースをレジストレーションおよびディスカバリーします。これにより、
    分散展開およびフェデレーション展開を構築できます。

Scorpio は FIWARE Generic Enabler です。したがって、"Powered by FIWARE " のプラットフォームの一部として統合できます。
FIWARE は、オープンソースプラットフォームコンポーネントの精選されたフレームワークであり、他のサードパーティ
プラットフォームコンポーネントと一緒に組み立てて、スマートソリューションの開発を加速することができます。この FIWARE GE
のロードマップは[こちら](./docs/roadmap.ja.md)に記述されています。

詳細については、[FIWARE developers](https://developers.fiware.org/) の Web サイトおよび [FIWARE](https://fiware.org/)
Web サイトを参照してください。FIWARE GEs および Incubated FIWARE GEs の完全なリストは、
[FIWARE Catalogue](https://catalogue.fiware.org/) にあります。

| :books: [ドキュメンテーション](https://scorpio.rtfd.io/) | :mortar_board: [アカデミー](https://fiware-academy.readthedocs.io/en/latest/core/scorpio) | :whale: [Docker Hub](https://hub.docker.com/r/scorpiobroker/scorpio/) | :dart: [ロードマップ](./docs/roadmap.ja.md) |
| ------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- |

## コンテンツ

-   [バックグラウンド](#background)
-   [インストールと構築](#installation-and-building)
-   [使用方法](#usage)
-   [API ウォークスルー](#api-walkthrough)
-   [テスト](#tests)
-   [その他のリソース](#further-resources)
-   [謝辞](#acknowledgements)
-   [クレジット](#credit-where-credit-is-due)
-   [行動規範](#code-of-conduct)
-   [ライセンス](#license)

<a name="background">

## バックグラウンド

Scorpio は、コンテキスト情報の管理とリクエストを可能にする NGSI-LD broker です。次の機能をサポートします:

-   コンテキストプロデューサーは、コンテキスト情報の作成、更新、追加、削除など、コンテキストを管理できます。
-   コンテキストコンシューマーは、エンティティを識別するか、エンティティタイプを提供することで関連するエンティティを
    検出し、GeoJSON 機能として提供されるプロパティ値、既存のリレーションシップ、地理的範囲に従ってフィルタリングする
    ことで、必要なコンテキスト情報を要求できます。
-   同期クエリ応答と非同期サブスクライブ/ノーティフィケーションの2つの対話スタイルがサポートされており、
    ノーティフィケーションはプロパティやリレーションシップの変更、または固定時間間隔に基づくことができます。
-   Scorpio は、指定された時間間隔内に測定されたプロパティ値などの履歴情報を要求するための NGSI-LD のオプションの
    時間インターフェイス (Temporal interface) を実装します。
-   Scorpio は、集中型、分散型、および統合型を含む複数の展開構成をサポートします。上記のコンテキストプロデューサーに
    加えて、それ自体が NGSI-LD インターフェイスを実装するコンテキストソースが存在する可能性があります。これらの
    コンテキストソースは、要求に応じて提供できる情報 (情報 (値) 自体ではない) に自分自身をレジストレーションできます。
    分散設定の Scorpio Broker は、レジストレーションに基づいて要求に応答するための情報を持つ可能性のあるコンテキスト
    ソースを検出し、さまざまなコンテキストソースからの情報を要求および集約して、要求しているコンテキストコンシューマーに
    提供できます。
-   フェデレーション設定では、コンテキストソース自体を NGSI-LD broker にすることができます。フェデレーションを使用して、
    情報を (部分的に) 共有したい複数のプロバイダーからの情報を組み合わせることができます。重要な違いは、通常、
    レジストレーションの粒度にあります。たとえば、"建物 A に関する情報がある" ではなく、"地理的領域内の
    エンティティタイプの建物のエンティティに関する情報がある" などです。
-   Scorpio は、前述のすべてのデプロイメント構成をサポートします。したがって、スケーラビリティと、進化的な方法で
    シナリオを拡張する可能性を提供します。たとえば、2つの別々のデプロイメントを組み合わせたり、スケーラビリティの
    理由から、異なるブローカーを使用したりできます。これは、単一のアクセスポイントを引き続き使用できるコンテキスト
    コンシューマーに対して完全に透過的です。

<a name="installation-and-building">

## インストールと構築

Scorpio は、マイクロサービスフレームワークとして Spring Cloud を使用し、ビルドツールとして Apache Maven を使用して
Java で開発されています。メッセージバスとして Apache Kafka が必要であり、データベースとして PostGIS 拡張機能を備えた
Postgres が必要です。

Scorpio に必要なソフトウェアコンポーネントのインストール方法に関する情報は、
[インストールガイド](./docs/ja/source/installationGuide.rst) に記載されています。Scorpio の構築と実行については、
[Scorpio の構築と実行ガイド](./docs/ja/source/buildScorpio.rst)を参照してください。

<a name="usage">

## 使用方法

デフォルトでは、ブローカーはポート 9090 で実行され、ブローカーとの対話のベース URL は http://localhost:9090/ngsi-ld/v1/
になります。

### 簡単な例

一般的に、次のようなペイロードを使用して HTTP POST リクエストを http://localhost:9090/ngsi-ld/v1/entities/ に送信する
ことでエンティティを作成できます:

```json
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
```

この例では、`@context` がペイロードにあるため、`Content-Type` ヘッダーを `application/ld+json` に設定する必要があります。

エンティティを受信するには、次のような HTTP GET を送信します:

`http://localhost:9090/ngsi-ld/v1/entities/<entityId>`

または、次のような GET を送信してクエリを実行します:

```text
http://localhost:9090/ngsi-ld/v1/entities/?type=Vehicle&limit=2
Accept: application/ld+json
Link: <http://<HOSTNAME_OF_WHERE_YOU_HAVE_AN_ATCONTEXT>/aggregatedContext.jsonld>; rel="http://www.w3.org/ns/json-ld#context";type="application/ld+json"
```

<a name="api-walkthrough">

## API ウォークスルー

Scorpio が提供する NGSI-LD API を使用して実行できることの詳細な例は、
[API ウォークスルー](./docs/ja/source/API_walkthrough.rst) にあります。

<a name="tests">

## テスト

Scorpio には2セットのテストがあります。ユニットテストには JUnit を使用し、システムテストには npm テストベースの
FIWARE NGSI-LD テストスイートを使用します。テストの詳細については、[テストガイド](./docs/ja/source/testing.rst)を
ご覧ください。

<a name="further-resources">

## その他のリソース

NGSI-L Dまたは JSON-LD の詳細については、次を参照ください:

-  [ETSI NGSI-LD 仕様](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.02.02_60/gs_CIM009v010202p.pdf) 
-  [ETSI NGSI-LD 入門](https://www.etsi.org/deliver/etsi_gr/CIM/001_099/008/01.01.01_60/gr_CIM008v010101p.pdf)
-  [JSON-LD ウェブサイト](https://json-ld.org/)
-  [FIWARE Academy Scorpio](https://fiware-academy.readthedocs.io/en/latest/core/scorpio/index.html)
  -  [FIWARE 601: Introduction to Linked Data](https://fiware-tutorials.readthedocs.io/en/latest/linked-data)
  -  [FIWARE 602: Linked Data Relationships and Data Models](https://fiware-tutorials.readthedocs.io/en/latest/relationships-linked-data)

-  [FIWARE global summit: The Scorpio NGSI-LD Broker. Features and supported architectures](https://www.slideshare.net/FI-WARE/fiware-global-summit-the-scorpio-ngsild-broker-features-and-supported-architectures)
-  [FIWARE global summit: NGSI-LD. An evolution from NGSI V2](https://www.slideshare.net/FI-WARE/fiware-global-summit-ngsild-an-evolution-from-ngsiv2)

一連のサンプル呼び出しは、Postman コレクションとして Examples フォルダーにあります。これらの例では2つの変数を使用して
います:

-   gatewayServer は `<brokerIP>:<brokerPort>` である必要があります。ローカルでデフォルト設定を使用する場合は、
    localhost:9090 になります。
-   link, Link header を介して @context を提供する例です。例では、Example @context をホストします。
    https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/Examples/index.json へのリンクを設定します。

<a name="acknowledgements">

## 謝辞

### EU Acknowledgetment

この活動は、助成金契約 No. 731993 (Autopilot), No. 814918 (Fed4IoT)、および No. 767498 (MIDIH, Open Call (MoLe))
に基づく欧州連合の Horizon 2020 研究およびイノベーションプログラムから資金提供を受けています。

<img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/flag_yellow_low.jpg" width="160">

-   [AUTOPILOT project: Automated driving Progressed by Internet Of Things](https://autopilot-project.eu/) <img src="https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/master/img/autopilot.png" width="160">
-   [Fed4IoT project](https://fed4iot.org/)
-   [MIDIH Project](https://midih.eu/), Open Call (MoLe)

<a name="credit-where-credit-is-due">

## クレジット

Scorpio に貢献してくれたすべての人に感謝します。これは、Scorpio 開発チーム全体とすべての外部貢献者に当てはまります。
完全なリストについては、[CREDITS](./CREDITS) ファイルをご覧ください。

<a name="code-of-conduct">

## 行動規範

FIWARE コミュニティの一部として、私たちは [FIWARE 行動規範](https://www.fiware.org/foundation/code-of-conduct/) を
遵守するために最善を尽くし、貢献者にも同じことを期待しています。

これには、プルリクエスト、イシュー、コメント、コード、およびコード内コメントが含まれます。

このリポジトリの所有者として、ここでのコミュニケーションは純粋に Scorpio と NGSI-LD 関連のトピックに限定します。

私たちは皆、異なる文化的背景から来た人間です。私たちは皆、さまざまな癖、習慣、マナーを持っています。そのため、誤解が
生じる可能性があります。Scorpio と NGSI-LD を前進させるために、コミュニケーションが善意で行われていることに疑いの余地は
ありません。寄稿者にも同じことを期待しています。しかし、誰かが繰り返し挑発したり、攻撃したり、議論を変えたり、誰かを
嘲笑したりしようとしている場合、私たちは自分の家を正しく利用し、これに終止符を打ちます。

解決すべき論争がある場合、このリポジトリの所有者としての私たちが最後の言葉を持っています。

<a name="license">

## ライセンス

Scorpio は [BSD-4-Clause](https://spdx.org/licenses/BSD-4-Clause.html) の下でライセンスされています。貢献には、この
[Contribution license](CONTRIBUTING.ja.md) が適用されます。

© 2020 NEC Laboratories Europe, NEC Technologies India
