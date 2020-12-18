************************
Scorpio の構成パラメータ
************************

このセクションでは、Scorpio broker に必要なすべての基本構成について説明します。これは、Scorpio のさまざまな
マイクロサービスの基本的なテンプレートとして使用できます。

さまざまな構成パラメータの説明
##############################

1. **server**:- ここには、ユーザーは、 **ポート** や Tomcat サーバーの **スレッドの最大数** などのさまざまなサーバー関連
パラメーターを定義できます。これは、マイクロサービス通信に関連しています。変更に注意してください。

.. code-block:: JSON

 server:
  port: XXXX
  tomcat:
    max:
      threads: XX
	  
2. **Entity Topics**:- これらは、Kafka での Scorpio の内部コミュニケーションに使用されるトピックです。
これを変更する場合は、ソースコードの内容も変更する必要があります。

.. code-block:: JSON

 entity:
   topic: XYZ
   create:
    topic: XYZ
   append:
    topic: XYZ
   update:
    topic: XYZ
   delete:
    topic: XYZ
   index:
    topic: XYZ

3. **batchoperations**:- NGSI-LD 操作によって定義されたバッチ操作の制限を定義するために使用されます。
これは HTTP サーバーの構成とハードウェアに関連しています。注意して変更してください。

.. code-block:: JSON

 batchoperations:
   maxnumber:
    create: XXXX
    update: XXXX
    upsert: XXXX
    delete: XXXX

4. **bootstrap**:- Kafka broker の URL を定義するために使用されます。Kafka の設定を変更した場合にのみ変更してください。

.. code-block:: JSON

 bootstrap:
   servers: URL

5. **Csources Topics**:- これらは、Kafka での Scorpio の内部コミュニケーションに使用されるトピックです。これを変更する
場合は、ソースコードの内容も変更する必要があります。

.. code-block:: JSON

  registration:
    topic: CONTEXT_REGISTRY

6. **append**:- エンティティの append overwrite オプションを定義するために使用されます。細心の注意を払って交換して
ください。

.. code-block:: JSON

 append:
   overwrite: noOverwrite


7. **spring**:- サービス名などのプロジェクトの基本的な詳細を定義するため、および Kafka, flyway, データソース、クラウドの
構成の詳細を提供するために使用されます。何をしているのかわからない限り、これらを変更しないでください！

.. code-block:: JSON

 spring:
  application:
    name: serviceName
  main:
    lazy-initialization: true
  kafka:
    admin:
      properties:
        cleanup:
          policy: compact
  flyway:
    baselineOnMigrate: true
  cloud:
    stream:
      kafka:
        binder:
          brokers: localhost:9092
      bindings:
         ATCONTEXT_WRITE_CHANNEL:
          destination: ATCONTEXT
          contentType: application/json
  datasource:
    url: "jdbc:postgresql://127.0.0.1:5432/ngb?ApplicationName=ngb_querymanager"
    username: ngb
    password: ngb
    hikari:
      minimumIdle: 5
      maximumPoolSize: 20
      idleTimeout: 30000
      poolName: SpringBootHikariCP
      maxLifetime: 2000000
      connectionTimeout: 30000


8. **query Topics**:- これらは、Kafka での Scorpio の内部コミュニケーションに使用されるトピックです。これを変更する
場合は、ソースコードの内容も変更する必要があります。

.. code-block:: JSON

 query:
  topic: QUERY
  result:
    topic: QUERY_RESULT

9. **atcontext**:- 混合コンテキストがヘッダーを介して提供されるシナリオで、Scorpio によって提供されるコンテキストの
URL を定義するために使用されます。

.. code-block:: JSON

 atcontext:
  url: http://<ScorpioHost>:<ScorpioPort>/ngsi-ld/contextes/

10. **Key**:- 逆シリアル化用のファイルを定義するために使用されます。変更しないでください！

.. code-block:: JSON

 key:
  deserializer: org.apache.kafka.common.serialization.StringDeserializer

11. **reader**:- データベースを Scorpio broker に構成するために使用され、すべての読み取り操作を実行するために必要です。
この例は、ローカルにインストールされた PostgresDB のデフォルト設定に基づいています。

.. code-block:: JSON

 reader:
  enabled: true
  datasource:
    url: "jdbc:postgresql://localhost:5432/ngb?ApplicationName=ngb_storagemanager_reader"
    username: ngb
    password: ngb
    hikari:
      minimumIdle: 5
      maximumPoolSize: 20
      idleTimeout: 30000
      poolName: SpringBootHikariCP_Reader
      maxLifetime: 2000000
      connectionTimeout: 30000

12. **writer**:- データベースを Scorpio broker に構成するために使用され、すべての書き込み操作を実行するために必要です。
この例は、ローカルにインストールされた PostgresDB のデフォルト構成に基づいています。

.. code-block:: JSON

 writer:
  enabled: true
  datasource:
    url: "jdbc:postgresql://localhost:5432/ngb?ApplicationName=ngb_storagemanager_writer"
    username: ngb
    password: ngb
    hikari:
      minimumIdle: 5
      maximumPoolSize: 20
      idleTimeout: 30000
      poolName: SpringBootHikariCP_Writer
      maxLifetime: 2000000
      connectionTimeout: 30000
