**************************************
docker-compose を介して Scorpio を起動
**************************************

コピーするコマンドを開始
########################


Scorpio を始める最も簡単な方法をお探しですか？これです。
::

	curl https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development/docker-compose-aaio.yml
	sudo docker-compose -f docker-compose-aaio.yml up


イントロダクション
##################

Scorpio を起動する最も簡単な方法は、docker-compose を使用することです。dockerhub に依存する2つのメイン docker-compose
ファイルを提供します。docker-compose-aaio.yml および docker-compose-dist.yml です。このファイルをそのまま使用して
Scorpio を起動できます。分散バリアントで Scorpio を実行する場合は、上記のコマンドで yml ファイルを交換します。

docker-compose-aaio.yml
#######################

ここでの AAIO は、ほぼすべてを1つにまとめたものです。このバリアントでは、Scorpio のコアコンポーネントと Spring Cloud
コンポーネントが1つのコンテナ内で開始されます。追加のコンテナは Kafka と Postgres のみです。テストおよび小規模から
中規模の展開の場合、これはおそらく使用したいものです。

docker-compose-dist.yml
#######################

このバリアントでは、各 Scorpio コンポーネントは異なるコンテナで開始されます。これにより、柔軟性が高くなり、個々の
コンポーネントを置き換えたり、一部のコアコンポーネントの新しいインスタンスを開始したりできます。

環境変数を介して Docker イメージを構成
######################################

Docker に環境変数を入力する方法は複数あります。それらすべてを通過するのではなく、docker-compose ファイルのみを
通過します。ただし、Scorpio 関連の部分はこれらすべてのバリアントに適用されます。Scorpio の構成は、Spring Cloud
構成システムを介して行われます。使用されるパラメータとデフォルト値の完全な概要については、AllInOneRunner の
application.yml を参照してください。
https://github.com/ScorpioBroker/ScorpioBroker/blob/development/AllInOneRunner/src/main/resources/application-aaio.yml。
新しい設定を提供するには、docker-compose ファイルの環境エントリを介してそれらを提供できます。設定する変数は spring_args
と呼ばれます。このオプションは Scorpio コンテナにのみ設定するため、次のように Scorpio コンテナエントリのサブパーツに
します。
::

	scorpio:
	  image: scorpiobroker/scorpio:scorpio-aaio_1.0.0
	  ports:
	    - "9090:9090"
	  depends_on:
	    - kafka
	    - postgres
	  environment:
	    spring_args: --maxLimit=1000

これにより、クエリのリプライの最大制限をデフォルトの500ではなく1000に設定します。


Docker を静かに
###############

一部の Docker コンテナは非常にノイズが多く、そのすべての出力が必要ではありません。簡単な解決策はこれを追加することです。
::

    logging:
      driver: none


docker-compose ファイルでそれぞれのコンテナー構成に追加します。 例えば、Kafka を静かにするために。
::

	kafka:
	  image: wurstmeister/kafka
	  hostname: kafka
	  ports:
	    - "9092"
	  environment:
	    KAFKA_ADVERTISED_HOST_NAME: kafka
	    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
	    KAFKA_ADVERTISED_PORT: 9092
	    KAFKA_LOG_RETENTION_MS: 10000
	    KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 5000
	  volumes:
	    - /var/run/docker.sock:/var/run/docker.sock
	  depends_on:
	    - zookeeper
	  logging:
	    driver: none

**************
構成パラメータ
**************

Scorpio は Spring Cloud/Boot 構成システムを使用しています。これは、対応するフォルダー内の application.yml ファイルを
介して行われます。AllInOneRunner には、使用可能なすべての構成オプションの完全なセットが含まれています。

これらは、上記のようにコマンドラインまたは Docker の場合に上書きできます。

+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| 構成オプション    | 説明                                            | デフォルト値                                                                        | 
+===================+=================================================+================================================================================+
| atcontext.url     | 内部コンテキストサーバーに使用される URL        | http://localhost:9090/ngsi-ld/contextes/                                       | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| bootstrap.servers | 内部 Kafka のホストとポート                     | kafka:9092 (default used for docker)                                           | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| broker.id a       | ブローカーの一意の id。フェデレーションに必要   | Broker1                                                                        | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| broker.parent.    | フェデレーション設定での親ブローカーのurl       | SELF (フェデレーションなしを意味する)                                               | 
| location.url      |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| broker.           | カバレッジの GeoJSON の説明。フェデレーション   | empty                                                                          | 
| geoCoverage       | 設定での登録に使用されます。                    |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| defaultLimit      | 制限が指定されていない場合のクエリのデフォルト  | 50                                                                             | 
|                   | の制限                                          |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| maxLimit          | クエリの結果の最大数                            | 500                                                                            | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| reader.datasource | ここで postgres の設定を変更する場合は、        | ngb                                                                            | 
| .hikari.password  | パスワードを設定します                          |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| reader.datasource | postgres への JDBC URL                          | jdbc:postgresql://postgres:5432/ngb?ApplicationName=ngb_storagemanager_reader  | 
| .hikari.url       |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| reader.datasource | postgres データベースのユーザー名               | ngb                                                                            | 
| .hikari.username  |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| writer.datasource | ここで postgres の設定を変更する場合は、        | ngb                                                                            | 
| .hikari.password  | パスワードを設定します                          |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| writer.datasource | postgres への JDBC URL                          | jdbc:postgresql://postgres:5432/ngb?ApplicationName=ngb_storagemanager_writer  | 
| .hikari.url       |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| writer.datasource | postgres データベースのユーザー名               | ngb                                                                            | 
| .hikari.username  |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+

*************************
ソースから Scorpio を構築
*************************

Scorpio は、マイクロサービスフレームワークとして Spring Cloud を使用し、ビルドツールとして Apache Maven を使用して
Java で開発されています。一部のテストでは、Apache Kafka メッセージバスを実行する必要があります (詳細については、
セットアップの章を参照してください)。これらのテストをスキップしたい場合は、実行 ``mvn clean package -DskipTests``
して個々のマイクロサービスを構築することができます。

ビルドに関する一般的な注意
##########################

このドキュメントのさらに下には、さまざまなフレーバーの正確なビルドコマンド/引数があります。このパートでは、
さまざまな引数がどのように機能するかについて概要を説明します。

Maven プロファイル
-----------------

現在、3つの利用可能な Maven ビルドプロファイルがあります。

デフォルト
~~~~~~~~~~

-P 引数を指定しない場合、Maven はマイクロサービスと AllInOneRunner の個別の jar ファイルを生成し、各 "full"
マイクロサービスがパッケージ化されます (これにより、AllInOneRunner のサイズは約500 MBになります)

docker
~~~~~~

これにより、Maven がトリガーされて各マイクロサービスの Docker コンテナが構築されます。

docker-aaio
~~~~~~~~~~~

これにより、Maven がトリガーされ、AllInOneRunner と Spring Cloud コンポーネント (eureka, configserver, gateway)
を含む1つの Docker コンテナが構築されます。

Maven の引数
~~~~~~~~~~~~

これらの引数は、コマンドラインの -D を介して提供されます。

skipTests
~~~~~~~~~ 

ビルドを高速化したい場合、または一部のテストで必要な Kafka
インスタンスを実行していない場合は、一般的に推奨されます。

skipDefault 
~~~~~~~~~~~

これは Scorpio ビルドの特別な議論です。この引数は、個々のマイクロサービスのスプリングの再パックを無効にし、より小さな
AllInOneRunner jar ファイルを許可します。この引数は、docker-aaio プロファイルと組み合わせてのみ使用する必要があります。

Spring プロファイル
-------------------

Spring は、jar ファイルの起動時にアクティブ化できるプロファイルもサポートしています。現在、Scorpio で活発に使用されて
いる3つのプロファイルがあります。デフォルトのプロファイルは、デフォルトのセットアップが個々のマイクロサービスであると
想定しています。例外は AllInOneRunner で、デフォルトでは docker-aaio セットアップで実行されていると想定されています。

現在、AllInOneRunner と組み合わせたゲートウェイを除いて、デフォルトのプロファイルですべてを実行できるはずです。これら
2つを一緒に使用するには、aaio spring プロファイルでゲートウェイを開始する必要があります。これは、これを開始コマンド
-Dspring.profiles.active=aaio にアタッチすることで実行できます。

さらに、一部のコンポーネントには、開発目的のみを目的とした開発プロファイルが用意されており、そのためにのみ使用する
必要があります。

セットアップ
############

Scorpio には2つのコンポーネントをインストールする必要があります。

Postgres
--------

`Postgres DB <https://www.postgresql.org/>`__ と `Postgis <https://postgis.net>`__ 拡張機能をダウンロードし、Web
サイトの指示に従ってセットアップしてください。

Scorpio は、Postgres 10 でテストおよび開発されています。

Scorpio が使用するデフォルトのユーザー名とパスワードは "ngb" です。別のユーザー名またはパスワードを使用する場合は、
StorageManager および RegistryManager を起動するときにパラメーターとしてそれらを指定する必要があります。例えば、

.. code:: console

    java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword

または

.. code:: console

    java -jar Registry/RegistryManager/target/RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar --spring.datasource.username=funkyusername --spring.datasource.password=funkypassword

postgres で対応するユーザー ("ngb" または選択した別のユーザー名) を作成することを忘れないでください。これは、
データベース接続のために Spring Cloud サービスによって使用されます。ターミナルにいる間に、postgres ユーザーとして psql
コンソールにログインします:

.. code:: console

    sudo -u postgres psql

次に、データベース "ngb" を作成します:

.. code:: console

    postgres=# create database ngb;

ユーザー "ngb" を作成し、スーパーユーザーにします:

.. code:: console

    postgres=# create user ngb with encrypted password 'ngb';
    postgres=# alter user ngb with superuser;

データベースに対する特権を付与します:

.. code:: console

    postgres=# grant all privileges on database ngb to ngb;

また、Postgis 拡張機能用の独自のデータベース/スキーマを作成します:

.. code:: console

    postgres=# CREATE DATABASE gisdb;
    postgres=# \connect gisdb;
    postgres=# CREATE SCHEMA postgis;
    postgres=# ALTER DATABASE gisdb SET search_path=public, postgis, contrib;
    postgres=# \connect gisdb;
    postgres=# CREATE EXTENSION postgis SCHEMA postgis;

Apache Kafka
------------

Scorpio は、マイクロサービス間の通信のために、`Apache Kafka <https://kafka.apache.org/>`__ を使用します。

Scorpio は、Kafka version 2.12-2.1.0 でテストおよび開発されています。

`Apache Kafka <https://kafka.apache.org/downloads>`__ をダウンロードし、Web サイトの指示に従ってください。

Kafka を開始するには、次の2つのコンポーネントを開始する必要があります。

zookeeper を開始、

.. code:: console

    <kafkafolder>/bin/[Windows]/zookeeper-server-start.[bat|sh] <kafkafolder>/config/zookeeper.properties

Kafka server を開始、

.. code:: console

    <kafkafolder>/bin/[Windows]/kafka-server-start.[bat|sh] <kafkafolder>/config/server.properties

詳細については、Kafka の `Webサイト <https://kafka.apache.org/>`__  をご覧ください 。

Docker コンテナの取得
~~~~~~~~~~~~~~~~~~~~~

現在の Maven ビルドは、Maven プロファイルを使用してビルドからトリガーする2種類の Docker コンテナ生成をサポートして
います。

最初のプロファイルは 'docker' と呼ばれ、次のように呼び出すことができます。

.. code:: console

    sudo mvn clean package -DskipTests -Pdocker

これにより、マイクロサービスごとに個別の Docker コンテナが生成されます。対応する docker-compose ファイルは
``docker-compose-dist.yml`` です。

2番目のプロファイルは 'docker-aaio' と呼ばれます (ほぼすべてが1つになっています)。これにより、Kafka メッセージバスと
postgres データベースを除くブローカーのすべてのコンポーネントに対して単一の Docker コンテナが生成されます。

aaio バージョンを取得するには、次のように Maven ビルドを実行します。

.. code:: console

    sudo mvn clean package -DskipTests -DskipDefault -Pdocker-aaio

対応する docker-compose ファイルは ``docker-compose-aaio.yml`` です。

Docker コンテナの起動
~~~~~~~~~~~~~~~~~~~~~

Docker コンテナを起動するには、対応する docker-compose ファイルを使用してください。つまり、

.. code:: console

    sudo docker-composer -f docker-compose-aaio.yml up

コンテナを適切に停止するには、

.. code:: console

    sudo docker-composer -f docker-compose-aaio.yml down

Kafka docker イメージと docker-compose に関する一般的な注意
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka docker コンテナーでは、環境変数 ``KAFKA_ADVERTISED_HOST_NAME`` を指定する必要があります。 これは、docker-compose
ファイルで Docker ホスト IP と一致するように変更する必要があります。``127.0.0.1`` を使用できますが、これにより、Kafka
をクラスターモードで 実行できなくなります。

詳細については、https://hub.docker.com/r/wurstmeister/kafka を参照してください。

Maven の外部で Docker ビルドを実行
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

jars のビルドを Docker ビルドから分離したい場合は、特定の VARS を Docker に提供する必要があります。次のリストは、
ルートディレクトリから docker build を実行した場合の、すべての変数とその意図された値を示しています。

-  ``BUILD_DIR_ACS = Core/AtContextServer``

-  ``BUILD_DIR_SCS = SpringCloudModules/config-server``

-  ``BUILD_DIR_SES = SpringCloudModules/eureka``

-  ``BUILD_DIR_SGW = SpringCloudModules/gateway``

-  ``BUILD_DIR_HMG = History/HistoryManager``

-  ``BUILD_DIR_QMG = Core/QueryManager``

-  ``BUILD_DIR_RMG = Registry/RegistryManager``

-  ``BUILD_DIR_EMG = Core/EntityManager``

-  ``BUILD_DIR_STRMG = Storage/StorageManager``

-  ``BUILD_DIR_SUBMG = Core/SubscriptionManager``

-  ``JAR_FILE_BUILD_ACS = AtContextServer-${project.version}.jar``

-  ``JAR_FILE_BUILD_SCS = config-server-${project.version}.jar``

-  ``JAR_FILE_BUILD_SES = eureka-server-${project.version}.jar``

-  ``JAR_FILE_BUILD_SGW = gateway-${project.version}.jar``

-  ``JAR_FILE_BUILD_HMG = HistoryManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_QMG = QueryManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_RMG = RegistryManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_EMG = EntityManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_STRMG = StorageManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_SUBMG = SubscriptionManager-${project.version}.jar``

-  ``JAR_FILE_RUN_ACS = AtContextServer.jar``

-  ``JAR_FILE_RUN_SCS = config-server.jar``

-  ``JAR_FILE_RUN_SES = eureka-server.jar``

-  ``JAR_FILE_RUN_SGW = gateway.jar``

-  ``JAR_FILE_RUN_HMG = HistoryManager.jar``

-  ``JAR_FILE_RUN_QMG = QueryManager.jar``

-  ``JAR_FILE_RUN_RMG = RegistryManager.jar``

-  ``JAR_FILE_RUN_EMG = EntityManager.jar``

-  ``JAR_FILE_RUN_STRMG = StorageManager.jar``

-  ``JAR_FILE_RUN_SUBMG = SubscriptionManager.jar``

コンポーネントの起動
####################

ビルド後、個々のコンポーネントを通常の Jar ファイルとして開始します。以下を実行して Spring Cloud サービスを開始します。

.. code:: console

    java -jar SpringCloudModules/eureka/target/eureka-server-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar SpringCloudModules/gateway/target/gateway-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar SpringCloudModules/config-server/target/config-server-<VERSIONNUMBER>-SNAPSHOT.jar

ブローカーコンポーネントを開始します。

.. code:: console

    java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/QueryManager/target/QueryManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Registry/RegistryManager/target/RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/EntityManager/target/EntityManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar History/HistoryManager/target/HistoryManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/SubscriptionManager/target/SubscriptionManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/AtContextServer/target/AtContextServer-<VERSIONNUMBER>-SNAPSHOT.jar

構成の変更
----------

構成可能なすべてのオプションは、application.properties ファイルにあります。それらを変更するには、2つのオプションが
あります。ビルド前にプロパティを変更するか、``--<OPTION_NAME>=<OPTION_VALUE>` を追加することでコンフィグを
上書きするか、など

.. code:: console

    java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword`

CORS サポートを有効化
---------------------

これらの設定オプションを提供することにより、ゲートウェイで cors サポートを有効にできます。- gateway.enablecors -
デフォルトは False です。一般的な有効化の場合は true に設定します - gateway.enablecors.allowall - デフォルトは False
です。すべてのオリジンからの CORS を有効にし、すべてのヘッダーとすべてのメソッドを許可するには、true に設定します。
安全ではありませんが、依然として非常に頻繁に使用されます。-gateway.enablecors.allowedorigin - 許可されたオリジンの
コンマ区切りリスト -gateway.enablecors.allowedheader - 許可されたヘッダーのコンマ区切りリス
-gateway.enablecors.allowedmethods - 許可されたメソッドのコンマ区切りリスト -gateway.enablecors.allowallmethods-
デフォルトは False です。すべてのメソッドを許可するには、true に設定します。true に設定すると、allowmethods
エントリが上書きされます。

トラブルシューティング
######################

Missing JAXB dependencies
-------------------------

eureka-server を起動すると、 **java.lang.TypeNotPresentException: Type javax.xml.bind.JAXBContext not present**  例外が
発生する場合があります。 その場合、マシンで Java 11 を実行している可能性が非常に高くなります。Java 9 パッケージ以降、
``javax.xml.bind`` は非推奨としてマークされ、Java 11 で最終的に完全に削除されました。

この問題を修正して eureka-server を実行するには、開始する前に、以下の JAXB Maven 依存関係を
``ScorpioBroker/SpringCloudModules/eureka/pom.xml`` に手動で追加する必要があります。

.. code:: xml

    ...
    <dependencies>
            ...
            <dependency>
                    <groupId>com.sun.xml.bind</groupId>
                    <artifactId>jaxb-core</artifactId>
                    <version>2.3.0.1</version>
            </dependency>
            <dependency>
                    <groupId>javax.xml.bind</groupId>
                    <artifactId>jaxb-api</artifactId>
                    <version>2.3.1</version>
            </dependency>
            <dependency>
                    <groupId>com.sun.xml.bind</groupId>
                    <artifactId>jaxb-impl</artifactId>
                    <version>2.3.1</version>
            </dependency>
            ...
    </dependencies>
    ...

これは、条件付き依存関係を使用して修正する必要があります。
