*********************
Docker コンテナの取得
*********************

現在の Maven ビルドは、Maven プロファイルを使用してビルドからトリガーする2種類の Docker コンテナー生成をサポートして
います。

最初のプロファイルは 'docker' と呼ばれ、次のように呼び出すことができます:

::

	mvn clean package -DskipTests -Pdocker

これにより、マイクロサービスごとに個別の Docker コンテナが生成されます。対応する docker-compose ファイルは
`docker-compose-dist.yml` です。

2番目のプロファイルは 'docker-aaio' と呼ばれます (ほぼすべてが1つになっています)。これにより、Kafka メッセージバスと
Postgres データベースを除くブローカーのすべてのコンポーネントに対して単一の Docker コンテナーが生成されます。

aaio バージョンを取得するには、次のように Maven ビルドを実行します:

::

	mvn clean package -DskipTests -Pdocker-aaio

対応するdocker-compose ファイルは `docker-compose-aaio.yml` です。

Kafkadocker イメージと docker-compose に関する一般的な注意
==========================================================

Kafka Docker コンテナーでは、環境変数 `KAFKA_ADVERTISED_HOST_NAME` を指定する必要があります。これは、docker-compose
ファイルで Docker ホスト IP と一致するように変更する必要があります。`127.0.0.1` を使用できますが、これにより、Kafka
をクラスターモードで実行できなくなります。

詳細については、https://hub.docker.com/r/wurstmeister/kafka を参照してください。

Maven の外部で Docker ビルドを実行
==================================

jar のビルドを Docker ビルドから分離したい場合は、特定の VARS を Docker に提供する必要があります。次のリストは、
ルートディレクトリから docker build を実行した場合の、すべての変数とその意図された値を示しています。
  
 - BUILD_DIR_ACS = Core/AtContextServer
 
 - BUILD_DIR_SCS = SpringCloudModules/config-server
 
 - BUILD_DIR_SES = SpringCloudModules/eureka
 
 - BUILD_DIR_SGW = SpringCloudModules/gateway
 
 - BUILD_DIR_HMG = History/HistoryManager
 
 - BUILD_DIR_QMG = Core/QueryManager
 
 - BUILD_DIR_RMG = Registry/RegistryManager
 
 - BUILD_DIR_EMG = Core/EntityManager
 
 - BUILD_DIR_STRMG = Storage/StorageManager
 
 - BUILD_DIR_SUBMG = Core/SubscriptionManager

 - JAR_FILE_BUILD_ACS = AtContextServer-${project.version}.jar
 
 - JAR_FILE_BUILD_SCS = config-server-${project.version}.jar
 
 - JAR_FILE_BUILD_SES = eureka-server-${project.version}.jar
 
 - JAR_FILE_BUILD_SGW = gateway-${project.version}.jar
 
 - JAR_FILE_BUILD_HMG = HistoryManager-${project.version}.jar
 
 - JAR_FILE_BUILD_QMG = QueryManager-${project.version}.jar
 
 - JAR_FILE_BUILD_RMG = RegistryManager-${project.version}.jar
 
 - JAR_FILE_BUILD_EMG = EntityManager-${project.version}.jar
 
 - JAR_FILE_BUILD_STRMG = StorageManager-${project.version}.jar
 
 - JAR_FILE_BUILD_SUBMG = SubscriptionManager-${project.version}.jar

 - JAR_FILE_RUN_ACS = AtContextServer.jar
 
 - JAR_FILE_RUN_SCS = config-server.jar
 
 - JAR_FILE_RUN_SES = eureka-server.jar
 
 - JAR_FILE_RUN_SGW = gateway.jar
 
 - JAR_FILE_RUN_HMG = HistoryManager.jar
 
 - JAR_FILE_RUN_QMG = QueryManager.jar
 
 - JAR_FILE_RUN_RMG = RegistryManager.jar
 
 - JAR_FILE_RUN_EMG = EntityManager.jar
 
 - JAR_FILE_RUN_STRMG = StorageManager.jar
 
 - JAR_FILE_RUN_SUBMG = SubscriptionManager.jar
