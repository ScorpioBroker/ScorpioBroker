Tests
=====

Scorpio には2セットのテストがあります。ユニットテストには JUnit を使用し、システムテストには npm test ベースの FIWARE
NGSI-LD Testsuite を使用します。

ユニットテストの実行
--------------------

Scorpio 内のロジックの多くは、Kafka と絡み合っています。したがって、多くの単体テストでは、実行中の Kafka インスタンスが
必要です。インストールの章の説明に従って、Kafka サーバーと zookeeper を起動します。実行することにより、goal test
を使用して Maven を介して明示的にテストを実行できます。

.. code:: console

    mvn test

Maven コマンドに -DskipTests を追加しない限り、テストは、goals パッケージ、インストール、検証、およびデプロイを使用
して実行されます。ルートディレクトリから Maven コマンドを実行してすべてのテストを実行することも、対応するディレクトリで
Maven コマンドを実行して個々のテストを実行することもできます。

FIWARE NGSI-LD Testsuite
------------------------

Testsuite を実行するには、コンポーネントの開始の章で説明されているようにScorpio のインスタンスを実行するか、
dockercontainer を使用する必要があります。Testsuite は、Testsuite をセットアップして開始する方法の完全な手順とともに
ここにあります。包括的なバージョンは次のとおりです。システムに npm をインストールします。
`ここ <https://github.com/FIWARE/NGSI-LD_TestSuite/archive/master.zip>`__ から Testsuite をダウンロードします。
Testsuite を抽出します。npm install を実行します。Testsuite フォルダーにすべての依存関係をインストールします。
4つの環境変数を設定する必要があります。TEST\_ENDPOINT はブローカーです。したがって、Scorpio の場合、デフォルトは
http://localhost:9090 である必要があります。WEB\_APP\_PORT は、Testsuite のポートです。これは、以下のすべてのポートと
一致する必要があります。 例えば、4444 - ACC\_ENDPOINT は Testsuit のエンドポイントです。例えば、
http://localhost:4444 - NOTIFY\_ENDPOINT は、テストのノーティフィケーションのエンドポイントです。/acc で終了する必要が
あります。Scorpio を起動します。``console    node accumulator/accumulator.js &'`` を実行して、accumulator/notification
エンドポイントを起動します。``console     npm test'`` でテストを開始します。
