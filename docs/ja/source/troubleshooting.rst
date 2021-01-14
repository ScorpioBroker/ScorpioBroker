*****************
トラブルシューティング
*****************

Missing JAXB dependencies
=========================

eureka-server を起動すると、**java.lang.TypeNotPresentException: Type javax.xml.bind.JAXBContext not present** 例外が
発生する可能性があります。その場合、マシンで Java 11 を実行している可能性が非常に高くなります。Java 9 パッケージ以降、
`javax.xml.bind` は非推奨としてマークされ、Java 11 で最終的に完全に削除されました。

この問題を修正して eureka-server を実行するには、開始する前に、以下の JAXB Maven 依存関係を
`ScorpioBroker/SpringCloudModules/eureka/pom.xml` に手動で追加する必要があります:


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
