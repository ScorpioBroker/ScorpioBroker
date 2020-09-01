*****************
Troubleshooting
*****************

Missing JAXB dependencies
=========================

When starting the eureka-server you may be facing the **java.lang.TypeNotPresentException: Type javax.xml.bind.JAXBContext not present** exception. It's very likely that you are running Java 11 on your machine then. Starting from Java 9 package `javax.xml.bind` has been marked deprecated and was finally completely removed in Java 11.

In order to fix this issue and get eureka-server running you need to manually add below JAXB Maven dependencies to `ScorpioBroker/SpringCloudModules/eureka/pom.xml` before starting:

.. code-block:: xml

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