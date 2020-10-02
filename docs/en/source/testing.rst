Tests
=====

Scorpio has two sets of tests. We use JUnit for unit tests and the
FIWARE NGSI-LD Testsuite, which is npm test based, for system tests.

Running unit tests
------------------

A lot of the logic within Scorpio is intertwined with Kafka. Hence a lot
of the unit tests require a running Kafka instance. Start the Kafka
server and zookeeper as described in the Installation chapter. You can
run tests explicitly through Maven with the goal test by running

.. code:: console

    mvn test

Unless you add a -DskipTests to your Maven command, tests will also be
run with the goals package, install, verify and deploy. You can run all
the tests by running the Maven command from the root directory or
individual tests by running the Maven command in the corresponding
directory.

FIWARE NGSI-LD Testsuite
------------------------

In order to run the Testsuite you have to have a running instance of
Scorpio as described in the Start the components chapter or use the
dockercontainer You can find the Testsuite here with full instructions
on how to setup and start the Testsuite. The comprehensive version is
this: - Install npm on your system - Download the Testsuite from
`here <https://github.com/FIWARE/NGSI-LD_TestSuite/archive/master.zip>`__
- extract the Testsuite - run npm install in the Testsuite folder to
install all dependencies - you need to set 4 environment vars -
TEST\_ENDPOINT, which is the broker. So default should be
http://localhost:9090 for Scorpio - WEB\_APP\_PORT, port for the
Testsuite. This should match the port in all below. E.g. 4444 -
ACC\_ENDPOINT, the endpoint for the testsuit, e.g. http://localhost:4444
- NOTIFY\_ENDPOINT, the notification endpoint for the tests. Has to end
with /acc. E.g. http://localhost:4444/acc - start Scorpio - start the
accumulator/notification endpoint by running
``console    node accumulator/accumulator.js &'`` - start the tests with
``console     npm test'``
