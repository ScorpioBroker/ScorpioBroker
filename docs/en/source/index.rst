=====================
Scorpio Broker
=====================

Scorpio broker implements the NGSI APIs through which the producers and consumers can interact with each other. For Example in the typical IoT based room, various sensors like temperature sensors, light sensors, etc are connected to the central application which uses those sensors output and acts as the consumer. There can be a lot of use cases for this central application i.e Scorpio. 

1. Scorpio doesn't encode the data for storing so it can be used as a data lake.

2. Scorpio provides several interfaces for querying the stored data so easily analytics can be done on the stored data. like it can be used to predict the situation of an ecosystem. Example:- In a huge building there can be several fire sensors, temperature sensors, and smoke sensors. In case of a false fire alarm, it can be verified by the collected fire data, temperature data and smoke data of the particular area. 

3. Scorpio can be used for determining the accuracy of any event. For example, In an automated car, the speed of the car can be known by several applications like GPS, speed camera and speedometer. Scorpio's internal data is stored in this way that any third-party application can use it and can find the accuracy and determine faulty device.

.. figure:: figures/useCaseDiagram.png

.. toctree::
    :maxdepth: 1
    :caption: Introduction
    :numbered:

    introduction.rst

.. toctree::
    :maxdepth: 1
    :caption: Beginner Guide
    :numbered:

    onepageTutorial.rst

.. toctree::
    :maxdepth: 1
    :caption: Developer Guide
    :numbered:

	installationGuide.rst
    hardwareRequirement.rst
    errorHandling.rst
    security.rst
	multivalue.rst

.. toctree::
    :maxdepth: 1
    :caption: Advance User Guide
    :numbered:

    systemOverview.rst
    callFlow.rst
    contributionGuideline.md
    API_walkthrough.rst
    docker.rst
    troubleshooting.rst
    
.. toctree::
    :maxdepth: 1
    :caption: Step-By-Step for NGSI-LD
    :numbered:

    linked-data.md
    relationships-linked-data.md
    working-with-linked-data.md
    ld-subscriptions-registrations.md
 