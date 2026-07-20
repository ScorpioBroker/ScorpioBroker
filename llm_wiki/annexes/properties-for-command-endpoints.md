---
type: Reference
title: "Properties for command endpoints"
description: "For each available command, a set of three endpoints is to be additionally created within the NGSI-LD system, by means of three dedicated Properties per command."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-H.3.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "H.3.2"
---

For each available command, a set of three endpoints is to be additionally created within the NGSI-LD system, by
means of three dedicated Properties per command. The first endpoint will manage that command's requests, the second
endpoint will manage its status, and the third endpoint will manage command's results.
This convention dictates that:

- The NGSI-LD Property that manages requests has the same name as the command, e.g. "<cmd_name1>".

- The NGSI-LD Property that manages results has the same name as the command plus the "-RESULT" suffix.

- The NGSI-LD Property that manages status has the same name as the command plus the "-STATUS" suffix. Each endpoint can receive multiple requests or responses, and it supports queueing of messages. For example, the command moveToLocation may receive a sequence of requests that are to be stored in an array and orderly processed depending on the arrival timestamps. A number of respective responses may be produced, as well. Thus, each endpoint, represented by its dedicated NGSI-LD Property, exploits the multi-Attribute feature (see clause 4.5.5), as follows: Command Request endpoint

"<cmd_name>": { "datasetId": a URI uniquely identifying the specific command request (optional, if the use case does not need command queueing), "type": "Property", "qos": an Integer, representing the desired QoS (optional, default=0), "value": custom parameters of the command (mandatory)

}

Command Status endpoint

"<cmd_name>-STATUS": { "datasetId": a URI uniquely identifying the specific status feedback message (optional, if the use case does not need queueing them), "type": "Property", "value": custom status of the command (mandatory)

}

Command Result endpoint

"<cmd_name>-RESULT": { "datasetId": a URI uniquely identifying the specific result feedback message (optional, if the use case does not need queueing them), "type": "Property", "value": custom result of the command (mandatory)

}
Usually, the Context Adapter (or the actuator behind it), upon receiving a command request with a specific datasetId,
will then generate status and result with the same datasetId, so that, when the status/result is received by the application,
it can link it back to the corresponding command that is generating the received feedback. The value of the request,
status and result is generic, and it is up to the specific application to define useful values. As an example, the PackML
language for the control of packaging machines defines a set of possible values for statuses during an actuation
workflow.


EXAMPLE 1: An example follows, where the NGSI-LD system represents a simple actuator. The example shows the NGSI-LD Entity representing a light that can change colour by manipulation of its brightness, hue and saturation values; further, it is possible to turn the lamp on and off. Apart from the id and the type, the actuator entity has a set of regular properties that represent the current status of the lamp. In the example these are colorRGB and is-on. Then it has the conventional Property named commands, signalling that it supports four commands: "turn-on", "set-saturation", "set-brightness", "set-hue". Further, it has four (times three) additional properties serving the purpose of command endpoints.

{

"id": "urn:ngsi-ld:pHueActuator:light1", "type": "Lamp",

REGULAR PROPERTIES "colorRGB": {"type": "Property", "value": "0xABABAB"}, "is-on": {"type": "Property", "value": true},

AVAILABLE COMMANDS
"commands": {
"type": "Property",
"value": ["turn-on", "set-saturation", "set-hue", "set-brightness"]

}

COMMAND ENDPOINTS
"turn-on": {"type": "Property", "value":}
"turn-on-STATUS": {"type": "Property", "value":}
"turn-on-RESULT": {"type": "Property", "value":}
"set-hue": ...
"set-hue-STATUS": ...
"set-hue-RESULT": ...
…

}

EXAMPLE 2: The following example shows an NGSI-LD Entity Fragment that can be used as a command

request to request that the lamp be turned off.

{

"id": "urn:ngsi-ld:pHueActuator:light1",
"type": "Lamp",
"turn-on": {
"type": "Property",
"qos": {
"type": "Property",
"value": 1
},
"value": false

} }

EXAMPLE 3: In the following example, the value of the command request is a more complex JSON Object, to show that complex actions can be conveyed by just one request. Further, the request has an identifier that makes it possible to enqueue it, together with other request that may arrive to the

same command endpoint within a timespan.

{

"id": "urn:ngsi-ld:pHueActuator:light1",
"type": "Lamp",
"set-hue": {
"type": "Property",
"qos": {
"type": "Property",
"value": 1
},
"datasetId": "myapp:mycommand:1342",
"value": {"red": "1 seconds", "green": "2 seconds"}

} }


In summary, the suggested convention prescribes a commands property that contains a list of commands supported by the actuator. For each of these commands, the convention requires a command endpoint consisting of three properties, the name of the command, e.g. "turn-on", the status property, which is the name of the command with "-STATUS" as suffix, and the result, which is the name of the command with "-RESULT" as suffix. Nevertheless, it is noted that such suffixes are just a convention to distinguish the endpoints. So far, two practical implementations exist, see clauses H.5 and H.6, that adopt the general scheme of this convention, with minor deviations. In fact, this convention is derived as a generalization that leverages the full potential of NGSI-LD sub-Attributes and multi-Attributes.

# Related

* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause H.3.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Properties for command endpoints
