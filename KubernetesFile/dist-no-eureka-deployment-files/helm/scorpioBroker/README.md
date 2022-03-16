# scorpioBroker

![Version: 0.1.2](https://img.shields.io/badge/Version-0.1.2-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 2.1.0](https://img.shields.io/badge/AppVersion-2.1.0-informational?style=flat-square)

A Helm chart for Kubernetes in which every microservices has its own container and hence have reserved resources for it and virtually isolated from others and best for production.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| atContextServer.contextLabel | string | `"at-context-server"` |  |
| atContextServer.enabled | bool | `true` |  |
| atContextServer.hpa.enabled | bool | `true` |  |
| atContextServer.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| atContextServer.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| atContextServer.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| atContextServer.image.pullPolicy | string | `"Always"` |  |
| atContextServer.image.repository | string | `"scorpiobroker/scorpio"` |  |
| atContextServer.image.tag | string | `"AtContextServer_2.1.0"` | tag of the image to be used |
| atContextServer.livenessProbe.enabled | bool | `true` |  |
| atContextServer.livenessProbe.failureThreshold | int | `6` |  |
| atContextServer.livenessProbe.initialDelaySeconds | int | `40` |  |
| atContextServer.livenessProbe.periodSeconds | int | `10` |  |
| atContextServer.name | string | `"at-context-server"` |  |
| atContextServer.readinessProbe.enabled | bool | `true` |  |
| atContextServer.readinessProbe.failureThreshold | int | `6` |  |
| atContextServer.readinessProbe.initialDelaySeconds | int | `40` |  |
| atContextServer.readinessProbe.periodSeconds | int | `10` |  |
| atContextServer.replicas | int | `1` |  |
| atContextServer.resources | object | `{}` |  |
| atContextServer.restartPolicy | string | `"Always"` |  |
| atContextServer.securityContext | object | `{}` |  |
| atContextServer.serviceAccount.enabled | bool | `false` |  |
| atContextServer.serviceAccount.name | string | `""` |  |
| atContextServer.terminationGracePeriodSeconds | int | `30` |  |
| atContextServer.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| atContextServer.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| atContextServer.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| atContextServer.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| configServer.configLabel | string | `"config-server"` |  |
| configServer.enabled | bool | `true` |  |
| configServer.hpa.enabled | bool | `true` |  |
| configServer.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| configServer.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| configServer.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| configServer.image.pullPolicy | string | `"Always"` |  |
| configServer.image.repository | string | `"scorpiobroker/scorpio"` |  |
| configServer.image.tag | string | `"config-server_1.1.0"` | tag of the image to be used |
| configServer.livenessProbe.enabled | bool | `true` |  |
| configServer.livenessProbe.failureThreshold | int | `6` |  |
| configServer.livenessProbe.initialDelaySeconds | int | `40` |  |
| configServer.livenessProbe.periodSeconds | int | `10` |  |
| configServer.name | string | `"config-server"` |  |
| configServer.readinessProbe.enabled | bool | `true` |  |
| configServer.readinessProbe.failureThreshold | int | `6` |  |
| configServer.readinessProbe.initialDelaySeconds | int | `40` |  |
| configServer.readinessProbe.periodSeconds | int | `10` |  |
| configServer.replicas | int | `1` |  |
| configServer.resources | object | `{}` |  |
| configServer.restartPolicy | string | `"Always"` |  |
| configServer.securityContext | object | `{}` |  |
| configServer.serviceAccount.enabled | bool | `false` |  |
| configServer.serviceAccount.name | string | `""` |  |
| configServer.terminationGracePeriodSeconds | int | `30` |  |
| configServer.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| configServer.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| configServer.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| configServer.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| entityManager.enabled | bool | `true` |  |
| entityManager.entityLabel | string | `"entity-manager"` |  |
| entityManager.hpa.enabled | bool | `true` |  |
| entityManager.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| entityManager.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| entityManager.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| entityManager.image.pullPolicy | string | `"Always"` |  |
| entityManager.image.repository | string | `"scorpiobroker/scorpio"` |  |
| entityManager.image.tag | string | `"EntityManager_2.1.0"` | tag of the image to be used |
| entityManager.livenessProbe.enabled | bool | `true` |  |
| entityManager.livenessProbe.failureThreshold | int | `6` |  |
| entityManager.livenessProbe.initialDelaySeconds | int | `40` |  |
| entityManager.livenessProbe.periodSeconds | int | `10` |  |
| entityManager.name | string | `"entity-manager"` |  |
| entityManager.readinessProbe.enabled | bool | `true` |  |
| entityManager.readinessProbe.failureThreshold | int | `6` |  |
| entityManager.readinessProbe.initialDelaySeconds | int | `40` |  |
| entityManager.readinessProbe.periodSeconds | int | `10` |  |
| entityManager.replicas | int | `1` |  |
| entityManager.resources | object | `{}` |  |
| entityManager.restartPolicy | string | `"Always"` |  |
| entityManager.securityContext | object | `{}` |  |
| entityManager.serviceAccount.enabled | bool | `false` |  |
| entityManager.serviceAccount.name | string | `""` |  |
| entityManager.terminationGracePeriodSeconds | int | `30` |  |
| entityManager.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| entityManager.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| entityManager.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| entityManager.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| eureka.enabled | bool | `true` |  |
| eureka.eurekaLabel | string | `"eureka"` |  |
| eureka.hpa.enabled | bool | `true` |  |
| eureka.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| eureka.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| eureka.hpa.targetCPUUtilizationPercentage | int | `80` | average CPU usage across all the pods exceeds 80%, HPA will spin up additional pods. |
| eureka.image.pullPolicy | string | `"Always"` | specification of the image pull policy |
| eureka.image.repository | string | `"scorpiobroker/scorpio"` |  |
| eureka.image.tag | string | `"eureka-server_2.1.0"` |  |
| eureka.livenessProbe.enabled | bool | `true` |  |
| eureka.livenessProbe.failureThreshold | int | `6` |  |
| eureka.livenessProbe.initialDelaySeconds | int | `40` |  |
| eureka.livenessProbe.periodSeconds | int | `10` |  |
| eureka.name | string | `"eureka"` |  |
| eureka.readinessProbe.enabled | bool | `true` |  |
| eureka.readinessProbe.failureThreshold | int | `6` |  |
| eureka.readinessProbe.initialDelaySeconds | int | `40` |  |
| eureka.readinessProbe.periodSeconds | int | `10` |  |
| eureka.replicas | int | `1` |  |
| eureka.resources | object | `{}` |  |
| eureka.restartPolicy | string | `"Always"` |  |
| eureka.securityContext | object | `{}` |  |
| eureka.service.nodePort | int | `30000` | port to be used by the service |
| eureka.service.port | int | `8761` | port to be used by the service |
| eureka.service.type | string | `"NodePort"` | service type |
| eureka.serviceAccount.enabled | bool | `false` |  |
| eureka.serviceAccount.name | string | `""` |  |
| eureka.terminationGracePeriodSeconds | int | `30` |  |
| eureka.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| eureka.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| eureka.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| eureka.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| gateway.enabled | bool | `true` |  |
| gateway.gatewayLabel | string | `"gateway"` |  |
| gateway.hpa.enabled | bool | `true` |  |
| gateway.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| gateway.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| gateway.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| gateway.image.pullPolicy | string | `"Always"` |  |
| gateway.image.repository | string | `"scorpiobroker/scorpio"` |  |
| gateway.image.tag | string | `"gateway_2.1.0"` |  |
| gateway.livenessProbe.enabled | bool | `true` |  |
| gateway.livenessProbe.failureThreshold | int | `6` |  |
| gateway.livenessProbe.initialDelaySeconds | int | `40` |  |
| gateway.livenessProbe.periodSeconds | int | `10` |  |
| gateway.name | string | `"gateway"` |  |
| gateway.readinessProbe.enabled | bool | `true` |  |
| gateway.readinessProbe.failureThreshold | int | `6` |  |
| gateway.readinessProbe.initialDelaySeconds | int | `40` |  |
| gateway.readinessProbe.periodSeconds | int | `10` |  |
| gateway.replicas | int | `1` |  |
| gateway.resources | object | `{}` |  |
| gateway.restartPolicy | string | `"Always"` |  |
| gateway.securityContext | object | `{}` |  |
| gateway.serviceAccount.enabled | bool | `false` |  |
| gateway.serviceAccount.name | string | `""` |  |
| gateway.terminationGracePeriodSeconds | int | `30` |  |
| gateway.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| gateway.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| gateway.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| gateway.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| historyManager.enabled | bool | `true` |  |
| historyManager.historyLabel | string | `"history-manager"` |  |
| historyManager.hpa.enabled | bool | `true` |  |
| historyManager.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| historyManager.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| historyManager.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| historyManager.image.pullPolicy | string | `"Always"` |  |
| historyManager.image.repository | string | `"scorpiobroker/scorpio"` |  |
| historyManager.image.tag | string | `"HistoryManager_2.1.0"` | tag of the image to be used |
| historyManager.livenessProbe.enabled | bool | `true` |  |
| historyManager.livenessProbe.failureThreshold | int | `6` |  |
| historyManager.livenessProbe.initialDelaySeconds | int | `40` |  |
| historyManager.livenessProbe.periodSeconds | int | `10` |  |
| historyManager.name | string | `"history-manager"` |  |
| historyManager.readinessProbe.enabled | bool | `true` |  |
| historyManager.readinessProbe.failureThreshold | int | `6` |  |
| historyManager.readinessProbe.initialDelaySeconds | int | `40` |  |
| historyManager.readinessProbe.periodSeconds | int | `10` |  |
| historyManager.replicas | int | `1` |  |
| historyManager.resources | object | `{}` |  |
| historyManager.restartPolicy | string | `"Always"` |  |
| historyManager.securityContext | object | `{}` |  |
| historyManager.serviceAccount.enabled | bool | `false` |  |
| historyManager.serviceAccount.name | string | `""` |  |
| historyManager.terminationGracePeriodSeconds | int | `30` |  |
| historyManager.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| historyManager.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| historyManager.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| historyManager.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| namespace | string | `"scorpio-broker"` |  |
| queryManager.enabled | bool | `true` |  |
| queryManager.hpa.enabled | bool | `true` |  |
| queryManager.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| queryManager.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| queryManager.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| queryManager.image.pullPolicy | string | `"Always"` |  |
| queryManager.image.repository | string | `"scorpiobroker/scorpio"` |  |
| queryManager.image.tag | string | `"QueryManager_2.1.0"` | tag of the image to be used |
| queryManager.livenessProbe.enabled | bool | `true` |  |
| queryManager.livenessProbe.failureThreshold | int | `6` |  |
| queryManager.livenessProbe.initialDelaySeconds | int | `40` |  |
| queryManager.livenessProbe.periodSeconds | int | `10` |  |
| queryManager.name | string | `"query-manager"` |  |
| queryManager.queryLabel | string | `"query-manager"` |  |
| queryManager.readinessProbe.enabled | bool | `true` |  |
| queryManager.readinessProbe.failureThreshold | int | `6` |  |
| queryManager.readinessProbe.initialDelaySeconds | int | `40` |  |
| queryManager.readinessProbe.periodSeconds | int | `10` |  |
| queryManager.replicas | int | `1` |  |
| queryManager.resources | object | `{}` |  |
| queryManager.restartPolicy | string | `"Always"` |  |
| queryManager.securityContext | object | `{}` |  |
| queryManager.serviceAccount.enabled | bool | `false` |  |
| queryManager.serviceAccount.name | string | `""` |  |
| queryManager.terminationGracePeriodSeconds | int | `30` |  |
| queryManager.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| queryManager.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| queryManager.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| queryManager.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| registryManager.enabled | bool | `true` |  |
| registryManager.hpa.enabled | bool | `true` |  |
| registryManager.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| registryManager.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| registryManager.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| registryManager.image.pullPolicy | string | `"Always"` |  |
| registryManager.image.repository | string | `"scorpiobroker/scorpio"` |  |
| registryManager.image.tag | string | `"RegistryManager_2.1.0"` | tag of the image to be used |
| registryManager.livenessProbe.enabled | bool | `true` |  |
| registryManager.livenessProbe.failureThreshold | int | `6` |  |
| registryManager.livenessProbe.initialDelaySeconds | int | `40` |  |
| registryManager.livenessProbe.periodSeconds | int | `10` |  |
| registryManager.name | string | `"registry-manager"` |  |
| registryManager.readinessProbe.enabled | bool | `true` |  |
| registryManager.readinessProbe.failureThreshold | int | `6` |  |
| registryManager.readinessProbe.initialDelaySeconds | int | `40` |  |
| registryManager.readinessProbe.periodSeconds | int | `10` |  |
| registryManager.registryLabel | string | `"registry-manager"` |  |
| registryManager.replicas | int | `1` |  |
| registryManager.resources | object | `{}` |  |
| registryManager.restartPolicy | string | `"Always"` |  |
| registryManager.securityContext | object | `{}` |  |
| registryManager.serviceAccount.enabled | bool | `false` |  |
| registryManager.serviceAccount.name | string | `""` |  |
| registryManager.terminationGracePeriodSeconds | int | `30` |  |
| registryManager.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| registryManager.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| registryManager.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| registryManager.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| registrySubscriptionManager.enabled | bool | `true` |  |
| registrySubscriptionManager.hpa.enabled | bool | `true` |  |
| registrySubscriptionManager.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| registrySubscriptionManager.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| registrySubscriptionManager.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| registrySubscriptionManager.image.pullPolicy | string | `"Always"` |  |
| registrySubscriptionManager.image.repository | string | `"scorpiobroker/scorpio"` |  |
| registrySubscriptionManager.image.tag | string | `"RegistrySubscriptionManager_2.1.0"` | tag of the image to be used |
| registrySubscriptionManager.livenessProbe.enabled | bool | `true` |  |
| registrySubscriptionManager.livenessProbe.failureThreshold | int | `6` |  |
| registrySubscriptionManager.livenessProbe.initialDelaySeconds | int | `40` |  |
| registrySubscriptionManager.livenessProbe.periodSeconds | int | `10` |  |
| registrySubscriptionManager.name | string | `"registry-subscription-manager"` |  |
| registrySubscriptionManager.readinessProbe.enabled | bool | `true` |  |
| registrySubscriptionManager.readinessProbe.failureThreshold | int | `6` |  |
| registrySubscriptionManager.readinessProbe.initialDelaySeconds | int | `40` |  |
| registrySubscriptionManager.readinessProbe.periodSeconds | int | `10` |  |
| registrySubscriptionManager.registryLabel | string | `"registry-subscription-manager"` |  |
| registrySubscriptionManager.replicas | int | `1` |  |
| registrySubscriptionManager.resources | object | `{}` |  |
| registrySubscriptionManager.restartPolicy | string | `"Always"` |  |
| registrySubscriptionManager.securityContext | object | `{}` |  |
| registrySubscriptionManager.serviceAccount.enabled | bool | `false` |  |
| registrySubscriptionManager.serviceAccount.name | string | `""` |  |
| registrySubscriptionManager.terminationGracePeriodSeconds | int | `30` |  |
| registrySubscriptionManager.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| registrySubscriptionManager.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| registrySubscriptionManager.updateStrategy.type | string | `"RollingUpdate"` | type of the update |
| subscriptionManager.enabled | bool | `true` |  |
| subscriptionManager.hpa.enabled | bool | `true` |  |
| subscriptionManager.hpa.maxReplicas | int | `5` | maximum number of pods that can be set by the autoscaler |
| subscriptionManager.hpa.minReplicas | int | `1` | minimum number of replicas to which the autoscaler can scale down |
| subscriptionManager.hpa.targetCPUUtilizationPercentage | int | `80` |  |
| subscriptionManager.image.pullPolicy | string | `"Always"` |  |
| subscriptionManager.image.repository | string | `"scorpiobroker/scorpio"` |  |
| subscriptionManager.image.tag | string | `"SubscriptionManager_2.1.0"` | tag of the image to be used |
| subscriptionManager.livenessProbe.enabled | bool | `true` |  |
| subscriptionManager.livenessProbe.failureThreshold | int | `6` |  |
| subscriptionManager.livenessProbe.initialDelaySeconds | int | `40` |  |
| subscriptionManager.livenessProbe.periodSeconds | int | `10` |  |
| subscriptionManager.name | string | `"subscription-manager"` |  |
| subscriptionManager.readinessProbe.enabled | bool | `true` |  |
| subscriptionManager.readinessProbe.failureThreshold | int | `6` |  |
| subscriptionManager.readinessProbe.initialDelaySeconds | int | `40` |  |
| subscriptionManager.readinessProbe.periodSeconds | int | `10` |  |
| subscriptionManager.replicas | int | `1` |  |
| subscriptionManager.resources | object | `{}` |  |
| subscriptionManager.restartPolicy | string | `"Always"` |  |
| subscriptionManager.securityContext | object | `{}` |  |
| subscriptionManager.serviceAccount.enabled | bool | `false` |  |
| subscriptionManager.serviceAccount.name | string | `""` |  |
| subscriptionManager.subscriptionLabel | string | `"subscription-manager"` |  |
| subscriptionManager.terminationGracePeriodSeconds | int | `30` |  |
| subscriptionManager.updateStrategy.rollingUpdate | object | `{"maxSurge":"25%","maxUnavailable":"25%"}` | new pods will be added gradually |
| subscriptionManager.updateStrategy.rollingUpdate.maxSurge | string | `"25%"` | number of pods that can be created above the desired amount while updating |
| subscriptionManager.updateStrategy.rollingUpdate.maxUnavailable | string | `"25%"` | number of pods that can be unavailable while updating |
| subscriptionManager.updateStrategy.type | string | `"RollingUpdate"` | type of the update |

