{{/*
Expand the name of the chart.
*/}}
{{- define "scorpio-broker-dist.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "scorpio-broker-dist.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}


{{- define "atContextServer.fullname" -}}
{{- if .Values.atContextServer.fullnameOverride }}
{{- .Values.atContextServer.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.atContextServer.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.atContextServer.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "configServer.fullname" -}}
{{- if .Values.configServer.fullnameOverride }}
{{- .Values.configServer.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.configServer.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.configServer.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "entityManager.fullname" -}}
{{- if .Values.entityManager.fullnameOverride }}
{{- .Values.entityManager.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.entityManager.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.entityManager.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "gateway.fullname" -}}
{{- if .Values.gateway.fullnameOverride }}
{{- .Values.gateway.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.gateway.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.gateway.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "eureka.fullname" -}}
{{- if .Values.eureka.fullnameOverride }}
{{- .Values.eureka.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.eureka.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.eureka.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "historyManager.fullname" -}}
{{- if .Values.historyManager.fullnameOverride }}
{{- .Values.historyManager.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.historyManager.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.historyManager.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "queryManager.fullname" -}}
{{- if .Values.queryManager.fullnameOverride }}
{{- .Values.queryManager.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.queryManager.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.queryManager.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "registryManager.fullname" -}}
{{- if .Values.registryManager.fullnameOverride }}
{{- .Values.registryManager.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.registryManager.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.registryManager.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "registrySubscriptionManager.fullname" -}}
{{- if .Values.registrySubscriptionManager.fullnameOverride }}
{{- .Values.registrySubscriptionManager.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.registrySubscriptionManager.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.registrySubscriptionManager.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "subscriptionManager.fullname" -}}
{{- if .Values.subscriptionManager.fullnameOverride }}
{{- .Values.subscriptionManager.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.subscriptionManager.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.subscriptionManager.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "scorpio-broker-dist.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}


{{/*
Common labels
*/}}
{{- define "scorpio-broker-dist.common.matchLabels" -}}
app: {{ template "scorpio-broker-dist.name" . }}
release: {{ .Release.Name }}
{{- end -}}

{{- define "scorpio-broker-dist.common.metaLabels" -}}
chart: {{ template "scorpio-broker-dist.chart" . }}
heritage: {{ .Release.Service }}
{{- end -}}

{{- define "atContextServer.labels" -}}
{{ include "atContextServer.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "atContextServer.matchLabels" -}}
component: {{ .Values.atContextServer.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "configServer.labels" -}}
{{ include "configServer.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "configServer.matchLabels" -}}
component: {{ .Values.configServer.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "eureka.labels" -}}
{{ include "eureka.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "eureka.matchLabels" -}}
component: {{ .Values.eureka.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "gateway.labels" -}}
{{ include "gateway.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "gateway.matchLabels" -}}
component: {{ .Values.gateway.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "entityManager.labels" -}}
{{ include "entityManager.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "entityManager.matchLabels" -}}
component: {{ .Values.entityManager.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "historyManager.labels" -}}
{{ include "historyManager.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "historyManager.matchLabels" -}}
component: {{ .Values.historyManager.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "queryManager.labels" -}}
{{ include "queryManager.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "queryManager.matchLabels" -}}
component: {{ .Values.queryManager.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "registryManager.labels" -}}
{{ include "registryManager.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}


{{- define "registryManager.matchLabels" -}}
component: {{ .Values.registryManager.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "registrySubscriptionManager.labels" -}}
{{ include "registrySubscriptionManager.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}


{{- define "registrySubscriptionManager.matchLabels" -}}
component: {{ .Values.registrySubscriptionManager.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}

{{- define "subscriptionManager.labels" -}}
{{ include "subscriptionManager.matchLabels" . }}
{{ include "scorpio-broker-dist.common.metaLabels" . }}
{{- end -}}

{{- define "subscriptionManager.matchLabels" -}}
component: {{ .Values.subscriptionManager.name | quote }}
{{ include "scorpio-broker-dist.common.matchLabels" . }}
{{- end -}}


{{/*
Selector labels
*/}}
{{- define "scorpio-broker-dist.selectorLabels" -}}
app.kubernetes.io/name: {{ include "scorpio-broker-dist.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "scorpio-broker-dist.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "scorpio-broker-dist.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
