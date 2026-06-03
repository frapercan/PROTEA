{{/*
Expand the name of the chart.
*/}}
{{- define "protea.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this
(by the DNS naming spec).
*/}}
{{- define "protea.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Common labels applied to every object the chart manages.
*/}}
{{- define "protea.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "protea.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: protea
{{- end -}}

{{/*
Selector labels for a given component (postgres, rabbitmq, api, worker-jobs, ...).
Call as: include "protea.selectorLabels" (dict "ctx" . "component" "api")
*/}}
{{- define "protea.selectorLabels" -}}
app.kubernetes.io/name: {{ include "protea.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Component labels = common labels + component-scoped selector labels.
Call as: include "protea.componentLabels" (dict "ctx" . "component" "api")
*/}}
{{- define "protea.componentLabels" -}}
{{ include "protea.labels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Fully-qualified name for a named component, e.g. protea-postgres.
Call as: include "protea.componentName" (dict "ctx" . "component" "postgres")
*/}}
{{- define "protea.componentName" -}}
{{- printf "%s-%s" (include "protea.fullname" .ctx) .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Image reference for PROTEA-owned services (api / workers / migrate).
*/}}
{{- define "protea.image" -}}
{{- $repo := .Values.image.repository -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}

{{/*
Image reference for the frontend service.
*/}}
{{- define "protea.frontendImage" -}}
{{- $repo := .Values.frontendImage.repository -}}
{{- $tag := default .Chart.AppVersion .Values.frontendImage.tag -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}

{{/*
Database URL (psycopg). Honours `database.externalUrl` when set; otherwise
builds the in-cluster URL from the embedded postgres service.
*/}}
{{- define "protea.databaseUrl" -}}
{{- if .Values.database.externalUrl -}}
{{- .Values.database.externalUrl -}}
{{- else -}}
{{- $host := printf "%s-postgres" (include "protea.fullname" .) -}}
{{- printf "postgresql+psycopg://%s:%s@%s:%d/%s" .Values.database.user .Values.database.password $host (int .Values.database.port) .Values.database.name -}}
{{- end -}}
{{- end -}}

{{/*
AMQP URL. Honours `amqp.externalUrl` when set; otherwise builds the
in-cluster URL from the embedded rabbitmq service.
*/}}
{{- define "protea.amqpUrl" -}}
{{- if .Values.amqp.externalUrl -}}
{{- .Values.amqp.externalUrl -}}
{{- else -}}
{{- $host := printf "%s-rabbitmq" (include "protea.fullname" .) -}}
{{- printf "amqp://%s:%s@%s:%d/" .Values.amqp.user .Values.amqp.password $host (int .Values.amqp.port) -}}
{{- end -}}
{{- end -}}

{{/*
Common environment block injected into api + worker pods.
*/}}
{{- define "protea.commonEnv" -}}
- name: PROTEA_DB_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "protea.componentName" (dict "ctx" . "component" "config") }}
      key: PROTEA_DB_URL
- name: PROTEA_AMQP_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "protea.componentName" (dict "ctx" . "component" "config") }}
      key: PROTEA_AMQP_URL
{{- with .Values.api.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
imagePullSecrets snippet shared across deployments.
*/}}
{{- define "protea.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{ toYaml . | indent 2 }}
{{- end }}
{{- end -}}
