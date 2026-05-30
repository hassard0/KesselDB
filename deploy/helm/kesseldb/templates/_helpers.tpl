{{/*
Standard helper templates. Generated from the helm v3 default chart
shape; trimmed to just the helpers actually used by KesselDB templates.
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "kesseldb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully-qualified app name. We truncate at 63 chars
because some k8s name fields are limited to this (by the DNS naming
spec).
*/}}
{{- define "kesseldb.fullname" -}}
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
Chart name + version, joined by dash. Used in image labels.
*/}}
{{- define "kesseldb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels — added to every object emitted by the chart.
*/}}
{{- define "kesseldb.labels" -}}
helm.sh/chart: {{ include "kesseldb.chart" . }}
{{ include "kesseldb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels — used by Service to find pods + by Deployment to
select replicas. MUST be a stable subset of labels (changing them on
an existing release breaks the selector → pods orphaned).
*/}}
{{- define "kesseldb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kesseldb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: kesseldb
{{- end -}}

{{/*
Name of the service account to use.
*/}}
{{- define "kesseldb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "kesseldb.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
