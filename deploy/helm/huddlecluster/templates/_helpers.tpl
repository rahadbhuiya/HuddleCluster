{{/*
Expand the name of the chart.
*/}}
{{- define "huddlecluster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "huddlecluster.fullname" -}}
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

{{/*
Chart name and version, for the chart label.
*/}}
{{- define "huddlecluster.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "huddlecluster.labels" -}}
helm.sh/chart: {{ include "huddlecluster.chart" . }}
{{ include "huddlecluster.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "huddlecluster.selectorLabels" -}}
app.kubernetes.io/name: {{ include "huddlecluster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Master-specific selector labels (so the master Service doesn't also
route to agent Pods, and vice versa).
*/}}
{{- define "huddlecluster.masterSelectorLabels" -}}
{{ include "huddlecluster.selectorLabels" . }}
app.kubernetes.io/component: master
{{- end }}

{{- define "huddlecluster.agentSelectorLabels" -}}
{{ include "huddlecluster.selectorLabels" . }}
app.kubernetes.io/component: agent
{{- end }}

{{/*
Service account name.
*/}}
{{- define "huddlecluster.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "huddlecluster.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Master Service DNS name within the cluster (used by agents and, for
HA, by peer masters to reach each other).
*/}}
{{- define "huddlecluster.masterServiceName" -}}
{{- printf "%s-master" (include "huddlecluster.fullname" .) }}
{{- end }}

{{/*
Secret name holding API keys — either the user's existingSecret, or
one this chart generates.
*/}}
{{- define "huddlecluster.authSecretName" -}}
{{- if .Values.master.auth.existingSecret }}
{{- .Values.master.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "huddlecluster.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Secret name holding the TLS cert/key — either the user's
existingSecret, or the chart's own (created outside this chart —
see README, cert-manager or `kubectl create secret tls` are the
recommended ways to populate it).
*/}}
{{- define "huddlecluster.tlsSecretName" -}}
{{- if .Values.master.tls.existingSecret }}
{{- .Values.master.tls.existingSecret }}
{{- else }}
{{- printf "%s-tls" (include "huddlecluster.fullname" .) }}
{{- end }}
{{- end }}
