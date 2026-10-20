{{- define "rpp.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rpp.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "rpp.labels" -}}
app.kubernetes.io/name: {{ include "rpp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "rpp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rpp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rpp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "rpp.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "rpp.watchNamespace" -}}
{{- default .Release.Namespace .Values.watchNamespace -}}
{{- end -}}

{{- define "rpp.crdGroup" -}}
{{- default "runpodpool.ownego.com" .Values.crdGroup -}}
{{- end -}}

{{- define "rpp.apiKeySecret" -}}
{{- if .Values.runpodApiKey.value -}}
{{- include "rpp.fullname" . -}}
{{- else -}}
{{- .Values.runpodApiKey.existingSecret -}}
{{- end -}}
{{- end -}}

{{/* Slots ConfigMap name of a pool entry, as the controller defaults it. */}}
{{- define "rpp.slotsConfigMap" -}}
{{- $nginx := default dict .spec.nginx -}}
{{- default (printf "%s-nginx-slots" .name) $nginx.slotsConfigMap -}}
{{- end -}}
