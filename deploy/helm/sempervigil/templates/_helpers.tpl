{{- define "sempervigil.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sempervigil.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "sempervigil" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "sempervigil.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sempervigil.labels" -}}
helm.sh/chart: {{ include "sempervigil.chart" . }}
app.kubernetes.io/name: {{ include "sempervigil.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- range $k, $v := .Values.commonLabels }}
{{ $k }}: {{ $v | quote }}
{{- end }}
{{- end -}}

{{- define "sempervigil.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sempervigil.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "sempervigil.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s" (include "sempervigil.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "sempervigil.componentName" -}}
{{- printf "sempervigil-%s" . | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "sempervigil.storageClaimName" -}}
{{- $root := index . 0 -}}
{{- $storage := index . 1 -}}
{{- if $storage.existingClaim -}}
{{- $storage.existingClaim -}}
{{- else -}}
{{- printf "%s-%s" (include "sempervigil.fullname" $root) (index . 2) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "sempervigil.secretName" -}}
{{- printf "%s-secrets" (include "sempervigil.fullname" .) -}}
{{- end -}}

{{- define "sempervigil.componentFullname" -}}
{{- $root := index . 0 -}}
{{- $name := index . 1 -}}
{{- printf "%s-%s" (include "sempervigil.fullname" $root) $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "sempervigil.logsVolume" -}}
{{- if .Values.storage.logs.enabled }}
persistentVolumeClaim:
  claimName: {{ include "sempervigil.storageClaimName" (list . .Values.storage.logs "logs") }}
{{- else }}
emptyDir: {}
{{- end }}
{{- end -}}
