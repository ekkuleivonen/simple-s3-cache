{{- define "simple-s3-cache.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "simple-s3-cache.fullname" -}}
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

{{- define "simple-s3-cache.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "simple-s3-cache.labels" -}}
helm.sh/chart: {{ include "simple-s3-cache.chart" . }}
app.kubernetes.io/name: {{ include "simple-s3-cache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "simple-s3-cache.selectorLabels" -}}
app.kubernetes.io/name: {{ include "simple-s3-cache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: cache
{{- end -}}

{{- define "simple-s3-cache.gatewaySelectorLabels" -}}
app.kubernetes.io/name: {{ include "simple-s3-cache.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gateway
{{- end -}}

{{- define "simple-s3-cache.topology" -}}
{{- $topology := default "single" .Values.topology -}}
{{- if not (has $topology (list "single" "peer" "gateway")) -}}
{{- fail "topology must be one of: single, peer, gateway" -}}
{{- end -}}
{{- $topology -}}
{{- end -}}

{{- define "simple-s3-cache.cacheReplicas" -}}
{{- if eq (include "simple-s3-cache.topology" .) "single" -}}
1
{{- else -}}
{{- int .Values.replicaCount -}}
{{- end -}}
{{- end -}}

{{- define "simple-s3-cache.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "simple-s3-cache.secretName" -}}
{{- default (printf "%s-upstream" (include "simple-s3-cache.fullname" .)) .Values.upstream.credentials.existingSecret -}}
{{- end -}}

{{- define "simple-s3-cache.peerServiceName" -}}
{{- printf "%s-peers" (include "simple-s3-cache.fullname" .) -}}
{{- end -}}

{{- define "simple-s3-cache.cacheServiceEnabled" -}}
{{- if kindIs "bool" .Values.cacheService.enabled -}}
{{- .Values.cacheService.enabled -}}
{{- else if eq (include "simple-s3-cache.topology" .) "gateway" -}}
false
{{- else -}}
true
{{- end -}}
{{- end -}}

{{- define "simple-s3-cache.peerURL" -}}
{{- $root := index . 0 -}}
{{- $ordinal := index . 1 -}}
{{- $fullname := include "simple-s3-cache.fullname" $root -}}
{{- $peerService := include "simple-s3-cache.peerServiceName" $root -}}
{{- printf "http://%s-%d.%s.%s.svc.cluster.local:8080" $fullname $ordinal $peerService $root.Release.Namespace -}}
{{- end -}}
