{{/*
Expand the name of the chart.
*/}}
{{- define "walrus.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "walrus.fullname" -}}
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
Chart label value.
*/}}
{{- define "walrus.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "walrus.labels" -}}
helm.sh/chart: {{ include "walrus.chart" . }}
app.kubernetes.io/part-of: walrus
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end }}

{{/*
WAL Capture selector labels.
*/}}
{{- define "walrus.walCapture.selectorLabels" -}}
app.kubernetes.io/name: wal-capture
app.kubernetes.io/part-of: walrus
{{- end }}

{{/*
Iceberg Writer selector labels.
*/}}
{{- define "walrus.icebergWriter.selectorLabels" -}}
app.kubernetes.io/name: iceberg-writer
app.kubernetes.io/part-of: walrus
{{- end }}

{{/*
Resolve the secret name.
*/}}
{{- define "walrus.secretName" -}}
{{- if .Values.secret.create }}
{{- include "walrus.fullname" . }}-secret
{{- else }}
{{- required "secret.existingSecretName is required when secret.create=false" .Values.secret.existingSecretName }}
{{- end }}
{{- end }}

{{/*
Resolve the staging PVC claim name.
*/}}
{{- define "walrus.stagingClaimName" -}}
{{- if .Values.persistence.staging.create }}
{{- include "walrus.fullname" . }}-staging
{{- else }}
{{- required "persistence.staging.existingClaim is required when persistence.staging.create=false" .Values.persistence.staging.existingClaim }}
{{- end }}
{{- end }}

{{/*
Resolve the warehouse PVC claim name.
*/}}
{{- define "walrus.warehouseClaimName" -}}
{{- if .Values.persistence.warehouse.create }}
{{- include "walrus.fullname" . }}-warehouse
{{- else }}
{{- required "persistence.warehouse.existingClaim is required when persistence.warehouse.create=false" .Values.persistence.warehouse.existingClaim }}
{{- end }}
{{- end }}

{{/*
Resolve image tag, defaulting to Chart.AppVersion.
*/}}
{{- define "walrus.walCapture.imageTag" -}}
{{- .Values.walCapture.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "walrus.icebergWriter.imageTag" -}}
{{- .Values.icebergWriter.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Render the pgiceberg.toml configuration from structured values.
*/}}
{{- define "walrus.tomlConfig" -}}
[source]
host = {{ .Values.config.source.host | quote }}
port = {{ .Values.config.source.port }}
database = {{ .Values.config.source.database | quote }}
user = {{ .Values.config.source.user | quote }}
password_env = {{ .Values.config.source.passwordEnv | quote }}
slot_name = {{ .Values.config.source.slotName | quote }}
publication_name = {{ .Values.config.source.publicationName | quote }}
{{- if .Values.config.source.tlsMode }}
tls_mode = {{ .Values.config.source.tlsMode | quote }}
{{- end }}
{{- if .Values.config.source.tlsCaCert }}
tls_ca_cert = {{ .Values.config.source.tlsCaCert | quote }}
{{- end }}

[source.tables]
{{- range $table, $opts := .Values.config.source.tables }}
{{- if and $opts $opts.pk }}
{{ $table | quote }} = { pk = [{{ range $i, $col := $opts.pk }}{{ if $i }}, {{ end }}{{ $col | quote }}{{ end }}] }
{{- else }}
{{ $table | quote }} = {}
{{- end }}
{{- end }}

[staging]
root = {{ .Values.config.staging.root | quote }}
cleanup_after_hours = {{ .Values.config.staging.cleanupAfterHours }}

[wal_capture]
max_parallel_tables = {{ .Values.config.walCapture.maxParallelTables }}
max_parallel_workers_per_table = {{ .Values.config.walCapture.maxParallelWorkersPerTable }}
rows_per_partition = {{ .Values.config.walCapture.rowsPerPartition | int64 }}
max_batch_rows = {{ .Values.config.walCapture.maxBatchRows | int64 }}
max_batch_bytes = {{ .Values.config.walCapture.maxBatchBytes | int64 }}
flush_interval_seconds = {{ .Values.config.walCapture.flushIntervalSeconds }}
idle_timeout_seconds = {{ .Values.config.walCapture.idleTimeoutSeconds }}

[iceberg_writer]
warehouse_path = {{ .Values.config.icebergWriter.warehousePath | quote }}
catalog_db_path = {{ .Values.config.icebergWriter.catalogDbPath | quote }}
poll_interval_seconds = {{ .Values.config.icebergWriter.pollIntervalSeconds }}
max_files_per_batch = {{ .Values.config.icebergWriter.maxFilesPerBatch }}
compaction_interval_hours = {{ .Values.config.icebergWriter.compactionIntervalHours }}
compaction_delete_threshold = {{ .Values.config.icebergWriter.compactionDeleteThreshold }}
max_retries = {{ .Values.config.icebergWriter.maxRetries }}
{{- end }}
