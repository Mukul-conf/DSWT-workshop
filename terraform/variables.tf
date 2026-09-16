# =============================================================================
# gemini-workshop variables.
#
# The only two things this workshop needs beyond an attendee's own Confluent
# Cloud account are a Confluent Cloud API key/secret (created by the attendee
# in the Console) and a Gemini API key (from Google AI Studio —
# https://aistudio.google.com/apikey).
# =============================================================================

variable "prefix" {
  description = "Short identifier that namespaces the Confluent environment/cluster (e.g. GEMINI)."
  type        = string
  default     = "GEMINI"

  validation {
    condition     = can(regex("^[A-Za-z0-9]{1,12}$", var.prefix))
    error_message = "prefix must be 1-12 alphanumeric characters (e.g. GEMINI)."
  }
}

variable "owner_email" {
  description = "Owner email, tagged on the Confluent environment."
  type        = string
}

variable "region" {
  description = "Cloud region for the Confluent cluster + Flink pool. The Gemini API endpoint is global and is not affected by this."
  type        = string
  default     = "us-east-1"
}

variable "enable_rtce" {
  description = <<-EOT
    Enable the Real-Time Context Engine on car_telemetry so it can be queried
    from an MCP client (race_standings is excluded — see modules/topics/main.tf).
    Set TF_VAR_enable_rtce=false for an org or region where RTCE isn't
    available — see modules/topics/variables.tf.
  EOT
  type        = bool
  default     = false
}

# --- Confluent Cloud ---

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key (Cloud resource scope, OrganizationAdmin on the attendee's own org)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "flink_max_cfu" {
  description = "Autoscaling ceiling for the Flink compute pool; CFUs are consumed on demand, not reserved"
  type        = number
  default     = 10
}

# --- Gemini (Google AI) — the one external AI dependency ---

variable "gemini_api_key" {
  description = <<-EOT
    Google AI Studio Gemini API key, backing the Flink AI connection that
    LAB 4's pit_strategy_agent uses. Create one at
    https://aistudio.google.com/apikey — no GCP project, service account, or
    Vertex AI setup required. The organizer typically creates one key and
    shares it with every attendee (see gemini-workshop/docs/ORGANIZER-GUIDE.md).
  EOT
  type        = string
  sensitive   = true
}

variable "gemini_model" {
  description = <<-EOT
    Gemini model ID used by the generateContent endpoint that backs
    llm_textgen_model. Google retires Gemini model IDs on their own schedule —
    gemini-2.0-flash (this variable's original default) was retired and, as of
    2026-09, generativelanguage.googleapis.com returns 404 for it, naming
    gemini-3.6-flash as its replacement. If AI_RUN_AGENT / CREATE MODEL starts
    failing with "model ... is no longer available", the error names the
    current replacement directly — put that model ID here (or export
    TF_VAR_gemini_model) and re-apply; don't assume this default stays current.
  EOT
  type        = string
  default     = "gemini-3.6-flash"
}
