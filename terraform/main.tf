# =============================================================================
# Gemini workshop — one attendee's Confluent Cloud environment.
#
# Fully self-contained: `modules/` below is a local copy of the main repo's
# terraform/modules/{environment,cluster,flink,topics} (byte-identical at the
# time it was copied — they're provider-agnostic, nothing in them references
# any specific LLM provider's credentials), so `terraform apply` run from
# inside gemini-workshop/terraform/ never reaches outside this directory. The
# only thing that differs from the main repo's self-service tier is the LAB 4
# LLM connection, which uses Google AI (Gemini).
#
# Like self-service there is no Postgres/CDC (driver_race_history is seeded by
# a bounded Flink INSERT — see gemini-workshop/scripts/seed.py) and no ECS
# simulator (the attendee runs `gemini-workshop/scripts/race.py` locally
# against this environment). LAB 3 and LAB 4 are attendee-written SQL, exactly
# as in gemini-workshop/docs/ATTENDEE-GUIDE.md — this tier only pre-deploys
# `llm_textgen_model` so CREATE AGENT has something to point at.
# =============================================================================

data "confluent_organization" "main" {}

locals {
  name_prefix = "RIVER-RACING-${var.prefix}"
}

# --- Confluent Cloud: environment, cluster, Flink pool ---

module "environment" {
  source           = "./modules/environment"
  environment_name = "${local.name_prefix}-ENV"
  owner_email      = var.owner_email
}

module "cluster" {
  source         = "./modules/cluster"
  environment_id = module.environment.environment_id
  cluster_name   = "${local.name_prefix}-CLUSTER"
  name_prefix    = local.name_prefix
  cloud_provider = "AWS"
  cloud_region   = var.region
  owner_email    = var.owner_email
}

module "flink" {
  source             = "./modules/flink"
  organization_id    = data.confluent_organization.main.id
  environment_id     = module.environment.environment_id
  environment_name   = "${local.name_prefix}-ENV"
  cluster_name       = "${local.name_prefix}-CLUSTER"
  name_prefix        = local.name_prefix
  cloud_provider     = "AWS"
  cloud_region       = var.region
  service_account_id = module.cluster.service_account_id
  max_cfu            = var.flink_max_cfu
  owner_email        = var.owner_email
}

# --- Topics: car_telemetry + race_standings (produced by the local simulator) ---

module "topics" {
  source              = "./modules/topics"
  organization_id     = data.confluent_organization.main.id
  environment_id      = module.environment.environment_id
  environment_name    = module.environment.environment_name
  cluster_id          = module.cluster.cluster_id
  cluster_name        = module.cluster.cluster_name
  compute_pool_id     = module.flink.compute_pool_id
  service_account_id  = module.cluster.service_account_id
  flink_rest_endpoint = module.flink.flink_rest_endpoint
  flink_api_key       = module.flink.flink_api_key
  flink_api_secret    = module.flink.flink_api_secret
  owner_email         = var.owner_email
  region              = var.region
  enable_rtce         = var.enable_rtce

  # See terraform/self-service/main.tf's identical comment: without this, the
  # role-binding edges and these statements become destroy-order siblings, and
  # a destroy can revoke the principal's permissions before its own statements
  # are dropped, which then 403s.
  depends_on = [module.cluster]
}

# --- Stream Catalog tag on the raw ingest topic (LAB 2 catalog story) ---

resource "confluent_tag" "raw_data" {
  schema_registry_cluster {
    id = module.cluster.schema_registry_id
  }
  rest_endpoint = module.cluster.schema_registry_rest_endpoint
  credentials {
    key    = module.cluster.sr_api_key
    secret = module.cluster.sr_api_secret
  }

  name        = "RAW_DATA"
  description = "Raw ingest topic — unprocessed sensor data"
  depends_on  = [module.topics]
}

# Confluent's Catalog API deletes tag bindings asynchronously; deleting the tag
# immediately after can still 409 because the binding removal hasn't propagated.
resource "time_sleep" "wait_for_tag_binding_removal" {
  depends_on       = [confluent_tag.raw_data]
  destroy_duration = "30s"
}

resource "confluent_tag_binding" "car_telemetry_raw_data" {
  schema_registry_cluster {
    id = module.cluster.schema_registry_id
  }
  rest_endpoint = module.cluster.schema_registry_rest_endpoint
  credentials {
    key    = module.cluster.sr_api_key
    secret = module.cluster.sr_api_secret
  }

  tag_name    = confluent_tag.raw_data.name
  entity_name = "${module.cluster.schema_registry_id}:${module.cluster.cluster_id}:car_telemetry"
  entity_type = "kafka_topic"

  depends_on = [time_sleep.wait_for_tag_binding_removal]
}

# --- LLM connection + model (Google AI / Gemini) — text generation only ---
# This is the one resource that differs from terraform/self-service. Confluent
# Flink's GOOGLEAI connection type takes a plain Gemini API key as a top-level
# `api_key` argument (like OPENAI/AZUREOPENAI — no AWS-style key/secret pair,
# no GCP service account, no Vertex AI project). See:
# https://docs.confluent.io/cloud/current/ai/ai-model-inference.html

resource "confluent_flink_connection" "gemini_textgen_connection" {
  organization { id = data.confluent_organization.main.id }
  environment { id = module.environment.environment_id }
  compute_pool { id = module.flink.compute_pool_id }
  principal { id = module.cluster.service_account_id }
  rest_endpoint = module.flink.flink_rest_endpoint
  credentials {
    key    = module.flink.flink_api_key
    secret = module.flink.flink_api_secret
  }

  display_name = "llm-textgen-connection"
  type         = "GOOGLEAI"
  endpoint     = "https://generativelanguage.googleapis.com/v1beta/models/${var.gemini_model}:generateContent"
  api_key      = var.gemini_api_key

  depends_on = [module.cluster]
}

resource "confluent_flink_statement" "llm_textgen_model" {
  organization { id = data.confluent_organization.main.id }
  environment { id = module.environment.environment_id }
  compute_pool { id = module.flink.compute_pool_id }
  principal { id = module.cluster.service_account_id }
  rest_endpoint = module.flink.flink_rest_endpoint
  credentials {
    key    = module.flink.flink_api_key
    secret = module.flink.flink_api_secret
  }

  # INPUT/OUTPUT column names (prompt/response) match terraform/self-service's
  # Bedrock model on purpose: CREATE AGENT / AI_RUN_AGENT in LAB 4 is the exact
  # same SQL regardless of which provider backs llm_textgen_model, so keeping
  # this contract identical means the attendee-facing SQL never has to change.
  statement = "CREATE MODEL `${local.name_prefix}-ENV`.`${local.name_prefix}-CLUSTER`.`llm_textgen_model` INPUT (prompt STRING) OUTPUT (response STRING) WITH ('provider' = 'googleai', 'task' = 'text_generation', 'googleai.connection' = '${confluent_flink_connection.gemini_textgen_connection.display_name}');"

  properties = {
    "sql.current-catalog"  = "${local.name_prefix}-ENV"
    "sql.current-database" = "${local.name_prefix}-CLUSTER"
  }

  depends_on = [confluent_flink_connection.gemini_textgen_connection]
}

# --- driver_race_history table ---
# No Postgres here, same as self-service: this creates the empty table, and
# gemini-workshop/scripts/cli.py seeds the 198 historical rows with a bounded
# Flink INSERT after apply (gemini-workshop/scripts/seed.py).

resource "confluent_flink_statement" "create_driver_race_history_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = module.environment.environment_id }
  compute_pool { id = module.flink.compute_pool_id }
  principal { id = module.cluster.service_account_id }
  rest_endpoint = module.flink.flink_rest_endpoint
  credentials {
    key    = module.flink.flink_api_key
    secret = module.flink.flink_api_secret
  }

  statement = <<-EOT
    CREATE TABLE `driver_race_history` (
      `race_id` STRING COMMENT 'Race identifier (e.g. bahrain_2026)',
      `gp_name` STRING COMMENT 'Grand Prix name',
      `race_date` DATE COMMENT 'Date the race was held',
      `car_number` INT COMMENT 'Car number identifier',
      `driver` STRING COMMENT 'Driver full name',
      `team` STRING COMMENT 'Constructor team name',
      `starting_grid` INT COMMENT 'Starting grid position',
      `finishing_pos` INT COMMENT 'Finishing position',
      `positions_gained` INT COMMENT 'Positions gained (start - finish)',
      `pit_stops` INT COMMENT 'Number of pit stops',
      `stint_1_tire` STRING COMMENT 'Tire compound for stint 1',
      `stint_2_tire` STRING COMMENT 'Tire compound for stint 2',
      `stint_3_tire` STRING COMMENT 'Tire compound for stint 3 (or n/a)'
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = module.environment.environment_name
    "sql.current-database" = module.cluster.cluster_name
  }

  depends_on = [module.topics]
}
