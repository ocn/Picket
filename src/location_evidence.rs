use crate::contract_intelligence::ContractCollectionStore;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationEvidenceClass {
    PublicNpc,
    AccessQualified,
    Operator,
}

impl LocationEvidenceClass {
    fn from_str(value: &str) -> Result<Self, sqlx::Error> {
        match value {
            "public_npc" => Ok(Self::PublicNpc),
            "access_qualified" => Ok(Self::AccessQualified),
            "operator" => Ok(Self::Operator),
            _ => Err(sqlx::Error::Protocol(format!(
                "unknown location evidence class: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocationEvidence {
    pub id: i64,
    pub location_id: i64,
    pub evidence_class: LocationEvidenceClass,
    pub structure_id: Option<i64>,
    pub station_id: Option<i64>,
    pub solar_system_id: i64,
    pub region_id: Option<i64>,
    pub provenance: String,
    pub actor: String,
    pub observed_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub superseded_by_actor: Option<String>,
    pub supersedes_id: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocationEvidenceAudit {
    pub action: String,
    pub actor: String,
    pub provenance: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NewOperatorEvidence {
    location_id: i64,
    structure_id: Option<i64>,
    station_id: Option<i64>,
    solar_system_id: i64,
    region_id: Option<i64>,
    provenance: String,
    actor: String,
    observed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SupersedingOperatorEvidence {
    evidence_id: i64,
    solar_system_id: i64,
    region_id: Option<i64>,
    provenance: String,
    actor: String,
    observed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum OperatorLocationEvidenceCliResult {
    Added(LocationEvidence),
    Listed(Vec<LocationEvidence>),
    Inspected {
        evidence: LocationEvidence,
        audit: Vec<LocationEvidenceAudit>,
    },
    Expired(LocationEvidence),
    Superseded(LocationEvidence),
}

pub async fn run_operator_location_evidence_cli_from_process_args(
    arguments: &[String],
) -> Option<i32> {
    let (command, values) = arguments.split_first()?;
    if command != "location-evidence" {
        return None;
    }
    let database_url = match std::env::var("CONTRACT_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("location-evidence requires CONTRACT_DATABASE_URL");
            return Some(2);
        }
    };
    let store = match ContractCollectionStore::connect(&database_url).await {
        Ok(store) => store,
        Err(error) => {
            eprintln!("cannot open location-evidence store: {error}");
            return Some(2);
        }
    };
    let values = values.iter().map(String::as_str).collect::<Vec<_>>();
    match execute_operator_location_evidence_cli(&store, &values, Utc::now()).await {
        Ok(result) => match serde_json::to_string(&result) {
            Ok(output) => {
                println!("{output}");
                Some(0)
            }
            Err(error) => {
                eprintln!("cannot render location-evidence result: {error}");
                Some(2)
            }
        },
        Err(error) => {
            eprintln!("location-evidence command failed: {error}");
            Some(2)
        }
    }
}

pub struct LocationEvidenceService<'a> {
    pool: &'a PgPool,
}

impl<'a> LocationEvidenceService<'a> {
    pub fn new(store: &'a ContractCollectionStore) -> Self {
        Self {
            pool: store.location_evidence_pool(),
        }
    }

    async fn add_operator(
        &self,
        evidence: NewOperatorEvidence,
        now: DateTime<Utc>,
    ) -> Result<LocationEvidence, sqlx::Error> {
        validate_operator_evidence(&evidence).map_err(sqlx::Error::Protocol)?;
        let mut transaction = self.pool.begin().await?;
        lock_location(&mut transaction, evidence.location_id).await?;
        let automatically_expired = sqlx::query("UPDATE location_evidence SET expired_at = $2, expired_by = 'system:expiry' WHERE location_id = $1 AND evidence_class = 'operator' AND superseded_at IS NULL AND expired_at IS NULL AND expires_at <= $2 RETURNING id, provenance")
            .bind(evidence.location_id)
            .bind(now)
            .fetch_all(&mut *transaction)
            .await?;
        for expired in automatically_expired {
            sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'expired', 'system:expiry', $2, $3)")
                .bind(expired.get::<i64, _>("id"))
                .bind(expired.get::<String, _>("provenance"))
                .bind(now)
                .execute(&mut *transaction)
                .await?;
        }
        let current = sqlx::query_scalar::<_, i64>("SELECT id FROM location_evidence WHERE location_id = $1 AND evidence_class = 'operator' AND superseded_at IS NULL AND expired_at IS NULL FOR UPDATE")
            .bind(evidence.location_id)
            .fetch_optional(&mut *transaction)
            .await?;
        if current.is_some() {
            return Err(sqlx::Error::Protocol(format!(
                "an active operator evidence record already exists for location {}",
                evidence.location_id
            )));
        }
        let row = sqlx::query("INSERT INTO location_evidence (location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at) VALUES ($1,'operator',$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id")
            .bind(evidence.location_id)
            .bind(evidence.structure_id)
            .bind(evidence.station_id)
            .bind(evidence.solar_system_id)
            .bind(evidence.region_id)
            .bind(&evidence.provenance)
            .bind(&evidence.actor)
            .bind(evidence.observed_at)
            .bind(evidence.expires_at)
            .fetch_one(&mut *transaction)
            .await?;
        let persisted = location_evidence_from_row(row)?;
        sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'added', $2, $3, $4)")
            .bind(persisted.id)
            .bind(&persisted.actor)
            .bind(&persisted.provenance)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        reactivate_parked_structure_resolution(
            &mut transaction,
            persisted.location_id,
            persisted.observed_at,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(persisted)
    }

    pub async fn resolve(
        &self,
        location_id: i64,
        now: DateTime<Utc>,
    ) -> Result<Option<LocationEvidence>, sqlx::Error> {
        if location_id <= 0 {
            return Err(sqlx::Error::Protocol(
                "location ID must be a positive integer".to_string(),
            ));
        }
        sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence WHERE location_id = $1 AND observed_at <= $2 AND superseded_at IS NULL AND expired_at IS NULL AND (expires_at IS NULL OR expires_at > $2) ORDER BY CASE evidence_class WHEN 'public_npc' THEN 0 WHEN 'access_qualified' THEN 1 WHEN 'operator' THEN 2 ELSE 3 END, observed_at DESC, id DESC LIMIT 1")
            .bind(location_id)
            .bind(now)
            .fetch_optional(self.pool)
            .await?
            .map(location_evidence_from_row)
            .transpose()
    }

    pub async fn record_public_npc(
        &self,
        location_id: i64,
        station_id: Option<i64>,
        solar_system_id: i64,
        region_id: Option<i64>,
        observed_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<LocationEvidence, sqlx::Error> {
        if observed_at > now {
            return Err(sqlx::Error::Protocol(
                "public evidence observation time cannot be in the future".to_string(),
            ));
        }
        validate_location_facts(location_id, None, station_id, solar_system_id, region_id)
            .map_err(sqlx::Error::Protocol)?;
        let mut transaction = self.pool.begin().await?;
        lock_location(&mut transaction, location_id).await?;
        let current = sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence WHERE location_id = $1 AND evidence_class = 'public_npc' AND superseded_at IS NULL AND expired_at IS NULL FOR UPDATE")
            .bind(location_id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(location_evidence_from_row)
            .transpose()?;
        let (persisted_region_id, supersedes_id) = if let Some(current) = &current {
            if observed_at < current.observed_at {
                transaction.commit().await?;
                return Ok(current.clone());
            }
            let same_station_and_system =
                current.station_id == station_id && current.solar_system_id == solar_system_id;
            let persisted_region_id = if same_station_and_system {
                region_id.or(current.region_id)
            } else {
                region_id
            };
            if same_station_and_system && current.region_id == persisted_region_id {
                transaction.commit().await?;
                return Ok(current.clone());
            }
            sqlx::query("UPDATE location_evidence SET superseded_at = $2, superseded_by_actor = 'system:public-contract-esi' WHERE id = $1")
                .bind(current.id)
                .bind(now)
                .execute(&mut *transaction)
                .await?;
            sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'superseded', 'system:public-contract-esi', 'unauthenticated public ESI location response', $2)")
                .bind(current.id)
                .bind(now)
                .execute(&mut *transaction)
                .await?;
            (persisted_region_id, Some(current.id))
        } else {
            (region_id, None)
        };
        let row = sqlx::query("INSERT INTO location_evidence (location_id, evidence_class, station_id, solar_system_id, region_id, provenance, actor, observed_at, supersedes_id) VALUES ($1,'public_npc',$2,$3,$4,'unauthenticated public ESI location response','system:public-contract-esi',$5,$6) RETURNING id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id")
            .bind(location_id)
            .bind(station_id)
            .bind(solar_system_id)
            .bind(persisted_region_id)
            .bind(observed_at)
            .bind(supersedes_id)
            .fetch_one(&mut *transaction)
            .await?;
        let persisted = location_evidence_from_row(row)?;
        sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'added', 'system:public-contract-esi', 'unauthenticated public ESI location response', $2)")
            .bind(persisted.id)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        reactivate_parked_structure_resolution(
            &mut transaction,
            persisted.location_id,
            persisted.observed_at,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(persisted)
    }

    pub async fn record_access_qualified(
        &self,
        structure_id: i64,
        solar_system_id: i64,
        region_id: Option<i64>,
        resolver_identity: &str,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<LocationEvidence, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let persisted = Self::record_access_qualified_in_transaction(
            &mut transaction,
            structure_id,
            solar_system_id,
            region_id,
            resolver_identity,
            observed_at,
            expires_at,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(persisted)
    }

    pub(crate) async fn record_access_qualified_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        structure_id: i64,
        solar_system_id: i64,
        region_id: Option<i64>,
        resolver_identity: &str,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<LocationEvidence, sqlx::Error> {
        if observed_at > now {
            return Err(sqlx::Error::Protocol(
                "access-qualified evidence observation time cannot be in the future".to_string(),
            ));
        }
        if expires_at <= observed_at {
            return Err(sqlx::Error::Protocol(
                "access-qualified evidence expiry must be after its observation time".to_string(),
            ));
        }
        validate_location_facts(
            structure_id,
            Some(structure_id),
            None,
            solar_system_id,
            region_id,
        )
        .map_err(sqlx::Error::Protocol)?;
        validate_access_resolver_identity(resolver_identity).map_err(sqlx::Error::Protocol)?;
        lock_location(transaction, structure_id).await?;
        let current = sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence WHERE location_id = $1 AND evidence_class = 'access_qualified' AND superseded_at IS NULL AND expired_at IS NULL FOR UPDATE")
            .bind(structure_id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(location_evidence_from_row)
            .transpose()?;
        if let Some(current) = &current {
            if observed_at < current.observed_at {
                return Ok(current.clone());
            }
            sqlx::query("UPDATE location_evidence SET superseded_at = $2, superseded_by_actor = $3 WHERE id = $1")
                .bind(current.id)
                .bind(now)
                .bind(resolver_identity)
                .execute(&mut **transaction)
                .await?;
            sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'superseded', $2, 'authenticated ESI structure response', $3)")
                .bind(current.id)
                .bind(resolver_identity)
                .bind(now)
                .execute(&mut **transaction)
                .await?;
        }
        let row = sqlx::query("INSERT INTO location_evidence (location_id, evidence_class, structure_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, supersedes_id) VALUES ($1, 'access_qualified', $1, $2, $3, 'authenticated ESI structure response', $4, $5, $6, $7) RETURNING id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id")
            .bind(structure_id)
            .bind(solar_system_id)
            .bind(region_id)
            .bind(resolver_identity)
            .bind(observed_at)
            .bind(expires_at)
            .bind(current.as_ref().map(|current| current.id))
            .fetch_one(&mut **transaction)
            .await?;
        let persisted = location_evidence_from_row(row)?;
        sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'added', $2, 'authenticated ESI structure response', $3)")
            .bind(persisted.id)
            .bind(resolver_identity)
            .bind(now)
            .execute(&mut **transaction)
            .await?;
        reactivate_parked_structure_resolution(
            transaction,
            persisted.location_id,
            persisted.observed_at,
            now,
        )
        .await?;
        Ok(persisted)
    }

    async fn list(&self, location_id: Option<i64>) -> Result<Vec<LocationEvidence>, sqlx::Error> {
        let rows = match location_id {
            Some(location_id) => sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence WHERE location_id = $1 ORDER BY id")
                .bind(location_id)
                .fetch_all(self.pool)
                .await?,
            None => sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence ORDER BY location_id, id")
                .fetch_all(self.pool)
                .await?,
        };
        rows.into_iter().map(location_evidence_from_row).collect()
    }

    async fn inspect(
        &self,
        evidence_id: i64,
    ) -> Result<(LocationEvidence, Vec<LocationEvidenceAudit>), sqlx::Error> {
        let evidence = sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence WHERE id = $1")
            .bind(evidence_id)
            .fetch_optional(self.pool)
            .await?
            .map(location_evidence_from_row)
            .transpose()?
            .ok_or_else(|| sqlx::Error::Protocol(format!("location evidence {evidence_id} does not exist")))?;
        let audit = sqlx::query("SELECT action, actor, provenance, occurred_at FROM location_evidence_audit WHERE location_evidence_id = $1 ORDER BY id")
            .bind(evidence_id)
            .fetch_all(self.pool)
            .await?
            .into_iter()
            .map(|row| LocationEvidenceAudit {
                action: row.get("action"),
                actor: row.get("actor"),
                provenance: row.get("provenance"),
                occurred_at: row.get("occurred_at"),
            })
            .collect();
        Ok((evidence, audit))
    }

    async fn expire_operator(
        &self,
        evidence_id: i64,
        actor: &str,
        provenance: &str,
        now: DateTime<Utc>,
    ) -> Result<LocationEvidence, sqlx::Error> {
        validate_mutation_actor(actor, provenance).map_err(sqlx::Error::Protocol)?;
        let mut transaction = self.pool.begin().await?;
        let location_id = evidence_location_id(&mut transaction, evidence_id).await?;
        lock_location(&mut transaction, location_id).await?;
        let row = sqlx::query("UPDATE location_evidence SET expired_at = $2, expired_by = $3 WHERE id = $1 AND evidence_class = 'operator' AND superseded_at IS NULL AND expired_at IS NULL RETURNING id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id")
            .bind(evidence_id)
            .bind(now)
            .bind(actor)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| sqlx::Error::Protocol(format!("location evidence {evidence_id} is not current operator evidence")))?;
        let evidence = location_evidence_from_row(row)?;
        sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'expired', $2, $3, $4)")
            .bind(evidence_id)
            .bind(actor)
            .bind(provenance)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(evidence)
    }

    async fn supersede_operator(
        &self,
        replacement: SupersedingOperatorEvidence,
        now: DateTime<Utc>,
    ) -> Result<LocationEvidence, sqlx::Error> {
        validate_mutation_actor(&replacement.actor, &replacement.provenance)
            .map_err(sqlx::Error::Protocol)?;
        if replacement.solar_system_id <= 0 || replacement.region_id.is_some_and(|id| id <= 0) {
            return Err(sqlx::Error::Protocol(
                "replacement solar system and region IDs must be positive integers".to_string(),
            ));
        }
        if replacement.expires_at <= replacement.observed_at {
            return Err(sqlx::Error::Protocol(
                "operator evidence expiry must be after its observation time".to_string(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let location_id = evidence_location_id(&mut transaction, replacement.evidence_id).await?;
        lock_location(&mut transaction, location_id).await?;
        let current = sqlx::query("SELECT id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id FROM location_evidence WHERE id = $1 AND evidence_class = 'operator' AND superseded_at IS NULL AND expired_at IS NULL FOR UPDATE")
            .bind(replacement.evidence_id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(location_evidence_from_row)
            .transpose()?
            .ok_or_else(|| sqlx::Error::Protocol(format!("location evidence {} is not current operator evidence", replacement.evidence_id)))?;
        sqlx::query("UPDATE location_evidence SET superseded_at = $2, superseded_by_actor = $3 WHERE id = $1")
            .bind(current.id)
            .bind(now)
            .bind(&replacement.actor)
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query("INSERT INTO location_evidence (location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, supersedes_id) VALUES ($1,'operator',$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id, location_id, evidence_class, structure_id, station_id, solar_system_id, region_id, provenance, actor, observed_at, expires_at, expired_at, superseded_at, superseded_by_actor, supersedes_id")
            .bind(current.location_id)
            .bind(current.structure_id)
            .bind(current.station_id)
            .bind(replacement.solar_system_id)
            .bind(replacement.region_id)
            .bind(&replacement.provenance)
            .bind(&replacement.actor)
            .bind(replacement.observed_at)
            .bind(replacement.expires_at)
            .bind(current.id)
            .fetch_one(&mut *transaction)
            .await?;
        let evidence = location_evidence_from_row(row)?;
        sqlx::query("INSERT INTO location_evidence_audit (location_evidence_id, action, actor, provenance, occurred_at) VALUES ($1, 'superseded', $2, $3, $4), ($5, 'added', $2, $3, $4)")
            .bind(current.id)
            .bind(&replacement.actor)
            .bind(&replacement.provenance)
            .bind(now)
            .bind(evidence.id)
            .execute(&mut *transaction)
            .await?;
        reactivate_parked_structure_resolution(
            &mut transaction,
            evidence.location_id,
            evidence.observed_at,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(evidence)
    }
}

pub(crate) async fn lock_location(
    transaction: &mut Transaction<'_, Postgres>,
    location_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(location_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn evidence_location_id(
    transaction: &mut Transaction<'_, Postgres>,
    evidence_id: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT location_id FROM location_evidence WHERE id = $1")
        .bind(evidence_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| {
            sqlx::Error::Protocol(format!("location evidence {evidence_id} does not exist"))
        })
}

pub async fn execute_operator_location_evidence_cli(
    store: &ContractCollectionStore,
    arguments: &[&str],
    now: DateTime<Utc>,
) -> Result<OperatorLocationEvidenceCliResult, String> {
    let (command, flags) = parse_command(arguments)?;
    let service = LocationEvidenceService::new(store);
    match command {
        "add" => {
            reject_unknown_options(
                &flags,
                &[
                    "--location-id",
                    "--structure-id",
                    "--station-id",
                    "--system-id",
                    "--region-id",
                    "--observed-at",
                    "--expires-at",
                    "--actor",
                    "--provenance",
                ],
            )?;
            let expires_at = required_datetime(&flags, "--expires-at")?;
            if expires_at <= now {
                return Err("operator evidence expiry must be in the future".to_string());
            }
            let observed_at = optional_datetime(&flags, "--observed-at")?.unwrap_or(now);
            if observed_at > now {
                return Err(
                    "operator evidence observation time cannot be in the future".to_string()
                );
            }
            service
                .add_operator(
                    NewOperatorEvidence {
                        location_id: required_i64(&flags, "--location-id")?,
                        structure_id: optional_i64(&flags, "--structure-id")?,
                        station_id: optional_i64(&flags, "--station-id")?,
                        solar_system_id: required_i64(&flags, "--system-id")?,
                        region_id: optional_i64(&flags, "--region-id")?,
                        provenance: required_value(&flags, "--provenance")?.to_string(),
                        actor: required_value(&flags, "--actor")?.to_string(),
                        observed_at,
                        expires_at,
                    },
                    now,
                )
                .await
                .map(OperatorLocationEvidenceCliResult::Added)
                .map_err(|error| error.to_string())
        }
        "list" => {
            reject_unknown_options(&flags, &["--location-id"])?;
            let location_id = optional_i64(&flags, "--location-id")?;
            if location_id.is_some_and(|id| id <= 0) {
                return Err("--location-id must be a positive integer".to_string());
            }
            service
                .list(location_id)
                .await
                .map(OperatorLocationEvidenceCliResult::Listed)
                .map_err(|error| error.to_string())
        }
        "inspect" => {
            reject_unknown_options(&flags, &["--id"])?;
            let (evidence, audit) = service
                .inspect(required_positive_i64(&flags, "--id")?)
                .await
                .map_err(|error| error.to_string())?;
            Ok(OperatorLocationEvidenceCliResult::Inspected { evidence, audit })
        }
        "expire" => {
            reject_unknown_options(&flags, &["--id", "--actor", "--provenance"])?;
            service
                .expire_operator(
                    required_positive_i64(&flags, "--id")?,
                    required_value(&flags, "--actor")?,
                    required_value(&flags, "--provenance")?,
                    now,
                )
                .await
                .map(OperatorLocationEvidenceCliResult::Expired)
                .map_err(|error| error.to_string())
        }
        "supersede" => {
            reject_unknown_options(
                &flags,
                &[
                    "--id",
                    "--system-id",
                    "--region-id",
                    "--observed-at",
                    "--expires-at",
                    "--actor",
                    "--provenance",
                ],
            )?;
            let observed_at = optional_datetime(&flags, "--observed-at")?.unwrap_or(now);
            let expires_at = required_datetime(&flags, "--expires-at")?;
            if observed_at > now {
                return Err(
                    "operator evidence observation time cannot be in the future".to_string()
                );
            }
            if expires_at <= now {
                return Err("operator evidence expiry must be in the future".to_string());
            }
            service
                .supersede_operator(
                    SupersedingOperatorEvidence {
                        evidence_id: required_positive_i64(&flags, "--id")?,
                        solar_system_id: required_i64(&flags, "--system-id")?,
                        region_id: optional_i64(&flags, "--region-id")?,
                        provenance: required_value(&flags, "--provenance")?.to_string(),
                        actor: required_value(&flags, "--actor")?.to_string(),
                        observed_at,
                        expires_at,
                    },
                    now,
                )
                .await
                .map(OperatorLocationEvidenceCliResult::Superseded)
                .map_err(|error| error.to_string())
        }
        _ => Err(
            "location-evidence command must be add, list, inspect, expire, or supersede"
                .to_string(),
        ),
    }
}

fn validate_operator_evidence(evidence: &NewOperatorEvidence) -> Result<(), String> {
    validate_location_facts(
        evidence.location_id,
        evidence.structure_id,
        evidence.station_id,
        evidence.solar_system_id,
        evidence.region_id,
    )?;
    validate_mutation_actor(&evidence.actor, &evidence.provenance)?;
    if evidence.expires_at <= evidence.observed_at {
        return Err("operator evidence expiry must be after its observation time".to_string());
    }
    Ok(())
}

fn validate_location_facts(
    location_id: i64,
    structure_id: Option<i64>,
    station_id: Option<i64>,
    solar_system_id: i64,
    region_id: Option<i64>,
) -> Result<(), String> {
    for (label, value) in [
        ("location ID", location_id),
        ("solar system ID", solar_system_id),
    ] {
        if value <= 0 {
            return Err(format!("{label} must be a positive integer"));
        }
    }
    for (label, value) in [
        ("structure ID", structure_id),
        ("station ID", station_id),
        ("region ID", region_id),
    ] {
        if value.is_some_and(|value| value <= 0) {
            return Err(format!("{label} must be a positive integer"));
        }
    }
    if structure_id.is_some() && station_id.is_some() {
        return Err("location evidence cannot identify both a station and a structure".to_string());
    }
    if structure_id.is_some_and(|structure_id| structure_id != location_id)
        || station_id.is_some_and(|station_id| station_id != location_id)
        || (structure_id.is_none() && station_id.is_none() && location_id != solar_system_id)
    {
        return Err(
            "location evidence facts must identify the contract location as its station, structure, or solar system"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_mutation_actor(actor: &str, provenance: &str) -> Result<(), String> {
    if actor.trim().is_empty() || provenance.trim().is_empty() {
        Err("operator evidence requires a non-empty actor and provenance".to_string())
    } else {
        Ok(())
    }
}

fn validate_access_resolver_identity(resolver_identity: &str) -> Result<(), String> {
    let Some(character_id) = resolver_identity.strip_prefix("character:") else {
        return Err("access-qualified evidence requires a character resolver identity".to_string());
    };
    if character_id
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .is_none()
    {
        return Err(
            "access-qualified evidence requires a positive character resolver identity".to_string(),
        );
    }
    Ok(())
}

async fn reactivate_parked_structure_resolution(
    transaction: &mut Transaction<'_, Postgres>,
    location_id: i64,
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE structure_resolution_state SET cache_expires_at = NULL, next_attempt_at = $3, first_denied_at = NULL, last_denied_at = NULL, denied_attempts = 0, parked_at = NULL, transient_failures = 0, last_error = NULL, last_failure_kind = NULL, updated_at = $3 WHERE structure_id = $1 AND parked_at IS NOT NULL AND $2 >= parked_at")
        .bind(location_id)
        .bind(observed_at)
        .bind(now)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn parse_command<'a>(
    arguments: &'a [&'a str],
) -> Result<(&'a str, BTreeMap<&'a str, &'a str>), String> {
    let Some((command, values)) = arguments.split_first() else {
        return Err("location-evidence command is required".to_string());
    };
    let mut flags = BTreeMap::new();
    let mut values = values.iter();
    while let Some(flag) = values.next() {
        if !flag.starts_with("--") || flags.contains_key(flag) {
            return Err(format!(
                "invalid or duplicate location-evidence option: {flag}"
            ));
        }
        let value = values
            .next()
            .ok_or_else(|| format!("missing value for location-evidence option: {flag}"))?;
        flags.insert(*flag, *value);
    }
    Ok((command, flags))
}

fn reject_unknown_options(flags: &BTreeMap<&str, &str>, allowed: &[&str]) -> Result<(), String> {
    flags
        .keys()
        .find(|flag| !allowed.contains(flag))
        .map(|flag| Err(format!("unknown location-evidence option: {flag}")))
        .unwrap_or(Ok(()))
}

fn required_value<'a>(flags: &BTreeMap<&str, &'a str>, name: &str) -> Result<&'a str, String> {
    flags
        .get(name)
        .copied()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

fn required_i64(flags: &BTreeMap<&str, &str>, name: &str) -> Result<i64, String> {
    required_value(flags, name)?
        .parse()
        .map_err(|_| format!("{name} must be an integer"))
}

fn required_positive_i64(flags: &BTreeMap<&str, &str>, name: &str) -> Result<i64, String> {
    let value = required_i64(flags, name)?;
    if value <= 0 {
        return Err(format!("{name} must be a positive integer"));
    }
    Ok(value)
}

fn optional_i64(flags: &BTreeMap<&str, &str>, name: &str) -> Result<Option<i64>, String> {
    flags
        .get(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("{name} must be an integer"))
        })
        .transpose()
}

fn required_datetime(flags: &BTreeMap<&str, &str>, name: &str) -> Result<DateTime<Utc>, String> {
    optional_datetime(flags, name)?.ok_or_else(|| format!("{name} is required"))
}

fn optional_datetime(
    flags: &BTreeMap<&str, &str>,
    name: &str,
) -> Result<Option<DateTime<Utc>>, String> {
    flags
        .get(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("{name} must be an RFC 3339 timestamp"))
        })
        .transpose()
}

fn location_evidence_from_row(row: sqlx::postgres::PgRow) -> Result<LocationEvidence, sqlx::Error> {
    Ok(LocationEvidence {
        id: row.get("id"),
        location_id: row.get("location_id"),
        evidence_class: LocationEvidenceClass::from_str(&row.get::<String, _>("evidence_class"))?,
        structure_id: row.get("structure_id"),
        station_id: row.get("station_id"),
        solar_system_id: row.get("solar_system_id"),
        region_id: row.get("region_id"),
        provenance: row.get("provenance"),
        actor: row.get("actor"),
        observed_at: row.get("observed_at"),
        expires_at: row.get("expires_at"),
        expired_at: row.get("expired_at"),
        superseded_at: row.get("superseded_at"),
        superseded_by_actor: row.get("superseded_by_actor"),
        supersedes_id: row.get("supersedes_id"),
    })
}
