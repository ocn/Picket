use serde::Deserialize;

// --- R2Z2 response models ---

#[derive(Debug, Deserialize)]
pub struct R2z2SequenceResponse {
    pub sequence: i64,
}

#[derive(Debug, Deserialize)]
pub struct R2z2KillmailResponse {
    pub killmail_id: i64,
    pub hash: String,
    pub zkb: Zkb,
}

/// Represents the top-level JSON object from the zKillboard RedisQ stream.
/// The `package` field can be null if there's no new killmail.
#[derive(Debug, Deserialize)]
pub struct RedisQResponse {
    pub package: Option<ZkDataNoEsi>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ZkDataNoEsi {
    #[serde(rename = "killID")]
    pub kill_id: i64,
    pub zkb: Zkb,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ZkData {
    #[serde(rename = "killID")]
    pub kill_id: i64,
    pub killmail: KillmailData,
    pub zkb: Zkb,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KillmailData {
    pub attackers: Vec<Attacker>,
    pub killmail_id: i64,
    pub killmail_time: String,
    pub solar_system_id: u32,
    pub victim: Victim,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct Zkb {
    #[serde(default, rename = "locationID")]
    pub location_id: Option<i64>,
    pub hash: String,
    #[serde(rename = "fittedValue")]
    pub fitted_value: f64,
    #[serde(rename = "droppedValue")]
    pub dropped_value: f64,
    #[serde(rename = "destroyedValue")]
    pub destroyed_value: f64,
    #[serde(rename = "totalValue")]
    pub total_value: f64,
    pub points: i64,
    pub npc: bool,
    pub solo: bool,
    pub awox: bool,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default, rename = "href")]
    pub esi: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Attacker {
    pub alliance_id: Option<u64>,
    pub corporation_id: Option<u64>,
    pub character_id: Option<u64>,
    pub faction_id: Option<u64>,
    pub damage_done: i64,
    pub final_blow: bool,
    pub security_status: f64,
    pub ship_type_id: Option<u32>,
    pub weapon_type_id: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Victim {
    pub alliance_id: Option<u64>,
    pub corporation_id: Option<u64>,
    pub character_id: Option<u64>,
    pub faction_id: Option<u64>,
    pub damage_taken: i64,
    pub items: Vec<VictimItem>,
    pub position: Option<Position>,
    pub ship_type_id: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VictimItem {
    pub item_type_id: i64,
    pub singleton: i64,
    pub flag: i64,
    pub quantity_destroyed: Option<i64>,
    pub quantity_dropped: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Position {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_r2z2_sequence_response_parse() {
        let json = r#"{"sequence": 96128620}"#;
        let resp: R2z2SequenceResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.sequence, 96128620);
    }

    #[test]
    fn test_r2z2_killmail_response_parse() {
        let json = r#"{
            "killmail_id": 123456789,
            "hash": "abc123def456",
            "zkb": {
                "locationID": 40000001,
                "hash": "abc123def456",
                "fittedValue": 1000000.0,
                "droppedValue": 500000.0,
                "destroyedValue": 500000.0,
                "totalValue": 1000000.0,
                "points": 10,
                "npc": false,
                "solo": false,
                "awox": false,
                "href": "https://esi.evetech.net/latest/killmails/123456789/abc123def456/"
            }
        }"#;

        let resp: R2z2KillmailResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.killmail_id, 123456789);
        assert_eq!(resp.hash, "abc123def456");
        assert_eq!(
            resp.zkb.esi,
            "https://esi.evetech.net/latest/killmails/123456789/abc123def456/"
        );
        assert!(!resp.zkb.npc);

        // Verify it converts to ZkDataNoEsi correctly
        let zk = ZkDataNoEsi {
            kill_id: resp.killmail_id,
            zkb: resp.zkb,
        };
        assert_eq!(zk.kill_id, 123456789);
    }

    #[test]
    fn test_r2z2_killmail_extra_fields_ignored() {
        // R2Z2 has extra fields like attackerCount that should be silently ignored
        let json = r#"{
            "killmail_id": 999,
            "hash": "xyz",
            "attackerCount": 5,
            "zkb": {
                "hash": "xyz",
                "fittedValue": 0.0,
                "droppedValue": 0.0,
                "destroyedValue": 0.0,
                "totalValue": 0.0,
                "points": 0,
                "npc": true,
                "solo": true,
                "awox": false,
                "href": "https://esi.evetech.net/latest/killmails/999/xyz/"
            }
        }"#;

        let resp: R2z2KillmailResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.killmail_id, 999);
    }
}
