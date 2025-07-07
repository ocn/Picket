use serde::Deserialize;

/// Represents the top-level JSON object from the zKillboard RedisQ stream.
/// The `package` field can be null if there's no new killmail.
#[derive(Debug, Deserialize)]
pub struct RedisQResponse {
    pub package: Option<ZkData>,
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

#[derive(Debug, Deserialize, Clone)]
pub struct Zkb {
    #[serde(rename = "locationID")]
    pub location_id: i64,
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
    #[serde(rename = "href")]
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
