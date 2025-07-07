use reqwest::Client;
use serde::Deserialize;
use std::error::Error;
use crate::config::{Name, Ship, System};

const ESI_URL: &str = "https://esi.evetech.net/latest/";
const FUZZWORK_URL: &str = "https://www.fuzzwork.co.uk/api/";

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Celestial {
    pub item_id: u64,
    pub type_id: u32,
    pub item_name: String,
    pub distance: f64,
}

#[derive(Clone)]
pub struct EsiClient {
    client: Client,
}

impl EsiClient {
    pub fn new() -> Self {
        EsiClient {
            client: Client::new(),
        }
    }

    async fn fetch<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, Box<dyn Error + Send + Sync>> {
        let url = format!("{}{}", ESI_URL, path);
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(format!("ESI API returned status: {}", response.status()).into());
        }
        let data: T = response.json().await?;
        Ok(data)
    }

    pub async fn get_system(&self, system_id: u32) -> Result<System, Box<dyn Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct EsiSystem {
            name: String,
            security_status: f64,
            constellation_id: u32,
            position: EsiPosition,
        }
        #[derive(Deserialize)]
        struct EsiPosition {
            x: f64,
            y: f64,
            z: f64,
        }
        #[derive(Deserialize)]
        struct EsiConstellation {
            #[allow(dead_code)]
            name: String,
            region_id: u32,
        }
        #[derive(Deserialize)]
        struct EsiRegion {
            name: String,
        }

        let system_info: EsiSystem = self.fetch(&format!("universe/systems/{}/", system_id)).await?;
        let constellation_info: EsiConstellation = self.fetch(&format!("universe/constellations/{}/", system_info.constellation_id)).await?;
        let region_info: EsiRegion = self.fetch(&format!("universe/regions/{}/", constellation_info.region_id)).await?;

        Ok(System {
            id: system_id,
            name: system_info.name,
            security_status: system_info.security_status,
            region_id: constellation_info.region_id,
            region: region_info.name,
            x: system_info.position.x,
            y: system_info.position.y,
            z: system_info.position.z,
        })
    }

    pub async fn get_ship(&self, ship_id: u32) -> Result<Ship, Box<dyn Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct EsiType {
            name: String,
            group_id: u32,
        }
        #[derive(Deserialize)]
        struct EsiGroup {
            name: String,
            category_id: u32,
        }
        
        let type_info: EsiType = self.fetch(&format!("universe/types/{}/", ship_id)).await?;
        let group_info: EsiGroup = self.fetch(&format!("universe/groups/{}/", type_info.group_id)).await?;

        Ok(Ship {
            id: ship_id,
            name: type_info.name,
            group_id: type_info.group_id,
            group: group_info.name,
        })
    }

    pub async fn get_name(&self, id: u64) -> Result<Name, Box<dyn Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct EsiName {
            id: u64,
            name: String,
            category: String,
        }
        let names: Vec<EsiName> = self.client.post(format!("{}universe/names/", ESI_URL))
            .json(&[id])
            .send()
            .await?
            .json()
            .await?;
        
        let name_info = names.into_iter().next().ok_or("No name found for ID")?;

        Ok(Name {
            id: name_info.id,
            name: name_info.name,
            category: name_info.category,
        })
    }

    pub async fn get_celestial(
        &self,
        system_id: u32,
        x: f64,
        y: f64,
        z: f64,
    ) -> Result<Celestial, Box<dyn Error + Send + Sync>> {
        let url = format!(
            "{}nearestCelestial.php?solarsystemid={}&x={}&y={}&z={}",
            FUZZWORK_URL, system_id, x, y, z
        );

        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(format!("Fuzzwork API returned status: {}", response.status()).into());
        }

        let celestial: Celestial = response.json().await?;
        Ok(celestial)
    }
}