use reqwest::Client;
use serde::Deserialize;
use std::error::Error;
use crate::config::System;

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
