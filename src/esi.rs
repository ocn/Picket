use crate::config::{EveAuthToken, StandingContact, System};
use reqwest::Client;
use serde::Deserialize;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

const ESI_URL: &str = "https://esi.evetech.net/latest/";
const FUZZWORK_URL: &str = "https://www.fuzzwork.co.uk/api/";
const ESI_AUTH_URL: &str = "https://login.eveonline.com/v2/oauth/token";
const ESI_VERIFY_URL: &str = "https://login.eveonline.com/oauth/verify";

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Celestial {
    #[serde(rename = "itemid")]
    pub item_id: u64,
    #[serde(rename = "typeid")]
    pub type_id: u32,
    #[serde(rename = "itemName")]
    pub item_name: String,
    pub distance: f64,
}

#[derive(Clone)]
pub struct EsiClient {
    client: Client,
}

impl Default for EsiClient {
    fn default() -> Self {
        Self::new()
    }
}

impl EsiClient {
    pub fn new() -> Self {
        EsiClient {
            client: Client::new(),
        }
    }

    async fn fetch<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
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

        let system_info: EsiSystem = self
            .fetch(&format!("universe/systems/{}/", system_id))
            .await?;
        let constellation_info: EsiConstellation = self
            .fetch(&format!(
                "universe/constellations/{}/",
                system_info.constellation_id
            ))
            .await?;
        let region_info: EsiRegion = self
            .fetch(&format!(
                "universe/regions/{}/",
                constellation_info.region_id
            ))
            .await?;

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

    pub async fn get_ship_group_id(
        &self,
        ship_id: u32,
    ) -> Result<u32, Box<dyn Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct EsiType {
            group_id: u32,
        }
        let type_info: EsiType = self.fetch(&format!("universe/types/{}/", ship_id)).await?;
        Ok(type_info.group_id)
    }

    pub async fn get_name(&self, id: u64) -> Result<String, Box<dyn Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct EsiName {
            name: String,
        }
        let names: Vec<EsiName> = self
            .client
            .post(format!("{}universe/names/", ESI_URL))
            .json(&[id])
            .send()
            .await?
            .json()
            .await?;

        let name_info = names.into_iter().next().ok_or("No name found for ID")?;
        Ok(name_info.name)
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
        tracing::trace!("Fetching celestial data from Fuzzwork: {}", url);

        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(format!("Fuzzwork API returned status: {}", response.status()).into());
        }

        let celestial: Celestial = response.json().await?;
        Ok(celestial)
    }

    pub async fn get_character_affiliation(
        &self,
        character_id: u64,
    ) -> Result<(u64, Option<u64>), Box<dyn Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct AffiliationResponse {
            corporation_id: u64,
            alliance_id: Option<u64>,
        }
        let url = format!("{}characters/affiliation/", ESI_URL);
        let response: Vec<AffiliationResponse> = self
            .client
            .post(&url)
            .json(&[character_id])
            .send()
            .await?
            .json()
            .await?;

        let affiliation = response
            .into_iter()
            .next()
            .ok_or("No affiliation found for character")?;
        Ok((affiliation.corporation_id, affiliation.alliance_id))
    }

    pub async fn exchange_code_for_token(
        &self,
        code: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<EveAuthToken, Box<dyn Error + Send + Sync>> {
        let params = [("grant_type", "authorization_code"), ("code", code)];
        let auth_response: serde_json::Value = self
            .client
            .post(ESI_AUTH_URL)
            .basic_auth(client_id, Some(client_secret))
            .form(&params)
            .send()
            .await?
            .json()
            .await?;

        let access_token = auth_response["access_token"]
            .as_str()
            .ok_or("Missing access_token")?
            .to_string();
        let refresh_token = auth_response["refresh_token"]
            .as_str()
            .ok_or("Missing refresh_token")?
            .to_string();
        let expires_in = auth_response["expires_in"]
            .as_u64()
            .ok_or("Missing expires_in")?;

        let verify_response: serde_json::Value = self
            .client
            .get(ESI_VERIFY_URL)
            .bearer_auth(&access_token)
            .send()
            .await?
            .json()
            .await?;
        tracing::info!("SSO Verify Response: {:?}", verify_response);

        let character_id = verify_response["CharacterID"]
            .as_u64()
            .ok_or("Missing CharacterID")?;
        let character_name = verify_response["CharacterName"]
            .as_str()
            .ok_or("Missing CharacterName")?
            .to_string();

        let (corporation_id, alliance_id) = self.get_character_affiliation(character_id).await?;

        let expires_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + expires_in;

        Ok(EveAuthToken {
            character_id,
            character_name,
            corporation_id,
            alliance_id,
            access_token,
            refresh_token,
            expires_at,
        })
    }

    pub async fn get_contacts(
        &self,
        entity_id: u64,
        token: &str,
        endpoint: &str,
    ) -> Result<Vec<StandingContact>, Box<dyn Error + Send + Sync>> {
        let url = format!("{}{}/{}/contacts/", ESI_URL, endpoint, entity_id);
        let contacts: Vec<StandingContact> = self
            .client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await?
            .json()
            .await?;
        Ok(contacts)
    }
}

// pub mod contracts {
//     use oauth2::basic::BasicClient;
//     use oauth2::{
//         AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge, RedirectUrl,
//         RefreshToken, Scope, TokenResponse,
//     };
//     use phf::phf_map;
//     use serde::{Deserialize, Serialize};
//     use std::collections::HashMap;
//     use std::fs;
//     use std::io::{self, Write};
//
//     // --- Structs and Types ---
//
//     #[derive(Debug, Serialize, Deserialize, Clone)]
//     pub struct Token {
//         pub access_token: String,
//         pub refresh_token: String,
//     }
//
//     #[derive(Debug, Serialize, Deserialize, Clone)]
//     pub struct EveSsoConfig {
//         pub client_id: String,
//         pub client_secret: String,
//         pub auth_url: String,
//         pub token_url: String,
//         pub redirect_url: String,
//     }
//
//     #[derive(Debug, Serialize, Deserialize, Clone)]
//     pub struct Contract {
//         pub collateral: f64,
//         pub contract_id: u64,
//         pub date_expired: String,
//         pub date_issued: String,
//         pub days_to_complete: i32,
//         pub end_location_id: u64,
//         pub for_corporation: bool,
//         pub issuer_corporation_id: u64,
//         pub issuer_id: u64,
//         pub reward: f64,
//         pub start_location_id: u64,
//         pub status: String,
//         #[serde(rename = "type")]
//         pub contract_type: String,
//         pub volume: f64,
//         #[serde(skip_serializing_if = "Option::is_none")]
//         pub reward_volume_ratio: Option<f64>,
//     }
//
//     #[derive(Debug, Serialize, Deserialize, Clone)]
//     pub struct ContractItem {
//         pub volume: f64,
//         pub reward: f64,
//         pub ratio: Option<f64>,
//     }
//
//     #[derive(Debug, Serialize, Deserialize, Clone)]
//     pub struct Trip {
//         pub contracts_for_trip: Vec<ContractItem>,
//         pub total_volume: f64,
//         pub total_reward: f64,
//     }
//
//     pub struct EsiContractClient {
//         config: EveSsoConfig,
//         http_client: reqwest::Client,
//     }
//
//     // --- Static Data ---
//
//     static SYSTEM_NAMES: phf::Map<u64, &'static str> = phf_map! {
//         60003760u64 => "Jita",
//         1043353719436u64 => "ARG-3R",
//         1043323292260u64 => "Turnur",
//         1041466299547u64 => "Turnur",
//         1042334218683u64 => "Turnur",
//         1044223724672u64 => "Hasateem",
//         1043235801721u64 => "Turnur",
//         1043136314480u64 => "Ahbazon",
//         1022167642188u64 => "Amamake",
//     };
//
//     // --- Implementations ---
//
//     impl EsiContractClient {
//         pub fn new() -> Self {
//             Self {
//                 config: EveSsoConfig {
//                     client_id: "96e9cea503904a089b64568845c34cb4".to_string(),
//                     client_secret: "9KpvVjBUiL39bEHHofFp7NNxIj9U46UyLG6xl7Lb".to_string(),
//                     auth_url: "https://login.eveonline.com/v2/oauth/authorize".to_string(),
//                     token_url: "https://login.eveonline.com/v2/oauth/token".to_string(),
//                     redirect_url: "https://pyfa-org.github.io/Pyfa/callback".to_string(),
//                 },
//                 http_client: reqwest::Client::new(),
//             }
//         }
//
//         fn get_oauth_client(&self) -> Result<(), String> {
//             Ok(
//                 BasicClient::new(ClientId::new(self.config.client_id.clone()))
//                     .set_client_secret(ClientSecret::new(self.config.client_secret.clone()))
//                     .set_auth_uri(
//                         oauth2::AuthUrl::new(self.config.auth_url.clone())
//                             .map_err(|e| e.to_string())?,
//                     )
//                     .set_redirect_uri(
//                         RedirectUrl::new(self.config.redirect_url.clone())
//                             .map_err(|e| e.to_string())?,
//                     ),
//             )
//
//             // Some(),
//             // ,
//             // Some(TokenUrl::new(self.config.token_url.clone()).map_err(|e| e.to_string())?),
//         }
//         pub async fn eve_sso_login(&self) -> Result<(), Box<dyn std::error::Error>> {
//             let client = self.get_oauth_client()?;
//             let contract_scopes = [
//                 "esi-search.search_structures.v1",
//                 "esi-universe.read_structures.v1",
//                 "esi-corporations.read_structures.v1",
//                 "esi-contracts.read_character_contracts.v1",
//                 "esi-contracts.read_corporation_contracts.v1",
//             ];
//             let scopes = contract_scopes.iter().map(|s| Scope::new(s.to_string()));
//
//             let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
//
//             let (auth_url, _csrf_token) = client
//                 .authorize_url(CsrfToken::new_random)
//                 .add_scopes(scopes)
//                 .set_pkce_challenge(pkce_challenge)
//                 .url();
//
//             println!("Opening browser for EVE SSO login...");
//             webbrowser::open(auth_url.as_str())?;
//
//             print!("Please enter your grant code: ");
//             io::stdout().flush()?;
//             let mut grant_code = String::new();
//             io::stdin().read_line(&mut grant_code)?;
//
//             let token_result = client
//                 .exchange_code(AuthorizationCode::new(grant_code.trim().to_string()))
//                 .set_pkce_verifier(pkce_verifier)
//                 .request_async(async_http_client)
//                 .await?;
//
//             println!("Access Token received, writing to accessToken.json");
//             fs::write("accessToken.json", serde_json::to_string(&token_result)?)?;
//
//             Ok(())
//         }
//
//         pub async fn eve_sso_refresh(&self) -> Result<String, Box<dyn std::error::Error>> {
//             let client = self.get_oauth_client()?;
//             let token_json = fs::read_to_string("accessToken.json")?;
//             let token: oauth2::StandardTokenResponse<
//                 oauth2::EmptyExtraTokenFields,
//                 oauth2::basic::BasicTokenType,
//             > = serde_json::from_str(&token_json)?;
//
//             let refresh_token = token.refresh_token().ok_or("No refresh token found")?;
//             let refresh_token_struct = RefreshToken::new(refresh_token.secret().clone());
//
//             let new_token = client
//                 .exchange_refresh_token(&refresh_token_struct)
//                 .request_async(async_http_client)
//                 .await?;
//
//             println!("Access Token refreshed, writing to accessToken.json");
//             fs::write("accessToken.json", serde_json::to_string(&new_token)?)?;
//
//             Ok(new_token.access_token().secret().clone())
//         }
//
//         pub async fn get_corporation_contracts(
//             &self,
//             corporation_id: u64,
//             access_token: &str,
//         ) -> Result<Vec<Contract>, reqwest::Error> {
//             let mut contracts = Vec::new();
//             let mut page = 1;
//
//             loop {
//                 let url = format!(
//                     "https://esi.evetech.net/latest/corporations/{}/contracts/?page={}",
//                     corporation_id, page
//                 );
//                 let response = self
//                     .http_client
//                     .get(&url)
//                     .bearer_auth(access_token)
//                     .send()
//                     .await?;
//
//                 if !response.status().is_success() {
//                     return Err(response.error_for_status().unwrap_err());
//                 }
//
//                 let mut page_contracts: Vec<Contract> = response.json().await?;
//                 if page_contracts.is_empty() {
//                     break;
//                 }
//                 contracts.append(&mut page_contracts);
//                 page += 1;
//             }
//
//             Ok(contracts)
//         }
//
//         pub fn get_system_name(location_id: u64) -> Result<String, String> {
//             SYSTEM_NAMES
//                 .get(&location_id)
//                 .map(|s| s.to_string())
//                 .ok_or_else(|| format!("Unknown location ID: {}", location_id))
//         }
//
//         pub async fn process_contracts(
//             &self,
//             access_token: &str,
//             corporation_id: u64,
//             max_volume: f64,
//         ) -> Result<HashMap<String, Vec<Trip>>, Box<dyn std::error::Error>> {
//             let mut contracts = self
//                 .get_corporation_contracts(corporation_id, access_token)
//                 .await?;
//
//             // Calculate reward/volume ratio
//             for contract in &mut contracts {
//                 if contract.volume > 0.0 {
//                     contract.reward_volume_ratio = Some(contract.reward / contract.volume);
//                 }
//             }
//
//             // Filter for available courier contracts with no collateral and a good ratio
//             let filtered_contracts: Vec<Contract> = contracts
//                 .into_iter()
//                 .filter(|c| {
//                     c.contract_type == "courier"
//                         && !matches!(
//                             c.status.as_str(),
//                             "finished" | "deleted" | "failed" | "in_progress"
//                         )
//                         && c.collateral <= 0.0
//                         && c.reward_volume_ratio.unwrap_or(0.0) >= 350.0
//                 })
//                 .collect();
//
//             println!("Processing {} contracts", filtered_contracts.len());
//
//             // Group contracts by route (start-end)
//             let mut grouped_contracts: HashMap<String, Vec<Contract>> = HashMap::new();
//             for contract in filtered_contracts {
//                 let start_system = Self::get_system_name(contract.start_location_id)?;
//                 let end_system = Self::get_system_name(contract.end_location_id)?;
//                 let key = format!("{}-{}", start_system, end_system);
//                 grouped_contracts.entry(key).or_default().push(contract);
//             }
//
//             let mut trips_by_route: HashMap<String, Vec<Trip>> = HashMap::new();
//
//             for (key, mut group) in grouped_contracts {
//                 let mut route_trips = Vec::new();
//
//                 // Continue creating trips as long as there are contracts left
//                 while !group.is_empty() {
//                     // Prepare items for the knapsack solver
//                     let items: Vec<knapsack::Item> = group
//                         .iter()
//                         .map(|c| knapsack::Item {
//                             // width
//                             weight: c.volume as usize,
//                             // height
//                             value: c.reward as usize,
//                         })
//                         .collect();
//
//                     let mut knapsack = knapsack::Knapsack::new(max_volume as usize);
//                     for item in items {
//                         knapsack.add_item(item);
//                     }
//
//                     let solution = knapsack.solve();
//                     if solution.items.is_empty() {
//                         break; // No more items can fit
//                     }
//
//                     let mut current_trip_contracts = Vec::new();
//                     let mut used_contract_indices = std::collections::HashSet::new();
//
//                     // Reconstruct the trip from the knapsack solution
//                     for solved_item in &solution.items {
//                         // Find the original contract that matches the solved item
//                         if let Some((index, contract)) = group.iter().enumerate().find(|(i, c)| {
//                             !used_contract_indices.contains(i)
//                                 && c.volume as usize == solved_item.weight
//                                 && c.reward as usize == solved_item.value
//                         }) {
//                             current_trip_contracts.push(contract.clone());
//                             used_contract_indices.insert(index);
//                         }
//                     }
//
//                     if current_trip_contracts.is_empty() {
//                         break;
//                     }
//
//                     let trip_items: Vec<ContractItem> = current_trip_contracts
//                         .iter()
//                         .map(|c| ContractItem {
//                             volume: c.volume,
//                             reward: c.reward,
//                             ratio: c.reward_volume_ratio,
//                         })
//                         .collect();
//
//                     route_trips.push(Trip {
//                         total_volume: trip_items.iter().map(|i| i.volume).sum(),
//                         total_reward: trip_items.iter().map(|i| i.reward).sum(),
//                         contracts_for_trip: trip_items,
//                     });
//
//                     // Remove used contracts from the group for the next iteration
//                     let mut i = 0;
//                     group.retain(|_| {
//                         let used = used_contract_indices.contains(&i);
//                         i += 1;
//                         !used
//                     });
//                 }
//
//                 // Sort trips for this route by total reward
//                 route_trips.sort_by(|a, b| {
//                     b.total_reward
//                         .partial_cmp(&a.total_reward)
//                         .unwrap_or(std::cmp::Ordering::Equal)
//                 });
//                 trips_by_route.insert(key, route_trips);
//             }
//
//             Ok(trips_by_route)
//         }
//     }
// }
