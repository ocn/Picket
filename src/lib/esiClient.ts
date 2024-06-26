import {Axios, AxiosResponse} from 'axios';
import pack from 'bin-pack';
import {AccessToken, AuthorizationCode} from 'simple-oauth2';
import promptSync from 'prompt-sync';
import {exec} from 'child_process';
import fs from 'fs';
import {ClosestCelestial, SolarSystem} from '../zKillSubscriber';
import * as util from 'util';


const ESI_URL = 'https://esi.evetech.net/latest/';
const GET_SOLAR_SYSTEM_URL = 'universe/systems/%1/';
const GET_CONSTELLATION_URL = 'universe/constellations/%1/';
const GET_REGION_URL = 'universe/regions/%1/';
const GET_TYPE_DATA_URL = 'universe/types/%1/';
const GET_ALLIANCE_URL = 'alliances/%1/';
const GET_CORPORATION_URL = 'corporations/%1/';
const GET_CHARACTER_URL = 'characters/%1/';

interface Token {
    access_token: string;
    refresh_token: string;
}

export interface EveSSOConfig {
    client: { id: string; secret: string };
    auth: { tokenPath: string; tokenHost: string; authorizePath: string };
}

export class EsiClient {
    private axios: Axios;
    private token?: Token;
    private contractScopes: string;
    private config: EveSSOConfig;

    constructor() {
        this.axios = new Axios({baseURL: ESI_URL, responseType: 'json', transformResponse: data => JSON.parse(data)});
        this.contractScopes = 'esi-search.search_structures.v1 ' +
            'esi-universe.read_structures.v1 ' +
            'esi-corporations.read_structures.v1 ' +
            'esi-contracts.read_character_contracts.v1 ' +
            'esi-contracts.read_corporation_contracts.v1';
        this.config = {
            client: {
                id: '96e9cea503904a089b64568845c34cb4',
                secret: '9KpvVjBUiL39bEHHofFp7NNxIj9U46UyLG6xl7Lb'
            },
            auth: {
                tokenHost: 'https://login.eveonline.com',
                tokenPath: 'v2/oauth/token',
                authorizePath: '/v2/oauth/authorize',
            }
        };
    }

    async eveSsoLogin() {

        const client = new AuthorizationCode(this.config);
        // eslint-disable-next-line @typescript-eslint/no-unused-vars

        const authorizationUri = client.authorizeURL({
            redirect_uri: 'https://pyfa-org.github.io/Pyfa/callback',
            scope: this.contractScopes,
            state: '1231231231231'
        });

        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const browserProcess = exec(`open '${authorizationUri}'`);

        const prompt = promptSync({sigint: true});
        const grantCode = prompt('Please enter your grant code: ');

        const tokenParams = {
            code: grantCode,
            redirect_uri: 'https://pyfa-org.github.io/Pyfa/callback',
            scope: this.contractScopes,
        };

        let accessToken: AccessToken;
        try {
            accessToken = await client.getToken(tokenParams);
            // write the token to a file
            console.log('Access Token:', accessToken.token);
            fs.writeFileSync('accessToken.json', JSON.stringify(accessToken));
        } catch (error: any) {
            console.log('Access Token Error', error.message);
            throw error;
        }
    }

    async eveSsoRefresh() {
        const client = new AuthorizationCode(this.config);
        // load token from file
        const accessTokenJSONString = fs.readFileSync('accessToken.json', 'utf8');
        let accessToken = client.createToken(JSON.parse(accessTokenJSONString));
        if (accessToken.expired()) {
            try {
                const refreshParams = {
                    scope: this.contractScopes,
                };
                accessToken = await accessToken.refresh(refreshParams);
                console.log('Access Token:', accessToken.token);
                fs.writeFileSync('accessToken.json', JSON.stringify(accessToken));
                return accessToken.token;
            } catch (error: any) {
                console.log('Error refreshing access token: ', error.message);
                throw new Error('Access Token refresh Error');
            }
        } else {
            console.log('Access Token:', accessToken.token);
            return accessToken.token;
        }
    }

    async fetch(path: string): Promise<AxiosResponse<any, any>> {
        return await this.axios.get(path);
    }

    async getSystemInfo(systemId: number): Promise<SolarSystem> {
        const systemData = await this.fetch(GET_SOLAR_SYSTEM_URL.replace('%1', systemId.toString()));
        if (systemData.data.error) {
            console.log('SYSTEM_FETCH_ERROR: ' + systemData.data.error);
            throw new Error('SYSTEM_FETCH_ERROR: ' + systemData.data.error);
        }
        const constData = await this.fetch(GET_CONSTELLATION_URL.replace('%1', systemData.data.constellation_id));
        if (systemData.data.error) {
            console.log('CONST_FETCH_ERROR: ' + systemData.data.error);
            throw new Error('CONST_FETCH_ERROR');
        }
        const regionData = await this.fetch(GET_REGION_URL.replace('%1', constData.data.region_id));
        if (systemData.data.error) {
            console.log('REGION_FETCH_ERROR: ' + systemData.data.error);
            throw new Error('REGION_FETCH_ERROR');
        }
        return {
            id: systemId,
            systemName: systemData.data.name,
            regionId: regionData.data.region_id,
            regionName: regionData.data.name,
            constellationId: constData.data.constellation_id,
            constellationName: constData.data.name,
            securityStatus: systemData.data.security_status,
        };
    }

    async getTypeName(typeId: number): Promise<string> {
        const itemData = await this.fetch(GET_TYPE_DATA_URL.replace('%1', typeId.toString()));
        if (itemData.data.error) {
            throw new Error('ITEM_FETCH_ERROR');
        }
        return itemData.data.name;
    }

    async getTypeGroupId(shipId: number): Promise<number> {
        const itemData = await this.fetch(GET_TYPE_DATA_URL.replace('%1', shipId.toString()));
        if (itemData.data.error) {
            throw new Error('ITEM_FETCH_ERROR');
        }
        return Number.parseInt(itemData.data.group_id);
    }

    async getAllianceName(allianceId: number): Promise<string> {
        const itemData = await this.fetch(GET_ALLIANCE_URL.replace('%1', allianceId.toString()));
        if (itemData.data.error) {
            throw new Error('ITEM_FETCH_ERROR');
        }
        return itemData.data.name;
    }

    async getCorporationName(corporationId: number): Promise<string> {
        const itemData = await this.fetch(GET_CORPORATION_URL.replace('%1', corporationId.toString()));
        if (itemData.data.error) {
            throw new Error('ITEM_FETCH_ERROR');
        }
        return itemData.data.name;
    }

    async getCharacterName(characterId: number): Promise<string> {
        const itemData = await this.fetch(GET_CHARACTER_URL.replace('%1', characterId.toString()));
        if (itemData.data.error) {
            throw new Error('ITEM_FETCH_ERROR');
        }
        return itemData.data.name;
    }

    async getCelestial(systemId: number, x: number, y: number, z: number): Promise<ClosestCelestial> {
        const axios = new Axios({
            baseURL: 'https://www.fuzzwork.co.uk/api/',
            responseType: 'json',
            transformResponse: data => JSON.parse(data)
        });
        const celestialData = await axios.get(`nearestCelestial.php?x=${x}&y=${y}&z=${z}&solarsystemid=${systemId}`);
        return {
            distance: celestialData.data.distance,
            itemId: celestialData.data.itemid,
            itemName: celestialData.data.itemName,
            typeId: celestialData.data.typeid
        };
    }

    async getCorporationContracts(corporationId: number): Promise<Contract[]> {
        const contracts = [];
        let page = 1;
        let response;

        do {
            response = await this.fetch(`corporations/${corporationId}/contracts/?page=${page}`);
            contracts.push(...response.data);
            page++;
        } while (response.data.length > 0);

        return contracts;
    }

    getSystemName(locationId: number): string {
        const systemNames: { [key: number]: string } = {
            60003760: 'Jita',
            1043353719436: 'ARG-3R',
            1043323292260: 'Turnur',
            1041466299547: 'Turnur',
            1042334218683: 'Turnur',
            1044223724672: 'Hasateem',
            1043235801721: 'Turnur',
            1043136314480: 'Ahbazon',
            1022167642188: 'Amamake',
            // Add more mappings here
        };
        const name = systemNames[locationId];
        if (!name || name === '') {
            throw new Error(`Unknown location ID: ${locationId}`);
        }
        return name;
    }

    async processContracts(accessToken: string, maxVolume = 201980) {
        // const data = fs.readFileSync(filePath, 'utf8');
        // let contracts: Contract[] = JSON.parse(data);
        const client = new EsiClient();
        client.axios.interceptors.request.use((config) => {
            if (!config.headers) {
                config.headers = {};
            }
            config.headers.Authorization = `Bearer ${accessToken}`;
            return config;
        });
        let contracts = await client.getCorporationContracts(98697633);

        // Calculate the ratio between the reward and the volume for each contract
        contracts.forEach(contract => {
            contract.reward = parseFloat(contract.reward.toFixed(2));
            contract.volume = parseFloat(contract.volume.toFixed(2));
            contract.rewardVolumeRatio = parseFloat((contract.reward / contract.volume).toFixed(2));
        });

        // Filter out contracts that are waiting to be accepted
        contracts = contracts.filter(contract => contract.type === 'courier' && !['finished', 'deleted', 'failed', 'in_progress'].includes(contract.status) && contract.collateral <= 0 && <number>contract.rewardVolumeRatio >= 350);
        console.log(`Processing ${contracts.length} contracts`);

        // Group contracts based on matching start_location_id and end_location_id values
        const groupedContracts: { [key: string]: Contract[] } = {};
        contracts.forEach(contract => {
            let startSystemName;
            let endSystemName;
            try {
                startSystemName = this.getSystemName(contract.start_location_id);
                endSystemName = this.getSystemName(contract.end_location_id);
            } catch (e) {
                console.log(e, contract);
                throw new Error(`${e}: ${util.inspect(contract, false, 5, true)}`);
            }
            const key = `${startSystemName}-${endSystemName}`;
            if (!groupedContracts[key]) {
                groupedContracts[key] = [];
            }
            groupedContracts[key].push(contract);
        });

        // For each group of contracts, apply the bin-packing algorithm and split them into trips
        const trips: { [key: string]: Trip[] } = {};
        for (const key in groupedContracts) {
            const group = groupedContracts[key];

            // Sort the contracts by volume and reward, highest values first
            group.sort((a, b) => {
                if (b.volume === a.volume) {
                    return b.reward - a.reward;
                }
                return <number>b.volume - <number>a.volume;
            });

            // Use the bin-packing algorithm to best-fit the contracts into the maximum volume
            const bins: PackItem[] = group.map(contract => ({
                width: contract.volume,
                height: contract.reward,
                item: contract,
            }));

            const result = pack(bins);

            // The maximum volume represents a single trip. When the maximum volume is reached for one trip, begin packing the remaining contracts into a separate maximum volume for a separate trip
            const groupTrips: Trip[] = [];
            let currentTrip: ContractItem[] = [];
            let currentVolume = 0;
            for (let i = 0; i < result.items.length; i++) {
                if (currentVolume + result.items[i].width > maxVolume) {
                    groupTrips.push({
                        contractsForTrip: currentTrip,
                        totalVolume: currentTrip.reduce((total, contractItem) => total + contractItem.volume, 0),
                        totalReward: currentTrip.reduce((total, contractItem) => total + contractItem.reward, 0),
                    });
                    currentTrip = [{
                        volume: result.items[i].item.width,
                        reward: result.items[i].item.height,
                        ratio: result.items[i].item.item.rewardVolumeRatio,
                    }];
                    currentVolume = result.items[i].width;
                } else {
                    currentTrip.push({
                        volume: result.items[i].item.width,
                        reward: result.items[i].item.height,
                        ratio: result.items[i].item.item.rewardVolumeRatio,
                    });
                    currentVolume += result.items[i].width;
                }
            }

            if (currentTrip.length > 0) {
                groupTrips.push({
                    contractsForTrip: currentTrip,
                    totalVolume: currentTrip.reduce((total, contractItem) => total + contractItem.volume, 0),
                    totalReward: currentTrip.reduce((total, contractItem) => total + contractItem.reward, 0),
                });
            }

            trips[key] = groupTrips;

            // // Prioritize contracts with a volume value equal to or under 60,000 ONLY IF the reward/volume ratio exceeds 350
            // const prioritizedContracts = group.filter(contract => contract.volume <= 60000 && contract.rewardVolumeRatio! > 500);
            // const remainingContracts = group.filter(contract => contract.volume > 60000 || contract.rewardVolumeRatio! <= 500);
            //
            // // Sort the prioritized contracts by volume and reward, highest values first
            // prioritizedContracts.sort((a, b) => {
            //     if (b.volume === a.volume) {
            //         return b.reward - a.reward;
            //     }
            //     return b.volume - a.volume;
            // });
            //
            // // Sort the remaining contracts by volume and reward, highest values first
            // remainingContracts.sort((a, b) => {
            //     if (b.volume === a.volume) {
            //         return b.reward - a.reward;
            //     }
            //     return b.volume - a.volume;
            // });
            //
            // // Use the bin-packing algorithm to best-fit the prioritized contracts into the remaining volume from the maximum
            // let bins: PackItem[] = prioritizedContracts.map(contract => ({
            //     width: contract.volume,
            //     height: contract.reward,
            //     item: contract,
            // }));
            //
            // let result = pack(bins);
            //
            // // The maximum volume represents a single trip. When the maximum volume is reached for one trip, begin packing the remaining contracts into a separate maximum volume for a separate trip
            // const groupTrips: Trip[] = [];
            // let currentTrip: ContractItem[] = [];
            // let currentVolume = 0;
            // for (let i = 0; i < result.items.length; i++) {
            //     if (currentVolume + result.items[i].width > maxVolume) {
            //         groupTrips.push({
            //             contractsForTrip: currentTrip,
            //             totalVolume: currentTrip.reduce((total, contractItem) => total + contractItem.volume, 0),
            //             totalReward: currentTrip.reduce((total, contractItem) => total + contractItem.reward, 0),
            //         });
            //         currentTrip = [{
            //             volume: result.items[i].item.width,
            //             reward: result.items[i].item.height,
            //             ratio: result.items[i].item.item.rewardVolumeRatio,
            //         }];
            //         currentVolume = result.items[i].width;
            //     } else {
            //         currentTrip.push({
            //             volume: result.items[i].item.width,
            //             reward: result.items[i].item.height,
            //             ratio: result.items[i].item.item.rewardVolumeRatio,
            //         });
            //         currentVolume += result.items[i].width;
            //     }
            // }
            //
            // // Use the bin-packing algorithm to best-fit the remaining contracts into the remaining volume from the maximum
            // bins = remainingContracts.map(contract => ({
            //     width: contract.volume,
            //     height: contract.reward,
            //     item: contract,
            // }));
            //
            // result = pack(bins);
            //
            // for (let i = 0; i < result.items.length; i++) {
            //     if (currentVolume + result.items[i].width > maxVolume) {
            //         groupTrips.push({
            //             contractsForTrip: currentTrip,
            //             totalVolume: currentTrip.reduce((total, contractItem) => total + contractItem.volume, 0),
            //             totalReward: currentTrip.reduce((total, contractItem) => total + contractItem.reward, 0),
            //         });
            //         currentTrip = [{
            //             volume: result.items[i].item.width,
            //             reward: result.items[i].item.height,
            //             ratio: result.items[i].item.item.rewardVolumeRatio,
            //         }];
            //         currentVolume = result.items[i].width;
            //     } else {
            //         currentTrip.push({
            //             volume: result.items[i].item.width,
            //             reward: result.items[i].item.height,
            //             ratio: result.items[i].item.item.rewardVolumeRatio,
            //         });
            //         currentVolume += result.items[i].width;
            //     }
            // }
            //
            // if (currentTrip.length > 0) {
            //     groupTrips.push({
            //         contractsForTrip: currentTrip,
            //         totalVolume: currentTrip.reduce((total, contractItem) => total + contractItem.volume, 0),
            //         totalReward: currentTrip.reduce((total, contractItem) => total + contractItem.reward, 0),
            //     });
            // }
            //
            // trips[key] = groupTrips;
        }

        for (const key in trips) {
            trips[key].sort((a, b) => b.totalReward - a.totalReward);
        }

        return {trips};
    }
}

export interface Contract {
    acceptor_id: number;
    assignee_id: number;
    availability: string;
    collateral: number;
    contract_id: number;
    date_expired: string;
    date_issued: string;
    days_to_complete: number;
    end_location_id: number;
    for_corporation: boolean;
    issuer_corporation_id: number;
    issuer_id: number;
    price: number;
    reward: number;
    start_location_id: number;
    status: string;
    title: string;
    type: string;
    volume: number;

    rewardVolumeRatio?: number;
}

interface PackItem {
    width: number;
    height: number;
    item: Contract;
}

export interface ContractItem {
    volume: number; // width
    reward: number; // height
    ratio?: number;
}

export interface Trip {
    contractsForTrip: ContractItem[];
    totalVolume: number;
    totalReward: number,
}