import {Axios, AxiosResponse} from 'axios';
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

export class EsiClient {
    private axios: Axios;

    constructor() {
        this.axios = new Axios({baseURL: ESI_URL, responseType: 'json', transformResponse: data => JSON.parse(data)});
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
}