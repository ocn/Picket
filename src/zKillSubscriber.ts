import {
    Client,
    ColorResolvable,
    DiscordAPIError,
    MessageEmbed,
    MessageEmbedOptions,
    MessageOptions,
    TextChannel
} from 'discord.js';
import {MessageEvent, WebSocket} from 'ws';
import {REST} from '@discordjs/rest';
import AsyncLock from 'async-lock';
import MemoryCache from 'memory-cache';
import ogs from 'open-graph-scraper';
import {APIEmbed} from 'discord-api-types/v10';
import * as fs from 'fs';
import * as util from 'util';
import {EsiClient} from './lib/esiClient';

export enum SubscriptionType {
    PUBLIC = 'public',
}

export enum LimitType {
    REGION = 'region',
    CONSTELLATION = 'constellation',
    SYSTEM = 'system',
    SHIP_INCLUSION_TYPE_ID = 'type',
    SHIP_EXCLUSION_TYPE_ID = 'excludedType',
    SECURITY_MAX_INCLUSIVE = 'securityMaxInclusive',
    SECURITY_MIN_INCLUSIVE = 'securityMinInclusive',
    SECURITY_MAX_EXCLUSIVE = 'securityMaxExclusive',
    SECURITY_MIN_EXCLUSIVE = 'securityMinExclusive',
    ALLIANCE = 'alliance',
    CORPORATION = 'corporation',
    CHARACTER = 'character',
    // A partial name of the entity type to require for sending
    NAME_FRAGMENT = 'nameFragment',
    MIN_NUM_INVOLVED = 'minNumInvolved',
    TIME_RANGE_START = 'startingTime',
    TIME_RANGE_END = 'endingTime',
    NPC_ONLY = 'npcOnly',
    LY_RANGE_TO_SYSTEM_WITH_NAME = 'lyRangeToSystemWithName',
}

export interface SubscriptionGuild {
    channels: Map<string, SubscriptionChannel>;
}

export interface SubscriptionChannel {
    subscriptions: Map<string, Subscription>;
}

export interface Subscription {
    subType: SubscriptionType
    id?: string,
    minValue: number,
    // Mapping of LimitType to the value(s) to compare against
    limitTypes: Map<LimitType, string>,
    inclusionLimitAlsoComparesAttacker: boolean,
    inclusionLimitAlsoComparesAttackerWeapons: boolean,
    exclusionLimitAlsoComparesAttacker: boolean,
    exclusionLimitAlsoComparesAttackerWeapons: boolean
}

export interface SubscriptionFlags {
    // If true, the limitTypes will be compared against the attacker's ship
    inclusionLimitAlsoComparesAttacker: boolean;
    // If true, the limitTypes will be compared against the weapon type IDs on the attacker's ship
    // zKillboard will sometimes list weapon type IDs as the attacking ship, instead of the actual ship type ID
    inclusionLimitAlsoComparesAttackerWeapons: boolean;
    // If true, the limitTypes will be compared against the attacker's ship
    exclusionLimitAlsoComparesAttacker: boolean;
    // If true, the limitTypes will be compared against the weapon type IDs on the attacker's ship
    // zKillboard will sometimes list weapon type IDs as the attacking ship, instead of the actual ship type ID
    exclusionLimitAlsoComparesAttackerWeapons: boolean;
}

export type AllianceDescription = {
    id: number,
    name: string,
    ticker: string,
}

export type PrepareEmbedFields = {
    guildId: string,
    channelId: string,
    subscription: Subscription,
    embedding: any,
    data: ZkData,
    matchedShip: FilterShipMatch | null,
    minNumInvolved: number | null,
    messageColor: ColorResolvable,
};

export type FilterShipMatch = {
    shipName: string | null,
    typeId: number | null,
    corpId: number | null,
    allianceId: number | null,
}

export class Attacker {
    alliance_id: number | null;
    corporation_id: number | null;
    damage_done: number;
    final_blow: boolean;
    security_status: number;
    ship_type_id?: number;
    weapon_type_id?: number;
    character_id?: number;
    faction_id?: number;

    constructor(
        alliance_id: number,
        corporation_id: number,
        damage_done: number,
        final_blow: boolean,
        security_status: number,
        weapon_type_id: number,
        ship_type_id?: number,
        character_id?: number,
        faction_id?: number
    ) {
        this.alliance_id = alliance_id;
        this.corporation_id = corporation_id;
        this.damage_done = damage_done;
        this.final_blow = final_blow;
        this.security_status = security_status;
        this.weapon_type_id = weapon_type_id;
        this.ship_type_id = ship_type_id;
        this.character_id = character_id;
        this.faction_id = faction_id;
    }
}

// class UniverseMap {
//     regions: Map<number, RegionMap>;
// }
//
// class RegionMap {
//     name: string;
//     systems: System[];
//     connections: Connection[];
// }
//
// class System {
//     id: number;
//     name: string;
//     hasStation: boolean;
//     region: string;
//     x: number;
//     y: number;
// }
//
// class Connection {
//     a: number;
//     b: number;
//     type: 'jc';
//     x1: string;
//     y1: string;
//     x2: string;
//     y2: string;
// }

export type Position = {
    x: number;
    y: number;
    z: number;
};

export type Victim = {
    alliance_id: number;
    corporation_id: number;
    damage_taken: number;
    items: VictimItem[];
    position: Position;
    ship_type_id?: number; // ship_type_id is now optional
    character_id?: number; // character_id is optional and may be present instead of ship_type_id
};

export type VictimItem = {
    item_type_id: number;
    singleton: number;
    flag: number;
    quantity_destroyed?: number;
    quantity_dropped?: number;
}

export type Zkb = {
    locationID: number;
    hash: string;
    fittedValue: number;
    droppedValue: number;
    destroyedValue: number;
    totalValue: number;
    points: number;
    npc: boolean;
    solo: boolean;
    awox: boolean;
    esi: string;
    url: string;
};

export type ZkData = {
    attackers: Attacker[];
    killmail_id: number;
    killmail_time: string;
    solar_system_id: number;
    victim: Victim;
    zkb: Zkb;
};

function hasLimitType(subscription: Subscription, limitType: LimitType): boolean {
    return subscription.limitTypes.has(limitType);
}

function getLimitType(subscription: Subscription, limitType: LimitType): string | undefined {
    if (subscription.limitTypes instanceof Map) {
        return subscription.limitTypes.get(limitType) as string | undefined;
    } else {
        console.log('subscription is not of type Map, exiting');
        console.log(`subscription.limitTypes: ${subscription.limitTypes}`);
        console.log(`subscription.limitTypes type: ${typeof subscription.limitTypes}`);
        process.exit(2);
    }
}

export interface ClosestCelestial {
    distance: number;
    itemId: number;
    typeId: number;
    itemName: string;
}

export interface SolarSystem {
    id: number;
    systemName: string;
    regionId: number;
    regionName: string;
    constellationId: number;
    constellationName: string;
    securityStatus: number;
}

export class ZKillSubscriber {
    protected static instance: ZKillSubscriber;
    protected doClient: Client;

    protected subscriptions: Map<string, SubscriptionGuild>;
    // Mapping of a solar system type ID to a description
    protected systems: Map<number, SolarSystem>;
    // Mapping of ship type ID to group ID
    protected ships: Map<number, number>;
    // Mapping of ship type ID to name
    protected names: Map<number, string>;
    protected rest: REST;

    protected asyncLock: AsyncLock;
    protected esiClient: EsiClient;

    protected constructor(client: Client, connect = true) {
        this.asyncLock = new AsyncLock();
        this.esiClient = new EsiClient();
        this.subscriptions = new Map<string, SubscriptionGuild>();
        this.systems = new Map<number, SolarSystem>();
        this.ships = new Map<number, number>();
        this.names = new Map<number, string>();
        this.doClient = client;
        this.rest = new REST({version: '9'}).setToken(process.env.DISCORD_BOT_TOKEN || '');
        if (connect) {
            ZKillSubscriber.connect(this);
        }
    }

    protected static connect(sub: ZKillSubscriber) {
        const websocket = new WebSocket('wss://zkillboard.com/websocket/');
        websocket.onmessage = sub.onMessage.bind(sub);
        websocket.onopen = () => {
            websocket.send(JSON.stringify({
                'action': 'sub',
                'channel': 'killstream'
            }));
        };
        websocket.onclose = (e) => {
            console.log('Socket is closed. Reconnect will be attempted in 1 second.', e.reason);
            setTimeout(function () {
                ZKillSubscriber.connect(sub);
            }, 1000);
        };
        websocket.onerror = (error) => {
            console.error('Socket encountered error: ', error.message, 'Closing socket');
            websocket.close();
        };
    }

    protected async onMessage(event: MessageEvent) {
        const data: ZkData = JSON.parse(event.data.toString());
        this.subscriptions.forEach((guild, guildId) => {
            const log_prefix = `["${data.killmail_id}"][${new Date()}] `;
            console.log(log_prefix);
            guild.channels.forEach((channel, channelId) => {
                channel.subscriptions.forEach(async (subscription) => {
                    try {
                        await this.process_subscription(subscription, data, guildId, channelId);
                    } catch (e) {
                        console.log(e);
                    }
                });
            });
        });
    }

    private init_subscription_flags(): SubscriptionFlags {
        return {
            inclusionLimitAlsoComparesAttacker: true,
            inclusionLimitAlsoComparesAttackerWeapons: true,
            exclusionLimitAlsoComparesAttacker: true,
            exclusionLimitAlsoComparesAttackerWeapons: true,
        };
    }

    private async process_subscription(
        subscription: Subscription,
        data: ZkData,
        guildId: string,
        channelId: string,
    ) {
        let color: ColorResolvable = 'GREEN';
        let requireSend = false;
        let matchedShip: FilterShipMatch | null = null;

        if (subscription.minValue > data.zkb.totalValue) {
            // console.log(`Channel ${channelId}: limiting kill due to minValue filter`);
            return;
        }

        if (subscription.limitTypes.size === 0) {
            await this.sendMessageToDiscord(guildId, channelId, subscription, data);
            return;
        }
        if (hasLimitType(subscription, LimitType.NPC_ONLY) && data.zkb.npc) {
            const val = (getLimitType(subscription, LimitType.NPC_ONLY) ?? 'false').toLowerCase();
            console.log(`Channel ${channelId}: NPC_ONLY filter value is ${val}`);
            if (val === 'true') {
                if (data.zkb.npc) {
                    console.log(`Channel ${channelId}: sending kill due to NPC only filter`);
                    requireSend = true;
                } else {
                    console.log(`Channel ${channelId}: limiting kill due to NPC only filter - not an NPC kill`);
                    return;
                }
            }
        }
        if (hasLimitType(subscription, LimitType.SHIP_INCLUSION_TYPE_ID)) {
            let nameFragment = '';
            if (hasLimitType(subscription, LimitType.NAME_FRAGMENT)) {
                nameFragment = <string>getLimitType(subscription, LimitType.NAME_FRAGMENT);
            }
            const __ret = await this.sendIfAnyShipsMatchLimitFilter(
                data,
                <string>getLimitType(subscription, LimitType.SHIP_INCLUSION_TYPE_ID),
                nameFragment,
                subscription.inclusionLimitAlsoComparesAttacker,
                subscription.inclusionLimitAlsoComparesAttackerWeapons,
            );
            requireSend = __ret.requireSend;
            color = __ret.color;
            matchedShip = __ret.matchedShip;
            if (!requireSend) {
                // console.log(`Channel ${channelId}: limiting kill due to inclusion ship filter`);
                return;
            }
        }
        if (!await this.checkSecurityMaxExclusive(subscription, data)) {
            console.log(`Channel ${channelId}: limiting kill due to max exclusive security filter`);
            return;
        }
        if (!await this.checkSecurityMinExclusive(subscription, data)) {
            console.log(`Channel ${channelId}: limiting kill due to min exclusive security filter`);
            return;
        }
        if (!await this.checkSecurityMaxInclusive(subscription, data)) {
            console.log(`Channel ${channelId}: limiting kill in due to max inclusive security filter`);
            return;
        }
        if (!await this.checkSecurityMinInclusive(subscription, data)) {
            console.log(`Channel ${channelId}: limiting kill due to min inclusive security filter`);
            return;
        }
        if (hasLimitType(subscription, LimitType.CHARACTER)) {
            const characterIdsStr = <string>getLimitType(subscription, LimitType.CHARACTER);

            if (hasLimitType(subscription, LimitType.ALLIANCE)) {
                // if the victim matches the character or alliance, not both, then one of the attackers must match the opposite (alliance or character), otherwise do not send
                const characterIds = characterIdsStr.split(',') || [];
                const allianceIds = getLimitType(subscription, LimitType.ALLIANCE)?.split(',') || [];

                const victimCharId = data.victim.character_id;
                if (victimCharId) {
                    const victimMatchesCharacter = characterIds.includes(victimCharId.toString());
                    const victimMatchesAlliance = allianceIds.includes(data.victim.alliance_id?.toString());

                    if (victimMatchesCharacter !== victimMatchesAlliance) {
                        // Victim matches either character or alliance, but not both
                        const attackerMatchesCharacter = data.attackers.some(attacker => attacker.character_id && characterIds.includes(attacker.character_id?.toString()));
                        const attackerMatchesAlliance = data.attackers.some(attacker => attacker.alliance_id && allianceIds.includes(attacker.alliance_id?.toString()));

                        if (victimMatchesCharacter && attackerMatchesAlliance || victimMatchesAlliance && attackerMatchesCharacter) {
                            requireSend = true;
                        }
                    }
                }
            } else {
                // just match based on matching character_id
                for (const characterId of characterIdsStr.split(',')) {
                    if (data.victim.character_id === Number(characterId)) {
                        requireSend = true;
                        color = 'RED';
                    }
                    if (!requireSend) {
                        for (const attacker of data.attackers) {
                            if (attacker.character_id === Number(characterId)) {
                                requireSend = true;
                                break;
                            }
                        }
                    }
                }
            }
            if (!requireSend) return;
        }
        if (hasLimitType(subscription, LimitType.CORPORATION)) {
            const corporationIds = <string>getLimitType(subscription, LimitType.CORPORATION);
            for (const corporationId of corporationIds.split(',')) {
                if (data.victim.corporation_id === Number(corporationId)) {
                    requireSend = true;
                    color = 'RED';
                }
                if (!requireSend) {
                    for (const attacker of data.attackers) {
                        if (attacker.corporation_id === Number(corporationId)) {
                            requireSend = true;
                            break;
                        }
                    }
                }
            }
            if (!requireSend) return;
        }
        if (hasLimitType(subscription, LimitType.ALLIANCE)) {
            const allianceIds = <string>getLimitType(subscription, LimitType.ALLIANCE);
            for (const allianceId of allianceIds.split(',')) {
                if (data.victim.alliance_id === Number(allianceId)) {
                    requireSend = true;
                    color = 'RED';
                }
                if (!requireSend) {
                    for (const attacker of data.attackers) {
                        if (attacker.alliance_id === Number(allianceId)) {
                            requireSend = true;
                            break;
                        }
                    }
                }
            }
            if (!requireSend) return;
        }
        if (hasLimitType(subscription, LimitType.REGION) ||
            hasLimitType(subscription, LimitType.CONSTELLATION) ||
            hasLimitType(subscription, LimitType.SYSTEM)) {
            requireSend = await this.isInLocationLimit(subscription, data.solar_system_id);
            if (!requireSend) {
                console.log(`Channel ${channelId}: limiting kill due to location filter`);
                return;
            }
        }
        let minNumInvolved: number | null = null;
        if (hasLimitType(subscription, LimitType.MIN_NUM_INVOLVED)) {
            minNumInvolved = Number(<string>getLimitType(subscription, LimitType.MIN_NUM_INVOLVED));
            const numInvolved = data.attackers.length + 1;
            if (numInvolved < minNumInvolved) {
                console.log(`Channel ${channelId}: limiting kill due to minimum number of involved parties filter: ${numInvolved} < ${minNumInvolved}`);
                return;
            }
        }
        if (hasLimitType(subscription, LimitType.TIME_RANGE_START) && hasLimitType(subscription, LimitType.TIME_RANGE_END)) {
            const startTime = Number(<string>getLimitType(subscription, LimitType.TIME_RANGE_START));
            const endTime = Number(<string>getLimitType(subscription, LimitType.TIME_RANGE_END));
            const killmailTime = new Date(data.killmail_time);
            const killmailHour = killmailTime.getUTCHours();

            if (startTime < endTime) {
                if (killmailHour < startTime || killmailHour > endTime) {
                    console.log(`Channel ${channelId}: limiting kill due to time range filter: ${killmailHour} not in range ${startTime} - ${endTime}`);
                    return;
                }
            } else {
                if (killmailHour < startTime && killmailHour > endTime) {
                    console.log(`Channel ${channelId}: limiting kill due to time range filter: ${killmailHour} not in range ${startTime} - ${endTime}`);
                    return;
                }
            }
        }
        if (requireSend) {
            console.log('sending filtered kill');
            await this.sendMessageToDiscord(
                guildId,
                channelId,
                subscription,
                data,
                matchedShip,
                minNumInvolved,
                color
            );
        }
    }

    public async checkSecurityMaxInclusive(subscription: Subscription, data: ZkData): Promise<boolean> {
        if (hasLimitType(subscription, LimitType.SECURITY_MAX_INCLUSIVE)) {
            const systemData = await this.getSystemData(data.solar_system_id);
            const maximumSecurityStatus = Number(<string>getLimitType(subscription, LimitType.SECURITY_MAX_INCLUSIVE));
            if (maximumSecurityStatus < systemData.securityStatus) {
                // console.log(`limiting kill in ${systemData.systemName} due to inclusive maximum security status filter: ${systemData.securityStatus} > ${maximumSecurityStatus}`);
                return false;
            }
        }
        return true;
    }

    public async checkSecurityMaxExclusive(subscription: Subscription, data: ZkData): Promise<boolean> {
        if (hasLimitType(subscription, LimitType.SECURITY_MAX_EXCLUSIVE)) {
            const systemData = await this.getSystemData(data.solar_system_id);
            const maximumSecurityStatus = Number(<string>getLimitType(subscription, LimitType.SECURITY_MAX_EXCLUSIVE));
            if (maximumSecurityStatus <= systemData.securityStatus) {
                // console.log(`limiting kill in ${systemData.systemName} due to exclusive maximum security status filter: ${systemData.securityStatus} >= ${maximumSecurityStatus}`);
                return false;
            }
        }
        return true;
    }

    public async checkSecurityMinInclusive(subscription: Subscription, data: ZkData): Promise<boolean> {
        if (hasLimitType(subscription, LimitType.SECURITY_MIN_INCLUSIVE)) {
            const systemData = await this.getSystemData(data.solar_system_id);
            const minimumSecurityStatus = Number(<string>getLimitType(subscription, LimitType.SECURITY_MIN_INCLUSIVE));
            if (minimumSecurityStatus > systemData.securityStatus) {
                // console.log(`limiting kill in ${systemData.systemName} due to inclusive minimum security status filter: ${systemData.securityStatus} < ${minimumSecurityStatus}`);
                return false;
            }
        }
        return true;
    }

    public async checkSecurityMinExclusive(subscription: Subscription, data: ZkData): Promise<boolean> {
        if (hasLimitType(subscription, LimitType.SECURITY_MIN_EXCLUSIVE)) {
            const systemData = await this.getSystemData(data.solar_system_id);
            const minimumSecurityStatus = Number(<string>getLimitType(subscription, LimitType.SECURITY_MIN_EXCLUSIVE));
            if (minimumSecurityStatus >= systemData.securityStatus) {
                // console.log(`limiting kill in ${systemData.systemName} due to exclusive minimum security status filter: ${systemData.securityStatus} <= ${minimumSecurityStatus}`);
                return false;
            }
        }
        return true;
    }

    private async sendIfAnyShipsMatchLimitFilter(
        data: ZkData,
        limitIds: string,
        nameFragment: string,
        alsoCompareAttackers: boolean,
        alsoCompareAttackerWeapons: boolean
    ) {
        const limitGroupOfShipIds = limitIds?.split(',') || [];
        const shouldCheckNameFragment = nameFragment != null && nameFragment != '';
        const shipTypeId = data.victim.ship_type_id;
        if (shipTypeId == null) {
            console.log('WARNING: shipTypeId is null');
            return {
                requireSend: false,
                color: <ColorResolvable>'GREEN',
                matchedShip: null,
            };
        }

        for (const permittedGroupOfShipIds of limitGroupOfShipIds) {
            const permittedGroupOfShipId = await this.getGroupIdForEntityId(Number(permittedGroupOfShipIds));

            // Check if the victim's ship matches the criteria
            if (await this.isShipMatch(shipTypeId, permittedGroupOfShipId, shouldCheckNameFragment, nameFragment)) {
                return {
                    requireSend: true,
                    color: <ColorResolvable>'RED',
                    matchedShip: {
                        shipName: await this.getNameForEntityId(shipTypeId),
                        typeId: shipTypeId,
                        corpId: data.victim.corporation_id,
                        allianceId: data.victim.alliance_id,
                    },
                };
            }

            // If the victim's ship doesn't match, check the attackers' ships
            if (alsoCompareAttackers) {
                for (const attacker of data.attackers) {
                    if (await this.isShipMatch(attacker.ship_type_id, permittedGroupOfShipId, shouldCheckNameFragment, nameFragment)) {
                        const id = attacker.ship_type_id;
                        if (id == null) {
                            console.log('WARNING: attacker.ship_type_id is null but matched?');
                            continue;
                        }
                        return {
                            requireSend: true,
                            color: <ColorResolvable>'GREEN',
                            matchedShip: {
                                shipName: await this.getNameForEntityId(id),
                                typeId: id,
                                corpId: attacker.corporation_id,
                                allianceId: attacker.alliance_id
                            },
                            matchedTypeId: id,
                        };
                    }
                    if ((alsoCompareAttackerWeapons && await this.isShipMatch(attacker.weapon_type_id, permittedGroupOfShipId, shouldCheckNameFragment, nameFragment))) {
                        const id = attacker.weapon_type_id;
                        if (id == null) {
                            console.log('WARNING: attacker.weapon_type_id is null but matched?');
                            continue;
                        }
                        return {
                            requireSend: true,
                            color: <ColorResolvable>'GREEN',
                            matchedShip: {
                                shipName: await this.getNameForEntityId(id),
                                typeId: id,
                                corpId: attacker.corporation_id,
                                allianceId: attacker.alliance_id
                            },
                        };
                    }
                }
            }
        }

        return {
            requireSend: false,
            color: <ColorResolvable>'RED',
            matchedShip: null,
            matchedTypeId: null,
        };
    }

    private async isShipMatch(shipTypeId: number | undefined, permittedGroupOfShipId: number, shouldCheckNameFragment: boolean, nameFragment: string) {
        if (shipTypeId != null) {
            const groupId = await this.getGroupIdForEntityId(shipTypeId);
            if (groupId === permittedGroupOfShipId) {
                if (shouldCheckNameFragment) {
                    const shipName = await this.getNameForEntityId(shipTypeId);
                    return shipName.includes(nameFragment);
                }
                return true;
            }
        }
        return false;
    }

    public async sendMessageToDiscord(
        guildId: string,
        channelId: string,
        subscription: Subscription,
        data: ZkData,
        matchedShip: FilterShipMatch | null = null,
        minNumInvolved: number | null = null,
        messageColor: ColorResolvable = 'GREY',
    ) {
        await this.asyncLock.acquire('sendKill', async (done) => {
            const cacheKey = `${channelId}_${data.killmail_id}`;
            if (MemoryCache.get(cacheKey)) {
                // Mail was already sent, prevent from sending twice
                done();
                return;
            }

            const channel = <TextChannel>this.doClient.channels.cache.get(channelId);
            if (!channel) {
                const owner = await this.doClient.users.fetch('146451271497416704');
                await owner.send(`The bot unsubscribed from channel ${channelId} because it was not found. This would delete the channel.`);
                // await this.unsubscribe(subscription.subType, guildId, channelId, subscription.id);
                done();
                return;
            }

            const embedding = await ogs({url: data.zkb.url}).catch(() => null);
            const params: PrepareEmbedFields = {
                guildId,
                channelId,
                subscription,
                embedding,
                data,
                matchedShip,
                minNumInvolved,
                messageColor,
            };
            const content: MessageOptions = await this.prepareMessageContent(params);

            try {
                console.log('content: ' + util.inspect(content, {depth: 5}));
                await channel.send(content);
                MemoryCache.put(cacheKey, 'send', 60000); // Prevent from sending again, cache it for 1 min
            } catch (e) {
                if (e instanceof DiscordAPIError && e.httpStatus === 403) {
                    await this.handlePermissionError(channel);
                } else {
                    console.log(e);
                }
            }
            done();
        });
    }

    private async prepareMessageContent(params: PrepareEmbedFields): Promise<MessageOptions> {
        if (params.matchedShip != null || params.minNumInvolved != null) {
            return {
                embeds: await this.prepareEmbedFields(params)
            };
        } else if (params.embedding?.error === false) {
            console.log('defaulting to standard embed');
            return {
                embeds: [{
                    title: params.embedding?.result.ogTitle,
                    description: params.embedding?.result.ogDescription,
                    thumbnail: {
                        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                        // @ts-ignore
                        url: params.embedding?.result.ogImage?.url,
                        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                        // @ts-ignore
                        height: params.embedding?.result.ogImage?.height,
                        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
                        // @ts-ignore
                        width: params.embedding?.result.ogImage?.width
                    },
                    url: params.data.zkb.url,
                    color: params.messageColor,
                }]
            };
        } else {
            return {content: params.data.zkb.url};
        }
    }

    private async prepareEmbedFields(params: PrepareEmbedFields): Promise<(MessageEmbed | MessageEmbedOptions | APIEmbed)[]> {
        console.log('prepareEmbedFields');
        const systemRegion = await this.getSystemData(params.data.solar_system_id);
        let victimDetails = '';
        let attackerDetails = '';
        let locationDetails = '';
        let victimShipName = '';
        let victimLink = 'Victim';
        let attackerLink = 'Attacker';

        const closestCelestial = await this.getClosestCelestial(
            systemRegion.id,
            params.data.victim.position.x,
            params.data.victim.position.y,
            params.data.victim.position.z
        );
        const distanceInUnits = this.distanceTo(closestCelestial);
        const closestCelestialName = closestCelestial.itemName.substring(0, 36);
        locationDetails += `on: [${closestCelestialName}](${this.strLocation(closestCelestial.itemId)}), ${distanceInUnits} away`;

        if (params.data.victim.ship_type_id != null) {
            try {
                victimShipName = await this.getNameForEntityId(params.data.victim.ship_type_id);
                // victimDetails += `Ship: [${victimShipName.substring(0, 18)}](${params.data.zkb.url})\n`;
            } catch (e) {
                console.log(e);
            }
        }
        if (params.data.victim.alliance_id != null) {
            try {
                const victimAllianceName = await this.getNameForAlliance(params.data.victim.alliance_id);
                victimDetails += `[${victimAllianceName.substring(0, 40)}](${this.strAllianceZk(params.data.victim.alliance_id)})`;
            } catch (e) {
                victimDetails += `N/A`;
                console.log(e);
            }
        }
        if (params.data.victim.corporation_id != null) {
            try {
                const victimCorporationName = await this.getNameForCorporation(params.data.victim.corporation_id);
                if (victimDetails.length !== 0) {
                } else {
                    victimDetails += `[${victimCorporationName.substring(0, 30)}](${this.strCorpZk(params.data.victim.corporation_id)})`;
                }
            } catch (e) {
                console.log(e);
            }
        }
        if (params.data.victim.character_id != null) {
            try {
                // const victimCharacterName = await this.getNameForCharacter(params.data.victim.character_id);
                victimLink = `[Victim](${this.strPilotZk(params.data.victim.character_id)})`;
            } catch (e) {
                console.log(e);
            }
        }
        console.log('victimparams.dataDone');


        console.log('attackerparams.data');
        let lastHitAttacker = null;
        for (const attacker of params.data.attackers) {
            if (attacker.final_blow) {
                lastHitAttacker = attacker;
                break;
            }
        }
        if (lastHitAttacker == null) {
            console.log('No final blow attacker found, using first attacker as last hit attacker');
            lastHitAttacker = params.data.attackers[0];
        }
        // if (lastHitAttacker.ship_type_id != null) {
        //     try {
        //         const attackerShipName = await this.getNameForE/ntityId(lastHitAttacker.ship_type_id);
        //         attackerDetails += `Ship: [${attackerShipName}](${this.strShipZk(lastHitAttacker.ship_type_id)})\n`;
        //     } catch (e) {
        //         console.log(e);
        //     }
        // }
        if (lastHitAttacker.alliance_id != null) {
            try {
                const attackerAllianceName = await this.getNameForAlliance(lastHitAttacker.alliance_id);
                attackerDetails += `[${attackerAllianceName.substring(0, 25)}](${this.strAllianceZk(lastHitAttacker.alliance_id)})`;
            } catch (e) {
                attackerDetails += `N/A`;
                console.log(e);
            }
        }
        if (lastHitAttacker.corporation_id != null) {
            try {
                const attackerCorporationName = await this.getNameForCorporation(lastHitAttacker.corporation_id);
                if (attackerDetails.length !== 0) {
                    attackerDetails += ' / ';
                }
                attackerDetails += `[${attackerCorporationName.substring(0, 15)}](${this.strCorpZk(lastHitAttacker.corporation_id)})`;
            } catch (e) {
                console.log(e);
            }
        }
        if (lastHitAttacker.character_id != null) {
            try {
                const attackerCharacterName = await this.getNameForCharacter(lastHitAttacker.character_id);
                attackerLink = `[Attacker](${this.strPilotZk(lastHitAttacker.character_id)})`;
            } catch (e) {
                console.log(e);
            }
        }
        // if (lastHitAttacker.faction_id != null) {
        //     try {
        //         const attackerFactionName = await this.getNameForFaction(lastHitAttacker.faction_id);
        //         attackerDetails += `Faction: [${attackerFactionName.substring(0, 18)}](${this.strFactionZk(lastHitAttacker.faction_id)})\n`;
        //     } catch (e) {
        //         console.log(e);
        //     }
        // }
        if (attackerDetails === '') {
            // ship_type_id
            if (lastHitAttacker.ship_type_id != null) {
                try {
                    const attackerShipName = await this.getNameForEntityId(lastHitAttacker.ship_type_id);
                    attackerDetails += `Ship: [${attackerShipName}](${this.strShipZk(lastHitAttacker.ship_type_id)})\n`;
                } catch (e) {
                    console.log(`failed to query ship entity name for attacker: ${e}`);
                }
            }
        }
        const mostCommonShip = this.findMostCommonShipTypeIdAndCount(params.data.attackers);
        console.log(`Most common ship type ID among attackers: ${mostCommonShip}`);

        let idOfIconToRender: number;
        let affiliationIconURLToRender: string;
        if (params.matchedShip?.typeId != null) {
            idOfIconToRender = params.matchedShip.typeId;
            if (params.matchedShip.allianceId) {
                affiliationIconURLToRender = this.strAllianceIconById(params.matchedShip.allianceId);
            } else if (params.matchedShip.corpId) {
                affiliationIconURLToRender = this.strCorporationIconById(params.matchedShip.corpId);
            } else {
                affiliationIconURLToRender = this.strItemRenderById(idOfIconToRender);
            }
        } else if (params.data.victim.ship_type_id != null) {
            idOfIconToRender = params.data.victim.ship_type_id;
            if (params.data.victim.alliance_id != null) {
                affiliationIconURLToRender = this.strAllianceIconById(params.data.victim.alliance_id);
            } else if (params.data.victim.corporation_id != null) {
                affiliationIconURLToRender = this.strCorporationIconById(params.data.victim.corporation_id);
            } else {
                affiliationIconURLToRender = this.strItemRenderById(idOfIconToRender);
            }
        } else if (lastHitAttacker.ship_type_id != null) {
            idOfIconToRender = lastHitAttacker.ship_type_id;
            if (lastHitAttacker.alliance_id != null) {
                affiliationIconURLToRender = this.strAllianceIconById(lastHitAttacker.alliance_id);
            } else if (lastHitAttacker.corporation_id != null) {
                affiliationIconURLToRender = this.strCorporationIconById(lastHitAttacker.corporation_id);
            } else {
                affiliationIconURLToRender = this.strItemRenderById(idOfIconToRender);
            }
        } else if (lastHitAttacker.weapon_type_id != null) {
            idOfIconToRender = lastHitAttacker.weapon_type_id;
            if (lastHitAttacker.alliance_id != null) {
                affiliationIconURLToRender = this.strAllianceIconById(lastHitAttacker.alliance_id);
            } else if (lastHitAttacker.corporation_id != null) {
                affiliationIconURLToRender = this.strCorporationIconById(lastHitAttacker.corporation_id);
            } else {
                affiliationIconURLToRender = this.strItemRenderById(idOfIconToRender);
            }
        } else {
            console.log(`failed to find an icon to render for ${params.data.zkb.url}`);
            throw new Error('failed to find an icon to render');
        }
        console.log('rendering icon: ' + this.strItemRenderById(idOfIconToRender));

        let attackerAlliances = '```';
        const allianceCountMap = new Map<string, number>();
        for (const attacker of params.data.attackers) {
            const id = attacker.alliance_id ? attacker.alliance_id : attacker.corporation_id;
            if (id == null) {
                console.log(`id for attacker ${attacker} is null, skipping`);
                continue;
            }
            let name = '';
            if (attacker.alliance_id) {
                try {
                    name = await this.getNameForAlliance(id);
                } catch (e) {
                    console.log(`Error getting alliance name for id ${id}: ${e}`);
                    name = 'Unknown';
                }
            } else {
                try {
                    name = await this.getNameForCorporation(id);
                } catch (e) {
                    console.log(`Error getting corporation name for id ${id}: ${e}`);
                    name = 'Unknown';
                }
            }
            if (allianceCountMap.has(name)) {
                const value = allianceCountMap.get(name);
                if (value == null) {
                    continue;
                }
                allianceCountMap.set(name, value + 1);
            } else {
                allianceCountMap.set(name, 1);
            }
        }
        // Separate entries that will be displayed from those that will be collapsed into "others"
        let othersCount = 0;
        const displayedEntries: [string, number][] = [];
        const threshold = 15;
        const sortedEntries = Array.from(allianceCountMap.entries()).sort((a, b) => b[1] - a[1]);

        sortedEntries.forEach(([key, value], index) => {
            if (value >= threshold || index === 0) {
                displayedEntries.push([key, value]);
            } else {
                othersCount += value;
            }
        });

        // Calculate maxNameLength based only on displayed entries
        let maxNameLength = 0;
        displayedEntries.forEach(([name]) => {
            if (name.length > maxNameLength) {
                maxNameLength = name.length;
            }
        });
        maxNameLength = Math.min(maxNameLength, 26);  // Cap max length at 26 characters
        const padding = 3;

        // Build the affiliation display string for displayed entries
        displayedEntries.forEach(([key, value]) => {
            const spaces = maxNameLength - Math.min(key.length, 26) + padding;
            const formattedKey = key.length > 26 ? key.slice(0, 26) + '-\n' + key.slice(26) : key;
            attackerAlliances += `${formattedKey}${' '.repeat(spaces)}x${value}\n`;
        });

        // Add the "others" entry if there were collapsed entries
        if (othersCount > 0) {
            const others = '...others';
            const spaces = maxNameLength - others.length + padding;
            attackerAlliances += `${others}${' '.repeat(spaces)}x${othersCount}\n`;
        }

        attackerAlliances += '```';

        console.log('attackerparams.dataDone');

        console.log(systemRegion);
        // convert params.data.killmail_time into a relative time
        const killmailTime = new Date(params.data.killmail_time);
        let relativeTime = this.getRelativeTime(params.data.killmail_time);
        relativeTime = `Posted ${relativeTime} later`;

        // convert the killmail_time `2023-01-17T01:53:02Z` to YYYY/MM/DD HH:MM
        // const killmailTimeFormatted = killmailTime.toISOString().replace(/T/, ' ').replace(/\..+/, '');

        console.log('total value: ' + params.data.zkb.totalValue);
        const killmail_value = this.abbreviateNumber(params.data.zkb.totalValue);
        console.log('killmail_value: ' + killmail_value);

        let title: string;
        let authorText: string;

        if (params.minNumInvolved != null) {
            authorText = `${params.data.attackers.length}+ ships in ${systemRegion.systemName} (${systemRegion.regionName})`;
            if (mostCommonShip != null) {
                const mostCommonShipName = await this.getNameForEntityId(mostCommonShip.shipTypeId);
                title = `\`${victimShipName}\` died to ${mostCommonShip.count}x \`${mostCommonShipName}\``;
            } else {
                title = `Missing 0`;
            }
        } else if (params.matchedShip?.shipName != null) {
            authorText = `${params.matchedShip.shipName} in ${systemRegion.systemName} (${systemRegion.regionName})`;
            if (mostCommonShip != null) {
                const mostCommonShipName = await this.getNameForEntityId(mostCommonShip.shipTypeId);
                if (params.messageColor === 'GREEN') {
                    authorText = `${params.matchedShip.shipName} in ${systemRegion.systemName} (${systemRegion.regionName})`;
                    title = `\`${victimShipName}\` destroyed`;
                } else {
                    authorText = `${params.matchedShip.shipName} in ${systemRegion.systemName} (${systemRegion.regionName})`;
                    title = `\`${victimShipName}\` died to ${mostCommonShip.count}x \`${mostCommonShipName}\``;
                }
            } else {
                title = `${relativeTime}`;
            }
        } else {
            title = params.embedding?.result.ogTitle;
            authorText = '';
        }
        authorText += `\n${relativeTime}`;

        console.log('TIME: ' + killmailTime.getTime());

        const related = `https://br.evetools.org/related/${systemRegion.id}/${this.formatDateToTimestamp(killmailTime)}`;

        const affiliation = `${attackerAlliances}[killed](${related}): ${victimDetails}\nin: [${systemRegion.systemName}](${this.strSystemDotlan(systemRegion.id)}) ([${systemRegion.regionName}](${this.strRegionDotlan(systemRegion.regionId)}))\n${locationDetails}`;

        const fields: { inline: boolean; name: string; value: string }[] = [];
        [
            {
                name: `(${params.data.attackers.length}) Attackers Involved`,
                value: affiliation,
                inline: false,
            },
        ].forEach((field) => fields.push(field));

        return [{
            title: title,
            author: {
                iconURL: affiliationIconURLToRender,
                name: authorText,
                url: params.data.zkb.url,
            },
            thumbnail: {
                url: this.strItemRenderById(idOfIconToRender),
                height: params.embedding?.result.ogImage?.height,
                width: params.embedding?.result.ogImage?.width
            },
            url: params.data.zkb.url,
            color: params.messageColor,
            fields: fields,
            timestamp: killmailTime.getTime(),
            footer: {
                text: `Value: ${killmail_value} • EVETime: ${killmailTime.toLocaleString('en-GB', {
                    hour: '2-digit',
                    minute: '2-digit',
                    year: '2-digit',
                    month: '2-digit',
                    day: '2-digit'
                })}`,
            }
        }];
    }

    private distanceTo(closestCelestial: ClosestCelestial) {
        const distance = (closestCelestial.distance / 1000);
        if (distance > 1500000) {
            return (distance / 150000000).toFixed(2) + ' au';
        } else {
            return Math.round(distance) + ' km';
        }
    }

    private formatDateToTimestamp(date: Date): string {
        const year = date.getUTCFullYear();
        const month = (date.getUTCMonth() + 1).toString().padStart(2, '0'); // Months are 0-indexed
        const day = date.getUTCDate().toString().padStart(2, '0');
        const hours = date.getUTCHours().toString().padStart(2, '0');

        // Always round down to the hour, so minutes are ignored
        const roundedTimestamp = `${year}${month}${day}${hours}00`;
        return roundedTimestamp;
    }

    private getRelativeTime(killmailTime: string): string {
        const killmailDate = new Date(killmailTime);
        const now = new Date();
        const diff = now.getTime() - killmailDate.getTime();

        const seconds = Math.floor(diff / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        const weeks = Math.floor(days / 7);
        const months = Math.floor(weeks / 4);
        const years = Math.floor(months / 12);

        if (years > 1) return `${years} years`;
        if (years === 1) return `1 year`;
        if (months > 1) return `${months} months`;
        if (months === 1) return `1 month`;
        if (weeks > 1) return `${weeks} weeks`;
        if (weeks === 1) return `1 week`;
        if (days > 1) return `${days} days`;
        if (days === 1) return `1 day`;
        if (hours > 1) return `${hours} hours`;
        if (hours === 1) return `1 hour`;
        if (minutes > 1) return `${minutes} minutes`;
        if (minutes === 1) return `1 minute`;
        if (seconds > 1) return `${seconds} seconds`;

        return `1 second`;
    }

    public abbreviateNumber(n: number) {
        if (n < 1e3) return n;
        if (n >= 1e3 && n < 1e6) return +(n / 1e3).toFixed(1) + 'K';
        if (n >= 1e6 && n < 1e9) return +(n / 1e6).toFixed(1) + 'mil';
        if (n >= 1e9 && n < 1e12) return +(n / 1e9).toFixed(1) + 'bil';
        if (n >= 1e12) return +(n / 1e12).toFixed(1) + 'tril';
    }

    findMostCommonShipTypeIdAndCount(attackers: Attacker[]): { shipTypeId: number, count: number } | null {
        const frequency: { [key: number]: number } = {};

        for (const attacker of attackers) {
            if (attacker.ship_type_id !== undefined) {
                if (frequency[attacker.ship_type_id]) {
                    frequency[attacker.ship_type_id]++;
                } else {
                    frequency[attacker.ship_type_id] = 1;
                }
            }
        }

        let maxCount = 0;
        let mostCommonShipTypeId = null;

        for (const shipTypeId in frequency) {
            if (frequency[shipTypeId] > maxCount) {
                maxCount = frequency[shipTypeId];
                mostCommonShipTypeId = Number(shipTypeId);
            }
        }

        return mostCommonShipTypeId ? {shipTypeId: mostCommonShipTypeId, count: maxCount} : null;
    }

    public getArticle(word: string, capitalize = true): string {
        console.log(word);
        const vowels = ['a', 'e', 'i', 'o', 'u'];
        let res = vowels.includes(word[0].toLowerCase()) ? 'An' : 'A';
        if (!capitalize) {
            res = res.toLowerCase();
        }
        return res;
    }

    private async handlePermissionError(channel: TextChannel) {
        const owner = await channel.guild.fetchOwner();
        await owner.send(`The bot unsubscribed from channel ${channel.name} on ${channel.guild.name} because it was not able to write in it! Fix the permissions and subscribe again!`);
        const subscriptionsInChannel = this.subscriptions.get(channel.guild.id)?.channels.get(channel.id);
        if (subscriptionsInChannel) {
            subscriptionsInChannel.subscriptions.forEach((subscription) => {
                this.unsubscribe(subscription.subType, channel.guild.id, channel.id, subscription.id);
            });
        }
    }

    public static getInstance(client?: Client, connect = true) {
        if (!this.instance && client)
            this.instance = new ZKillSubscriber(client, connect);
        else if (!this.instance) {
            throw new Error('Instance needs to be created with a client once.');
        }

        return this.instance;
    }

    public subscribe(
        subType: SubscriptionType,
        guildId: string,
        channel: string,
        limitTypes: Map<LimitType, string>,
        flags: SubscriptionFlags,
        id?: string,
        minValue = 0,
    ) {
        if (!this.subscriptions.has(guildId)) {
            this.subscriptions.set(guildId, {channels: new Map<string, SubscriptionChannel>()});
        }
        const guild = this.subscriptions.get(guildId);
        if (!guild?.channels.has(channel)) {
            guild?.channels.set(channel, {subscriptions: new Map<string, Subscription>()});
        }
        const guildChannel = guild?.channels.get(channel);
        const ident = `${subType}${id ? id : ''}`;
        if (!guildChannel?.subscriptions.has(ident)) {
            guildChannel?.subscriptions.set(ident, {
                subType,
                id,
                minValue,
                limitTypes,
                inclusionLimitAlsoComparesAttacker: flags.inclusionLimitAlsoComparesAttacker,
                inclusionLimitAlsoComparesAttackerWeapons: flags.inclusionLimitAlsoComparesAttackerWeapons,
                exclusionLimitAlsoComparesAttacker: flags.exclusionLimitAlsoComparesAttacker,
                exclusionLimitAlsoComparesAttackerWeapons: flags.exclusionLimitAlsoComparesAttackerWeapons
            });
        }
        fs.writeFileSync('./config/' + guildId + '.json', JSON.stringify(this.generateObject(guild)), 'utf8');
    }

    public async unsubscribe(subType: SubscriptionType, guildId: string, channel: string, id?: string) {
        if (!this.subscriptions.has(guildId)) {
            return;
        }
        const guild = this.subscriptions.get(guildId);
        if (!guild?.channels.has(channel)) {
            return;
        }
        const guildChannel = guild.channels.get(channel);
        const ident = `${subType}${id ? id : ''}`;
        if (!guildChannel?.subscriptions.has(ident)) {
            return;
        }
        guildChannel.subscriptions.delete(ident);
        fs.writeFileSync('./config/' + guildId + '.json', JSON.stringify(this.generateObject(guild)), 'utf8');
    }

    public async unsubscribeGuild(guildId: string) {
        if (this.subscriptions.has(guildId)) {
            this.subscriptions.delete(guildId);
            fs.unlinkSync('./config/' + guildId + '.json');
            return;
        }
    }

    public async listGuildChannelSubscriptions(guildId: string, channel: string) {
        if (this.subscriptions.has(guildId)) {
            const guild = this.subscriptions.get(guildId);
            if (guild?.channels.has(channel)) {
                return guild.channels.get(channel);
            }
        }
    }

    private generateObject(object: any): any {
        const keys = Object.keys(object);
        const newObject: any = {};
        for (const key of keys) {
            if (object[key] instanceof Map) {
                newObject[key] = this.generateObject(Object.fromEntries(object[key]));
            } else if (Array.isArray(object[key])) {
                newObject[key] = this.generateObject(object[key]);
            } else if (typeof object[key] === 'object') {
                newObject[key] = this.generateObject(object[key]);
            } else {
                newObject[key] = object[key];
            }
        }
        return newObject;
    }

    private createChannelMap(object: any): Map<string, SubscriptionChannel> {
        const map = new Map<string, SubscriptionChannel>();
        const keys = Object.keys(object);
        for (const key of keys) {
            map.set(key, {subscriptions: this.createSubscriptionMap(object[key].subscriptions)});
        }
        return map;
    }

    private createSubscriptionMap(object: any): Map<string, Subscription> {
        console.log('Creating subscription map');
        const map = new Map<string, Subscription>();
        const keys = Object.keys(object);
        for (const key of keys) {
            console.log('Creating subscription for ' + key);
            if (object[key].limitTypes === undefined) {
                object[key].limitTypes = new Map<LimitType, string>;
            }
            if (object[key].limitTypes instanceof Object) {
                console.log('Converting limitTypes from Object to Map');
                const properties = Object.entries(object[key].limitTypes);
                object[key].limitTypes = new Map(properties);
                console.log('LimitTypes = ' + object[key].limitTypes);
            }
            map.set(key, object[key]);
        }
        return map;
    }

    private async getSystemData(systemId: number): Promise<SolarSystem> {
        return await this.asyncLock.acquire('fetchSystem', async (done) => {
            let system = this.systems.get(systemId);
            if (!system) {
                console.log('found undefined system with id ' + systemId);
                system = await this.esiClient.getSystemInfo(systemId);
                this.systems.set(systemId, system);
                fs.writeFileSync('./config/systems.json', JSON.stringify(Object.fromEntries(this.systems)), 'utf8');
            }
            if (system.securityStatus >= 0.45) {
                console.log('rounding security status: ' + system.securityStatus);
                // round to nearest tenth decimal
                system.securityStatus = Math.round(system.securityStatus * 10) / 10;
            }
            done(undefined, system);
            return;
        });
    }

    private async isInLocationLimit(subscription: Subscription, solar_system_id: number) {
        const systemData = await this.getSystemData(solar_system_id);
        if (hasLimitType(subscription, LimitType.SYSTEM) &&
            (getLimitType(subscription, LimitType.SYSTEM)?.split(',') || []).indexOf(systemData.id.toString()) !== -1) {
            return true;
        }
        if (subscription.limitTypes.has(LimitType.CONSTELLATION) &&
            (getLimitType(subscription, LimitType.CONSTELLATION)?.split(',') || []).indexOf(systemData.constellationId.toString()) !== -1) {
            return true;
        }
        if (subscription.limitTypes.has(LimitType.REGION) &&
            (getLimitType(subscription, LimitType.REGION)?.split(',') || []).indexOf(systemData.regionId.toString()) !== -1) {
            return true;
        }
        return false;
    }

    private async getGroupIdForEntityId(shipId: number): Promise<number> {
        return await this.asyncLock.acquire('fetchShip', async (done) => {

            let group = this.ships.get(shipId);
            if (group) {
                done(undefined, group);
                return;
            }
            group = await this.esiClient.getTypeGroupId(shipId);
            this.ships.set(shipId, group);
            fs.writeFileSync('./config/ships.json', JSON.stringify(Object.fromEntries(this.ships)), 'utf8');

            done(undefined, group);
        });
    }

    private async getNameForEntityId(shipId: number): Promise<string> {
        return await this.asyncLock.acquire('fetchName', async (done) => {

            let name = this.names.get(shipId);
            if (name) {
                done(undefined, name);
                return;
            }
            name = await this.esiClient.getTypeName(shipId);
            this.names.set(shipId, name);
            fs.writeFileSync('./config/names.json', JSON.stringify(Object.fromEntries(this.names)), 'utf8');

            done(undefined, name);
        });
    }

    private async getNameForAlliance(allianceId: number): Promise<string> {
        return await this.asyncLock.acquire('fetchName', async (done) => {

            let name = this.names.get(allianceId);
            if (name) {
                done(undefined, name);
                return;
            }
            name = await this.esiClient.getAllianceName(allianceId);
            this.names.set(allianceId, name);
            fs.writeFileSync('./config/names.json', JSON.stringify(Object.fromEntries(this.names)), 'utf8');

            done(undefined, name);
        });
    }

    private async getNameForCorporation(corporationId: number): Promise<string> {
        return await this.asyncLock.acquire('fetchName', async (done) => {

            let name = this.names.get(corporationId);
            if (name) {
                done(undefined, name);
                return;
            }
            name = await this.esiClient.getCorporationName(corporationId);
            this.names.set(corporationId, name);
            fs.writeFileSync('./config/names.json', JSON.stringify(Object.fromEntries(this.names)), 'utf8');

            done(undefined, name);
        });
    }

    private async getNameForCharacter(characterId: number): Promise<string> {
        return await this.asyncLock.acquire('fetchName', async (done) => {

            let name = this.names.get(characterId);
            if (name) {
                done(undefined, name);
                return;
            }
            name = await this.esiClient.getCharacterName(characterId);
            this.names.set(characterId, name);
            fs.writeFileSync('./config/names.json', JSON.stringify(Object.fromEntries(this.names)), 'utf8');

            done(undefined, name);
        });
    }

    // private async getNameForFaction(factionId: number): Promise<string> {
    //     return await this.asyncLock.acquire('fetchName', async (done) => {
    //
    //         let name = this.names.get(factionId);
    //         if (name) {
    //             done(undefined, name);
    //             return;
    //         }
    //         name = await this.esiClient.getFactionName(factionId);
    //         this.names.set(factionId, name);
    //         fs.writeFileSync('./config/names.json', JSON.stringify(Object.fromEntries(this.names)), 'utf8');
    //
    //         done(undefined, name);
    //     });
    // }

    private async getClosestCelestial(systemId: number, x: number, y: number, z: number): Promise<ClosestCelestial> {
        return await this.esiClient.getCelestial(systemId, x, y, z);
    }

    public withConfig(base_dir = './config/'): ZKillSubscriber {
        const files = fs.readdirSync(base_dir, {withFileTypes: true});
        for (const file of files) {
            if (file.name.match(/\d+\.json$/)) {
                const guildId = file.name.match(/(\d*)\.json$/);
                if (guildId && guildId.length > 0 && guildId[0]) {
                    const fileContent = fs.readFileSync(base_dir + file.name, 'utf8');
                    const parsedFileContent = JSON.parse(fileContent);
                    this.subscriptions.set(guildId[1], {channels: this.createChannelMap(parsedFileContent.channels)});
                }
            }
        }
        return this;
    }

    public withSystems(base_dir = './config/'): ZKillSubscriber {
        if (fs.existsSync(base_dir + 'systems.json')) {
            const fileContent = fs.readFileSync(base_dir + 'systems.json', 'utf8');
            try {
                const data = JSON.parse(fileContent);
                for (const key in data) {
                    this.systems.set(Number.parseInt(key), data[key] as SolarSystem);
                }
            } catch (e) {
                console.log('failed to parse systems.json');
            }
        }
        return this;
    }

    public withShips(base_dir = './config/'): ZKillSubscriber {
        if (fs.existsSync(base_dir + 'ships.json')) {
            const fileContent = fs.readFileSync(base_dir + 'ships.json', 'utf8');
            try {
                const data = JSON.parse(fileContent);
                for (const key in data) {
                    this.ships.set(Number.parseInt(key), data[key]);
                }
            } catch (e) {
                console.log('failed to parse ships.json');
            }
        }
        return this;
    }

    public withNames(base_dir = './config/'): ZKillSubscriber {
        if (fs.existsSync(base_dir + 'names.json')) {
            const fileContent = fs.readFileSync(base_dir + 'names.json', 'utf8');
            try {
                const data = JSON.parse(fileContent);
                for (const key in data) {
                    this.names.set(Number.parseInt(key), data[key]);
                }
            } catch (e) {
                console.log('failed to parse names.json');
            }
        }
        return this;
    }

    strPilotZk(characterId: number): string {
        try {
            return `https://zkillboard.com/character/${characterId.toString()}/`;
        } catch {
            return '';
        }
    }

    strCorpZk(corporationId: number): string {
        try {
            return `https://zkillboard.com/corporation/${corporationId.toString()}/`;
        } catch {
            return '';
        }
    }

    strAllianceZk(allianceId: number): string {
        try {
            return `https://zkillboard.com/alliance/${allianceId.toString()}/`;
        } catch {
            return '';
        }
    }

    strFactionZk(factionId: number): string {
        try {
            return `https://zkillboard.com/factions/${factionId.toString()}/`;
        } catch {
            return '';
        }
    }

    strShipZk(shipTypeId: number): string {
        try {
            return `https://zkillboard.com/ship/${shipTypeId.toString()}/`;
        } catch {
            return '';
        }
    }

    strLocation(locationId: number): string {
        try {
            return `https://zkillboard.com/location/${locationId.toString()}/`;
        } catch {
            return '';
        }
    }

    strSystemDotlan(systemId: number): string {
        try {
            return `http://evemaps.dotlan.net/system/${systemId.toString()}`;
        } catch {
            return '';
        }
    }

    strRegionDotlan(regionId: number): string {
        try {
            return `http://evemaps.dotlan.net/region/${regionId.toString()}`;
        } catch {
            return '';
        }
    }

    strItemRenderById(itemId: number): string {
        try {
            return `https://images.evetech.net/types/${itemId.toString()}/icon`;
        } catch {
            return '';
        }
    }

    strAllianceIconById(allianceId: number): string {
        try {
            return `https://images.evetech.net/alliances/${allianceId.toString()}/logo?size=64`;
        } catch {
            return '';
        }
    }

    strCorporationIconById(corporationId: number): string {
        try {
            return `https://images.evetech.net/corporations/${corporationId.toString()}/logo?size=64`;
        } catch {
            return '';
        }
    }
}



