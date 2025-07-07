import {Client, Intents} from 'discord.js';
import {
    KillmailData,
    LimitType,
    Subscription,
    SubscriptionFlags,
    SubscriptionType,
    ZkData,
    ZKillSubscriber
} from '../zKillSubscriber';
import * as fs from 'fs';
import * as path from 'path';

jest.setTimeout(30000);

const readTestData = (filePath: string): ZkData => {
    const absolutePath = path.join(__dirname, 'resources', filePath);
    const fileContent = fs.readFileSync(absolutePath, 'utf-8');
    return JSON.parse(fileContent);
};

describe('ZK Subscriber', () => {
    it('should send message to discord', async () => {
        const client = new Client({intents: [Intents.FLAGS.GUILDS]});
        await client.login(process.env.DISCORD_BOT_TOKEN);
        console.log('logged in');
        const sub = ZKillSubscriber.getInstance(client, false)
            .withSystems('../config/')
            .withShips('../config/')
            .withNames('../config/');
        const flags: SubscriptionFlags = {
            inclusionLimitAlsoComparesAttacker: true,
            inclusionLimitAlsoComparesAttackerWeapons: true,
            exclusionLimitAlsoComparesAttacker: true,
            exclusionLimitAlsoComparesAttackerWeapons: true,
        };
        const subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            'limitTypes': new Map([
                [LimitType.SECURITY_MIN_INCLUSIVE, '0.0'],
                [LimitType.SECURITY_MAX_EXCLUSIVE, '0.5'],
            ]),
            'minValue': 0,
            'subType': SubscriptionType.PUBLIC,
        };

        client.once('ready', async () => {
            sub.subscribe(SubscriptionType.PUBLIC, '888224317991706685', '1115807643748012072', new Map(), flags, String(1));

            let zk_data = readTestData('115769073_ostingele.json');
            await sub.sendMessageToDiscord('888224317991706685', '1115807643748012072', subscription, zk_data.killmail, zk_data.zkb, {
                shipName: 'Zirnitra',
                typeId: 52907,
                corpId: 98588237,
                allianceId: 99012162,
            }, null, 'GREEN');

            zk_data = readTestData('115787551_astrahus.json');
            await sub.sendMessageToDiscord('888224317991706685', '1115807643748012072', subscription, zk_data.killmail, zk_data.zkb, {
                shipName: 'Astrahus',
                typeId: 35832,
                corpId: 1089040789,
                allianceId: 386292982,
            }, null, 'RED');

            zk_data = readTestData('115797013_guardian_fight.json');
            await sub.sendMessageToDiscord('888224317991706685', '1115807643748012072', subscription, zk_data.killmail, zk_data.zkb, null, 30, 'GREEN');

            zk_data = readTestData('119689329_nid_solo.json');
            await sub.sendMessageToDiscord('888224317991706685', '1115807643748012072', subscription, zk_data.killmail, zk_data.zkb, null, 1, 'GREEN');

            // await sub.sendMessageToDiscord('888224317991706685', '1115807643748012072', subscription, zk_data, {
            //     shipName: 'Mobile Small Warp Disruptor II',
            //     typeId: 26892,
            //     corpId: 98636576,
            //     allianceId: 99005338,
            // }, 3, 'RED');
        });
        await new Promise(resolve => setTimeout(resolve, 20000));
    });
    it('should compare sec status correctly', async () => {
        const client = new Client({intents: [Intents.FLAGS.GUILDS]});
        await client.login(process.env.DISCORD_BOT_TOKEN);
        console.log('logged in');
        const sub = ZKillSubscriber.getInstance(client, false)
            .withSystems('../config/')
            .withShips('../config/')
            .withNames('../config/');
        const data: KillmailData = {
            attackers: [],
            killmail_id: 0,
            killmail_time: '',
            solar_system_id: 0,
            victim: {
                alliance_id: 0,
                corporation_id: 0,
                damage_taken: 0,
                items: [],
                position: { x: 0, y: 0, z: 0 }
            }
        };
        const lowsec0point1 = [
            // {'solarSystemID': '30001442', 'solarSystemName': 'Saranen', 'security': 0.0962943885,'ls': true},
            // {'solarSystemID': '30002080', 'solarSystemName': 'Arifsdald', 'security': 0.0927691123,'ls': true},
            // {'solarSystemID': '30002412', 'solarSystemName': 'Ennur', 'security': 0.0583488141,'ls': true},
            // {'solarSystemID': '30002420', 'solarSystemName': 'Egbinger', 'security': 0.0345551972,'ls': true},
            // {'solarSystemID': '30003560', 'solarSystemName': 'Hoshoun', 'security': 0.0851373893,'ls': true},
            // {'solarSystemID': '30003562', 'solarSystemName': 'Ziriert', 'security': 0.0595832109,'ls': true},
            // {'solarSystemID': '30003563', 'solarSystemName': 'Misaba', 'security': 0.0640079707,'ls': true},
            // {'solarSystemID': '30003564', 'solarSystemName': 'Rephirib', 'security': 0.0956335663,'ls': true},
            // {'solarSystemID': '30003600', 'solarSystemName': 'Agaullores', 'security': 0.0712008819,'ls': true},
            // {'solarSystemID': '30003789', 'solarSystemName': 'Brarel', 'security': 0.0943159914,'ls': true},
            // {'solarSystemID': '30003791', 'solarSystemName': 'Annancale', 'security': 0.0940761659,'ls': true},
            // {'solarSystemID': '30003793', 'solarSystemName': 'Harroule', 'security': 0.0850605913,'ls': true},
            // {'solarSystemID': '30003804', 'solarSystemName': 'Pain', 'security': 0.0940808457,'ls': true},
            // {'solarSystemID': '30003819', 'solarSystemName': 'Barleguet', 'security': 0.0682008642,'ls': true},
            // {'solarSystemID': '30003820', 'solarSystemName': 'Vestouve', 'security': 0.0425947655,'ls': true},
            // {'solarSystemID': '30003821', 'solarSystemName': 'Ausmaert', 'security': 0.0748431865,'ls': true},
            // {'solarSystemID': '30003822', 'solarSystemName': 'Espigoure', 'security': 0.0499266216,'ls': true},
            // {'solarSystemID': '30003856', 'solarSystemName': 'Athounon', 'security': 0.0762059116,'ls': true},
            // {'solarSystemID': '30003934', 'solarSystemName': 'Cabeki', 'security': 0.0897861682,'ls': true},
            // {'solarSystemID': '30003935', 'solarSystemName': 'Irmalin', 'security': 0.062148134,'ls': true},
            // {'solarSystemID': '30004256', 'solarSystemName': 'Edilkam', 'security': 0.0720004564,'ls': true},
            // {'solarSystemID': '30004258', 'solarSystemName': 'Khnar', 'security': 0.0916075577,'ls': true},
            // {'solarSystemID': '30004260', 'solarSystemName': 'Yiratal', 'security': 0.0379865815,'ls': true},
            // {'solarSystemID': '30004261', 'solarSystemName': 'Balas', 'security': 0.0436347183,'ls': true},
            // {'solarSystemID': '30004262', 'solarSystemName': 'Pemsah', 'security': 0.0479459662,'ls': true},
            // {'solarSystemID': '30004263', 'solarSystemName': 'Feshur', 'security': 0.0355186657,'ls': true},
            // {'solarSystemID': '30004264', 'solarSystemName': 'Hoseen', 'security': 0.0757257082,'ls': true},
            // {'solarSystemID': '30004265', 'solarSystemName': 'Yekh', 'security': 0.0444337419,'ls': true},
            // {'solarSystemID': '30004278', 'solarSystemName': 'Sheri', 'security': 0.0568531743,'ls': true},
            // {'solarSystemID': '30004297', 'solarSystemName': 'Efu', 'security': 0.0923133982,'ls': true},
            // {'solarSystemID': '30004298', 'solarSystemName': 'Tisot', 'security': 0.0898759449,'ls': true},
            // {'solarSystemID': '30004299', 'solarSystemName': 'Sakht', 'security': 0.0414994338,'ls': true},
            // {'solarSystemID': '30004300', 'solarSystemName': 'Naga', 'security': 0.0336844625,'ls': true},
            // {'solarSystemID': '30004301', 'solarSystemName': 'Anath', 'security': 0.0413269226,'ls': true},
            // {'solarSystemID': '30004306', 'solarSystemName': 'Karan', 'security': 0.044736305,'ls': true},
            {'solarSystemID': '30004309', 'solarSystemName': 'Hophib', 'security': 0.0291474894,'ls': true},
            {'solarSystemID': '30004706', 'solarSystemName': 'UHKL-N', 'security': -0.0052409493, 'ls': false}
        ];
        const highsec0point45 = [
            {'solarSystemID': '30002634', 'solarSystemName': 'Balle', 'security': 0.4608890986, 'hs': true},
            {'solarSystemID': '30002645', 'solarSystemName': 'Carrou', 'security': 0.4405891678, 'hs': false},
            {'solarSystemID': '30002647', 'solarSystemName': 'Ignoitton', 'security': 0.4387547559, 'hs': false},
            {'solarSystemID': '30002650', 'solarSystemName': 'Ney', 'security': 0.4567548925, 'hs': true},
            {'solarSystemID': '30002651', 'solarSystemName': 'Fasse', 'security': 0.4257427311, 'hs': false},
        ];

        const highsecOnlyLimit = new Map<LimitType, string>([
            [LimitType.SECURITY_MIN_INCLUSIVE, '0.5'],
        ]);
        const lowsecOnlyLimit = new Map<LimitType, string>([
            [LimitType.SECURITY_MIN_EXCLUSIVE, '0.0'],
            [LimitType.SECURITY_MAX_EXCLUSIVE, '0.5'],
        ]);
        const nullsecOnlyLimit = new Map<LimitType, string>([
            [LimitType.SECURITY_MAX_INCLUSIVE, '0.0'],
        ]);

        const highsecSub: Subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            limitTypes: highsecOnlyLimit,
            minValue: 0,
            subType: SubscriptionType.PUBLIC
        };
        for (const system of highsec0point45) {
            data.solar_system_id = Number(system.solarSystemID);
            const result = await sub.checkSecurityMinInclusive(highsecSub, data);
            console.log(system.solarSystemName + ' ' + result);
            expect(result).toBe(system['hs']);
        }

        const lowsecSub: Subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            limitTypes: lowsecOnlyLimit,
            minValue: 0,
            subType: SubscriptionType.PUBLIC
        };
        for (const system of lowsec0point1) {
            data.solar_system_id = Number(system.solarSystemID);
            let result = await sub.checkSecurityMinExclusive(lowsecSub, data);
            expect(result).toBe(system['ls']);
            result = await sub.checkSecurityMaxExclusive(lowsecSub, data);
            expect(result).toBe(true);
        }

        const nullsecSub: Subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            limitTypes: nullsecOnlyLimit,
            minValue: 0,
            subType: SubscriptionType.PUBLIC
        };
        for (const system of lowsec0point1) {
            data.solar_system_id = Number(system.solarSystemID);
            const result = await sub.checkSecurityMaxInclusive(nullsecSub, data);
            expect(result).toBe(!system['ls']);
        }
    });
    it('should match npc kills', async () => {
        const client = new Client({intents: [Intents.FLAGS.GUILDS]});
        await client.login(process.env.DISCORD_BOT_TOKEN);
        console.log('logged in');
        const sub = ZKillSubscriber.getInstance(client, false)
            .withSystems('../config/')
            .withShips('../config/')
            .withNames('../config/');
        const data: KillmailData = {
            attackers: [],
            killmail_id: 0,
            killmail_time: '',
            solar_system_id: 0,
            victim: {
                alliance_id: 0,
                corporation_id: 0,
                damage_taken: 0,
                items: [],
                position: { x: 0, y: 0, z: 0 }
            }
        };
        const lowsec0point1 = [
            // {'solarSystemID': '30001442', 'solarSystemName': 'Saranen', 'security': 0.0962943885,'ls': true},
            // {'solarSystemID': '30002080', 'solarSystemName': 'Arifsdald', 'security': 0.0927691123,'ls': true},
            // {'solarSystemID': '30002412', 'solarSystemName': 'Ennur', 'security': 0.0583488141,'ls': true},
            // {'solarSystemID': '30002420', 'solarSystemName': 'Egbinger', 'security': 0.0345551972,'ls': true},
            // {'solarSystemID': '30003560', 'solarSystemName': 'Hoshoun', 'security': 0.0851373893,'ls': true},
            // {'solarSystemID': '30003562', 'solarSystemName': 'Ziriert', 'security': 0.0595832109,'ls': true},
            // {'solarSystemID': '30003563', 'solarSystemName': 'Misaba', 'security': 0.0640079707,'ls': true},
            // {'solarSystemID': '30003564', 'solarSystemName': 'Rephirib', 'security': 0.0956335663,'ls': true},
            // {'solarSystemID': '30003600', 'solarSystemName': 'Agaullores', 'security': 0.0712008819,'ls': true},
            // {'solarSystemID': '30003789', 'solarSystemName': 'Brarel', 'security': 0.0943159914,'ls': true},
            // {'solarSystemID': '30003791', 'solarSystemName': 'Annancale', 'security': 0.0940761659,'ls': true},
            // {'solarSystemID': '30003793', 'solarSystemName': 'Harroule', 'security': 0.0850605913,'ls': true},
            // {'solarSystemID': '30003804', 'solarSystemName': 'Pain', 'security': 0.0940808457,'ls': true},
            // {'solarSystemID': '30003819', 'solarSystemName': 'Barleguet', 'security': 0.0682008642,'ls': true},
            // {'solarSystemID': '30003820', 'solarSystemName': 'Vestouve', 'security': 0.0425947655,'ls': true},
            // {'solarSystemID': '30003821', 'solarSystemName': 'Ausmaert', 'security': 0.0748431865,'ls': true},
            // {'solarSystemID': '30003822', 'solarSystemName': 'Espigoure', 'security': 0.0499266216,'ls': true},
            // {'solarSystemID': '30003856', 'solarSystemName': 'Athounon', 'security': 0.0762059116,'ls': true},
            // {'solarSystemID': '30003934', 'solarSystemName': 'Cabeki', 'security': 0.0897861682,'ls': true},
            // {'solarSystemID': '30003935', 'solarSystemName': 'Irmalin', 'security': 0.062148134,'ls': true},
            // {'solarSystemID': '30004256', 'solarSystemName': 'Edilkam', 'security': 0.0720004564,'ls': true},
            // {'solarSystemID': '30004258', 'solarSystemName': 'Khnar', 'security': 0.0916075577,'ls': true},
            // {'solarSystemID': '30004260', 'solarSystemName': 'Yiratal', 'security': 0.0379865815,'ls': true},
            // {'solarSystemID': '30004261', 'solarSystemName': 'Balas', 'security': 0.0436347183,'ls': true},
            // {'solarSystemID': '30004262', 'solarSystemName': 'Pemsah', 'security': 0.0479459662,'ls': true},
            // {'solarSystemID': '30004263', 'solarSystemName': 'Feshur', 'security': 0.0355186657,'ls': true},
            // {'solarSystemID': '30004264', 'solarSystemName': 'Hoseen', 'security': 0.0757257082,'ls': true},
            // {'solarSystemID': '30004265', 'solarSystemName': 'Yekh', 'security': 0.0444337419,'ls': true},
            // {'solarSystemID': '30004278', 'solarSystemName': 'Sheri', 'security': 0.0568531743,'ls': true},
            // {'solarSystemID': '30004297', 'solarSystemName': 'Efu', 'security': 0.0923133982,'ls': true},
            // {'solarSystemID': '30004298', 'solarSystemName': 'Tisot', 'security': 0.0898759449,'ls': true},
            // {'solarSystemID': '30004299', 'solarSystemName': 'Sakht', 'security': 0.0414994338,'ls': true},
            // {'solarSystemID': '30004300', 'solarSystemName': 'Naga', 'security': 0.0336844625,'ls': true},
            // {'solarSystemID': '30004301', 'solarSystemName': 'Anath', 'security': 0.0413269226,'ls': true},
            // {'solarSystemID': '30004306', 'solarSystemName': 'Karan', 'security': 0.044736305,'ls': true},
            {'solarSystemID': '30004309', 'solarSystemName': 'Hophib', 'security': 0.0291474894,'ls': true},
            {'solarSystemID': '30004706', 'solarSystemName': 'UHKL-N', 'security': -0.0052409493, 'ls': false}
        ];
        const highsec0point45 = [
            {'solarSystemID': '30002634', 'solarSystemName': 'Balle', 'security': 0.4608890986, 'hs': true},
            {'solarSystemID': '30002645', 'solarSystemName': 'Carrou', 'security': 0.4405891678, 'hs': false},
            {'solarSystemID': '30002647', 'solarSystemName': 'Ignoitton', 'security': 0.4387547559, 'hs': false},
            {'solarSystemID': '30002650', 'solarSystemName': 'Ney', 'security': 0.4567548925, 'hs': true},
            {'solarSystemID': '30002651', 'solarSystemName': 'Fasse', 'security': 0.4257427311, 'hs': false},
        ];

        const highsecOnlyLimit = new Map<LimitType, string>([
            [LimitType.SECURITY_MIN_INCLUSIVE, '0.5'],
        ]);
        const lowsecOnlyLimit = new Map<LimitType, string>([
            [LimitType.SECURITY_MIN_EXCLUSIVE, '0.0'],
            [LimitType.SECURITY_MAX_EXCLUSIVE, '0.5'],
        ]);
        const nullsecOnlyLimit = new Map<LimitType, string>([
            [LimitType.SECURITY_MAX_INCLUSIVE, '0.0'],
        ]);

        const highsecSub: Subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            limitTypes: highsecOnlyLimit,
            minValue: 0,
            subType: SubscriptionType.PUBLIC
        };
        for (const system of highsec0point45) {
            data.solar_system_id = Number(system.solarSystemID);
            const result = await sub.checkSecurityMinInclusive(highsecSub, data);
            console.log(system.solarSystemName + ' ' + result);
            expect(result).toBe(system['hs']);
        }

        const lowsecSub: Subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            limitTypes: lowsecOnlyLimit,
            minValue: 0,
            subType: SubscriptionType.PUBLIC
        };
        for (const system of lowsec0point1) {
            data.solar_system_id = Number(system.solarSystemID);
            let result = await sub.checkSecurityMinExclusive(lowsecSub, data);
            expect(result).toBe(system['ls']);
            result = await sub.checkSecurityMaxExclusive(lowsecSub, data);
            expect(result).toBe(true);
        }

        const nullsecSub: Subscription = {
            'inclusionLimitAlsoComparesAttacker': true,
            'inclusionLimitAlsoComparesAttackerWeapons': true,
            'exclusionLimitAlsoComparesAttacker': true,
            'exclusionLimitAlsoComparesAttackerWeapons': true,
            limitTypes: nullsecOnlyLimit,
            minValue: 0,
            subType: SubscriptionType.PUBLIC
        };
        for (const system of lowsec0point1) {
            data.solar_system_id = Number(system.solarSystemID);
            const result = await sub.checkSecurityMaxInclusive(nullsecSub, data);
            expect(result).toBe(!system['ls']);
        }
    });
});
