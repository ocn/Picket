import {SlashCommandBuilder, SlashCommandSubcommandBuilder} from '@discordjs/builders';
import {CommandInteraction} from 'discord.js';
import {AbstractCommand} from './abstractCommand';
import {LimitType, SubscriptionType, ZKillSubscriber} from '../zKillSubscriber';

export class SubscribeCommand extends AbstractCommand {
    protected name = 'zkill-subscribe';

    protected ID = 'id';
    protected MIN_VALUE = 'min-value';
    protected MIN_NUM_INVOLVED = 'min-num-involved';
    protected LIMIT_REGION_IDS = 'limit-region-ids';
    protected LIMIT_CONSTELLATION_IDS = 'limit-constellation-ids';
    protected LIMIT_SYSTEM_IDS = 'limit-system-ids';
    protected LIMIT_INCLUDED_SHIP_IDS = 'limit-included-ship-ids';
    protected LIMIT_EXCLUDED_SHIP_IDS = 'limit-excluded-ship-ids';
    protected LIMIT_SECURITY_MAX_INCL = 'limit-security-max-incl';
    protected LIMIT_SECURITY_MIN_INCL = 'limit-security-min-incl';
    protected LIMIT_SECURITY_MAX_EXCL = 'limit-security-max-excl';
    protected LIMIT_SECURITY_MIN_EXCL = 'limit-security-min-excl';
    protected LIMIT_ALLIANCE_IDS = 'limit-alliance-ids';
    protected LIMIT_CORPORATION_IDS = 'limit-corporation-ids';
    protected LIMIT_CHARACTER_IDS = 'limit-character-ids';
    protected LIMIT_TIME_RANGE_START = 'limit-time-range-start';
    protected LIMIT_TIME_RANGE_END = 'limit-time-range-end';
    protected INCLUSION_LIMIT_COMPARES_ATTACKERS = 'in-limit-compares-attackers';
    protected INCLUSION_LIMIT_COMPARES_ATTACKER_WEAPONS = 'in-limit-compares-attacker-weps';
    protected EXCLUSION_LIMIT_COMPARES_ATTACKERS = 'ex-limit-compares-attackers';
    protected EXCLUSION_LIMIT_COMPARES_ATTACKER_WEAPONS = 'ex-limit-compares-attacker-weps';
    protected REQUIRED_NAME_FRAGMENT = 'required-name-fragment';
    protected NPC_ONLY = 'npc-only';

    async executeCommand(interaction: CommandInteraction): Promise<void> {
        const sub = ZKillSubscriber.getInstance();
        if (!interaction.inGuild()) {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            await interaction.reply('Subscription is not possible in PM!');
            return;
        }
        const subCommand = interaction.options.getSubcommand(true) as SubscriptionType;
        const id = interaction.options.getNumber(this.ID, true);
        const minValue = interaction.options.getNumber(this.MIN_VALUE);
        const minNumInvolved = interaction.options.getNumber(this.MIN_NUM_INVOLVED);
        const limitCharacter = interaction.options.getString(this.LIMIT_CHARACTER_IDS);
        const limitCorporation = interaction.options.getString(this.LIMIT_CORPORATION_IDS);
        const limitAlliance = interaction.options.getString(this.LIMIT_ALLIANCE_IDS);
        const limitRegion = interaction.options.getString(this.LIMIT_REGION_IDS);
        const limitConstellation = interaction.options.getString(this.LIMIT_CONSTELLATION_IDS);
        const limitSystem = interaction.options.getString(this.LIMIT_SYSTEM_IDS);
        const limitShipsIncluded = interaction.options.getString(this.LIMIT_INCLUDED_SHIP_IDS);
        const limitShipsExcluded = interaction.options.getString(this.LIMIT_EXCLUDED_SHIP_IDS);
        const limitSecurityMaxExcl = interaction.options.getString(this.LIMIT_SECURITY_MAX_EXCL);
        const limitSecurityMinExcl = interaction.options.getString(this.LIMIT_SECURITY_MIN_EXCL);
        const limitSecurityMaxIncl = interaction.options.getString(this.LIMIT_SECURITY_MAX_INCL);
        const limitSecurityMinIncl = interaction.options.getString(this.LIMIT_SECURITY_MIN_INCL);
        const timeRangeStart = interaction.options.getString(this.LIMIT_TIME_RANGE_START);
        const timeRangeEnd = interaction.options.getString(this.LIMIT_TIME_RANGE_END);
        const requiredNameFragment = interaction.options.getString(this.REQUIRED_NAME_FRAGMENT);
        let npcOnly = interaction.options.getBoolean(this.NPC_ONLY);
        let inclusionLimitComparesAttackers = interaction.options.getBoolean(this.INCLUSION_LIMIT_COMPARES_ATTACKERS);
        let inclusionLimitComparesAttackerWeapons = interaction.options.getBoolean(this.INCLUSION_LIMIT_COMPARES_ATTACKER_WEAPONS);
        let exclusionLimitComparesAttackers = interaction.options.getBoolean(this.EXCLUSION_LIMIT_COMPARES_ATTACKERS);
        let exclusionLimitComparesAttackerWeapons = interaction.options.getBoolean(this.EXCLUSION_LIMIT_COMPARES_ATTACKER_WEAPONS);
        if (inclusionLimitComparesAttackers == null) {
            inclusionLimitComparesAttackers = true;
        }
        if (inclusionLimitComparesAttackerWeapons == null) {
            inclusionLimitComparesAttackerWeapons = true;
        }
        if (exclusionLimitComparesAttackers == null) {
            exclusionLimitComparesAttackers = true;
        }
        if (exclusionLimitComparesAttackerWeapons == null) {
            exclusionLimitComparesAttackerWeapons = true;
        }
        if (npcOnly == null) {
            npcOnly = false;
        }
        let reply = 'We subscribed to zkillboard channel: ' + interaction.options.getSubcommand();
        const limitTypes = new Map<LimitType, string>();
        if (npcOnly) {
            limitTypes.set(LimitType.NPC_ONLY, String(npcOnly));
            reply += '\nNPC Only: + ' + npcOnly;
        }
        if (limitAlliance) {
            limitTypes.set(LimitType.ALLIANCE, limitAlliance);
            reply += '\nAlliance filter: + ' + limitAlliance;
        }
        if (limitCorporation) {
            limitTypes.set(LimitType.CORPORATION, limitCorporation);
            reply += '\nCorporation filter: + ' + limitCorporation;
        }
        if (limitCharacter) {
            limitTypes.set(LimitType.CHARACTER, limitCharacter);
            reply += '\nCharacter filter: + ' + limitCharacter;
        }
        if (limitRegion) {
            limitTypes.set(LimitType.REGION, limitRegion);
            reply += '\nRegion filter: + ' + limitRegion;
        }
        if (limitConstellation) {
            limitTypes.set(LimitType.CONSTELLATION, limitConstellation);
            reply += '\nConstellation filter: + ' + limitRegion;
        }
        if (limitSystem) {
            limitTypes.set(LimitType.SYSTEM, limitSystem);
            reply += '\nSystem filter: + ' + limitRegion;
        }
        if (limitShipsIncluded) {
            limitTypes.set(LimitType.SHIP_INCLUSION_TYPE_ID, limitShipsIncluded);
            reply += '\nShip ID Inclusion filter: + ' + limitShipsIncluded;
        }
        if (limitShipsExcluded) {
            limitTypes.set(LimitType.SHIP_EXCLUSION_TYPE_ID, limitShipsExcluded);
            reply += '\nShip ID Exclusion filter: - ' + limitShipsExcluded;
        }
        if (limitSecurityMaxIncl) {
            limitTypes.set(LimitType.SECURITY_MAX_INCLUSIVE, limitSecurityMaxIncl);
            reply += '\nMax Security filter: + ' + limitSecurityMaxIncl;
        }
        if (limitSecurityMinIncl) {
            limitTypes.set(LimitType.SECURITY_MIN_INCLUSIVE, limitSecurityMinIncl);
            reply += '\nMin Security filter: + ' + limitSecurityMinIncl;
        }
        if (limitSecurityMaxExcl) {
            limitTypes.set(LimitType.SECURITY_MAX_EXCLUSIVE, limitSecurityMaxExcl);
            reply += '\nMax Security filter: - ' + limitSecurityMaxExcl;
        }
        if (limitSecurityMinExcl) {
            limitTypes.set(LimitType.SECURITY_MIN_EXCLUSIVE, limitSecurityMinExcl);
            reply += '\nMin Security filter: - ' + limitSecurityMinExcl;
        }
        if (minNumInvolved) {
            limitTypes.set(LimitType.MIN_NUM_INVOLVED, minNumInvolved.toString());
            reply += '\nMin Num Involved: + ' + minNumInvolved;
        }
        if (timeRangeStart) {
            limitTypes.set(LimitType.TIME_RANGE_START, timeRangeStart);
            reply += '\nTime Range Start: + ' + timeRangeStart;
        }
        if (timeRangeEnd) {
            limitTypes.set(LimitType.TIME_RANGE_END, timeRangeEnd);
            reply += '\nTime Range End: + ' + timeRangeEnd;
        }
        if (requiredNameFragment) {
            limitTypes.set(LimitType.NAME_FRAGMENT, requiredNameFragment);
            reply += '\nRequired name fragment: + ' + requiredNameFragment;
        }
        sub.subscribe(
            subCommand,
            interaction.guildId,
            interaction.channelId,
            limitTypes,
            inclusionLimitComparesAttackers,
            inclusionLimitComparesAttackerWeapons,
            exclusionLimitComparesAttackers,
            exclusionLimitComparesAttackerWeapons,
            id ? String(id) : undefined,
            minValue ? minValue : 0,
        );

        if (id) {
            reply += ' ID: ' + id;
        }
        if (minValue) {
            reply += ' Min Value: ' + minValue.toLocaleString('en');
        }
        await interaction.reply({content: reply, ephemeral: true});
    }

    getCommand(): SlashCommandBuilder {
        const slashCommand = new SlashCommandBuilder().setName(this.name)
            .setDescription('Subscribe to zkill');


        slashCommand.addSubcommand(new SlashCommandSubcommandBuilder().setName('public')
            .addNumberOption(option =>
                option.setName(this.ID)
                    .setDescription('ID for public feed')
                    .setRequired(true)
            )
            .addNumberOption(option =>
                option.setName(this.MIN_VALUE)
                    .setDescription('Minimum isk to show the entry')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_INCLUDED_SHIP_IDS)
                    .setDescription('Limit to ship id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_EXCLUDED_SHIP_IDS)
                    .setDescription('Limit to ship id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_CHARACTER_IDS)
                    .setDescription('Limit to character id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_CORPORATION_IDS)
                    .setDescription('Limit to corporation id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_ALLIANCE_IDS)
                    .setDescription('Limit to alliance id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_REGION_IDS)
                    .setDescription('Limit to region id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_SECURITY_MAX_INCL)
                    .setDescription('Limit to a maximum security, inclusive')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_SECURITY_MIN_INCL)
                    .setDescription('Limit to a minimum security, inclusive')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_SECURITY_MAX_EXCL)
                    .setDescription('Limit to a maximum security, exclusive')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_SECURITY_MIN_EXCL)
                    .setDescription('Limit to a minimum security, exclusive')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_CONSTELLATION_IDS)
                    .setDescription('Limit to constellation id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_SYSTEM_IDS)
                    .setDescription('Limit to system id, comma seperated ids')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_TIME_RANGE_START)
                    .setDescription('Limit to time range start, integer value between 0 - 23 hours')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.LIMIT_TIME_RANGE_END)
                    .setDescription('Limit to time range end, integer value between 0 - 23 hours')
                    .setRequired(false)
            )
            .addNumberOption(option =>
                option.setName(this.MIN_NUM_INVOLVED)
                    .setDescription('Minimum number of involved parties on the killmail')
                    .setRequired(false)
            )
            .addStringOption(option =>
                option.setName(this.REQUIRED_NAME_FRAGMENT)
                    .setDescription('Require a name fragment in the name of the matched type IDs')
                    .setRequired(false)
            )
            .addBooleanOption(option =>
                option.setName(this.NPC_ONLY)
                    .setDescription('Enable if only NPC kills should be considered')
                    .setRequired(false)
            )
            .addBooleanOption(option =>
                option.setName(this.INCLUSION_LIMIT_COMPARES_ATTACKERS)
                    .setDescription('Enable if attackers should be considered when sending mails')
                    .setRequired(false)
            )
            .addBooleanOption(option =>
                option.setName(this.INCLUSION_LIMIT_COMPARES_ATTACKER_WEAPONS)
                    .setDescription('Enable if attackers should be considered when sending mails')
                    .setRequired(false)
            )
            .addBooleanOption(option =>
                option.setName(this.EXCLUSION_LIMIT_COMPARES_ATTACKERS)
                    .setDescription('Enable if attackers should be considered when rejecting mails')
                    .setRequired(false)
            )
            .addBooleanOption(option =>
                option.setName(this.EXCLUSION_LIMIT_COMPARES_ATTACKER_WEAPONS)
                    .setDescription('Enable if attackers should be considered when rejecting mails')
                    .setRequired(false)
            )
            .setDescription('Subscribe public feed to channel'));

        return slashCommand;

    }

}

