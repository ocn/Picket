import {SlashCommandBuilder, SlashCommandSubcommandBuilder} from '@discordjs/builders';
import {CommandInteraction} from 'discord.js';
import {AbstractCommand} from './abstractCommand';
import {SubscriptionType, ZKillSubscriber} from '../zKillSubscriber';

export class UnsubscribeCommand extends AbstractCommand {
    protected name = 'zkill-unsubscribe';

    async executeCommand(interaction: CommandInteraction): Promise<void> {
        const sub = ZKillSubscriber.getInstance();
        if(!interaction.inGuild()) {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            await interaction.reply('Subscription is not possible in PM!');
            return;
        }
        const subCommand = interaction.options.getSubcommand(true) as SubscriptionType;
        const id = interaction.options.getNumber('id', true);
        await sub.unsubscribe(subCommand, interaction.guildId, interaction.channelId, id ? String(id) : undefined);
        await interaction.reply({
            content: 'Unsubscribed to zkillboard channel: ' + interaction.options.getSubcommand() + ' ' + id,
            ephemeral: true
        });
    }

    getCommand(): SlashCommandBuilder {
        const slashCommand = new SlashCommandBuilder().setName(this.name)
            .setDescription('Unsubscribe from zkill');
        slashCommand.addSubcommand( new SlashCommandSubcommandBuilder().setName('feed')
            .setDescription('Unsubscribe feed from channel')
            .addNumberOption(option =>
                option.setName('id')
                    .setDescription('ID for the feed')
                    .setRequired(true)
            ));
        return slashCommand;

    }

}
