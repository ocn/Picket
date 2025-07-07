import {SlashCommandBuilder, SlashCommandSubcommandBuilder} from '@discordjs/builders';
import {CommandInteraction} from 'discord.js';
import {AbstractCommand} from './abstractCommand';
import {SubscriptionType, ZKillSubscriber} from '../zKillSubscriber';

export class UnsubscribeCommand extends AbstractCommand {
    protected name = 'zkill-unsubscribe';

    executeCommand(interaction: CommandInteraction): void {
        const sub = ZKillSubscriber.getInstance();
        if(!interaction.inGuild()) {
            // @ts-ignore
            interaction.reply('Subscription is not possible in PM!');
            return;
        }
        const subCommand = interaction.options.getSubcommand(true);
        const id = interaction.options.getString('id', false);
        sub.unsubscribe(subCommand as SubscriptionType, interaction.guildId, interaction.channelId, id ? id : undefined);
        interaction.reply({
            content: 'Unsubscribed to zkillboard channel: ' + interaction.options.getSubcommand() + ' ' + interaction.options.getString('id'),
            ephemeral: true
        });
    }

    getCommand(): SlashCommandBuilder {
        const slashCommand = new SlashCommandBuilder().setName(this.name)
            .setDescription('Unsubscribe from zkill');
        slashCommand.addSubcommand( new SlashCommandSubcommandBuilder().setName(SubscriptionType.PUBLIC)
            .setDescription('Unsubscribe feed from channel')
            .addStringOption(option =>
                option.setName('id')
                    .setDescription('ID for the feed')
                    .setRequired(true)
            ));
        return slashCommand;

    }

}
