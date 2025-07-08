/* eslint-disable @typescript-eslint/no-var-requires */
import * as fs from 'fs';
import * as path from 'path';

// --- Old Format Interfaces ---
interface OldSubscription {
    subType: string;
    id?: string;
    minValue: number;
    limitTypes: { [key: string]: string };
}

interface OldSubscriptionChannel {
    subscriptions: { [key: string]: OldSubscription };
}

interface OldSubscriptionGuild {
    channels: { [key: string]: OldSubscriptionChannel };
}

// --- New Rust AST Format Interfaces ---
interface Action {
    channel_id: string;
    ping_type?: 'Here' | 'Everyone';
}

type Filter =
    | { TotalValue: { min?: number; max?: number } }
    | { Region: number[] }
    | { System: number[] }
    | { Security: string }
    | { Alliance: number[] }
    | { Corporation: number[] }
    | { Character: number[] }
    | { ShipType: number[] }
    | { IsNpc: boolean }
    | { NameFragment: string };

type FilterNode =
    | { Condition: Filter }
    | { And: FilterNode[] };

interface NewSubscription {
    id: string;
    description: string;
    filter: FilterNode;
    action: Action;
}

// --- Main Migration Logic ---

function parseIds(value: string): number[] {
    return value.split(',').map(id => parseInt(id.trim(), 10)).filter(id => !isNaN(id));
}

function generateDescription(filterNode: FilterNode): string {
    if (!filterNode || !('And' in filterNode) || !filterNode.And) {
        return "General killmails.";
    }

    const conditions = filterNode.And.map(item => 'Condition' in item ? item.Condition : null).filter(c => c !== null) as Filter[];
    let parts = [];

    const totalValueCondition = conditions.find(c => 'TotalValue' in c);
    if (totalValueCondition && 'TotalValue' in totalValueCondition) {
        const totalValue = totalValueCondition.TotalValue;
        if (totalValue.min && totalValue.max) {
            parts.push(`valued between ${totalValue.min / 1000000}M and ${totalValue.max / 1000000}M ISK`);
        } else if (totalValue.min) {
            parts.push(`valued over ${totalValue.min / 1000000}M ISK`);
        } else if (totalValue.max) {
            parts.push(`valued under ${totalValue.max / 1000000}M ISK`);
        }
    }

    const shipTypeCondition = conditions.find(c => 'ShipType' in c);
    if (shipTypeCondition && 'ShipType' in shipTypeCondition) {
        parts.push(`involving one of ${shipTypeCondition.ShipType.length} specific ship types`);
    }

    const regionCondition = conditions.find(c => 'Region' in c);
    if (regionCondition && 'Region' in regionCondition) {
        parts.push(`in one of ${regionCondition.Region.length} specific regions`);
    }

    const securityCondition = conditions.find(c => 'Security' in c);
    if (securityCondition && 'Security' in securityCondition) {
        const security = securityCondition.Security;
        const [min, max] = security.split('..=').map(parseFloat);
        if (min <= 0.0 && max <= 0.0) {
            parts.push("in nullsec");
        } else if (min > 0.0 && max < 0.5) {
            parts.push("in lowsec");
        } else if (min >= 0.5) {
            parts.push("in highsec");
        } else {
            parts.push(`in ${security} security space`);
        }
    }

    const allianceCondition = conditions.find(c => 'Alliance' in c);
    if (allianceCondition && 'Alliance' in allianceCondition) {
        parts.push(`involving alliance ID ${allianceCondition.Alliance[0]}`);
    }
    
    const characterCondition = conditions.find(c => 'Character' in c);
    if(characterCondition && 'Character' in characterCondition) {
        parts.push(`involving character ID ${characterCondition.Character[0]}`);
    }

    if (parts.length === 0) {
        return "General killmail notifications.";
    }

    return "Alerts for killmails " + parts.join(', ') + ".";
}


function transformLimitsToFilters(oldSub: OldSubscription): FilterNode[] {
    const filters: Filter[] = [];
    let securityMin = -1.0;
    let securityMax = 1.0;
    let securityFilterExists = false;

    if (oldSub.minValue > 0) {
        filters.push({ TotalValue: { min: oldSub.minValue } });
    }

    for (const limitType in oldSub.limitTypes) {
        const value = oldSub.limitTypes[limitType];
        switch (limitType) {
            case 'region':
                filters.push({ Region: parseIds(value) });
                break;
            case 'type':
                filters.push({ ShipType: parseIds(value) });
                break;
            case 'alliance':
                filters.push({ Alliance: parseIds(value) });
                break;
            case 'corporation':
                filters.push({ Corporation: parseIds(value) });
                break;
            case 'character':
                filters.push({ Character: parseIds(value) });
                break;
            case 'npcOnly':
                filters.push({ IsNpc: value.toLowerCase() === 'true' });
                break;
            case 'minNumInvolved':
                filters.push({ Pilots: { min: Number(value) } });
                break;
            case 'nameFragment':                                                                                                                                             │
                filters.push({ NameFragment: value });
                break;
            
            // --- Correct Security Logic ---
            
            case 'securityMinInclusive':
                securityMin = Math.max(securityMin, parseFloat(value));
                securityFilterExists = true;
                break;
            case 'securityMaxInclusive':
                securityMax = Math.min(securityMax, parseFloat(value));
                securityFilterExists = true;
                break;
            case 'securityMinExclusive':
                securityMin = Math.max(securityMin, parseFloat(value) === 0.4 ? 0.45 : parseFloat(value) + 0.0001);
                securityFilterExists = true;
                break;
            case 'securityMaxExclusive':
                securityMax = Math.min(securityMax, parseFloat(value) === 0.1 ? 0.0 : parseFloat(value) - 0.0001);
                securityFilterExists = true;
                break;
        }
    }

    if (securityFilterExists) {
        filters.push({ Security: `${securityMin.toFixed(4)}..=${securityMax.toFixed(4)}` });
    }

    return filters.map(f => ({ Condition: f }));
}

function migrateGuildFile(filePath: string): NewSubscription[] {
    const oldConfig: OldSubscriptionGuild = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const newSubscriptions: NewSubscription[] = [];

    for (const channelId in oldConfig.channels) {
        const channel = oldConfig.channels[channelId];
        for (const subKey in channel.subscriptions) {
            const oldSub = channel.subscriptions[subKey];
            const subId = oldSub.id?.toString() || subKey.replace(oldSub.subType, '') || subKey;

            const conditions = transformLimitsToFilters(oldSub);
            const filterNode: FilterNode = { And: conditions };

            const newSub: NewSubscription = {
                id: subId,
                description: generateDescription(filterNode),
                action: {
                    channel_id: channelId,
                },
                filter: filterNode
            };
            newSubscriptions.push(newSub);
        }
    }
    return newSubscriptions;
}

// --- Execution ---

const configDir = path.join(__dirname, 'config');
const sourceFile = '888224317991706685.json';
const sourceFilePath = path.join(configDir, sourceFile);

if (fs.existsSync(sourceFilePath)) {
    try {
        const migratedSubs = migrateGuildFile(sourceFilePath);
        const newFilePath = path.join(configDir, `${path.basename(sourceFile, '.json')}.new.json`);
        
        fs.writeFileSync(newFilePath, JSON.stringify(migratedSubs, null, 2));
        
        console.log(`Successfully migrated ${sourceFile} to ${path.basename(newFilePath)}.`);
        console.log(`Found and migrated ${migratedSubs.length} subscriptions.`);
    } catch (e) {
        console.error(`Failed to migrate ${sourceFile}:`, e);
    }
} else {
    console.error(`Source file not found: ${sourceFilePath}`);
}
