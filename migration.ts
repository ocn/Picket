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
    channel_id: number;
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
    | { IsNpc: boolean };

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
                // To be > 0.0, it must be >= 0.0001 (or some small epsilon)
                // To be > 0.4, it must be >= 0.45 (the next security tier)
                securityMin = Math.max(securityMin, parseFloat(value) === 0.4 ? 0.45 : parseFloat(value) + 0.0001);
                securityFilterExists = true;
                break;
            case 'securityMaxExclusive':
                // To be < 0.5, it must be <= 0.4499...
                // To be < 0.1, it must be <= 0.0
                securityMax = Math.min(securityMax, parseFloat(value) === 0.1 ? 0.0 : parseFloat(value) - 0.0001);
                securityFilterExists = true;
                break;
        }
    }

    if (securityFilterExists) {
        // Use toFixed(4) to avoid floating point representation issues
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

            const newSub: NewSubscription = {
                id: subId,
                description: `Migrated subscription: ${subId}`,
                action: {
                    channel_id: Number(channelId),
                },
                filter: {
                    And: conditions
                }
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