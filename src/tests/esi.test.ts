import pack from 'bin-pack';
import fs from 'fs';
import * as util from 'util';
import {EsiClient} from '../lib/esiClient';

describe('ESI Client', () => {
    it('should return type ID names', async () => {
        const client = new EsiClient();
        const typeName = await client.getTypeName(587);
        expect(typeName).toBe('Rifter');

        const structure_ids = [35832, 35833, 35827, 35825, 35826, 35835, 35836, 35947, 47366, 35943, 47351, 35921, 47323, 35924, 47330, 35925, 47332, 35926, 47327, 35949, 47334, 35944, 37846, 37849, 37847, 37850, 37848, 37843, 37844, 35922, 47298, 35923, 47325, 35949, 47334, 47069, 35940, 47338, 35945, 47364, 47073, 23057, 37599, 40362, 12235, 27563, 28191, 27570, 25270, 12237, 17184, 17180, 27675, 19470, 33149, 16221, 12240, 20176, 27557, 16696, 27551, 17402, 28351, 27857, 27576, 27674, 24652, 12239, 24657];
        const names = [];
        for (const structure_id of structure_ids) {
            const structureName = await client.getTypeName(structure_id);
            names.push(`${structure_id} - ${structureName}`);
        }
        console.log(names.join('\n'));
        await (async () => {
            console.log('done');
        })();
    });

    it('should login or load cached sso token', async () => {
        const client = new EsiClient();
        const token = await client.eveSsoLogin();
        console.log(token);
    }, 100000);

    it('should generate SW courier trip plans', async () => {
        const client = new EsiClient();
        const token = await client.eveSsoRefresh();

        const tripName = 'Jita-Turnur';
        // const tripName = 'Turnur-Jita';

        let maxVolume;
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        if (tripName === 'Turnur-Jita') {
            maxVolume = 209687;
        } else {
            maxVolume = 321580;
        }

        const trips = await client.processContracts(<string>token.access_token, maxVolume);
        // Filter the trips to only include the trip with the specified name
        const filteredTrips = trips.trips[tripName];

        console.log(util.inspect(filteredTrips, false, 5, true /* enable colors */));

        filteredTrips.forEach((trip, i) => {
            const flattenedTrip = trip.contractsForTrip.map(contract => ({
                volume: contract.volume,
                reward: contract.reward,
                ratio: contract.ratio,
                trip: i + 1,
                totalVolume: trip.totalVolume,
                totalReward: trip.totalReward,
            }));
            console.table(flattenedTrip);
        });
    }, 100000);
});