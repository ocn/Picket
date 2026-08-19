pub struct CompactLocation<'a> {
    pub system: Option<LocationSystem<'a>>,
    pub region: Option<LocationRegion<'a>>,
    pub on: Option<LocationOn<'a>>,
    pub range: Option<LocationRange<'a>>,
}

pub struct LocationSystem<'a> {
    pub name: &'a str,
    pub id: u32,
}

pub struct LocationRegion<'a> {
    pub name: &'a str,
    pub id: u32,
}

pub struct LocationOn<'a> {
    pub name: &'a str,
    pub id: u64,
    pub suffix: Option<&'a str>,
}

pub struct LocationRange<'a> {
    pub light_years: f64,
    pub reference_system: &'a str,
    pub destination_system: &'a str,
}

pub fn compact_location_description(location: CompactLocation<'_>) -> String {
    let mut lines = Vec::new();
    let system = location.system.map(|system| {
        format!(
            "[{}](http://evemaps.dotlan.net/system/{})",
            system.name, system.id
        )
    });
    let region = location.region.map(|region| {
        format!(
            "[{}](http://evemaps.dotlan.net/region/{})",
            region.name, region.id
        )
    });
    match (system, region) {
        (Some(system), Some(region)) => lines.push(format!("**in:** {system} ({region})")),
        (Some(system), None) => lines.push(format!("**in:** {system}")),
        (None, Some(region)) => lines.push(format!("**in:** {region}")),
        (None, None) => {}
    }
    if let Some(on) = location.on {
        let suffix = on
            .suffix
            .filter(|suffix| !suffix.is_empty())
            .map(|suffix| format!(", {suffix}"))
            .unwrap_or_default();
        lines.push(format!(
            "**on:** [{}](https://zkillboard.com/location/{}/){suffix}",
            on.name, on.id
        ));
    }
    if let Some(range) = location
        .range
        .filter(|range| range.light_years.is_finite() && range.light_years > 0.0)
    {
        lines.push(format!(
            "**range:** {:.1} LY from {} ([Supers](https://evemaps.dotlan.net/jump/Nyx,555/{}:{})|[FAX](https://evemaps.dotlan.net/jump/Lif,555/{}:{})|[Blops](https://evemaps.dotlan.net/jump/Sin,555/{}:{}))",
            range.light_years,
            range.reference_system,
            range.reference_system,
            range.destination_system,
            range.reference_system,
            range.destination_system,
            range.reference_system,
            range.destination_system,
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        compact_location_description, CompactLocation, LocationOn, LocationRange, LocationRegion,
        LocationSystem,
    };

    #[test]
    fn compact_location_description_preserves_the_killfeed_in_on_range_grammar() {
        assert_eq!(
            compact_location_description(CompactLocation {
                system: Some(LocationSystem {
                    name: "Jita",
                    id: 30_000_142,
                }),
                region: Some(LocationRegion {
                    name: "The Forge",
                    id: 10_000_002,
                }),
                on: Some(LocationOn {
                    name: "Jita IV - Moon 4",
                    id: 60_003_760,
                    suffix: Some("12.0 km away"),
                }),
                range: Some(LocationRange {
                    light_years: 8.0,
                    reference_system: "Turnur",
                    destination_system: "Jita",
                }),
            }),
            "**in:** [Jita](http://evemaps.dotlan.net/system/30000142) ([The Forge](http://evemaps.dotlan.net/region/10000002))\n**on:** [Jita IV - Moon 4](https://zkillboard.com/location/60003760/), 12.0 km away\n**range:** 8.0 LY from Turnur ([Supers](https://evemaps.dotlan.net/jump/Nyx,555/Turnur:Jita)|[FAX](https://evemaps.dotlan.net/jump/Lif,555/Turnur:Jita)|[Blops](https://evemaps.dotlan.net/jump/Sin,555/Turnur:Jita))"
        );
    }
}
