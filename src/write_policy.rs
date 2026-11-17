use serde::{
    de::{value::MapAccessDeserializer, Error, MapAccess, Visitor},
    Deserialize, Deserializer,
};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

#[derive(Default)]
pub struct WritePolicy {
    guilds: HashMap<Id, GuildGrants>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyLoadError {
    InvalidJson,
    InvalidSchema,
    MissingFile,
    UnreadableFile,
    InvalidPath,
    SymlinkPath,
    WritableStatePath,
}

impl fmt::Display for PolicyLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidJson => "invalid_json",
            Self::InvalidSchema => "invalid_schema",
            Self::MissingFile => "missing_file",
            Self::UnreadableFile => "unreadable_file",
            Self::InvalidPath => "invalid_path",
            Self::SymlinkPath => "symlink_path",
            Self::WritableStatePath => "writable_state_path",
        })
    }
}

impl std::error::Error for PolicyLoadError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    version: u8,
    #[serde(default, deserialize_with = "deserialize_guilds")]
    guilds: HashMap<Id, GuildGrants>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct GuildGrants {
    #[serde(default)]
    allowed_user_ids: Vec<Id>,
    #[serde(default)]
    allowed_role_ids: Vec<Id>,
}

#[derive(Eq, Hash, PartialEq)]
struct Id(u64);

struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = T;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                T::deserialize(MapAccessDeserializer::new(map))
            }
        }

        deserializer
            .deserialize_map(ObjectVisitor(PhantomData))
            .map(Self)
    }
}

fn deserialize_guilds<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<HashMap<Id, GuildGrants>, D::Error> {
    struct GuildsVisitor;

    impl<'de> Visitor<'de> for GuildsVisitor {
        type Value = HashMap<Id, GuildGrants>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a guild map")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut guilds = HashMap::new();
            while let Some((guild, grants)) = map.next_entry::<Id, Object<GuildGrants>>()? {
                if guilds.insert(guild, grants.0).is_some() {
                    return Err(M::Error::custom("duplicate guild"));
                }
            }
            Ok(guilds)
        }
    }

    deserializer.deserialize_map(GuildsVisitor)
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.starts_with('0')
            || value.is_empty()
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(D::Error::custom("invalid ID"));
        }
        value
            .parse()
            .map(Self)
            .map_err(|_| D::Error::custom("invalid ID"))
    }
}

impl WritePolicy {
    pub fn load(path: &Path, state_dir: &Path) -> Result<Self, PolicyLoadError> {
        let path = absolute(path)?;
        reject_symlinks(&path)?;
        let path = normalize(&path);
        let state_dir = absolute(state_dir)?;
        let lexical_state = normalize(&state_dir);
        let resolved_state = resolve_state_dir(&state_dir)?;
        if path.starts_with(&lexical_state) || path.starts_with(&resolved_state) {
            return Err(PolicyLoadError::WritableStatePath);
        }
        let path = fs::canonicalize(path).map_err(file_error)?;
        if path.starts_with(&lexical_state) || path.starts_with(&resolved_state) {
            return Err(PolicyLoadError::WritableStatePath);
        }
        if !fs::metadata(&path).map_err(file_error)?.is_file() {
            return Err(PolicyLoadError::UnreadableFile);
        }
        let bytes = fs::read(path).map_err(file_error)?;
        let json = std::str::from_utf8(&bytes).map_err(|_| PolicyLoadError::InvalidJson)?;
        Self::from_json(json)
    }

    pub fn from_json(json: &str) -> Result<Self, PolicyLoadError> {
        let Object(document): Object<PolicyDocument> =
            serde_json::from_str(json).map_err(|error| {
                if error.is_data() {
                    PolicyLoadError::InvalidSchema
                } else {
                    PolicyLoadError::InvalidJson
                }
            })?;
        if document.version != 1
            || document
                .guilds
                .iter()
                .any(|(guild, grants)| grants.allowed_role_ids.contains(guild))
        {
            return Err(PolicyLoadError::InvalidSchema);
        }
        Ok(Self {
            guilds: document.guilds,
        })
    }

    pub fn allows(&self, guild_id: u64, user_id: u64, role_ids: &[u64]) -> bool {
        self.guilds.get(&Id(guild_id)).is_some_and(|grants| {
            grants.allowed_user_ids.contains(&Id(user_id))
                || role_ids
                    .iter()
                    .any(|role| grants.allowed_role_ids.contains(&Id(*role)))
        })
    }
}

fn absolute(path: &Path) -> Result<PathBuf, PolicyLoadError> {
    if path.as_os_str().is_empty() {
        return Err(PolicyLoadError::InvalidPath);
    }
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|_| PolicyLoadError::InvalidPath)
    }
}

fn reject_symlinks(path: &Path) -> Result<(), PolicyLoadError> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                prefix.pop();
            }
            Component::CurDir => {}
            component => {
                prefix.push(component.as_os_str());
                match fs::symlink_metadata(&prefix) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(PolicyLoadError::SymlinkPath)
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(file_error(error)),
                }
            }
        }
    }
    Ok(())
}

fn resolve_state_dir(path: &Path) -> Result<PathBuf, PolicyLoadError> {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            component => {
                resolved.push(component.as_os_str());
                match fs::canonicalize(&resolved) {
                    Ok(canonical) => resolved = canonical,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        match fs::symlink_metadata(&resolved) {
                            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                            _ => return Err(PolicyLoadError::InvalidPath),
                        }
                    }
                    Err(_) => return Err(PolicyLoadError::InvalidPath),
                }
            }
        }
    }
    match fs::metadata(&resolved) {
        Ok(metadata) if metadata.is_dir() => Ok(resolved),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(resolved),
        _ => Err(PolicyLoadError::InvalidPath),
    }
}

fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn file_error(error: io::Error) -> PolicyLoadError {
    match error.kind() {
        io::ErrorKind::NotFound => PolicyLoadError::MissingFile,
        io::ErrorKind::InvalidInput => PolicyLoadError::InvalidPath,
        _ => PolicyLoadError::UnreadableFile,
    }
}
