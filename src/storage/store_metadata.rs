/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::{
    collections::HashMap,
    fs::{self, create_dir_all, OpenOptions},
    path::PathBuf,
};

use bytes::Bytes;
use once_cell::sync::OnceCell;
use relative_path::RelativePathBuf;
use std::io;

use crate::{
    option::Mode,
    parseable::{JOIN_COMMUNITY, PARSEABLE},
    rbac::{role::model::DefaultPrivilege, user::User},
    storage::ObjectStorageError,
    utils::uid,
};

use super::PARSEABLE_METADATA_FILE_NAME;

// Expose some static variables for internal usage
pub static STORAGE_METADATA: OnceCell<StaticStorageMetadata> = OnceCell::new();
pub const CURRENT_STORAGE_METADATA_VERSION: &str = "v4";
// For use in global static
#[derive(Debug, PartialEq, Eq)]
pub struct StaticStorageMetadata {
    pub mode: String,
    pub deployment_id: uid::Uid,
}

// Type for serialization and deserialization
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageMetadata {
    pub version: String,
    pub mode: String,
    pub staging: PathBuf,
    pub storage: String,
    #[serde(default = "crate::utils::uid::gen")]
    pub deployment_id: uid::Uid,
    pub users: Vec<User>,
    pub streams: Vec<String>,
    pub server_mode: Mode,
    #[serde(default)]
    pub roles: HashMap<String, Vec<DefaultPrivilege>>,
    #[serde(default)]
    pub default_role: Option<String>,
}

impl Default for StorageMetadata {
    fn default() -> Self {
        Self {
            version: CURRENT_STORAGE_METADATA_VERSION.to_string(),
            mode: PARSEABLE.storage.name().to_owned(),
            staging: PARSEABLE.options.staging_dir().to_path_buf(),
            storage: PARSEABLE.storage.get_endpoint(),
            deployment_id: uid::gen(),
            server_mode: PARSEABLE.options.mode,
            users: Vec::new(),
            streams: Vec::new(),
            roles: HashMap::default(),
            default_role: None,
        }
    }
}

impl StorageMetadata {
    pub fn global() -> &'static StaticStorageMetadata {
        STORAGE_METADATA
            .get()
            .expect("gloabal static is initialized")
    }

    pub fn set_global(self) {
        let metadata = StaticStorageMetadata {
            mode: self.mode,
            deployment_id: self.deployment_id,
        };

        STORAGE_METADATA.set(metadata).expect("only set once")
    }
}

/// deals with the staging directory creation and metadata resolution
/// always returns remote metadata as it is source of truth
/// overwrites staging metadata while updating storage info
pub async fn resolve_parseable_metadata(
    parseable_metadata: &Option<Bytes>,
) -> Result<StorageMetadata, ObjectStorageError> {
    let staging_metadata = get_staging_metadata()?;
    let remote_metadata = parseable_metadata
        .as_ref()
        .map(|meta| serde_json::from_slice(meta).expect("parseable config is valid json"));

    // Env Change needs to be updated
    let check = determine_environment(staging_metadata, remote_metadata);
    // flags for if metadata needs to be synced
    let mut overwrite_staging = false;
    let mut overwrite_remote = false;

    let res = match check {
        EnvChange::None(metadata) => {
            // overwrite staging anyways so that it matches remote in case of any divergence
            overwrite_staging = true;
            if PARSEABLE.options.mode ==  Mode::All {
                metadata.server_mode.standalone_after_distributed()?;
            }
            Ok(metadata)
        },
        EnvChange::NewRemote => {
            Err("Could not start the server because staging directory indicates stale data from previous deployment, please choose an empty staging directory and restart the server")
        }
        EnvChange::NewStaging(mut metadata) => {
            // if server is started in ingest mode,we need to make sure that query mode has been started
            // i.e the metadata is updated to reflect the server mode = Query
            if metadata.server_mode== Mode::All && PARSEABLE.options.mode == Mode::Ingest {
                Err("Starting Ingest Mode is not allowed, Since Query Server has not been started yet")
            } else {
                create_dir_all(PARSEABLE.options.staging_dir())?;
                metadata.staging = PARSEABLE.options.staging_dir().canonicalize()?;
                // this flag is set to true so that metadata is copied to staging
                overwrite_staging = true;
                // overwrite remote in all and query mode
                // because staging dir has changed.
                match PARSEABLE.options.mode {
                    Mode::All => {
                        metadata.server_mode.standalone_after_distributed()
                            .map_err(|err| {
                                ObjectStorageError::Custom(err.to_string())
                            })?;
                            overwrite_remote = true;
                    },
                    Mode::Query => {
                        overwrite_remote = true;
                        metadata.server_mode = PARSEABLE.options.mode;
                        metadata.staging = PARSEABLE.options.staging_dir().to_path_buf();
                    },
                    Mode::Ingest => {
                        // if ingest server is started fetch the metadata from remote
                        // update the server mode for local metadata
                        metadata.server_mode = PARSEABLE.options.mode;
                        metadata.staging = PARSEABLE.options.staging_dir().to_path_buf();
                      },
                }
                Ok(metadata)
            }
        }
        EnvChange::CreateBoth => {
            create_dir_all(PARSEABLE.options.staging_dir())?;
            let metadata = StorageMetadata::default();
            // new metadata needs to be set
            // if mode is query or all then both staging and remote
            match PARSEABLE.options.mode {
                Mode::All | Mode::Query => overwrite_remote = true,
                _ => (),
            }
            // else only staging
            overwrite_staging = true;
            Ok(metadata)
        }
    };

    let mut metadata = res.map_err(|err| {
        let err = format!("{}. {}", err, JOIN_COMMUNITY);
        let err: Box<dyn std::error::Error + Send + Sync + 'static> = err.into();
        ObjectStorageError::UnhandledError(err)
    })?;

    metadata.server_mode = PARSEABLE.options.mode;
    if overwrite_remote {
        put_remote_metadata(&metadata).await?;
    }

    if overwrite_staging {
        put_staging_metadata(&metadata)?;
    }

    Ok(metadata)
}

fn determine_environment(
    staging_metadata: Option<StorageMetadata>,
    remote_metadata: Option<StorageMetadata>,
) -> EnvChange {
    match (staging_metadata, remote_metadata) {
        (Some(staging), Some(remote)) => {
            // if both staging and remote have same deployment id but different server modes
            if staging.deployment_id == remote.deployment_id
                && remote.server_mode == Mode::All
                && matches!(PARSEABLE.options.mode, Mode::Query | Mode::Ingest)
            {
                EnvChange::NewStaging(remote)
            } else if staging.deployment_id != remote.deployment_id {
                // if deployment id is different
                EnvChange::NewRemote
            } else {
                // if deployment id is same
                EnvChange::None(remote)
            }
        }
        (None, Some(remote)) => EnvChange::NewStaging(remote),
        (Some(_), None) => EnvChange::NewRemote,
        (None, None) => EnvChange::CreateBoth,
    }
}

// variant contain remote metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvChange {
    /// No change in env i.e both staging and remote have same id
    /// or deployment id of staging is not matching with that of remote
    None(StorageMetadata),
    /// Metadata not found in storage. Treated as possible misconfiguration on user side.
    NewRemote,
    /// If a new staging is found then we just copy remote metadata to this staging.
    NewStaging(StorageMetadata),
    /// Fresh remote and staging, hence create a new metadata file on both
    CreateBoth,
}

pub fn get_staging_metadata() -> io::Result<Option<StorageMetadata>> {
    let path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME)
        .to_path(PARSEABLE.options.staging_dir());
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => match err.kind() {
            io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(err),
        },
    };

    let meta: StorageMetadata = serde_json::from_slice(&bytes).unwrap();

    Ok(Some(meta))
}

pub async fn put_remote_metadata(metadata: &StorageMetadata) -> Result<(), ObjectStorageError> {
    let client = PARSEABLE.storage.get_object_store();
    client.put_metadata(metadata).await
}

pub fn put_staging_metadata(meta: &StorageMetadata) -> io::Result<()> {
    let mut staging_metadata = meta.clone();
    staging_metadata.server_mode = PARSEABLE.options.mode;
    staging_metadata.staging = PARSEABLE.options.staging_dir().to_path_buf();
    let path = PARSEABLE
        .options
        .staging_dir()
        .join(PARSEABLE_METADATA_FILE_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer(&mut file, &staging_metadata)?;
    Ok(())
}
