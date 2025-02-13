use tracing::warn;

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
use crate::metrics::METRICS;
use crate::storage::{ObjectStorage, ObjectStorageError, ObjectStoreFormat};
use std::sync::Arc;

/// Helper struct type created by copying stats values from metadata
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub events: u64,
    pub ingestion: u64,
    pub storage: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct FullStats {
    pub lifetime_stats: Stats,
    pub current_stats: Stats,
    pub deleted_stats: Stats,
}

pub fn get_current_stats(stream_name: &str, format: &'static str) -> Option<FullStats> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    let events_ingested = METRICS
        .events_ingested
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let ingestion_size = METRICS
        .events_ingested_size
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let storage_size = METRICS
        .storage_size
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get() as u64;
    let events_deleted = METRICS
        .events_deleted
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let events_deleted_size = METRICS
        .events_deleted_size
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let deleted_events_storage_size = METRICS
        .deleted_events_storage_size
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get() as u64;
    let lifetime_events_ingested = METRICS
        .lifetime_events_ingested
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let lifetime_ingestion_size = METRICS
        .lifetime_events_ingested_size
        .get_metric_with_label_values(&event_labels)
        .ok()?
        .get() as u64;
    let lifetime_events_storage_size = METRICS
        .lifetime_events_storage_size
        .get_metric_with_label_values(&storage_size_labels)
        .ok()?
        .get() as u64;

    Some(FullStats {
        lifetime_stats: Stats {
            events: lifetime_events_ingested,
            ingestion: lifetime_ingestion_size,
            storage: lifetime_events_storage_size,
        },
        current_stats: Stats {
            events: events_ingested,
            ingestion: ingestion_size,
            storage: storage_size,
        },
        deleted_stats: Stats {
            events: events_deleted,
            ingestion: events_deleted_size,
            storage: deleted_events_storage_size,
        },
    })
}

pub async fn update_deleted_stats(
    storage: Arc<dyn ObjectStorage>,
    stream_name: &str,
    meta: ObjectStoreFormat,
    dates: Vec<String>,
) -> Result<(), ObjectStorageError> {
    let mut num_row: i64 = 0;
    let mut storage_size: i64 = 0;
    let mut ingestion_size: i64 = 0;

    let mut manifests = meta.snapshot.manifest_list;
    manifests.retain(|item| dates.iter().any(|date| item.manifest_path.contains(date)));
    if !manifests.is_empty() {
        for manifest in manifests {
            let manifest_date = manifest.time_lower_bound.date_naive().to_string();
            let _ = METRICS.events_ingested_date.remove_label_values(&[
                stream_name,
                "json",
                &manifest_date,
            ]);
            let _ = METRICS.events_ingested_size_date.remove_label_values(&[
                stream_name,
                "json",
                &manifest_date,
            ]);
            let _ = METRICS.events_storage_size_date.remove_label_values(&[
                "data",
                stream_name,
                "parquet",
                &manifest_date,
            ]);
            num_row += manifest.events_ingested as i64;
            ingestion_size += manifest.ingestion_size as i64;
            storage_size += manifest.storage_size as i64;
        }
    }
    METRICS
        .events_deleted
        .with_label_values(&[stream_name, "json"])
        .add(num_row);
    METRICS
        .events_deleted_size
        .with_label_values(&[stream_name, "json"])
        .add(ingestion_size);
    METRICS
        .deleted_events_storage_size
        .with_label_values(&["data", stream_name, "parquet"])
        .add(storage_size);
    METRICS
        .events_ingested
        .with_label_values(&[stream_name, "json"])
        .sub(num_row);
    METRICS
        .events_ingested_size
        .with_label_values(&[stream_name, "json"])
        .sub(ingestion_size);
    METRICS
        .storage_size
        .with_label_values(&["data", stream_name, "parquet"])
        .sub(storage_size);
    let stats = get_current_stats(stream_name, "json");
    if let Some(stats) = stats {
        if let Err(e) = storage.put_stats(stream_name, &stats).await {
            warn!("Error updating stats to objectstore due to error [{}]", e);
        }
    }

    Ok(())
}

pub fn delete_stats(stream_name: &str, format: &'static str) -> prometheus::Result<()> {
    let event_labels = event_labels(stream_name, format);
    let storage_size_labels = storage_size_labels(stream_name);

    METRICS.events_ingested.remove_label_values(&event_labels)?;
    METRICS
        .events_ingested_size
        .remove_label_values(&event_labels)?;
    METRICS
        .storage_size
        .remove_label_values(&storage_size_labels)?;
    METRICS.events_deleted.remove_label_values(&event_labels)?;
    METRICS
        .events_deleted_size
        .remove_label_values(&event_labels)?;
    METRICS
        .deleted_events_storage_size
        .remove_label_values(&storage_size_labels)?;
    METRICS
        .lifetime_events_ingested
        .remove_label_values(&event_labels)?;
    METRICS
        .lifetime_events_ingested_size
        .remove_label_values(&event_labels)?;
    METRICS
        .lifetime_events_storage_size
        .remove_label_values(&storage_size_labels)?;

    METRICS
        .events_ingested_date
        .remove_label_values(&event_labels)?;
    METRICS
        .events_ingested_size_date
        .remove_label_values(&event_labels)?;
    METRICS
        .events_storage_size_date
        .remove_label_values(&storage_size_labels)?;

    Ok(())
}

pub fn event_labels<'a>(stream_name: &'a str, format: &'static str) -> [&'a str; 2] {
    [stream_name, format]
}

pub fn storage_size_labels(stream_name: &str) -> [&str; 3] {
    ["data", stream_name, "parquet"]
}

pub fn event_labels_date<'a>(
    stream_name: &'a str,
    format: &'static str,
    date: &'a str,
) -> [&'a str; 3] {
    [stream_name, format, date]
}

pub fn storage_size_labels_date<'a>(stream_name: &'a str, date: &'a str) -> [&'a str; 4] {
    ["data", stream_name, "parquet", date]
}
