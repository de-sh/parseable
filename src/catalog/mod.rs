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

use chrono::NaiveDate;
use column::Column;
use relative_path::RelativePathBuf;
use snapshot::ManifestItem;

use crate::query::PartialTimeFilter;

pub mod column;
pub mod manifest;
pub mod snapshot;

pub trait Snapshot {
    fn manifests(&self, time_predicates: &[PartialTimeFilter]) -> Vec<ManifestItem>;
}

pub trait ManifestFile {
    #[allow(unused)]
    fn file_name(&self) -> &str;
    #[allow(unused)]
    fn ingestion_size(&self) -> u64;
    #[allow(unused)]
    fn file_size(&self) -> u64;
    fn num_rows(&self) -> u64;
    fn columns(&self) -> &[Column];
}

impl ManifestFile for manifest::File {
    fn file_name(&self) -> &str {
        &self.file_path
    }

    fn ingestion_size(&self) -> u64 {
        self.ingestion_size
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }
}

/// Partition the path to which this manifest belongs.
/// Useful when uploading the manifest file.
#[inline(always)]
pub fn partition_path(
    stream: &str,
    lower_bound: NaiveDate,
    upper_bound: NaiveDate,
) -> RelativePathBuf {
    if lower_bound == upper_bound {
        RelativePathBuf::from_iter([stream, &format!("date={}", lower_bound.format("%Y-%m-%d"))])
    } else {
        RelativePathBuf::from_iter([
            stream,
            &format!(
                "date={}:{}",
                lower_bound.format("%Y-%m-%d"),
                upper_bound.format("%Y-%m-%d")
            ),
        ])
    }
}
