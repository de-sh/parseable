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

pub mod prom_utils;
pub mod storage;

use crate::{handlers::http::metrics_path, stats::FullStats};
use actix_web::Responder;
use actix_web_prometheus::{PrometheusMetrics, PrometheusMetricsBuilder};
use error::MetricsError;
use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};

pub const METRICS_NAMESPACE: &str = env!("CARGO_PKG_NAME");

pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::new);

pub struct Metrics {
    pub events_ingested: IntGaugeVec,
    pub events_ingested_size: IntGaugeVec,
    pub storage_size: IntGaugeVec,
    pub events_deleted: IntGaugeVec,
    pub events_deleted_size: IntGaugeVec,
    pub deleted_events_storage_size: IntGaugeVec,
    pub lifetime_events_ingested: IntGaugeVec,
    pub lifetime_events_ingested_size: IntGaugeVec,
    pub lifetime_events_storage_size: IntGaugeVec,
    pub events_ingested_date: IntGaugeVec,
    pub events_ingested_size_date: IntGaugeVec,
    pub events_storage_size_date: IntGaugeVec,
    pub staging_files: IntGaugeVec,
    pub query_execute_time: HistogramVec,
    pub query_cache_hit: IntCounterVec,
    pub alerts_states: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            events_ingested: IntGaugeVec::new(
                Opts::new("events_ingested", "Events ingested").namespace(METRICS_NAMESPACE),
                &["stream", "format"],
            )
            .expect("metric can be created"),
            events_ingested_size: IntGaugeVec::new(
                Opts::new("events_ingested_size", "Events ingested size bytes")
                    .namespace(METRICS_NAMESPACE),
                &["stream", "format"],
            )
            .expect("metric can be created"),
            storage_size: IntGaugeVec::new(
                Opts::new("storage_size", "Storage size bytes").namespace(METRICS_NAMESPACE),
                &["type", "stream", "format"],
            )
            .expect("metric can be created"),
            events_deleted: IntGaugeVec::new(
                Opts::new("events_deleted", "Events deleted").namespace(METRICS_NAMESPACE),
                &["stream", "format"],
            )
            .expect("metric can be created"),
            events_deleted_size: IntGaugeVec::new(
                Opts::new("events_deleted_size", "Events deleted size bytes")
                    .namespace(METRICS_NAMESPACE),
                &["stream", "format"],
            )
            .expect("metric can be created"),
            deleted_events_storage_size: IntGaugeVec::new(
                Opts::new(
                    "deleted_events_storage_size",
                    "Deleted events storage size bytes",
                )
                .namespace(METRICS_NAMESPACE),
                &["type", "stream", "format"],
            )
            .expect("metric can be created"),
            lifetime_events_ingested: IntGaugeVec::new(
                Opts::new("lifetime_events_ingested", "Lifetime events ingested")
                    .namespace(METRICS_NAMESPACE),
                &["stream", "format"],
            )
            .expect("metric can be created"),
            lifetime_events_ingested_size: IntGaugeVec::new(
                Opts::new(
                    "lifetime_events_ingested_size",
                    "Lifetime events ingested size bytes",
                )
                .namespace(METRICS_NAMESPACE),
                &["stream", "format"],
            )
            .expect("metric can be created"),
            lifetime_events_storage_size: IntGaugeVec::new(
                Opts::new(
                    "lifetime_events_storage_size",
                    "Lifetime events storage size bytes",
                )
                .namespace(METRICS_NAMESPACE),
                &["type", "stream", "format"],
            )
            .expect("metric can be created"),
            events_ingested_date: IntGaugeVec::new(
                Opts::new(
                    "events_ingested_date",
                    "Events ingested on a particular date",
                )
                .namespace(METRICS_NAMESPACE),
                &["stream", "format", "date"],
            )
            .expect("metric can be created"),
            events_ingested_size_date: IntGaugeVec::new(
                Opts::new(
                    "events_ingested_size_date",
                    "Events ingested size in bytes on a particular date",
                )
                .namespace(METRICS_NAMESPACE),
                &["stream", "format", "date"],
            )
            .expect("metric can be created"),
            events_storage_size_date: IntGaugeVec::new(
                Opts::new(
                    "events_storage_size_date",
                    "Events storage size in bytes on a particular date",
                )
                .namespace(METRICS_NAMESPACE),
                &["type", "stream", "format", "date"],
            )
            .expect("metric can be created"),
            staging_files: IntGaugeVec::new(
                Opts::new("staging_files", "Active Staging files").namespace(METRICS_NAMESPACE),
                &["stream"],
            )
            .expect("metric can be created"),
            query_execute_time: HistogramVec::new(
                HistogramOpts::new("query_execute_time", "Query execute time")
                    .namespace(METRICS_NAMESPACE),
                &["stream"],
            )
            .expect("metric can be created"),
            query_cache_hit: IntCounterVec::new(
                Opts::new("QUERY_CACHE_HIT", "Full Cache hit").namespace(METRICS_NAMESPACE),
                &["stream"],
            )
            .expect("metric can be created"),
            alerts_states: IntCounterVec::new(
                Opts::new("alerts_states", "Alerts States").namespace(METRICS_NAMESPACE),
                &["stream", "name", "state"],
            )
            .expect("metric can be created"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.events_ingested.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.events_ingested_size.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.storage_size.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.events_deleted.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.events_deleted_size.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.deleted_events_storage_size.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.lifetime_events_ingested.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.lifetime_events_ingested_size.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.lifetime_events_storage_size.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.events_ingested_date.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.events_ingested_size_date.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.events_storage_size_date.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.staging_files.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.query_execute_time.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.query_cache_hit.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(self.alerts_states.clone()))
            .expect("metric can be registered");
    }

    pub async fn fetch_from_storage(&self, stream_name: &str, stats: FullStats) {
        self.events_ingested
            .with_label_values(&[stream_name, "json"])
            .set(stats.current_stats.events as i64);
        self.events_ingested_size
            .with_label_values(&[stream_name, "json"])
            .set(stats.current_stats.ingestion as i64);
        self.storage_size
            .with_label_values(&["data", stream_name, "parquet"])
            .set(stats.current_stats.storage as i64);
        self.events_deleted
            .with_label_values(&[stream_name, "json"])
            .set(stats.deleted_stats.events as i64);
        self.events_deleted_size
            .with_label_values(&[stream_name, "json"])
            .set(stats.deleted_stats.ingestion as i64);
        self.deleted_events_storage_size
            .with_label_values(&["data", stream_name, "parquet"])
            .set(stats.deleted_stats.storage as i64);

        self.lifetime_events_ingested
            .with_label_values(&[stream_name, "json"])
            .set(stats.lifetime_stats.events as i64);
        self.lifetime_events_ingested_size
            .with_label_values(&[stream_name, "json"])
            .set(stats.lifetime_stats.ingestion as i64);
        self.lifetime_events_storage_size
            .with_label_values(&["data", stream_name, "parquet"])
            .set(stats.lifetime_stats.storage as i64);
    }
}

pub fn build_metrics_handler() -> PrometheusMetrics {
    let registry = prometheus::Registry::new();
    METRICS.register(&registry);

    let prometheus = PrometheusMetricsBuilder::new(METRICS_NAMESPACE)
        .registry(registry)
        .endpoint(metrics_path().as_str())
        .build()
        .expect("Prometheus initialization");

    prom_process_metrics(&prometheus);
    prometheus
}

#[cfg(target_os = "linux")]
fn prom_process_metrics(metrics: &PrometheusMetrics) {
    use prometheus::process_collector::ProcessCollector;
    metrics
        .registry
        .register(Box::new(ProcessCollector::for_self()))
        .expect("metric can be registered");
}

#[cfg(not(target_os = "linux"))]
fn prom_process_metrics(_metrics: &PrometheusMetrics) {}

use actix_web::HttpResponse;

pub async fn get() -> Result<impl Responder, MetricsError> {
    Ok(HttpResponse::Ok().body(format!("{:?}", build_metrics_handler())))
}

pub mod error {

    use actix_web::http::header::ContentType;
    use http::StatusCode;

    #[derive(Debug, thiserror::Error)]
    pub enum MetricsError {
        #[error("{0}")]
        Custom(String, StatusCode),
    }

    impl actix_web::ResponseError for MetricsError {
        fn status_code(&self) -> http::StatusCode {
            match self {
                Self::Custom(_, _) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }

        fn error_response(&self) -> actix_web::HttpResponse<actix_web::body::BoxBody> {
            actix_web::HttpResponse::build(self.status_code())
                .insert_header(ContentType::plaintext())
                .body(self.to_string())
        }
    }
}
