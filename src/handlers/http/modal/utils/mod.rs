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

use std::{future::Future, pin::Pin};

use actix_web::{
    dev::Payload,
    error::{ErrorPayloadTooLarge, JsonPayloadError},
    FromRequest, HttpRequest,
};
use bytes::BytesMut;
use futures::StreamExt;
use serde::de::DeserializeOwned;

use crate::handlers::http::MAX_EVENT_PAYLOAD_SIZE;

pub mod ingest_utils;
pub mod logstream_utils;
pub mod rbac_utils;

pub struct JsonWithSize<T> {
    pub json: T,
    pub byte_size: usize,
}

impl<T: DeserializeOwned + 'static> FromRequest for JsonWithSize<T> {
    type Error = actix_web::error::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(_: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let limit = MAX_EVENT_PAYLOAD_SIZE;

        // Take ownership of payload for async processing
        let mut payload = payload.take();

        Box::pin(async move {
            // Buffer to collect all bytes
            let mut body = BytesMut::new();
            let mut byte_size = 0;

            // Collect all bytes from the payload stream
            while let Some(chunk) = payload.next().await {
                let chunk = chunk?;
                byte_size += chunk.len();

                // Check the size limit
                if byte_size > limit {
                    return Err(ErrorPayloadTooLarge(byte_size));
                }

                // Extend our buffer with the chunk
                body.extend_from_slice(&chunk);
            }

            // Convert the collected bytes to Bytes
            let bytes = body.freeze();

            // Deserialize the JSON payload
            let json =
                serde_json::from_slice::<T>(&bytes).map_err(JsonPayloadError::Deserialize)?;

            Ok(JsonWithSize { json, byte_size })
        })
    }
}
