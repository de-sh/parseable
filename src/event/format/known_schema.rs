/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};
use tracing::{error, warn};

/// Predefined JSON with known textual logging formats
const FORMATS_JSON: &str = include_str!("../../../resources/formats.json");

/// Global instance of EventProcessor containing predefined schema definitions
pub static KNOWN_SCHEMA_LIST: Lazy<EventProcessor> =
    Lazy::new(|| EventProcessor::new(FORMATS_JSON));

#[derive(Debug, thiserror::Error)]
#[error("Unacceptable text/JSON for known log format")]
pub struct Unacceptable;

/// Defines a schema for extracting structured data from logs using regular expressions
#[derive(Debug)]
pub struct SchemaDefinition {
    /// Regular expression pattern used to match and capture fields from log strings
    pattern: Option<Regex>,
    // Maps field names to regex capture groups
    field_mappings: Vec<String>,
}

impl SchemaDefinition {
    /// Extracts structured data from a log event string using a defined regex pattern
    ///
    /// This function checks if the given object already contains all expected fields
    /// or attempts to extract them from a log event string if a pattern is available.
    ///
    /// # Arguments
    /// * `obj` - The JSON object to check or extract fields into
    /// * `extract_log` - Optional field name containing the raw log text
    ///
    /// # Returns
    /// * `true` - If all expected fields are already present in the object OR if extraction was successful
    /// * `false` - If extraction failed or no pattern was available and fields were missing
    pub fn check_or_extract(
        &self,
        obj: &mut Map<String, Value>,
        extract_log: Option<&str>,
    ) -> bool {
        if self
            .field_mappings
            .iter()
            .all(|field| obj.contains_key(field))
        {
            return true;
        }

        let Some(pattern) = self.pattern.as_ref() else {
            return false;
        };

        let Some(event) = extract_log
            .and_then(|field| obj.get(field))
            .and_then(|s| s.as_str())
        else {
            return false;
        };

        let Some(captures) = pattern.captures(event) else {
            return false;
        };
        let mut extracted_fields = Map::new();

        // With named capture groups, you can iterate over the field names
        for field_name in self.field_mappings.iter() {
            if let Some(value) = captures.name(field_name) {
                extracted_fields.insert(
                    field_name.to_owned(),
                    Value::String(value.as_str().to_string()),
                );
            }
        }

        obj.extend(extracted_fields);

        true
    }
}

/// Configuration structure loaded from JSON for defining log formats
#[derive(Debug, Deserialize)]
struct Format {
    name: String,
    regex: Vec<Pattern>,
}

/// Configuration for a single pattern within a log format
#[derive(Debug, Deserialize)]
struct Pattern {
    pattern: Option<String>,
    fields: Vec<String>,
}

/// Manages a collection of schema definitions for various log formats
#[derive(Debug)]
pub struct EventProcessor {
    /// Map of format names to their corresponding schema definitions
    pub schema_definitions: HashMap<String, SchemaDefinition>,
}

impl EventProcessor {
    /// Parses given formats from JSON text and stores them in-memory
    fn new(json_text: &str) -> Self {
        let mut processor = EventProcessor {
            schema_definitions: HashMap::new(),
        };

        let formats: Vec<Format> =
            serde_json::from_str(json_text).expect("Known formats are stored as JSON text");

        for format in formats {
            for regex in &format.regex {
                // Compile the regex pattern if present
                // NOTE: we only warn if the pattern doesn't compile
                let pattern = regex.pattern.as_ref().and_then(|pattern| {
                    Regex::new(pattern)
                        .inspect_err(|err| {
                            error!("Error compiling regex pattern: {err}; Pattern: {pattern}")
                        })
                        .ok()
                });

                processor.schema_definitions.insert(
                    format.name.clone(),
                    SchemaDefinition {
                        pattern,
                        field_mappings: regex.fields.clone(),
                    },
                );
            }
        }

        processor
    }

    /// Extracts fields from logs embedded within a JSON string
    ///
    /// # Arguments
    /// * `json` - JSON value containing log entries
    /// * `log_source` - Name of the log format to use for extraction
    /// * `extract_log` - Optional field name containing the raw log text
    ///
    /// # Returns
    /// * `Ok` - The original JSON will now contain extracted fields
    /// * `Err(Unacceptable)` - JSON provided is acceptable for the known format
    pub fn extract_from_inline_log(
        &self,
        json: &mut Value,
        log_source: &str,
        extract_log: Option<&str>,
    ) -> Result<(), Unacceptable> {
        let Some(schema) = self.schema_definitions.get(log_source) else {
            warn!("Unknown log format: {log_source}");
            return Ok(());
        };

        match json {
            Value::Array(list) => {
                for event in list {
                    let Value::Object(event) = event else {
                        continue;
                    };
                    if !schema.check_or_extract(event, extract_log) {
                        return Err(Unacceptable);
                    }
                }
            }
            Value::Object(event) => {
                if !schema.check_or_extract(event, extract_log) {
                    return Err(Unacceptable);
                }
            }
            _ => unreachable!("We don't accept events of the form: {json}"),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const TEST_CONFIG: &str = r#"
    [
        {
            "name": "apache_access",
            "regex": [
                {
                    "pattern": "^(?P<ip>[\\d.]+) - - \\[(?P<timestamp>[^\\]]+)\\] \"(?P<method>\\w+) (?P<path>[^\\s]+) HTTP/[\\d.]+\" (?P<status>\\d+) (?P<bytes>\\d+)",
                    "fields": ["ip", "timestamp", "method", "path", "status", "bytes"]
                }
            ]
        },
        {
            "name": "custom_app_log",
            "regex": [
                {
                    "pattern": "\\[(?P<level>\\w+)\\] \\[(?P<timestamp>[^\\]]+)\\] (?P<message>.*)",
                    "fields": ["level", "timestamp", "message"]
                }
            ]
        }
    ]
    "#;

    #[test]
    fn test_apache_log_extraction() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let schema = processor.schema_definitions.get("apache_access").unwrap();

        // Create a mutable object for check_or_extract to modify
        let mut obj = Map::new();
        let log_field = "raw_log";
        obj.insert(log_field.to_string(), Value::String(
            "192.168.1.1 - - [10/Oct/2023:13:55:36 +0000] \"GET /index.html HTTP/1.1\" 200 2326".to_string()
        ));

        // Use check_or_extract instead of extract
        let result = schema.check_or_extract(&mut obj, Some(log_field));
        assert!(result, "Failed to extract fields from valid log");

        // Verify extracted fields were added to the object
        assert_eq!(obj.get("ip").unwrap().as_str().unwrap(), "192.168.1.1");
        assert_eq!(
            obj.get("timestamp").unwrap().as_str().unwrap(),
            "10/Oct/2023:13:55:36 +0000"
        );
        assert_eq!(obj.get("method").unwrap().as_str().unwrap(), "GET");
        assert_eq!(obj.get("path").unwrap().as_str().unwrap(), "/index.html");
        assert_eq!(obj.get("status").unwrap().as_str().unwrap(), "200");
        assert_eq!(obj.get("bytes").unwrap().as_str().unwrap(), "2326");
    }

    #[test]
    fn test_custom_log_extraction() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let schema = processor.schema_definitions.get("custom_app_log").unwrap();

        // Create a mutable object for check_or_extract to modify
        let mut obj = Map::new();
        let log_field = "raw_log";
        obj.insert(
            log_field.to_string(),
            Value::String(
                "[ERROR] [2023-10-10T13:55:36Z] Failed to connect to database".to_string(),
            ),
        );

        // Use check_or_extract instead of extract
        let result = schema.check_or_extract(&mut obj, Some(log_field));
        assert!(result, "Failed to extract fields from valid log");

        // Verify extracted fields were added to the object
        assert_eq!(obj.get("level").unwrap().as_str().unwrap(), "ERROR");
        assert_eq!(
            obj.get("timestamp").unwrap().as_str().unwrap(),
            "2023-10-10T13:55:36Z"
        );
        assert_eq!(
            obj.get("message").unwrap().as_str().unwrap(),
            "Failed to connect to database"
        );
    }

    #[test]
    fn test_fields_already_exist() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let schema = processor.schema_definitions.get("custom_app_log").unwrap();

        // Create an object that already has all required fields
        let mut obj = Map::new();
        obj.insert("level".to_string(), Value::String("ERROR".to_string()));
        obj.insert(
            "timestamp".to_string(),
            Value::String("2023-10-10T13:55:36Z".to_string()),
        );
        obj.insert(
            "message".to_string(),
            Value::String("Database error".to_string()),
        );

        // check_or_extract should return true without modifying anything
        let result = schema.check_or_extract(&mut obj, None);
        assert!(result, "Should return true when fields already exist");

        // Verify the original values weren't changed
        assert_eq!(
            obj.get("message").unwrap().as_str().unwrap(),
            "Database error"
        );
    }

    #[test]
    fn test_no_match() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let schema = processor.schema_definitions.get("apache_access").unwrap();

        // Create an object with non-matching log text
        let mut obj = Map::new();
        let log_field = "raw_log";
        obj.insert(
            log_field.to_string(),
            Value::String("This is not an Apache log line".to_string()),
        );

        // check_or_extract should return false
        let result = schema.check_or_extract(&mut obj, Some(log_field));
        assert!(!result, "Should not extract fields from invalid log format");

        // Verify no fields were added
        assert!(!obj.contains_key("ip"));
        assert!(!obj.contains_key("method"));
    }

    #[test]
    fn test_no_pattern_missing_fields() {
        // Create a schema definition with no pattern
        let schema = SchemaDefinition {
            pattern: None,
            field_mappings: vec!["field1".to_string(), "field2".to_string()],
        };

        // Create an object missing the required fields
        let mut obj = Map::new();
        obj.insert(
            "other_field".to_string(),
            Value::String("value".to_string()),
        );

        // check_or_extract should return false
        let result = schema.check_or_extract(&mut obj, Some("log"));
        assert!(
            !result,
            "Should return false when no pattern and missing fields"
        );
    }

    #[test]
    fn test_extract_from_inline_log_object() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let mut json_value = json!({
            "id": "12345",
            "raw_log": "[ERROR] [2023-10-10T13:55:36Z] Failed to connect to database"
        });

        // Updated to handle check_or_extract
        let result = if let Value::Object(ref mut obj) = json_value {
            let schema = processor.schema_definitions.get("custom_app_log").unwrap();
            schema.check_or_extract(obj, Some("raw_log"));
            json_value
        } else {
            json_value
        };

        let obj = result.as_object().unwrap();
        assert!(obj.contains_key("level"));
        assert!(obj.contains_key("timestamp"));
        assert!(obj.contains_key("message"));
        assert_eq!(obj.get("level").unwrap().as_str().unwrap(), "ERROR");
    }

    #[test]
    fn test_extract_from_inline_log_array() {
        let processor = EventProcessor::new(TEST_CONFIG);

        let mut json_value = json!([
            {
                "id": "12345",
                "raw_log": "[ERROR] [2023-10-10T13:55:36Z] Failed to connect to database"
            },
            {
                "id": "12346",
                "raw_log": "[INFO] [2023-10-10T13:55:40Z] Application started"
            }
        ]);

        // Updated to handle check_or_extract for array
        if let Value::Array(ref mut array) = json_value {
            for item in array {
                if let Value::Object(ref mut obj) = item {
                    let schema = processor.schema_definitions.get("custom_app_log").unwrap();
                    schema.check_or_extract(obj, Some("raw_log"));
                }
            }
        }

        let array = json_value.as_array().unwrap();
        assert_eq!(array.len(), 2);

        let first = array[0].as_object().unwrap();
        assert_eq!(first.get("level").unwrap().as_str().unwrap(), "ERROR");
        assert_eq!(
            first.get("message").unwrap().as_str().unwrap(),
            "Failed to connect to database"
        );

        let second = array[1].as_object().unwrap();
        assert_eq!(second.get("level").unwrap().as_str().unwrap(), "INFO");
        assert_eq!(
            second.get("message").unwrap().as_str().unwrap(),
            "Application started"
        );
    }

    #[test]
    fn test_unknown_log_format() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let mut json_value = json!({
            "id": "12345",
            "raw_log": "Some log message"
        });

        // Try to extract with a non-existent format
        if let Value::Object(ref mut obj) = json_value {
            if let Some(schema) = processor.schema_definitions.get("nonexistent_format") {
                schema.check_or_extract(obj, Some("raw_log"));
            }
        }

        // Should return original JSON without modification
        let obj = json_value.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("raw_log"));
        assert!(!obj.contains_key("level"));
    }

    #[test]
    fn test_missing_log_field() {
        let processor = EventProcessor::new(TEST_CONFIG);
        let schema = processor.schema_definitions.get("custom_app_log").unwrap();

        // Create an object that doesn't have the log field
        let mut obj = Map::new();
        obj.insert("id".to_string(), Value::String("12345".to_string()));

        // check_or_extract should return false
        let result = schema.check_or_extract(&mut obj, Some("raw_log"));
        assert!(!result, "Should return false when log field is missing");

        // Verify no fields were added
        assert!(!obj.contains_key("level"));
        assert!(!obj.contains_key("timestamp"));
    }
}
