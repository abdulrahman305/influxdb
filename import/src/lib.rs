use schema::InfluxFieldType;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use serde::*;
use std::collections::{HashMap, HashSet};

pub mod aggregate_tsm_schema;

/// This struct is used to build up schemas from TSM snapshots that we are going to use to bulk
/// ingest. They will be merged, then validated to check for anomalies that will complicate bulk
/// ingest such as tags/fields with the same name, or fields with different types across the whole
/// dataset. It is not the same as an IOx schema, although it is similar and some of the merge code
/// is similar. It's a transient data structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateTSMSchema {
    pub org_id: String,
    pub bucket_id: String,
    pub measurements: HashMap<String, AggregateTSMMeasurement>,
}

impl AggregateTSMSchema {
    pub fn types_are_valid(&self) -> bool {
        self.measurements.values().all(|m| {
            m.fields.values().all(|f| {
                f.types.len() == 1
                    && InfluxFieldType::try_from(f.types.iter().next().unwrap()).is_ok()
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateTSMMeasurement {
    // Map of tag name -> tag; note that the schema we get from the TSM tool has these as arrays.
    // Using HashMaps internally to detect duplicates, so we have to do some custom serialisation
    // for tags and fields here.
    #[serde(
        serialize_with = "serialize_map_values",
        deserialize_with = "deserialize_tags"
    )]
    pub tags: HashMap<String, AggregateTSMTag>,
    #[serde(
        serialize_with = "serialize_map_values",
        deserialize_with = "deserialize_fields"
    )]
    pub fields: HashMap<String, AggregateTSMField>,
}

fn serialize_map_values<S, K, V>(value: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    serializer.collect_seq(value.values())
}

fn deserialize_tags<'de, D>(deserializer: D) -> Result<HashMap<String, AggregateTSMTag>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<AggregateTSMTag> = Deserialize::deserialize(deserializer)?;
    Ok(v.into_iter().map(|t| (t.name.clone(), t)).collect())
}

fn deserialize_fields<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, AggregateTSMField>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<AggregateTSMField> = Deserialize::deserialize(deserializer)?;
    Ok(v.into_iter().map(|f| (f.name.clone(), f)).collect())
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregateTSMTag {
    pub name: String,
    pub values: HashSet<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregateTSMField {
    pub name: String,
    pub types: HashSet<String>,
}

impl TryFrom<Vec<u8>> for AggregateTSMSchema {
    type Error = serde_json::Error;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&data)
    }
}

impl TryFrom<&str> for AggregateTSMSchema {
    type Error = serde_json::Error;

    fn try_from(data: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parses() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        assert_eq!(schema.org_id, "1234");
        assert_eq!(schema.bucket_id, "5678");
        assert_eq!(schema.measurements.len(), 1);
        assert!(schema.measurements.contains_key("cpu"));
        let measurement = schema.measurements.get("cpu").unwrap();
        assert_eq!(measurement.tags.len(), 1);
        let tag = &measurement.tags.values().next().unwrap();
        assert_eq!(tag.name, "host");
        assert_eq!(
            tag.values,
            HashSet::from(["server".to_string(), "desktop".to_string()])
        );
        let field = &measurement.fields.values().next().unwrap();
        assert_eq!(field.name, "usage");
        assert_eq!(field.types, HashSet::from(["Float".to_string()]));
        // exercise the Vec<u8> tryfrom impl too
        assert_eq!(schema, json.as_bytes().to_vec().try_into().unwrap());
        // now exercise the serialise code too
        let schema = AggregateTSMSchema {
            org_id: "1234".to_string(),
            bucket_id: "5678".to_string(),
            measurements: HashMap::from([(
                "cpu".to_string(),
                AggregateTSMMeasurement {
                    tags: HashMap::from([(
                        "host".to_string(),
                        AggregateTSMTag {
                            name: "host".to_string(),
                            values: HashSet::from(["server".to_string(), "desktop".to_string()]),
                        },
                    )]),
                    fields: HashMap::from([(
                        "usage".to_string(),
                        AggregateTSMField {
                            name: "usage".to_string(),
                            types: HashSet::from(["Float".to_string()]),
                        },
                    )]),
                },
            )]),
        };
        let _json = serde_json::to_string(&schema).unwrap();
        // ^ not asserting on the value because vector ordering changes so it would be flakey. it's
        // enough that it serialises without error
    }

    #[tokio::test]
    async fn type_validation_happy() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float"] }
              ]
            }
          }
        }
        "#;
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        assert!(schema.types_are_valid());
    }

    #[tokio::test]
    async fn type_validation_invalid_type() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["FloatyMcFloatFace"] }
              ]
            }
          }
        }
        "#;
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        assert!(!schema.types_are_valid());
    }

    #[tokio::test]
    async fn type_validation_multiple_types() {
        let json = r#"
        {
          "org_id": "1234",
          "bucket_id": "5678",
          "measurements": {
            "cpu": {
              "tags": [
                { "name": "host", "values": ["server", "desktop"] }
              ],
             "fields": [
                { "name": "usage", "types": ["Float", "Integer"] }
              ]
            }
          }
        }
        "#;
        let schema: AggregateTSMSchema = json.try_into().unwrap();
        assert!(!schema.types_are_valid());
    }
}
