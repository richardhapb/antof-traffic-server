use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use uuid::Uuid;

use crate::models::errors::EventError;

use crate::data::connect_to_db;

// Types of Alerts events
#[derive(Serialize, Deserialize, Debug, sqlx::Type)]
#[sqlx(type_name = "varchar", rename_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum AlertType {
    Accident,
    Jam,
    Hazard,
    Misc,
    Construction,
    #[serde(rename = "ROAD_CLOSED")]
    RoadClosed,
}

impl AlertType {
    pub fn as_str(&self) -> &str {
        match self {
            AlertType::Accident => "ACCIDENT",
            AlertType::Jam => "JAM",
            AlertType::Hazard => "HAZARD",
            AlertType::Misc => "MISC",
            AlertType::Construction => "CONSTRUCTION",
            AlertType::RoadClosed => "ROAD_CLOSED",
        }
    }

    pub fn from(string: &str) -> Result<Self, EventError> {
        match string {
            "ACCIDENT" => Ok(AlertType::Accident),
            "CONSTRUCTION" => Ok(AlertType::Construction),
            "HAZARD" => Ok(AlertType::Hazard),
            "JAM" => Ok(AlertType::Jam),
            "MISC" => Ok(AlertType::Misc),
            "ROAD_CLOSED" => Ok(AlertType::RoadClosed),
            _ => Err(EventError::DeserializeError("Invalid Alert type")),
        }
    }
}

// Location (API response containing an object in this element)
#[derive(Serialize, Deserialize, Debug, FromRow, Type)]
#[sqlx(type_name = "int")]
pub struct Location {
    #[serde(default)]
    id: i32,
    x: f64,
    y: f64,
}

/// # API RESPONSE
/// Element: Type -                       Description
///
/// location: Coordinates -               Location per report (X Y - Long-lat)
/// uuid: String -                        Unique system ID
/// magvar Integer (0-359)                Event direction (Driver heading at report time. 0 degrees at North, according to the driver’s device)
/// type: See alert type table            Event type
/// subtype: See alert sub types table -  Event sub type - depends on atof parameter
/// reportDescription: String -           Report description (supplied when available)
/// street: String -                      Street name (as is written in database, no canonical form, may be null)
/// city: String -                        City and state name [City, State] in case both are available, [State] if not associated with a city. (supplied when available)
/// country: String -                     (see two letters codes in http://en.wikipedia.org/wiki/ISO_3166-1)
/// roadType: Integer -                   Road type (see road types)
/// reportRating: Integer -               User rank between 1-6 ( 6 = high ranked user)
/// jamUuid: string -                     If the alert is connected to a jam - jam ID
/// Reliability: 0-10 -                   Reliability score based on user reactions and reporter level
/// confidence: 0-10 -                    Confidence score based on user reactions
/// reportByMunicipalityUser: Boolean -   Alert reported by municipality user (partner) Optional.
/// nThumbsUp: integer -                  Number of thumbs up by users

/// ## Road type
/// Value    |    Type
///   1        Streets
///   2        Primary Street
///   3        Freeways
///   4        Ramps
///   5        Trails
///   6        Primary
///   7        Secondary
///   8, 14    4X4 Trails
///   15       Ferry crossing
///   9        Walkway
///   10       Pedestrian
///   11       Exit
///   16       Stairway
///   17       Private road
///   18       Railroads
///   19       Runway/Taxiway
///   20       Parking lot road
///   21       Service road

// Main alerts structure
#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct Alert {
    pub uuid: Uuid,
    pub reliability: Option<i16>,
    #[serde(rename = "type")]
    #[sqlx(skip)]
    pub alert_type: Option<AlertType>,
    #[sqlx(rename = "type")]
    #[serde(skip)]
    pub alert_type_string: Option<String>,
    #[serde(rename = "roadType")]
    pub road_type: Option<i16>,
    pub magvar: Option<f32>,
    pub subtype: Option<String>,
    #[serde(skip)]
    pub location_id: Option<i32>,
    #[sqlx(skip)]
    pub location: Option<Location>,
    pub street: Option<String>,
    #[serde(rename = "pubMillis")]
    pub pub_millis: i64,
    pub end_pub_millis: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AlertsGroup {
    pub alerts: Vec<Alert>,
}

impl Alert {
    pub async fn fill_end_pub_millis(last_data: &AlertsGroup) -> Result<u64, sqlx::Error> {
        let mut uuids = Vec::with_capacity(last_data.alerts.len());

        for alert in &last_data.alerts {
            uuids.push(alert.uuid);
        }

        let pool = connect_to_db().await?;

        let result = sqlx::query!(
            r#"
        UPDATE alerts SET end_pub_millis = EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'UTC')
        WHERE uuid <> ALL($1::uuid[]) AND end_pub_millis IS NULL
    "#,
            &uuids
        )
        .execute(&pool)
        .await?;

        Ok(result.rows_affected())
    }
}

impl AlertsGroup {
    pub async fn bulk_insert(&self) -> Result<u64, sqlx::Error> {
        if self.alerts.is_empty() {
            return Ok(0);
        }

        let pg_pool = connect_to_db().await?;

        let (
            mut uuids,
            mut reliabilities,
            mut types,
            mut road_types,
            mut magvars,
            mut subtypes,
            mut streets,
            mut pub_millis,
            mut end_pub_millis,
        ) = (
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
            Vec::with_capacity(self.alerts.len()),
        );

        let mut locations = Vec::with_capacity(self.alerts.len());

        // Single iteration over alerts
        for alert in &self.alerts {
            uuids.push(alert.uuid);
            reliabilities.push(alert.reliability.map(|ar| ar as i16));
            types.push(
                alert
                    .alert_type
                    .as_ref()
                    .map(|at| at.as_str())
                    .unwrap_or_default(),
            );
            road_types.push(alert.road_type.map(|rt| rt as i16));
            magvars.push(alert.magvar.map(|am| am as f32));
            locations.push(&alert.location);
            subtypes.push(alert.subtype.clone());
            streets.push(alert.street.clone());
            pub_millis.push(alert.pub_millis as i64);
            end_pub_millis.push(alert.end_pub_millis.map(|e| e as i64));
        }

        let locations_ids = sqlx::query!(
            r#"
            INSERT INTO alerts_location(x, y) SELECT * FROM UNNEST($1::real[], $2::real[])
            RETURNING id
            "#,
            &locations
                .iter()
                .filter_map(|l| l.as_ref().map(|loc| loc.x as f32))
                .collect::<Vec<f32>>(),
            &locations
                .iter()
                .filter_map(|l| l.as_ref().map(|loc| loc.y as f32))
                .collect::<Vec<f32>>()
        )
        .fetch_all(&pg_pool)
        .await?;

        let location_ids = locations_ids.iter().map(|l| l.id).collect::<Vec<i32>>();

        let result = sqlx::query!(
        r#"
        INSERT INTO alerts(uuid, reliability, type, road_type, magvar, subtype, location_id, street, pub_millis, end_pub_millis)
        SELECT * FROM UNNEST($1::uuid[], $2::smallint[], $3::varchar[], $4::smallint[], $5::real[],
                            $6::varchar[], $7::int[], $8::varchar[], $9::bigint[], $10::bigint[])
        ON CONFLICT (uuid) DO NOTHING
        "#,
        &uuids,
        &reliabilities.iter().filter_map(|r| *r).collect::<Vec<i16>>(),
        &types as _,
        &road_types as _,
        &magvars.iter().filter_map(|m| *m).collect::<Vec<f32>>(),
        &subtypes.iter().filter_map(|s| s.as_deref().map(String::from)).collect::<Vec<String>>(),
        &location_ids,
        &streets.iter().filter_map(|s| s.as_deref().map(String::from)).collect::<Vec<String>>(),
        &pub_millis,
        &end_pub_millis as _
    )
    .execute(&pg_pool)
    .await?;

        Ok(result.rows_affected())
    }
}

