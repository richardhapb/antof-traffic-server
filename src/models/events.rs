use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::types::Uuid;
use std::env;

use sqlx::PgPool;

// Types of events
#[derive(Serialize, Deserialize, Debug, sqlx::Type)]
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

// Location (API response containing an object in this element)
#[derive(Serialize, Deserialize, Debug)]
pub struct Location {
    #[serde(default)]
    id: u32,
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

// Main alerts structure
#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct Alerts {
    pub uuid: String,
    pub reliability: u8,
    #[serde(rename = "type")]
    #[sqlx(rename = "type")]
    pub alert_type: AlertType,
    pub road_type: Option<u8>,
    pub magvar: u16,
    pub subtype: String,
    #[sqlx(rename = "location_id")]
    pub location: Location,
    pub street: String,
    #[serde(rename = "pubMillis")]
    pub pub_millis: u64,
    pub end_pub_millis: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AlertsGroup {
    alerts: Vec<Alerts>,
}

impl AlertsGroup {
    pub async fn bulk_insert(&self) -> Result<(), sqlx::Error> {
        if self.alerts.is_empty() {
            return Ok(());
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

            uuids.push(
                Uuid::parse_str(&alert.uuid).map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            );
            reliabilities.push(alert.reliability as i16);
            types.push(&alert.alert_type);
            road_types.push(alert.road_type.map(|rt| rt as i16));
            magvars.push(alert.magvar as f32);
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
            &locations.iter().map(|l| l.x as f32).collect::<Vec<f32>>(),
            &locations.iter().map(|l| l.y as f32).collect::<Vec<f32>>()
        ).fetch_all(&pg_pool).await?;

        let location_ids = locations_ids.iter().map(|l| l.id).collect::<Vec<i32>>();

        sqlx::query!(
        r#"
        INSERT INTO alerts(uuid, reliability, type, road_type, magvar, subtype, location_id, street, pub_millis, end_pub_millis)
        SELECT * FROM UNNEST($1::uuid[], $2::smallint[], $3::varchar[], $4::smallint[], $5::real[],
                            $6::varchar[], $7::int[], $8::varchar[], $9::bigint[], $10::bigint[])
        ON CONFLICT (uuid) DO NOTHING
        "#,
        &uuids,
        &reliabilities,
        &types as _,
        &road_types as _,
        &magvars,
        &subtypes,
        &location_ids,
        &streets,
        &pub_millis,
        &end_pub_millis as _
    )
    .execute(&pg_pool)
    .await?;

        Ok(())
    }
}

// I need implement this
async fn connect_to_db() -> Result<PgPool, sqlx::Error> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    PgPool::connect(&database_url).await
}
