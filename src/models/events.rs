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
    pub uuid: String,
    pub reliability: u8,
    #[serde(rename = "type")]
    #[sqlx(rename = "type")]
    pub alert_type: AlertType,
    #[sqlx(rename = "roadType")]
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
    alerts: Vec<Alert>,
}

/// ## Element     |      Value     |     Description
/// pubMillis: Timestamp -             Publication date (Unix time – milliseconds since epoch)
/// type: String -                     TRAFFIC_JAM
/// line List of x and y coordinates - Traffic jam line string (supplied when available)
/// speed: Float -                     Current average speed on jammed segments in meters/seconds
/// speedKMH: Float -                  Current average speed on jammed segments in Km/h
/// length: Integer -                  Jam length in meters
/// delay: Integer -                   Delay of jam compared to free flow speed, in seconds (in case of block, -1)
/// street: String -                   Street name (as is written in database, no canonical form. (supplied when available)
/// city: String -                     City and state name [City, State] in case both are available, [State] if not associated with a city. (supplied when available)
/// country: String -                  available on EU (world) serve
/// roadType: Integer -                Road type
/// startNode: String -                Nearest Junction/steet/city to jam start (supplied when available)
/// endNode: String -                  Nearest Junction/steet/city to jam end (supplied when available)
/// level: 0-5 -                       Traffic congestion level (0 = free flow 5 = blocked).
/// uuid: Longinteger -                Unique jam ID
/// turnLine: Coordinates -            A set of coordinates of a turn - only when the jam is in a turn (supplied when available)
/// turnType: String -                 What kind of turn is it - left, right, exit R or L, continue straight or NONE (no info) (supplied when available)
/// blockingAlertUuid: string -        if the jam is connected to a block (see alerts)&nbsp;

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

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct Jam {
    pub uuid: u64,
    pub level: u8,
    #[serde(rename = "speedKMH")]
    pub speed_kmh: f32,
    pub length: u8,
    #[serde(rename = "endNode")]
    pub end_node: String,
    #[serde(rename = "roadType")]
    pub road_type: Option<u8>,
    pub delay: i16,
    pub street: String,
    #[serde(rename = "pubMillis")]
    pub pub_millis: u64,
    pub end_pub_millis: Option<u64>,
    pub segments: Vec<JamSegments>,
    pub line: Vec<JamLine>,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct JamLine {
    #[serde(default)]
    pub id: u32,
    #[serde(default)]
    pub jams_uuid: u64,
    pub position: Option<u8>,
    pub x: f64,
    pub y: f64,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct JamSegments {
    #[serde(default)]
    pub id: u32,
    #[serde(default)]
    pub jams_uuid: u64,
    pub position: Option<u8>,
    #[serde(rename = "ID")]
    pub segment_id: u32,
    #[serde(rename = "fromNode")]
    pub from_node: u64,
    #[serde(rename = "toNode")]
    pub to_node: u64,
    #[serde(rename = "isForward")]
    pub is_forward: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JamsGroup {
    jams: Vec<Jam>,
}

impl Alert {
    pub async fn fill_end_pub_millis(last_data: &AlertsGroup) -> Result<u64, sqlx::Error> {
        let mut uuids = Vec::with_capacity(last_data.alerts.len());

        for alert in &last_data.alerts {
            let uuid =
                Uuid::parse_str(&alert.uuid).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
            uuids.push(uuid);
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
            uuids.push(
                Uuid::parse_str(&alert.uuid).map_err(|e| sqlx::Error::Protocol(e.to_string()))?,
            );
            reliabilities.push(alert.reliability as i16);
            types.push(alert.alert_type.as_str());
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

        Ok(result.rows_affected())
    }
}

impl JamsGroup {
    pub async fn bulk_insert(&self) -> Result<u64, sqlx::Error> {
        if self.jams.is_empty() {
            return Ok(0);
        }

        let pg_pool = connect_to_db().await?;

        let (
            mut uuids,
            mut levels,
            mut speed_kmhs,
            mut lengths,
            mut end_nodes,
            mut road_types,
            mut delays,
            mut streets,
            mut pub_millies,
            mut end_pub_millies,
        ) = (
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
            Vec::with_capacity(self.jams.len()),
        );

        for jam in &self.jams {
            uuids.push(jam.uuid as i64);
            levels.push(jam.level as i16);
            speed_kmhs.push(jam.speed_kmh as f32);
            lengths.push(jam.length as i16);
            end_nodes.push(jam.end_node.clone());
            road_types.push(jam.road_type.map(|rt| rt as i16));
            delays.push(jam.delay as i16);
            streets.push(jam.street.clone());
            pub_millies.push(jam.pub_millis as i64);
            end_pub_millies.push(jam.end_pub_millis.map(|e| e as i64));

            let lines = jam.line.iter().map(|l| (l.x, l.y)).collect::<Vec<(f64, f64)>>();
            let segments = jam.segments.iter().map(|s| (s.segment_id, s.from_node, s.to_node, s.is_forward)).collect::<Vec<(u32, u64, u64, bool)>>();

            sqlx::query!(
                r#"
                INSERT INTO jams_line(jams_uuid, x, y) SELECT * FROM UNNEST($1::bigint[], $2::real[], $3::real[])
                "#,
                &vec![jam.uuid as i64; lines.len()],
                &lines.iter().map(|l| l.0 as f32).collect::<Vec<f32>>(),
                &lines.iter().map(|l| l.1 as f32).collect::<Vec<f32>>()
            ).execute(&pg_pool).await?;

            sqlx::query!(
                r#"
                INSERT INTO jams_segments(jams_uuid, ID, from_node, to_node, is_forward) SELECT * FROM UNNEST($1::bigint[], $2::int[], $3::bigint[], $4::bigint[], $5::bool[])
                "#,
                &vec![jam.uuid as i64; segments.len()],
                &segments.iter().map(|s| s.0 as i32).collect::<Vec<i32>>(),
                &segments.iter().map(|s| s.1 as i64).collect::<Vec<i64>>(),
                &segments.iter().map(|s| s.2 as i64).collect::<Vec<i64>>(),
                &segments.iter().map(|s| s.3).collect::<Vec<bool>>()
            ).execute(&pg_pool).await?;

        }


        let result = sqlx::query!(
        r#"
        INSERT INTO jams(uuid, level, speed_kmh, length, end_node, road_type, delay, street, pub_millis, end_pub_millis)
        SELECT * FROM UNNEST($1::bigint[], $2::smallint[], $3::real[], $4::smallint[], $5::varchar[], $6::smallint[], $7::smallint[], $8::varchar[], $9::bigint[], $10::bigint[])
        ON CONFLICT (uuid) DO NOTHING
        "#,
            &uuids,
            &levels,
            &speed_kmhs,
            &lengths,
            &end_nodes,
            &road_types as _,
            &delays,
            &streets,
            &pub_millies,
            &end_pub_millies as _
        ).execute(&pg_pool).await?;


        Ok(result.rows_affected())
    }
}

// I need implement this
async fn connect_to_db() -> Result<PgPool, sqlx::Error> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    PgPool::connect(&database_url).await
}
