use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use uuid::Uuid;

use crate::data::connect_to_db;

// Custom errors for Events
pub enum EventError<'a> {
    ApiRequestError,
    DatabaseError(&'a str),
    SerializeError(&'a str),
    DeserializeError(&'a str),
    RequestDataError(&'a str),
}

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
    pub uuid: i64,
    pub level: Option<i8>,
    #[serde(rename = "speedKMH")]
    pub speed_kmh: Option<f32>,
    pub length: Option<i16>,
    #[serde(rename = "endNode")]
    pub end_node: Option<String>,
    #[serde(rename = "roadType")]
    pub road_type: Option<i8>,
    pub delay: Option<i16>,
    pub street: Option<String>,
    #[serde(rename = "pubMillis")]
    pub pub_millis: i64,
    pub end_pub_millis: Option<i64>,
    pub segments: Option<Vec<JamSegment>>,
    pub line: Option<Vec<JamLine>>,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct JamLine {
    #[serde(default)]
    pub id: Option<i32>,
    #[serde(default)]
    pub jams_uuid: i64,
    pub position: Option<i8>,
    pub x: f64,
    pub y: f64,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct JamSegment {
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default)]
    pub jams_uuid: i64,
    pub position: Option<i8>,
    #[serde(rename = "ID")]
    pub segment_id: i64,
    #[serde(rename = "fromNode")]
    pub from_node: i64,
    #[serde(rename = "toNode")]
    pub to_node: i64,
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

impl Jam {
    pub async fn fill_end_pub_millis(last_data: &JamsGroup) -> Result<u64, sqlx::Error> {
        let mut uuids = Vec::with_capacity(last_data.jams.len());

        for jam in &last_data.jams {
            let uuid = jam.uuid as i64;
            uuids.push(uuid);
        }

        let pool = connect_to_db().await?;

        let result = sqlx::query!(
            r#"
        UPDATE jams SET end_pub_millis = EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'UTC')
        WHERE uuid <> ALL($1::bigint[]) AND end_pub_millis IS NULL
    "#,
            &uuids
        )
        .execute(&pool)
        .await?;

        Ok(result.rows_affected())
    }
}

impl JamLine {
    pub fn new(id: Option<i32>, jams_uuid: i64, position: Option<i8>, x: f64, y: f64) -> Self {
        return JamLine {
            id,
            jams_uuid,
            position,
            x,
            y,
        };
    }
}

impl JamSegment {
    pub fn new(
        id: Option<i64>,
        jams_uuid: i64,
        position: Option<i8>,
        segment_id: i64,
        from_node: i64,
        to_node: i64,
        is_forward: bool,
    ) -> Self {
        return JamSegment {
            id,
            jams_uuid,
            position,
            segment_id,
            from_node,
            to_node,
            is_forward,
        };
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

        let mut lines: Vec<JamLine> = vec![];
        let mut segments: Vec<JamSegment> = vec![];

        // Prepare data for insertion: jams and internal objects lines and segments
        for jam in &self.jams {
            uuids.push(jam.uuid as i64);
            levels.push(jam.level.map(|l| l as i16));
            speed_kmhs.push(jam.speed_kmh.map(|k| k as f32));
            lengths.push(jam.length.map(|l| l as i16));
            end_nodes.push(jam.end_node.clone());
            road_types.push(jam.road_type.map(|rt| rt as i16));
            delays.push(jam.delay.map(|d| d as i16));
            streets.push(jam.street.clone());
            pub_millies.push(jam.pub_millis as i64);
            end_pub_millies.push(jam.end_pub_millis.map(|e| e as i64));

            // Initialize the position index at 1 equal to the original data
            if let Some(line_vec) = &jam.line {
                for (i, line) in line_vec.iter().enumerate() {
                    lines.push(JamLine::new(
                        None,
                        jam.uuid,
                        Some(i as i8 + 1),
                        line.x,
                        line.y,
                    ));
                }
            }

            // Initialize the position index at 1 equal to the original data
            if let Some(segments_vec) = &jam.segments {
                for (i, segment) in segments_vec.iter().enumerate() {
                    segments.push(JamSegment::new(
                        None,
                        jam.uuid,
                        Some(i as i8 + 1),
                        segment.segment_id,
                        segment.from_node,
                        segment.to_node,
                        segment.is_forward,
                    ));
                }
            }
        }

        let result = sqlx::query!(
        r#"
        INSERT INTO jams(uuid, level, speed_kmh, length, end_node, road_type, delay, street, pub_millis, end_pub_millis)
        SELECT * FROM UNNEST($1::bigint[], $2::smallint[], $3::real[], $4::smallint[], $5::varchar[], $6::smallint[], $7::smallint[], $8::varchar[], $9::bigint[], $10::bigint[])
        ON CONFLICT (uuid) DO NOTHING
        "#,
            &uuids,
            &levels.iter().filter_map(|l| *l).collect::<Vec<i16>>(),
            &speed_kmhs.iter().filter_map(|s| *s).collect::<Vec<f32>>(),
            &lengths.iter().filter_map(|l| *l).collect::<Vec<i16>>(),
            &end_nodes as _,
            &road_types as _,
            &delays.iter().filter_map(|d| *d).collect::<Vec<i16>>(),
            &streets.iter().filter_map(|s| s.as_deref().map(String::from)).collect::<Vec<String>>(),
            &pub_millies,
            &end_pub_millies as _
        ).execute(&pg_pool).await?;

        sqlx::query!(
                r#"
                INSERT INTO jams_line(jams_uuid, position, x, y) SELECT * FROM UNNEST($1::bigint[], $2::smallint[], $3::real[], $4::real[])
                ON CONFLICT (id) DO NOTHING
                "#,
                &lines.iter().map(|l| l.jams_uuid as i64).collect::<Vec<i64>>(),
                &lines.iter().filter_map(|l| l.position.map(|p| p as i16)).collect::<Vec<i16>>(),
                &lines.iter().map(|l| l.x as f32).collect::<Vec<f32>>(),
                &lines.iter().map(|l| l.y as f32).collect::<Vec<f32>>()
            ).execute(&pg_pool).await?;

        sqlx::query!(
                r#"
                INSERT INTO jams_segments(jams_uuid, position, segment_id, from_node, to_node, is_forward) SELECT * FROM UNNEST($1::bigint[], $2::smallint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bool[])
                ON CONFLICT (id) DO NOTHING
                "#,
                &segments.iter().map(|s| s.jams_uuid as i64).collect::<Vec<i64>>(),
                &segments.iter().filter_map(|s| s.position.map(|p| p as i16)).collect::<Vec<i16>>(),
                &segments.iter().map(|s| s.segment_id as i64).collect::<Vec<i64>>(),
                &segments.iter().map(|s| s.from_node as i64).collect::<Vec<i64>>(),
                &segments.iter().map(|s| s.to_node as i64).collect::<Vec<i64>>(),
                &segments.iter().map(|s| s.is_forward).collect::<Vec<bool>>()
            ).execute(&pg_pool).await?;

        Ok(result.rows_affected())
    }
}
