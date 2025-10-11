use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::utils::connect_to_db;

/// # API RESPONSE
/// Element: type     |     Description
/// --------------------------------------------------
/// * pubMillis: Timestamp             |  Publication date (Unix time – milliseconds since epoch)
/// * type: String                     |  TRAFFIC_JAM
/// * line List of x and y coordinates |  Traffic jam line string (supplied when available)
/// * speed: Float                     |  Current average speed on jammed segments in meters/seconds
/// * speedKMH: Float                  |  Current average speed on jammed segments in Km/h
/// * length: Integer                  |  Jam length in meters
/// * delay: Integer                   |  Delay of jam compared to free flow speed, in seconds (in case of block, -1)
/// * street: String                   |  Street name (as is written in database, no canonical form. (supplied when available)
/// * city: String                     |  City and state name [City, State] in case both are available, [State] if not associated with a city. (supplied when available)
/// * country: String                  |  available on EU (world) serve
/// * roadType: Integer                |  Road type
/// * startNode: String                |  Nearest Junction/steet/city to jam start (supplied when available)
/// * endNode: String                  |  Nearest Junction/steet/city to jam end (supplied when available)
/// * level: 0 -                       |  Traffic congestion level (0 = free flow 5 = blocked).
/// * uuid: Longinteger                |  Unique jam ID
/// * turnLine: Coordinates            |  A set of coordinates of a turn - only when the jam is in a turn (supplied when available)
/// * turnType: String                 |  What kind of turn is it - left, right, exit R or L, continue straight or NONE (no info) (supplied when available)
/// * blockingAlertUuid: string        |  if the jam is connected to a block (see alerts)&nbsp;
/// ---
/// ## Road type
/// ###  Value    |    Type
/// ---------------------
/// *   1     |   Streets
/// *   2     |   Primary Street
/// *   3     |   Freeways
/// *   4     |   Ramps
/// *   5     |   Trails
/// *   6     |   Primary
/// *   7     |   Secondary
/// *   8, 14 |   4X4 Trails
/// *   15    |   Ferry crossing
/// *   9     |   Walkway
/// *   10    |   Pedestrian
/// *   11    |   Exit
/// *   16    |   Stairway
/// *   17    |   Private road
/// *   18    |   Railroads
/// *   19    |   Runway/Taxiway
/// *   20    |   Parking lot road
/// *   21    |   Service road
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
    #[serde(rename(serialize = "pub_millis", deserialize = "pubMillis"))]
    #[serde(alias = "pub_millis")]
    pub pub_millis: i64,
    pub end_pub_millis: Option<i64>,
    pub segments: Option<Vec<JamSegment>>,
    pub line: Option<Vec<JamLine>>,
}

/// Jam line, that represents the line with coordinates where is the jam
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

/// Jam segment, that represents the nodes connections
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

/// Struct with vector of `Jam`
#[derive(Serialize, Deserialize, Debug)]
pub struct JamsGroup {
    jams: Option<Vec<Jam>>,
}

impl JamLine {
    pub fn new(id: Option<i32>, jams_uuid: i64, position: Option<i8>, x: f64, y: f64) -> Self {
        JamLine {
            id,
            jams_uuid,
            position,
            x,
            y,
        }
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
        JamSegment {
            id,
            jams_uuid,
            position,
            segment_id,
            from_node,
            to_node,
            is_forward,
        }
    }
}

impl JamsGroup {
    /// Insert a group of jams from API in a bulk insert
    /// ensuring the efficiency and avoid bucles
    pub async fn bulk_insert(&self) -> Result<u64, sqlx::Error> {
        let jams = vec![];
        let jams = self.jams.as_ref().unwrap_or(&jams);

        if jams.is_empty() {
            return Ok(0);
        }

        let pg_pool = connect_to_db().await?;
        let jams_len = jams.len();

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
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
            Vec::with_capacity(jams_len),
        );

        let mut lines: Vec<JamLine> = vec![];
        let mut segments: Vec<JamSegment> = vec![];

        // Prepare data for insertion: jams and internal objects lines and segments
        for jam in jams {
            uuids.push(jam.uuid);
            levels.push(jam.level.map(|l| l as i16));
            speed_kmhs.push(jam.speed_kmh);
            lengths.push(jam.length);
            end_nodes.push(jam.end_node.clone());
            road_types.push(jam.road_type.map(|rt| rt as i16));
            delays.push(jam.delay);
            streets.push(jam.street.clone());
            pub_millies.push(jam.pub_millis);
            end_pub_millies.push(jam.end_pub_millis);

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

    /// Fill end pub millis field in database with current time for all
    /// data without it and is not present in last data
    pub async fn fill_end_pub_millis(&self) -> Result<u64, sqlx::Error> {
        if let Some(jams) = &self.jams {
            let mut uuids = Vec::with_capacity(jams.len());

            for jam in jams.iter() {
                let uuid = jam.uuid;
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
        } else {
            Ok(0)
        }
    }
}
