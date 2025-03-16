use serde::{Deserialize, Serialize, ser::StdError};
use serde_json::{Value, json};
use sqlx::{FromRow, Type};
use std::{env, error::Error, fs, path::Path};
use uuid::Uuid;

use memcache::Client;

use ndarray::{Array1, Array2};

use chrono::{DateTime, Datelike, Local, TimeZone, Timelike, Utc};
use chrono_tz::America::Santiago;

use crate::models::errors::EventError;

use crate::data::connect_to_db;

type FutureError = Box<dyn StdError + Send + Sync + 'static>;

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

#[derive(Debug)]
pub struct AlertGrouper {
    grid: (Array2<f64>, Array2<f64>),
    x_len: usize,
    y_len: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlertData {
    pub alert: Alert,

    // Calculated data
    pub group: Option<usize>, // Segment of the city
    pub day_type: Option<char>,
    pub week_day: Option<usize>,
    pub day: Option<usize>,
    pub hour: Option<usize>,
    pub minute: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AlertsGroup {
    pub alerts: Vec<Alert>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AlertsDataGroup {
    pub alerts: Vec<AlertData>,
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

impl AlertData {
    pub fn new(
        alert: Alert,
        group: Option<usize>,
        day: Option<usize>,
        week_day: Option<usize>,
        day_type: Option<char>,
        hour: Option<usize>,
        minute: Option<usize>,
    ) -> Self {
        let utc_timestamp = alert.pub_millis;

        // Convert milliseconds timestamp to DateTime<Utc>
        let utc_time =
            DateTime::<Utc>::from_timestamp_millis(utc_timestamp).expect("Invalid timestamp");

        // Convert to Santiago timezone
        let cl_time = Santiago.from_utc_datetime(&utc_time.naive_utc());

        // Extract time components
        let hour = Some(hour.unwrap_or(cl_time.hour() as usize));
        let minute = Some(minute.unwrap_or(cl_time.minute() as usize));
        let day = Some(day.unwrap_or(cl_time.day() as usize));
        let week_day = Some(week_day.unwrap_or(cl_time.weekday().num_days_from_monday() as usize));

        // Determine if weekend or holiday
        let day_type = Some(day_type.unwrap_or_else(|| {
            if cl_time.weekday().num_days_from_monday() >= 5 {
                'f' // Weekend
            } else {
                's' // Regular day
            }
        }));

        Self {
            alert,
            group,
            day,
            week_day,
            day_type,
            hour,
            minute,
        }
    }
}

impl AlertGrouper {
    pub fn group(
        &mut self,
        alerts: AlertsGroup,
        grid_dim: (usize, usize),
    ) -> Result<AlertsDataGroup, Box<dyn Error>> {
        let mut alerts_data: AlertsDataGroup = AlertsDataGroup { alerts: vec![] };

        self.get_grid(grid_dim.0, grid_dim.1);

        for alert in alerts.alerts.into_iter() {
            let location = if let Some(location) = &alert.location {
                location
            } else {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Alert location is required",
                )));
            };

            let x = location.x;
            let y = location.y;

            let alert_data = AlertData::new(
                alert, // Take ownership
                Some(match self.get_quadrant_indexes((x, y)) {
                    Ok((x, y)) => self.calc_quadrant(x, y),
                    Err(_) => 0, // Or handle the error appropriately
                }),
                None,
                None,
                None,
                None,
                None,
            );

            alerts_data.alerts.push(alert_data);
        }

        Ok(alerts_data)
    }

    pub fn get_quadrant_indexes(
        &self,
        point: (f64, f64),
    ) -> Result<(usize, usize), Box<dyn Error>> {
        let mut x_pos: Option<usize> = None;
        let mut y_pos: Option<usize> = None;

        let (x_grid, y_grid) = &self.grid;

        for xi in 0..x_grid.ncols() - 1 {
            if point.0 >= x_grid[(0, xi)] && point.0 <= x_grid[(0, xi + 1)] {
                x_pos = Some(xi);
                break;
            }
        }

        for yi in 0..y_grid.nrows() - 1 {
            if point.1 >= y_grid[(yi, 0)] && point.1 <= y_grid[(yi + 1, 0)] {
                y_pos = Some(yi);
                break;
            }
        }

        match (x_pos, y_pos) {
            (Some(x), Some(y)) => Ok((x, y)),
            _ => Err(format!("Point {:?} is not in any quadrant", point).into()),
        }
    }

    pub fn calc_quadrant(&self, x_pos: usize, y_pos: usize) -> usize {
        self.y_len * x_pos + y_pos + 1
    }

    fn get_grid(&mut self, xdiv: usize, ydiv: usize) -> Result<(), Box<dyn Error>> {
        let xmin = -7840929.70977753;
        let xmax = -7832727.689695883;
        let ymin = -2719838.3349531214;
        let ymax = -2690876.189690313;

        let bounds_x = Array1::linspace(xmin, xmax, xdiv);
        let bounds_y = Array1::linspace(ymin, ymax, ydiv);

        self.grid = meshgrid(&bounds_x, &bounds_y)?;
        self.x_len = xdiv - 1;
        self.y_len = ydiv - 1;

        Ok(())
    }
}

fn meshgrid(
    x: &Array1<f64>,
    y: &Array1<f64>,
) -> Result<(Array2<f64>, Array2<f64>), Box<dyn Error>> {
    let nx = x.len();
    let ny = y.len();

    let mut x_grid = Array2::zeros((ny, nx));
    let mut y_grid = Array2::zeros((ny, nx));

    for i in 0..ny {
        x_grid.column_mut(i).assign(&x);
    }

    for j in 0..nx {
        y_grid.column_mut(j).assign(&y);
    }

    Ok((x_grid, y_grid))
}

const HD_CACHE_KEY: &str = "holidays_data";
const HD_CACHE_EXPIRY: u32 = 86400; // 24 hours in seconds

// Synchronous function to get data from cache or file
pub fn get_holidays() -> Result<Vec<String>, FutureError> {
    // Try memcached first
    let memclient = Client::connect("memcache://127.0.0.1:11211")?;

    if let Ok(Some(cached_data)) = memclient.get::<Vec<String>>(HD_CACHE_KEY) {
        // Trigger async update in background
        tokio::spawn(async move {
            if let Err(e) = update_holidays().await {
                eprintln!("Background update failed: {}", e);
            }
        });

        return Ok(cached_data);
    }

    // If not in cache, try loading from file
    match fs::read_to_string("data/holidays.json") {
        Ok(contents) => {
            let data: Value = serde_json::from_str(&contents)?;
            let holidays: Vec<String> = data
                .get("holidays")
                .and_then(|h| h.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|h| h.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            // Trigger async update in background
            tokio::spawn(async move {
                if let Err(e) = update_holidays().await {
                    eprintln!("Background update failed: {}", e);
                }
            });

            Ok(holidays)
        }
        Err(e) => {
            eprintln!("Error reading file: {}", e);
            // If file read fails, do a blocking update
            update_holidays_blocking()
        }
    }
}

// Async function to update data
async fn update_holidays() -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let holidays_api: String = env::var("HOLIDAYS_API")?;

    let current_year = Local::now().year();
    let years: Vec<i32> = (2024..=current_year).collect();
    let mut holidays = Vec::new();
    let client = reqwest::Client::new();

    for year in &years {
        let url = holidays_api.replace("{year}", &year.to_string());
        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await?
            .json::<Value>()
            .await?;

        if let Some(data) = response.get("data").and_then(|d| d.as_array()) {
            for holiday in data {
                if let Some(date) = holiday.get("date").and_then(|d| d.as_str()) {
                    holidays.push(date.to_string());
                }
            }
        }
    }

    // Ensure directory exists and save to file
    let data_dir = Path::new("data");
    if !data_dir.exists() {
        fs::create_dir_all(data_dir)?;
    }

    let json_data = json!({ "holidays": holidays });
    fs::write(
        "data/holidays.json",
        serde_json::to_string_pretty(&json_data)?,
    )?;

    // Update cache
    let memclient = Client::connect("memcache://127.0.0.1:11211")?;
    memclient.set(HD_CACHE_KEY, &holidays, HD_CACHE_EXPIRY)?;

    Ok(holidays)
}

// Fallback blocking update function
fn update_holidays_blocking() -> Result<Vec<String>, FutureError> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(update_holidays())
}

