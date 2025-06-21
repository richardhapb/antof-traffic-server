use crate::cache::{
    ALERTS_CACHE_KEY, CacheService, MAX_PUB_MILLIS_CACHE_KEY, MIN_PUB_MILLIS_CACHE_KEY,
};
use crate::errors::{CacheError, UpdateError};
use crate::get_time_range;
use crate::models::{
    alerts::{AlertsDataGroup, AlertsGroup},
    jams::JamsGroup,
};
use crate::server::FilterParams;
use crate::utils::{calculate_params, connect_to_db, group_alerts};

use std::cmp::{max, min};
use std::sync::Arc;

use chrono::Utc;
use tracing::{debug, error, info};

pub const ALERTS_BEGIN_TIMESTAMP: i64 = 1727740800000; // 2024-10-01

// Threshold between last update and until param in request
pub const UPDATE_UNTIL_THRESHOLD: i64 = 600000; // 10 minutes

/// Get data from database, based on filters passed as args
///
/// # Params
/// * params: Filters from url request
/// * cache_service: Global state with cache connection
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_database(
    params: &FilterParams,
    cache_service: Arc<CacheService>,
) -> Result<AlertsDataGroup, UpdateError> {
    info!("Retrieving data from database...");
    let pool = connect_to_db().await?;

    let (since, until) = get_time_range!(params);

    let min_millis = cache_service.get_or_default(MIN_PUB_MILLIS_CACHE_KEY, since);
    let max_millis = cache_service.get_or_default(MAX_PUB_MILLIS_CACHE_KEY, until);

    // Calculate params
    let params = calculate_params(min_millis, max_millis, params);
    info!("Params calculated: {:?}", params);

    let query = format!(
        r#"
    SELECT a.*, l.x, l.y
    FROM alerts a
    LEFT JOIN alerts_location l ON a.location_id = l.id
    WHERE pub_millis
    BETWEEN {} and {}
    "#,
        since, until
    );

    let alerts: AlertsGroup = match sqlx::query_as(&query).fetch_all(&pool).await {
        Ok(alerts) => AlertsGroup { alerts },
        Err(e) => {
            error!("Error retrieving data from database {}", e);
            return Err(UpdateError::Database(e));
        }
    };

    info!("Grouping...");

    let alerts = group_alerts(alerts, Arc::clone(&cache_service)).await?;

    // Update data in cache and concatenate
    let mut alerts = concat_alerts_and_storage_to_cache(cache_service, alerts)?;
    alerts.filter_range(since, until);

    debug!("Data found: {}", alerts.alerts.len());

    Ok(alerts)
}

/// Retrieve data from cache if it exists
///
/// # Params
/// * `memclient`: The reference to memcache client connection
/// * `params`: Parameters of request, that shoud contain `since` and `until`
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_cache(
    cache_service: Arc<CacheService>,
    params: &FilterParams,
) -> Result<AlertsDataGroup, CacheError> {
    info!("Retrieving data...");

    let (since, until) = get_time_range!(params);

    let mut alerts: AlertsDataGroup = cache_service.get_or_err(ALERTS_CACHE_KEY)?;

    alerts.filter_range(since, until);
    info!("Data found: {}", alerts.alerts.len());

    Ok(alerts)
}

/// Insert the values to database and update end_pub_millis fields
///
/// # Params
/// * alerts: New alerts from API
/// * jams: New jams from API
pub async fn insert_and_update_data(
    alerts: &AlertsGroup,
    jams: &JamsGroup,
) -> Result<(), UpdateError> {
    // Database insertions
    let alerts_inserted = alerts.bulk_insert().await?;
    let j́ams_inserted = jams.bulk_insert().await?;

    // End pub millis update
    let alerts_ends = alerts.fill_end_pub_millis().await?;
    let jams_ends = jams.fill_end_pub_millis().await?;

    info!(
        "{} alerts and {} jams inserted to database",
        alerts_inserted, j́ams_inserted
    );
    info!(
        "{} alerts and {} jams end time updated",
        alerts_ends, jams_ends
    );

    Ok(())
}

/// Concatenate alerts with cache data and update the cache with the result
///
/// # Parameters
/// * `cache_service`: Pointer to the cache state
/// * `alerts`: New alerts to concatenate
///
/// # Returns
/// * The concatenated alerts.
pub fn concat_alerts_and_storage_to_cache(
    cache_service: Arc<CacheService>,
    alerts: AlertsDataGroup,
) -> Result<AlertsDataGroup, UpdateError> {
    let prev_alerts: AlertsDataGroup =
        cache_service.get_or_default(ALERTS_CACHE_KEY, AlertsDataGroup { alerts: vec![] });

    // Concat to previuos alerts in cache
    let alerts = prev_alerts.concat(alerts);

    // Update the millis data

    info!("Setting data of alerts in cache");
    cache_service.store_alerts(&alerts)?;
    info!("Data inserted to cache");

    Ok(alerts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CacheError;
    use crate::utils::test::{setup_alerts, setup_cache, setup_test_db};
    use memcache::{CommandError, MemcacheError};
    use serial_test::serial;

    #[tokio::test]
    async fn test_get_data_from_cache_empty() {
        let cache_service = setup_cache().await;
        cache_service.client.delete(ALERTS_CACHE_KEY).unwrap();

        let until = Some(Utc::now().timestamp() * 1000);

        // Is empty, should return an error
        let result = get_data_from_cache(
            cache_service,
            &FilterParams {
                since: Some(ALERTS_BEGIN_TIMESTAMP),
                until,
            },
        )
        .await;

        // Should return an error indicating doesnt exist
        assert!(matches!(
            result,
            Err(CacheError::NotFound(MemcacheError::CommandError(
                CommandError::KeyNotFound,
            )))
        ))
    }

    #[tokio::test]
    #[serial]
    async fn test_get_data_from_cache_with_data() {
        setup_test_db().await;
        let cache_service = setup_cache().await;
        let alerts = setup_alerts();

        // Insert two alerts to database
        alerts.bulk_insert().await.unwrap();

        // Remove any cache data
        cache_service.client.delete(ALERTS_CACHE_KEY).unwrap();

        // Group and insert data to cache
        let alerts = group_alerts(alerts, Arc::clone(&cache_service))
            .await
            .unwrap();
        cache_service.store_alerts(&alerts).unwrap();

        let until = Some(Utc::now().timestamp() * 1000);

        // Only retrieve one alert
        let since = Some(
            max(
                alerts.alerts.first().unwrap().alert.pub_millis,
                alerts.alerts.get(1).unwrap().alert.pub_millis,
            ) - 1000,
        );

        println!("{:?} - {:?}", since, until);

        // Only retrieve one alert - since limits to the max pub_millis - 1000
        let alerts = get_data_from_cache(cache_service.clone(), &FilterParams { since, until })
            .await
            .unwrap();

        // Clean
        cache_service.client.delete(ALERTS_CACHE_KEY).unwrap();

        assert_eq!(alerts.alerts.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_data_from_database() {
        setup_test_db().await;
        let cache_service = setup_cache().await;
        let alerts = setup_alerts();

        // Insert two alerts to database
        alerts.bulk_insert().await.unwrap();

        // This since value is lower than only one alert
        let filters = FilterParams {
            since: Some(1735980027000),
            until: None,
        };

        let alerts = get_data_from_database(&filters, Arc::clone(&cache_service))
            .await
            .unwrap();

        // Should return only one because the since filter
        assert_eq!(alerts.alerts.len(), 1);

        // Second test case

        // This since value is lower than two alerts
        let filters = FilterParams {
            since: Some(1730980027000),
            until: None,
        };

        let alerts = get_data_from_database(&filters, cache_service)
            .await
            .unwrap();

        // Should return both alerts
        assert_eq!(alerts.alerts.len(), 2);
    }

    #[test]
    fn test_time_range_macro() {
        let now = Utc::now().timestamp() * 1000;
        let params = FilterParams {
            since: Some(ALERTS_BEGIN_TIMESTAMP - 20),
            until: Some(now + 100),
        };
        let (since, until) = get_time_range!(params);

        // `since` can't be lower than `ALERTS_BEGIN_TIMESTAMP`
        assert_eq!(since, ALERTS_BEGIN_TIMESTAMP);
        // `until` can't be upper than current time
        assert_eq!(until, now);

        // Second case

        let params = FilterParams {
            since: Some(ALERTS_BEGIN_TIMESTAMP + 20),
            until: Some(now - 100),
        };

        let (since, until) = get_time_range!(params);

        assert_eq!(since, ALERTS_BEGIN_TIMESTAMP + 20);
        assert_eq!(until, now - 100);
    }
}
