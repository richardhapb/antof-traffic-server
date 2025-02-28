extern crate dotenv;
mod api;
mod models;

use crate::models::events::{Alert, Jam};
use dotenv::dotenv;
use tokio::runtime::Runtime;

fn main() {
    dotenv().ok();

    let rt = Runtime::new().expect("Failed to create Tokio runtime");

    let (alerts, jams) = api::request_and_parse().unwrap_or_else(|err| {
        eprint!("{}", err);
        std::process::exit(1);
    });

    let insertions = rt.block_on(async {
        alerts.bulk_insert().await.unwrap_or_else(|err| {
            eprint!("{}", err);
            std::process::exit(1);
        })
    });

    println!("{} alerts inserted", insertions);

    let insertions = rt.block_on(async {
        jams.bulk_insert().await.unwrap_or_else(|err| {
            eprint!("{}", err);
            std::process::exit(1);
        })
    });

    println!("{} jams inserted", insertions);

    let updates = rt.block_on(async {
        Alert::fill_end_pub_millis(&alerts)
            .await
            .unwrap_or_else(|err| {
                eprint!("{}", err);
                std::process::exit(1);
            })
    });

    println!();
    println!("{} end reports in alerts updated", updates);

    let updates = rt.block_on(async {
        Jam::fill_end_pub_millis(&jams)
            .await
            .unwrap_or_else(|err| {
                eprint!("{}", err);
                std::process::exit(1);
            })
    });

    println!("{} end reports in jams updated", updates);
}
