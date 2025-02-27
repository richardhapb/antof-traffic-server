extern crate dotenv;
mod models;
mod api;

use dotenv::dotenv;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let result = api::request_and_parse().unwrap_or_else( |err| {
        eprint!("{}", err);
        std::process::exit(1);
    });

    result.bulk_insert().await.unwrap_or_else( |err| {
        eprint!("{}", err);
        std::process::exit(1);
    });

    println!("{:?}", result);
}
