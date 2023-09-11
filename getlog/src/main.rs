use async_std::task;
use ethers::{
    core::types::{Address, BlockNumber, Filter, U256},
    providers::{Middleware, Provider, StreamExt, Ws},
};
use eyre::Result;
use futures::future::join_all;
use futures::{join, try_join, AsyncWriteExt};
use mysql::prelude::*;
use mysql::*;
use std::string::String;
use std::sync::Arc;
// use std::thread;
// use std::time::Duration;

const WETH_ADDRESS: &str = "0xff2B4721F997c242fF406a626f17df083Bd2C568";
const WSS_URL: &str = "wss://eth.getblock.io/ab0b1aa0-b490-4dc0-9bda-817c897a4580/mainnet";

async fn get_ws_client() -> Provider<Ws> {
    Provider::<Ws>::connect(WSS_URL).await.unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    let _t: std::result::Result<(), Box<dyn std::error::Error>> = query_db().await;
    // let client = Arc::new(get_ws_client().await);
    // let tasks = vec![
    //     task::spawn(get_history_logs(client.clone())),
    //     task::spawn(getbalance(client.clone())),
    // ];
    // join_all(tasks).await;
    Ok(())
}
// "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" // Transfer
// "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925" //Approval
// "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31" // ApprovalForAll

async fn get_history_logs(client: Arc<Provider<Ws>>) -> Result<()> {
    let history_log_filter = Filter::new()
        .from_block(18090483)
        //  .event("Transfer(address,address,uint256)")
        .address(ethers::types::ValueOrArray::Value(
            WETH_ADDRESS.parse::<Address>()?,
        ));

    let logs = client.get_logs(&history_log_filter).await?;
    for log in logs.iter() {
        let h256_str = format!("{:?}", log.topics[0]);
        match h256_str.as_str() {
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => println!(
                "blocknumber: {:?}, sc: {:?}, from{:?},to{:?},id{:?},txhash{:?},logindex{:?}",
                log.block_number,
                log.address,
                Address::from(log.topics[1]),
                Address::from(log.topics[2]),
                log.topics[3].to_low_u64_be(),
                log.transaction_hash.unwrap(),
                log.log_index.unwrap(),
            ),
            _ => println!("others"),
        }
    }
    println!("{} tx found!", logs.iter().len());
    Ok(())
}

// async fn subscribe_new_logs(client: Arc<Provider<Ws>>) -> Result<> {}

async fn getbalance(client: Arc<Provider<Ws>>) -> Result<()> {
    let from_addr: &str = "0xc175006ED9Ee10210f466a043a300789a83C7420";
    //none is the latest blocknumber
    let balance = client.get_balance(from_addr, None).await?;
    println!("{balance}");
    Ok(())
}

async fn getlatestblocknumber(client: Arc<Provider<Ws>>) -> Result<()> {
    let last_block = client
        .get_block(BlockNumber::Latest)
        .await?
        .unwrap()
        .number
        .unwrap();
    // thread::sleep(Duration::from_secs(12));
    println!("last_block: {last_block}");
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct Transfer {
    blocknumber: u64,
    //  timestamp: Option<String>,
    address: Address,
    // func: Option<String>,
    from: Address,
    to: Address,
    tokenid: u64,
    txhash: Option<String>,
    logindex: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Approval {
    blocknumber: u64,
    //timestamp: Option<String>,
    address: Address,
    //  func: Option<String>,
    owner: Address,
    approved: Option<String>,
    tokenid: u64,
    txhash: Option<String>,
    logindex: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct ApprovalForAll {
    blocknumber: u64,
    //  timestamp: Option<String>,
    address: Address,
    //func: Option<String>,
    owner: Address,
    operator: Address,
    approved: Option<String>,
    txhash: Option<String>,
    logindex: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Owner {
    address: Address,
    owner: Address,
    tokenid: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

async fn get_db_conn() -> std::result::Result<PooledConn, Box<dyn std::error::Error>> {
    let mysql_url: &str = "mysql://root:123456@localhost:3306/testUser";
    let pool = Pool::new(mysql_url)?;
    let mut conn = pool.get_conn()?;
    Ok(conn)
}

async fn query_db() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let customer_id: i32 = 19;
    let account_name: String = "bar".to_string();
    let query_seq = format!(
        "SELECT customer_id, amount, account_name from payment where customer_id={} and account_name='{}'",
        customer_id,
        account_name
    );
    // println!("{query_seq}");
    match get_db_conn().await {
        Ok(mut conn) => {
            let val: Vec<Payment> =
                conn.query_map(query_seq, |(customer_id, amount, account_name)| Payment {
                    customer_id,
                    amount,
                    account_name,
                })?;
            if val.iter().len() > 0 {
                for log in val.iter() {
                    let customer_id = log.customer_id;
                    let amount = log.amount;
                    let account_name: String = log.account_name.clone().unwrap_or_default(); // 可以有空数据
                    println!("customer_id={customer_id}, amount = {amount}, account_name = {account_name}");
                }
            } else {
                conn.exec_drop(
                    r"INSERT INTO payment (customer_id, amount, account_name)
            VALUES (:customer_id, :amount, :account_name)",
                    params! {
                        "customer_id" => customer_id,
                                "amount" => 100,
                                "account_name" => account_name,
                    },
                )?;
            }
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(())
}

// async fn insert_db(
//     customer_id: i32,
//     amount: i32,
//     account_name: String,
// ) -> std::result::Result<(), Box<dyn std::error::Error>> {
//     conn.exec_drop(
//         r"INSERT INTO payment (customer_id, amount, account_name)
// VALUES (:customer_id, :amount, :account_name)",
//         params! {
//             "customer_id" => customer_id,
//                     "amount" => amount,
//                     "account_name" => Some(account_name.into()),
//         },
//     )?;
//     Ok(())
// }
