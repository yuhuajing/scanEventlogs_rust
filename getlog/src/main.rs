use async_std::task;
use ethers::{
    core::types::{Address, BlockNumber, Filter, U256},
    providers::{Middleware, Provider, StreamExt, Ws},
};
use eyre::Result;
use futures::future::join_all;
// use futures::{join, try_join, AsyncWriteExt};
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
    let client = Arc::new(get_ws_client().await);
    let tasks = vec![
        task::spawn(getlogs(client.clone())),
        task::spawn(getbalance(client.clone())),
    ];
    join_all(tasks).await;
    Ok(())
}
// "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" // Transfer
// "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925" //Approval
// "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31" // ApprovalForAll
async fn get_history_logs(client: Arc<Provider<Ws>>) -> Result<()> {
    let history_log_filter = Filter::new()
        .from_block(17943452)
        //  .event("Transfer(address,address,uint256)")
        .address(ethers::types::ValueOrArray::Value(
            WETH_ADDRESS.parse::<Address>()?,
        ));

    let logs = client.get_logs(&history_log_filter).await?;
    for log in logs.iter() {
        let h256_str = format!("{:?}", log.topics[0]);
        match h256_str.as_str() {
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => println!(
                "block: {:?}, tx: {:?}, token: {:?}, topic{:?}, from{:?},to{:?},value{:?}",
                log.block_number,
                //log.timestamp,
                log.address,
                //fun
                Address::from(log.topics[1]),
                Address::from(log.topics[2]),
                U256::decode(log.topics[3])
                log.transaction_hash,
                log.logindex,
              //  format!("{:?}", log.topics[0]),
            ),
            // "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => println!(
            //     "block: {:?}, tx: {:?}, token: {:?}, topic{:?}, from{:?},to{:?},value{:?}",
            //     log.block_number,
            //     log.transaction_hash,
            //     log.address,
            //     format!("{:?}", log.topics[0]),
            //     Address::from(log.topics[1]),
            //     Address::from(log.topics[2]),
            //     U256::decode(log.data.clone())
            // ),
            // "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => println!(
            //     "block: {:?}, tx: {:?}, token: {:?}, topic{:?}, from{:?},to{:?},value{:?}",
            //     log.block_number,
            //     log.transaction_hash,
            //     log.address,
            //     format!("{:?}", log.topics[0]),
            //     Address::from(log.topics[1]),
            //     Address::from(log.topics[2]),
            //     U256::decode(log.data.clone())
            // ),
            _ => println!("others"),
        }
    }
    println!("{} tx found!", logs.iter().len());
    Ok(())
}

async fn subscribe_new_logs(client: Arc<Provider<Ws>>) -> Result<()> {}

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
    blocknumber: i64,
    //  timestamp: Option<String>,
    address: Address,
    // func: Option<String>,
    from: Address,
    to: Address,
    tokenid: i64,
    txhash: Option<String>,
    logindex: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct Approval {
    blocknumber: i64,
    //timestamp: Option<String>,
    address: Address,
    //  func: Option<String>,
    owner: Address,
    approved: Option<String>,
    tokenid: i64,
    txhash: Option<String>,
    logindex: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct ApprovalForAll {
    blocknumber: i64,
    //  timestamp: Option<String>,
    address: Address,
    //func: Option<String>,
    owner: Address,
    operator: Address,
    approved: Option<String>,
    txhash: Option<String>,
    logindex: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct Owner {
    address: Address,
    owner: Address,
    tokenid: i64,
}
