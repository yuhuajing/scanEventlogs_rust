//main
use async_std::task;
use ethers::{
    core::types::{Address, BlockNumber, Filter, Log, U64},
    providers::{Middleware, Provider, StreamExt, Ws},
};
use eyre::Result;
use futures::future::join_all;
use std::string::String;
use std::sync::Arc;
mod common;
mod database;

#[tokio::main]
async fn main() -> Result<()> {
    let address: String = common::TARGET_ADDRESS.to_string().to_lowercase();
    // let mypool = database::MyPool::new(common::MYSQL_CONN_URL)?;
    let mypool: database::MyPool = match database::MyPool::new(common::MYSQL_CONN_URL).await {
        Ok(pool) => pool,
        Err(_) => todo!(),
    };
    let _t: std::result::Result<(), Box<dyn std::error::Error>> = mypool
        .create_table_insert_owner(
            address.clone(),
            common::CRERATE_TABLE_TRANSFER,
            common::CRERATE_TABLE_APPROVAL,
            common::CRERATE_TABLE_APPROVALFORALL,
            common::CRERATE_TABLE_OWNER,
            common::QUERY_OWNER_STATE,
            common::INSERT_OWNER_STATE,
        )
        .await;
    let client = Arc::new(get_ws_client().await);
    let db_block: u64 = match mypool
        .query_db_latest_blocknum(
            common::TARGET_ADDRESS,
            common::QUERY_TRANSFER_LATESTBLOCK_STATE,
        )
        .await
    {
        Ok(blocknumber) => blocknumber,
        Err(_) => todo!(),
    };

    let mut from_block: U64 = U64::default();

    if db_block == 0 {
        from_block = U64::from(common::FROM_BLOCK);
    } else {
        from_block = U64::from(db_block);
    }

    let last_block: U64 = client
        .clone()
        .get_block(BlockNumber::Latest)
        .await?
        .unwrap()
        .number
        .unwrap();

    println!("fromBlock:{}", from_block.as_u64());
    println!("latestBlock:{}", last_block.as_u64());

    let tasks = vec![
        task::spawn(get_history_logs(
            client.clone(),
            from_block.clone(),
            last_block,
        )),
        task::spawn(subscribe_new_logs(client.clone(), from_block)),
    ];
    join_all(tasks).await;

    Ok(())
}

async fn get_history_logs(client: Arc<Provider<Ws>>, from_block: U64, to_block: U64) -> Result<()> {
    let history_log_filter = Filter::new()
        .from_block(BlockNumber::Number(from_block)) //17971969
        .to_block(BlockNumber::Number(to_block)) //17971994
        .address(ethers::types::ValueOrArray::Value(
            common::TARGET_ADDRESS.parse::<Address>()?,
        ));

    let logs = client.get_logs(&history_log_filter).await?;
    for log in logs.iter() {
        let _ = insert_log(log).await;
    }
    Ok(())
}

async fn subscribe_new_logs(client: Arc<Provider<Ws>>, from_block: U64) -> Result<()> {
    let subscribe_log_filter = Filter::new()
        .from_block(BlockNumber::Number(from_block))
        .address(ethers::types::ValueOrArray::Value(
            common::TARGET_ADDRESS.parse::<Address>()?,
        ));

    let mut logs = client.subscribe_logs(&subscribe_log_filter).await?;
    while let Some(log) = logs.next().await {
        let _ = insert_log(log).await;
    }
    Ok(())
}

async fn get_ws_client() -> Provider<Ws> {
    Provider::<Ws>::connect(common::WSS_URL).await.unwrap()
}

async fn insert_log(log: Log) -> Result<()> {
    // let mypool = database::MyPool::new(common::MYSQL_CONN_URL)?;
    let mypool: database::MyPool = match database::MyPool::new(common::MYSQL_CONN_URL).await {
        Ok(pool) => pool,
        Err(_) => todo!(),
    };
    let topic = format!("{:?}", log.topics[0]);
    let mut patternx = 0;
    if topic.as_str() == common::TRANSFER_EVENT {
        patternx = 1;
    } else if topic.as_str() == common::APPROVAL_EVENT {
        patternx = 2;
    } else if topic.as_str() == common::APPROVALFORALL_EVENT {
        patternx = 3;
    }

    match patternx {
        1 => {
            let _tx: std::result::Result<(), Box<dyn std::error::Error>> = mypool
                .insert_transfer_log_db(
                    log.clone(),
                    common::QUERY_TRANSFER_STATE,
                    common::INSERT_TRANSFER_STATE,
                    common::UPDATE_OWNER_STATE,
                )
                .await;
        }
        2 => {
            let _tx: std::result::Result<(), Box<dyn std::error::Error>> = mypool
                .insert_approval_log_db(
                    log.clone(),
                    common::QUERY_APPROVAL_STATE,
                    common::INSERT_APPROVAL_STATE,
                )
                .await;
        }
        3 => {
            let _tx: std::result::Result<(), Box<dyn std::error::Error>> = mypool
                .insert_approvalforall_log_db(
                    log.clone(),
                    common::QUERY_APPROVALFORALL_STATE,
                    common::INSERT_APPROVALFORALL_STATE,
                )
                .await;
        }
        _ => {
            println!("Other")
        }
    }
}

// 数据库 
use ethers::{
    abi::AbiEncode,
    core::types::{Address, Log, U256},
};
use mysql::prelude::*;
use mysql::*;
use std::string::String;
use std::sync::Arc;

#[derive(Clone)]
pub struct MyPool {
    pool: Arc<Pool>,
}

impl MyPool {
    pub async fn new(mysql_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            pool: Arc::new(Pool::new(mysql_url)?),
        })
    }

    pub async fn query_db_latest_blocknum(
        &self,
        target_address: &str,
        query_latest_block_stat: &str,
    ) -> std::result::Result<u64, Box<dyn std::error::Error>> {
        let mut conn = self.pool.get_conn()?;
        let mut query_state: &str = query_latest_block_stat;
        let binding = query_state.replace("{address}", target_address);
        query_state = binding.as_str();
        let mut results = conn.query_iter(query_state)?;
        if let Some(row) = results.next() {
            let blocknumber: u64 = row?.get(0).unwrap_or(0);
            return Ok(blocknumber + 1);
        }
        Ok(0)
    }

    pub async fn create_table_insert_owner(
        &self,
        address: String,
        create_transfer: &str,
        create_approval: &str,
        create_approvalforall: &str,
        create_owner: &str,
        query_owner: &str,
        insert_owner: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.pool.get_conn()?;
        match conn.query_drop(create_transfer) {
            Ok(_) => {
                println!("Transfer table existed_or_created successfully");
            }
            Err(err) => {
                eprintln!("Error creating transfer table: {:?}", err);
            }
        }

        //let create_approval_state = conn.clone().prep(CRERATE_TABLE_APPROVAL)?;
        match conn.query_drop(create_approval) {
            Ok(_) => {
                println!("Approval table existed_or_created successfully");
            }
            Err(err) => {
                eprintln!("Error creating transfer table: {:?}", err);
            }
        }

        //  let create_approvalforall_state = conn.clone().prep(CRERATE_TABLE_APPROVALFORALL)?;
        match conn.query_drop(create_approvalforall) {
            Ok(_) => {
                println!("ApprovalForAll table existed_or_created successfully");
            }
            Err(err) => {
                eprintln!("Error creating transfer table: {:?}", err);
            }
        }

        //  let create_owner_state = conn.clone().prep(CRERATE_TABLE_OWNER)?;
        match conn.query_drop(create_owner) {
            Ok(_) => {
                println!("Owner table existed_or_created successfully");
            }
            Err(err) => {
                eprintln!("Error creating transfer table: {:?}", err);
            }
        }

        let mut query_owner_state: &str = query_owner;
        let binding = query_owner_state.replace("{address}", address.as_str());
        query_owner_state = binding.as_str();
        let mut results = conn.query_iter(query_owner_state)?;
        if let Some(row) = results.next() {
            let count: i64 = row?.get(0).unwrap_or(0);
            if count == 0 {
                let mut new_conn = self.pool.get_conn()?;
                for i in (0..=514).rev() {
                    let insert_owner_state = new_conn.prep(insert_owner)?;
                    match new_conn.exec_drop(
                        &insert_owner_state,
                        params! {
                            "address" => address.clone(),
                            "owner"=> String::default(),
                            "tokenid" => i,
                        },
                    ) {
                        Ok(_) => {
                            println!("Owner table initialized successfully");
                        }
                        Err(err) => {
                            eprintln!("Error Owner table initialized: {:?}", err);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn insert_transfer_log_db(
        &self,
        log: Log,
        query_transfer: &str,
        insert_transfer: &str,
        update_owner: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (blocknumber, address, from_address, to_address, txhash, logindex) = parselog(log);
        let mut transfer_query: &str = query_transfer;
        let transfer_binding = transfer_query
            .replace("{txhash}", txhash.as_str())
            .replace("{logindex}", logindex.to_string().as_str());
        transfer_query = transfer_binding.as_str();

        let mut conn = self.pool.get_conn()?;
        let token_id: u64 = log.topics[3].to_low_u64_be();
        let mut results = conn.query_iter(transfer_query)?;
        if let Some(row) = results.next() {
            let count: i64 = row?.get(0).unwrap_or(0);
            if count == 0 {
                let mut new_conn = self.pool.get_conn()?;
                let insert_transfer_stmt = new_conn.prep(insert_transfer)?;
                new_conn.exec_drop(
                    &insert_transfer_stmt,
                    params! {
                        "blocknumber" => blocknumber,
                        "address" => address.clone(),
                        "from_address" =>from_address,
                        "to_address" => to_address.clone(),
                        "tokenid" =>token_id.clone(),
                        "txhash" => txhash,
                        "logindex" => logindex,
                    },
                )?;
                let update_owner_state = new_conn.prep(update_owner)?;
                new_conn.exec_drop(
                    &update_owner_state,
                    params! {
                        "address" => address,
                        "owner" => to_address,
                        "tokenid" => token_id,
                    },
                )?;
            }
        }
    }

    pub async fn insert_approval_log_db(
        &self,
        log: Log,
        query_approval: &str,
        insert_approval: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (blocknumber, address, from_address, to_address, txhash, logindex) = parselog(log);
        let mut approval_query: &str = query_approval;
        let approval_binding = approval_query
            .replace("{txhash}", txhash.as_str())
            .replace("{logindex}", logindex.to_string().as_str());
        approval_query = approval_binding.as_str();

        let mut conn = self.pool.get_conn()?;
        let token_id: u64 = log.topics[3].to_low_u64_be();
        let mut results = conn.query_iter(approval_query)?;
        if let Some(row) = results.next() {
            let count: i64 = row?.get(0).unwrap_or(0);
            if count == 0 {
                let mut new_conn = self.pool.get_conn()?;
                let insertstmt = new_conn.prep(insert_approval)?;
                new_conn.exec_drop(
                    insertstmt,
                    params! {
                        "blocknumber" => blocknumber,
                        "address" => address,
                        "owner" =>from_address,
                        "approved" => to_address,
                        "tokenid" => token_id,
                        "txhash" => txhash,
                        "logindex" => logindex,
                    },
                )?;
            }
        }
    }

    pub async fn insert_approvalforall_log_db(
        &self,
        log: Log,
        query_approvalforall: &str,
        approvalforall_event: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (blocknumber, address, from_address, to_address, txhash, logindex) = parselog(log);

        let mut approvalforall_query: &str = query_approvalforall;
        let approvalforallbinding = approvalforall_query
            .replace("{txhash}", txhash.as_str())
            .replace("{logindex}", logindex.to_string().as_str());
        approvalforall_query = approvalforallbinding.as_str();

        let mut conn = self.pool.get_conn()?;
        let mut results = conn.query_iter(approvalforall_query)?;
        if let Some(row) = results.next() {
            let count: i64 = row?.get(0).unwrap_or(0);
            if count == 0 {
                let mut new_conn = self.pool.get_conn()?;
                let insertstmt = new_conn.prep(insert_approvalforall)?;
                new_conn.exec_drop(
                insertstmt,
                params! {
                    "blocknumber" => blocknumber,
                    "address" => address,
                    "owner" =>from_address,
                    "operator" => to_address,
                    "approved" => AbiEncode::encode_hex(U256::from_big_endian(&log.data[29..32])),
                    "txhash" => txhash,
                    "logindex" => logindex,
                },
            )?;
            }
        }
    }

    fn parselog(log: Log) -> (u64, String, String, String, String, u64) {
        let blocknumber: u64 = log.block_number.unwrap().as_u64();
        let address: String = {
            let mut body = AbiEncode::encode_hex(Address::from(log.address)).split_off(26);
            body.insert_str(0, "0x");
            body
        };
        let from_address: String = {
            let mut body = AbiEncode::encode_hex(Address::from(log.topics[1])).split_off(26);
            body.insert_str(0, "0x");
            body
        };
        let to_address: String = {
            let mut body = AbiEncode::encode_hex(Address::from(log.topics[2])).split_off(26);
            body.insert_str(0, "0x");
            body
        };
        let txhash: String = AbiEncode::encode_hex(log.transaction_hash.unwrap());
        let logindex: u64 = log.log_index.unwrap().as_u64();
        (
            blocknumber,
            address,
            from_address,
            to_address,
            txhash,
            logindex,
        )
    }
}
