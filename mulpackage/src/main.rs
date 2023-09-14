use async_std::task;
use ethers::{
    abi::AbiEncode,
    core::types::{Address, BlockNumber, Filter, Log, U256, U64},
    providers::{Middleware, Provider, StreamExt, Ws},
};
use eyre::Result;
use futures::future::join_all;
use mysql::prelude::*;
use mysql::*;
use std::string::String;
use std::sync::Arc;
mod common;

#[tokio::main]
async fn main() -> Result<()> {
    let address: String = common::TARGET_ADDRESS.to_string().to_lowercase();
    let _t: std::result::Result<(), Box<dyn std::error::Error>> =
        create_table_insert_owner(address.clone()).await;
    let client = Arc::new(get_ws_client().await);
    let db_block: u64 = match query_db_latest_blocknum(common::TARGET_ADDRESS).await {
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
        let h256_str = format!("{:?}", log.topics[0]);
        let _tx: std::result::Result<(), Box<dyn std::error::Error>> =
            insert_log_db(log.clone(), h256_str).await;
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
        let h256_str = format!("{:?}", log.topics[0]);
        let _tx: std::result::Result<(), Box<dyn std::error::Error>> =
            insert_log_db(log.clone(), h256_str).await;
    }
    Ok(())
}

async fn get_ws_client() -> Provider<Ws> {
    Provider::<Ws>::connect(common::WSS_URL).await.unwrap()
}

// async fn get_db_conn() -> std::result::Result<PooledConn, Box<dyn std::error::Error>> {
//     let mysql_url: &str = common::MYSQL_CONN_URL;
//     let pool = Pool::new(mysql_url)?;
//     let mut conn = pool.get_conn()?;
//     Ok(conn)
// }

async fn get_pool() -> std::result::Result<Pool, Box<dyn std::error::Error>> {
    let mysql_url: &str = common::MYSQL_CONN_URL;
    let pool = Pool::new(mysql_url)?;
    // let mut conn = pool.get_conn()?;
    Ok(pool)
}

async fn create_table_insert_owner(
    address: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    match get_pool().await {
        Ok(pool) => {
            //  let create_owner_state = conn.clone().prep(common::CRERATE_TABLE_OWNER)?;
            let mut conn = pool.get_conn()?;
            match conn.query_drop(common::CRERATE_TABLE_TRANSFER) {
                Ok(_) => {
                    println!("Transfer table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            //let create_approval_state = conn.clone().prep(common::CRERATE_TABLE_APPROVAL)?;
            match conn.query_drop(common::CRERATE_TABLE_APPROVAL) {
                Ok(_) => {
                    println!("Approval table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            //  let create_approvalforall_state = conn.clone().prep(common::CRERATE_TABLE_APPROVALFORALL)?;
            match conn.query_drop(common::CRERATE_TABLE_APPROVALFORALL) {
                Ok(_) => {
                    println!("ApprovalForAll table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            //  let create_owner_state = conn.clone().prep(common::CRERATE_TABLE_OWNER)?;
            match conn.query_drop(common::CRERATE_TABLE_OWNER) {
                Ok(_) => {
                    println!("Owner table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            let mut query_owner_state: &str = common::QUERY_OWNER_STATE;
            let binding = query_owner_state.replace("{address}", common::TARGET_ADDRESS);
            query_owner_state = binding.as_str();
            let mut results = conn.query_iter(query_owner_state)?;
            if let Some(row) = results.next() {
                let count: i64 = row?.get(0).unwrap_or(0);
                if count == 0 {
                    //drop(con n); // 释放 conn 引用
                    let mut new_conn = pool.get_conn()?;
                    for i in (0..=514).rev() {
                        let insert_owner_state = new_conn.prep(common::INSERT_OWNER_STATE)?;
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
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(())
}

async fn insert_log_db(
    log: Log,
    topic: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
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

    let mut transfer_query: &str = common::QUERY_TRANSFER_STATE;
    let transfer_binding = transfer_query
        .replace("{txhash}", txhash.as_str())
        .replace("{logindex}", logindex.to_string().as_str());
    transfer_query = transfer_binding.as_str();

    let mut approval_query: &str = common::QUERY_APPROVAL_STATE;
    let approval_binding = approval_query
        .replace("{txhash}", txhash.as_str())
        .replace("{logindex}", logindex.to_string().as_str());
    approval_query = approval_binding.as_str();

    let mut approvalforall_query: &str = common::QUERY_APPROVALFORALL_STATE;
    let approvalforallbinding = approvalforall_query
        .replace("{txhash}", txhash.as_str())
        .replace("{logindex}", logindex.to_string().as_str());
    approvalforall_query = approvalforallbinding.as_str();

    match get_pool().await {
        Ok(pool) => match topic.as_str() {
            common::TRANSFER_EVENT => {
                println!("transfer_event");
                let mut conn = pool.get_conn()?;
                let token_id: u64 = log.topics[3].to_low_u64_be();
                let mut results = conn.query_iter(transfer_query)?;
                if let Some(row) = results.next() {
                    let count: i64 = row?.get(0).unwrap_or(0);
                    if count == 0 {
                        // drop(conn); // 释放 conn 引用
                        let mut new_conn = pool.get_conn()?;
                        let insert_transfer_stmt = new_conn.prep(common::INSERT_TRANSFER_STATE)?;
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
                        let update_owner_state = new_conn.prep(common::UPDATE_OWNER_STATE)?;
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
            common::APPROVAL_EVENT => {
                println!("approval_event");
                let mut conn = pool.get_conn()?;
                let token_id: u64 = log.topics[3].to_low_u64_be();
                let mut results = conn.query_iter(approval_query)?;
                if let Some(row) = results.next() {
                    let count: i64 = row?.get(0).unwrap_or(0);
                    if count == 0 {
                        //drop(conn); // 释放 conn 引用
                        let mut new_conn = pool.get_conn()?;
                        let insertstmt = new_conn.prep(common::INSERT_APPROVAL_STATE)?;
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
            common::APPROVALFORALL_EVENT => {
                println!("approval_for_all_event");
                let mut conn = pool.get_conn()?;
                let mut results = conn.query_iter(approvalforall_query)?;
                if let Some(row) = results.next() {
                    let count: i64 = row?.get(0).unwrap_or(0);
                    if count == 0 {
                        //drop(conn); // 释放 conn 引用
                        let mut new_conn = pool.get_conn()?;
                        let insertstmt = new_conn.prep(common::INSERT_APPROVALFORALL_STATE)?;
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
            _ => {
                println!("Other")
            }
        },
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(())
}

async fn query_db_latest_blocknum(
    target_address: &str,
) -> std::result::Result<u64, Box<dyn std::error::Error>> {
    match get_pool().await {
        Ok(pool) => {
            let mut conn = pool.get_conn()?;
            let mut query_state: &str = common::QUERY_TRANSFER_LATESTBLOCK_STATE;
            let binding = query_state.replace("{address}", target_address);
            query_state = binding.as_str();
            let mut results = conn.query_iter(query_state)?;
            if let Some(row) = results.next() {
                let blocknumber: u64 = row?.get(0).unwrap_or(0);
                return Ok(blocknumber + 1);
            }
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(0)
}
