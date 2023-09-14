use ethers::{
    abi::AbiEncode,
    core::types::{Address, Log, U256},
};
use mysql::prelude::*;
use mysql::*;
use std::string::String;
// mysql
pub const MYSQL_CONN_URL: &str = "mysql://root:123456@localhost:3306/testUser";
// mysql create table

pub async fn get_pool() -> std::result::Result<Pool, Box<dyn std::error::Error>> {
    let mysql_url: &str = MYSQL_CONN_URL;
    let pool = Pool::new(mysql_url)?;
    // let mut conn = pool.get_conn()?;
    Ok(pool)
}

pub async fn query_db_latest_blocknum(
    target_address: &str,
    query_latest_block_stat: &str,
) -> std::result::Result<u64, Box<dyn std::error::Error>> {
    match get_pool().await {
        Ok(pool) => {
            let mut conn = pool.get_conn()?;
            let mut query_state: &str = query_latest_block_stat;
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

pub async fn create_table_insert_owner(
    address: String,
    create_transfer: &str,
    create_approval: &str,
    create_approvalforall: &str,
    create_owner: &str,
    query_owner: &str,
    insert_owner: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    match get_pool().await {
        Ok(pool) => {
            //  let create_owner_state = conn.clone().prep(CRERATE_TABLE_OWNER)?;
            let mut conn = pool.get_conn()?;
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
                    //drop(con n); // 释放 conn 引用
                    let mut new_conn = pool.get_conn()?;
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
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(())
}

pub async fn insert_log_db(
    log: Log,
    topic: String,
    query_transfer: &str,
    query_approval: &str,
    query_approvalforall: &str,
    insert_transfer: &str,
    insert_approval: &str,
    insert_approvalforall: &str,
    update_owner: &str,
    transfer_event: &str,
    approval_event: &str,
    approvalforall_event: &str,
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

    let mut transfer_query: &str = query_transfer;
    let transfer_binding = transfer_query
        .replace("{txhash}", txhash.as_str())
        .replace("{logindex}", logindex.to_string().as_str());
    transfer_query = transfer_binding.as_str();

    let mut approval_query: &str = query_approval;
    let approval_binding = approval_query
        .replace("{txhash}", txhash.as_str())
        .replace("{logindex}", logindex.to_string().as_str());
    approval_query = approval_binding.as_str();

    let mut approvalforall_query: &str = query_approvalforall;
    let approvalforallbinding = approvalforall_query
        .replace("{txhash}", txhash.as_str())
        .replace("{logindex}", logindex.to_string().as_str());
    approvalforall_query = approvalforallbinding.as_str();

    match get_pool().await {
        Ok(pool) => match topic.as_str() {
            transfer_event => {
                println!("transfer_event");
                let mut conn = pool.get_conn()?;
                let token_id: u64 = log.topics[3].to_low_u64_be();
                let mut results = conn.query_iter(transfer_query)?;
                if let Some(row) = results.next() {
                    let count: i64 = row?.get(0).unwrap_or(0);
                    if count == 0 {
                        // drop(conn); // 释放 conn 引用
                        let mut new_conn = pool.get_conn()?;
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
            approval_event => {
                println!("approval_event");
                let mut conn = pool.get_conn()?;
                let token_id: u64 = log.topics[3].to_low_u64_be();
                let mut results = conn.query_iter(approval_query)?;
                if let Some(row) = results.next() {
                    let count: i64 = row?.get(0).unwrap_or(0);
                    if count == 0 {
                        //drop(conn); // 释放 conn 引用
                        let mut new_conn = pool.get_conn()?;
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
            approvalforall_event => {
                println!("approval_for_all_event");
                let mut conn = pool.get_conn()?;
                let mut results = conn.query_iter(approvalforall_query)?;
                if let Some(row) = results.next() {
                    let count: i64 = row?.get(0).unwrap_or(0);
                    if count == 0 {
                        //drop(conn); // 释放 conn 引用
                        let mut new_conn = pool.get_conn()?;
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

// pub async fn get_db_conn() -> std::result::Result<PooledConn, Box<dyn std::error::Error>> {
//     let mysql_url: &str = MYSQL_CONN_URL;
//     let pool = Pool::new(mysql_url)?;
//     let mut conn = pool.get_conn()?;
//     Ok(conn)
// }
