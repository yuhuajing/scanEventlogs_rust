use async_std::task;
use ethers::{
    abi::AbiEncode,
    core::types::{Address, BlockNumber, Filter, Log, H160, H256, U256, U64},
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
//const WSS_URL: &str = "wss://eth.getblock.io/ab0b1aa0-b490-4dc0-9bda-817c897a4580/mainnet";

#[tokio::main]
async fn main() -> Result<()> {
    let address: String = "0xff2B4721F997c242fF406a626f17df083Bd2C568"
        .to_string()
        .to_lowercase();

    let _t: std::result::Result<(), Box<dyn std::error::Error>> =
        create_table_insert_owner(address.clone()).await;

    // let customer_id: i32 = 19;
    //let address: String = "0xff2B4721F997c242fF406a626f17df083Bd2C568".to_string();
    // let _t: std::result::Result<(), Box<dyn std::error::Error>> =
    //     query_try_insert_db(customer_id, 0, account_name).await;

    let client = Arc::new(get_ws_client().await);

    // let last_block: U64 = client
    //     .clone()
    //     .get_block(BlockNumber::Latest)
    //     .await?
    //     .unwrap()
    //     .number
    //     .unwrap();

    let db_block: u64 = match query_db_latest_blocknum(
        0,
        address.clone(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        0,
    )
    .await
    {
        Ok(blocknumber) => blocknumber,
        Err(_) => todo!(),
    };

    let mut from_block: U64 = U64::default();

    if db_block == 0 {
        from_block = U64::from(17971966);
    } else {
        from_block = U64::from(db_block);
    }

    println!("{}", from_block.as_u64());

    // let _t: std::result::Result<(), Box<dyn std::error::Error>> = update_owner_db(
    //     address,
    //     1,
    //     "0x1E0049783F008A0085193E00003D00cd54003c71".to_string(),
    // )
    // .await;

    let tasks = vec![
        task::spawn(get_history_logs(client.clone(), from_block)),
        // task::spawn(getbalance(client.clone())),
    ];
    join_all(tasks).await;

    Ok(())
}

async fn get_history_logs(client: Arc<Provider<Ws>>, from_block: U64) -> Result<()> {
    let history_log_filter = Filter::new()
        .from_block(BlockNumber::Number(from_block)) //17971969
        .to_block(17971994)
        //.to_block(BlockNumber::Number(to_block)) //17971994
        //  .event("Transfer(address,address,uint256)")
        .address(ethers::types::ValueOrArray::Value(
            WETH_ADDRESS.parse::<Address>()?,
        ));

    let logs = client.get_logs(&history_log_filter).await?;
    for log in logs.iter() {
        let h256_str = format!("{:?}", log.topics[0]);
        let _tx: std::result::Result<(), Box<dyn std::error::Error>> =
            insert_log_db(log.clone(), h256_str).await;
    }
    println!("{} tx found!", logs.iter().len());
    Ok(())
}

async fn subscribe_new_logs(client: Arc<Provider<Ws>>, from_block: U64) -> Result<()> {
    let subscribe_log_filter = Filter::new()
        .from_block(BlockNumber::Number(from_block))
        //  .event("Transfer(address,address,uint256)")
        .address(ethers::types::ValueOrArray::Value(
            WETH_ADDRESS.parse::<Address>()?,
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
    Provider::<Ws>::connect(WSS_URL).await.unwrap()
}

async fn get_db_conn() -> std::result::Result<PooledConn, Box<dyn std::error::Error>> {
    let mysql_url: &str = "mysql://root:123456@localhost:3306/testUser";
    let pool = Pool::new(mysql_url)?;
    let mut conn = pool.get_conn()?;
    Ok(conn)
}

async fn create_table_insert_owner(
    address: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    match get_db_conn().await {
        Ok(mut conn) => {
            match conn.query_drop(
                r"CREATE TABLE IF NOT EXISTS transfer (
                    blocknumber int not null,
                    address text,
                    from_address text,
                    to_address text,
                    tokenid int,
                    txhash text,
                    logindex int
                )",
            ) {
                Ok(_) => {
                    println!("Transfer table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            match conn.query_drop(
                r"CREATE TABLE IF NOT EXISTS approval (
                    blocknumber int not null,
                    address text,
                    owner text,
                    approved text,
                    tokenid int,
                    txhash text,
                    logindex int
                )",
            ) {
                Ok(_) => {
                    println!("Approval table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            match conn.query_drop(
                r"CREATE TABLE IF NOT EXISTS approvalforall (
                    blocknumber int not null,
                    address text,
                    owner text,
                    operator text,
                    approved text,
                    txhash text,
                    logindex int
                )",
            ) {
                Ok(_) => {
                    println!("ApprovalForAll table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            match conn.query_drop(
                r"CREATE TABLE IF NOT EXISTS owner (
                    address text,
                    owner text,
                    tokenid int
                )",
            ) {
                Ok(_) => {
                    println!("Owner table existed_or_created successfully");
                }
                Err(err) => {
                    eprintln!("Error creating transfer table: {:?}", err);
                }
            }

            let query_seq = format!("SELECT * from owner where address='{}'", address);

            let val: Vec<Owner> = conn.query_map(
                //"SELECT customer_id, amount, account_name from payment where account_name='clay'",
                query_seq,
                |(address, owner, tokenid)| Owner {
                    address,
                    owner,
                    tokenid,
                },
            )?;

            if val.iter().len() == 0 {
                for i in (0..=514).rev() {
                    conn.exec_drop(
                        r"INSERT INTO owner (address, tokenid)
                VALUES (:address, :tokenid)",
                        params! {
                            "address" => address.clone(),
                            "owner"=> String::default(),
                            "tokenid" => i,
                        },
                    )?;
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
    let TRANSFER_EVENT: String =
        String::from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    let APPROVAL_EVENT: String =
        String::from("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925");
    let APPROVALFORALL_EVENT: String =
        String::from("0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31");
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
    let transfer_query = format!(
        "SELECT * from transfer where txhash='{}' and logindex={}",
        txhash, logindex
    );
    let approval_query = format!(
        "SELECT * from approval where txhash='{}' and logindex={}",
        txhash, logindex
    );
    let approvalforall_query = format!(
        "SELECT * from approvalforall where txhash='{}' and logindex={}",
        txhash, logindex
    );

    match get_db_conn().await {
        Ok(mut conn) => {
            match topic.as_str() {
                // match topic.as_str() {
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => {
                    let updatestmt = conn.prep(
                        r"update owner set owner=:owner where address=:address and tokenid=:tokenid",
                    )?;
                    let token_id: u64 = log.topics[3].to_low_u64_be();
                    let val: Vec<Transfer> = conn.query_map(
                        transfer_query,
                        |(
                            blocknumber,
                            address,
                            from_address,
                            to_address,
                            tokenid,
                            txhash,
                            logindex,
                        )| {
                            Transfer {
                                blocknumber,
                                address,
                                from_address,
                                to_address,
                                tokenid,
                                txhash,
                                logindex,
                            }
                        },
                    )?;

                    if val.is_empty() {
                        // transfer
                        conn.exec_drop(
                    r"INSERT INTO transfer (blocknumber, address, from_address, to_address, tokenid, txhash, logindex)
            VALUES (:blocknumber, :address, :from_address, :to_address, :tokenid, :txhash, :logindex)",
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
                        conn.exec_drop(
                            &updatestmt,
                        params! {
                            "address" => address,
                            "owner" => to_address,
                            "tokenid" => token_id,
                        },
                    )?;
                    }
                }
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925" => {
                    let token_id: u64 = log.topics[3].to_low_u64_be();
                    let val: Vec<Approval> = conn.query_map(
                        approval_query,
                        |(blocknumber, address, owner, approved, tokenid, txhash, logindex)| {
                            Approval {
                                blocknumber,
                                address,
                                owner,
                                approved,
                                tokenid,
                                txhash,
                                logindex,
                            }
                        },
                    )?;

                    if val.is_empty() {
                        conn.exec_drop(
                        r"INSERT INTO approval (blocknumber, address, owner, approved, tokenid, txhash, logindex)
                VALUES (:blocknumber, :address, :owner, :approved, :tokenid, :txhash, :logindex)",
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
                "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31" => {
                    let val: Vec<ApprovalForAll> = conn.query_map(
                        approvalforall_query,
                        |(blocknumber, address, owner, operator, approved, txhash, logindex)| {
                            ApprovalForAll {
                                blocknumber,
                                address,
                                owner,
                                operator,
                                approved,
                                txhash,
                                logindex,
                            }
                        },
                    )?;

                    if val.is_empty() {
                        conn.exec_drop(
                        r"INSERT INTO approvalforall (blocknumber, address, owner, operator, approved, txhash, logindex)
                VALUES (:blocknumber, :address, :owner, :operator,:approved, :txhash, :logindex)",
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
                _ => {
                    println!("Other")
                }
            }
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(())
}

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
    address: Option<String>,
    // func: Option<String>,
    from_address: Option<String>,
    to_address: Option<String>,
    tokenid: u64,
    txhash: Option<String>,
    logindex: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Approval {
    blocknumber: u64,
    //timestamp: Option<String>,
    address: Option<String>,
    //  func: Option<String>,
    owner: Option<String>,
    approved: Option<String>,
    tokenid: u64,
    txhash: Option<String>,
    logindex: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct ApprovalForAll {
    blocknumber: u64,
    //  timestamp: Option<String>,
    address: Option<String>,
    //func: Option<String>,
    owner: Option<String>,
    operator: Option<String>,
    approved: Option<String>,
    txhash: Option<String>,
    logindex: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Owner {
    address: Option<String>,
    owner: Option<String>,
    tokenid: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

async fn query_db_latest_blocknum(
    _blocknumber: u64,
    address: String,
    _owner: String,
    _operator: String,
    _approved: String,
    _txhash: String,
    _logindex: u64,
) -> std::result::Result<u64, Box<dyn std::error::Error>> {
    let query_seq = format!(
        "SELECT * from transfer where address='{}' order by blocknumber desc limit 1",
        address
    );

    match get_db_conn().await {
        Ok(mut conn) => {
            let val: Vec<Transfer> = conn.query_map(
                query_seq,
                |(blocknumber, address, from_address, to_address, tokenid, txhash, logindex)| {
                    Transfer {
                        blocknumber,
                        address,
                        from_address,
                        to_address,
                        tokenid,
                        txhash,
                        logindex,
                    }
                },
            )?;

            if !val.is_empty() {
                for log in val.iter() {
                    let blocknumber = log.blocknumber;
                    return Ok(blocknumber + 1);
                }
            }
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }

    Ok(0)
}

async fn query_try_insert_db(
    customer_id: i32,
    amount: i32,
    account_name: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // let query_seq = format!(
    //     "SELECT customer_id, amount, account_name from payment where customer_id={} and account_name='{}'",
    //     customer_id,
    //     account_name
    // );
    let query_seq = format!(
        "SELECT customer_id, amount, account_name from payment where account_name='{}' order by customer_id desc limit 1",
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

async fn update_owner_db(
    address: String,
    token_id: u64,
    to_address: String,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    match get_db_conn().await {
        Ok(mut conn) => {
            let updatestmt = conn.prep(
                r"update owner set owner=:owner where address=:address and tokenid=:tokenid",
            )?;
            conn.exec_drop(
                &updatestmt,
                params! {
                    "address" => address,
                    "owner" => to_address,
                    "tokenid" => token_id,
                },
            )?;
        //     conn.exec_drop(
        //         r"INSERT INTO owner (address, tokenid)
        // VALUES (:address, :tokenid)",
        //         params! {
        //             "address" => address.clone(),
        //             "owner"=> String::default(),
        //             "tokenid" => i,
        //         },
        //     )?;
        }
        Err(err) => {
            eprintln!("Error: {:?}", err);
        }
    }
    Ok(())
}
