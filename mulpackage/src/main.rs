use async_std::task;
use ethers::{
    core::types::{Address, BlockNumber, Filter, U64},
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
    // let mypool = database::MyPool::new(common::MYSQL_CONN_URL)?;
    let mypool: database::MyPool = match database::MyPool::new(common::MYSQL_CONN_URL).await {
        Ok(pool) => pool,
        Err(_) => todo!(),
    };
    let history_log_filter = Filter::new()
        .from_block(BlockNumber::Number(from_block)) //17971969
        .to_block(BlockNumber::Number(to_block)) //17971994
        .address(ethers::types::ValueOrArray::Value(
            common::TARGET_ADDRESS.parse::<Address>()?,
        ));

    let logs = client.get_logs(&history_log_filter).await?;
    for log in logs.iter() {
        let h256_str = format!("{:?}", log.topics[0]);
        // if h256_str ==
        let _tx: std::result::Result<(), Box<dyn std::error::Error>> = mypool
            .insert_log_db(
                log.clone(),
                h256_str,
                common::QUERY_TRANSFER_STATE,
                common::QUERY_APPROVAL_STATE,
                common::QUERY_APPROVALFORALL_STATE,
                common::INSERT_TRANSFER_STATE,
                common::INSERT_APPROVAL_STATE,
                common::INSERT_APPROVALFORALL_STATE,
                common::UPDATE_OWNER_STATE,
                common::TRANSFER_EVENT,
                common::APPROVAL_EVENT,
                common::APPROVALFORALL_EVENT,
            )
            .await;
    }
    Ok(())
}

async fn subscribe_new_logs(client: Arc<Provider<Ws>>, from_block: U64) -> Result<()> {
    // let mypool = database::MyPool::new(common::MYSQL_CONN_URL)?;
    let mypool: database::MyPool = match database::MyPool::new(common::MYSQL_CONN_URL).await {
        Ok(pool) => pool,
        Err(_) => todo!(),
    };
    let subscribe_log_filter = Filter::new()
        .from_block(BlockNumber::Number(from_block))
        .address(ethers::types::ValueOrArray::Value(
            common::TARGET_ADDRESS.parse::<Address>()?,
        ));

    let mut logs = client.subscribe_logs(&subscribe_log_filter).await?;
    while let Some(log) = logs.next().await {
        let h256_str = format!("{:?}", log.topics[0]);
        let _tx: std::result::Result<(), Box<dyn std::error::Error>> = mypool
            .insert_log_db(
                log.clone(),
                h256_str,
                common::QUERY_TRANSFER_STATE,
                common::QUERY_APPROVAL_STATE,
                common::QUERY_APPROVALFORALL_STATE,
                common::INSERT_TRANSFER_STATE,
                common::INSERT_APPROVAL_STATE,
                common::INSERT_APPROVALFORALL_STATE,
                common::UPDATE_OWNER_STATE,
                common::TRANSFER_EVENT,
                common::APPROVAL_EVENT,
                common::APPROVALFORALL_EVENT,
            )
            .await;
    }
    Ok(())
}

async fn get_ws_client() -> Provider<Ws> {
    Provider::<Ws>::connect(common::WSS_URL).await.unwrap()
}
