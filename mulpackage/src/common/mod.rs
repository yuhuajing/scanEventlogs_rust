pub const TARGET_ADDRESS: &str = "0xff2B4721F997c242fF406a626f17df083Bd2C568";
pub const WSS_URL: &str = "wss://eth.getblock.io/ab0b1aa0-b490-4dc0-9bda-817c897a4580/mainnet";
pub const FROM_BLOCK: u64 = 17971966;
pub const TRANSFER_EVENT: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const APPROVAL_EVENT: &str =
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
pub const APPROVALFORALL_EVENT: &str =
    "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31";

// mysql
// pub const MYSQL_CONN_URL: &str = "mysql://root:123456@localhost:3306/testUser";

// mysql create table
pub const CRERATE_TABLE_TRANSFER: &str = r"CREATE TABLE IF NOT EXISTS transfer (
    blocknumber int not null,
    address text,
    from_address text,
    to_address text,
    tokenid int,
    txhash text,
    logindex int
)";
pub const CRERATE_TABLE_APPROVAL: &str = r"CREATE TABLE IF NOT EXISTS approval (
    blocknumber int not null,
    address text,
    owner text,
    approved text,
    tokenid int,
    txhash text,
    logindex int
)";
pub const CRERATE_TABLE_APPROVALFORALL: &str = r"CREATE TABLE IF NOT EXISTS approvalforall (
    blocknumber int not null,
    address text,
    owner text,
    operator text,
    approved text,
    txhash text,
    logindex int
)";
pub const CRERATE_TABLE_OWNER: &str = r"CREATE TABLE IF NOT EXISTS owner (
    address text,
    owner text,
    tokenid int
)";

//query
pub const QUERY_OWNER_STATE: &str = r"SELECT count(*) from owner where address='{address}'";
pub const QUERY_TRANSFER_STATE: &str =
    r"SELECT count(*) from transfer where txhash='{txhash}' and logindex={logindex}";
pub const QUERY_TRANSFER_LATESTBLOCK_STATE: &str =
    r"SELECT blocknumber from transfer where address='{address}' order by blocknumber desc limit 1";
pub const QUERY_APPROVAL_STATE: &str =
    r"SELECT count(*) from approval where txhash='{txhash}' and logindex={logindex}";
pub const QUERY_APPROVALFORALL_STATE: &str =
    r"SELECT count(*) from approvalforall where txhash='{txhash}' and logindex={logindex}";
//insert
pub const INSERT_OWNER_STATE: &str =
    r"INSERT INTO owner (address, tokenid) VALUES (:address, :tokenid)";
pub const INSERT_TRANSFER_STATE: &str = r"INSERT INTO transfer (blocknumber, address, from_address, to_address, tokenid, txhash, logindex)
VALUES (:blocknumber, :address, :from_address, :to_address, :tokenid, :txhash, :logindex)";
pub const INSERT_APPROVAL_STATE: &str = r"INSERT INTO approval (blocknumber, address, owner, approved, tokenid, txhash, logindex)
VALUES (:blocknumber, :address, :owner, :approved, :tokenid, :txhash, :logindex)";
pub const INSERT_APPROVALFORALL_STATE: &str = r"INSERT INTO approvalforall (blocknumber, address, owner, operator, approved, txhash, logindex)
VALUES (:blocknumber, :address, :owner, :operator,:approved, :txhash, :logindex)";
//update
pub const UPDATE_OWNER_STATE: &str =
    r"update owner set owner=:owner where address=:address and tokenid=:tokenid";
