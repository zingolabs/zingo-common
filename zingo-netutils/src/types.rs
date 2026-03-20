use zcash_client_backend::proto::service::LightdInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    pub height: u64,
    pub hash: Vec<u8>,
}

impl From<zcash_client_backend::proto::service::BlockId> for BlockRef {
    fn from(value: zcash_client_backend::proto::service::BlockId) -> Self {
        Self {
            height: value.height,
            hash: value.hash,
        }
    }
}

impl From<BlockRef> for zcash_client_backend::proto::service::BlockId {
    fn from(value: BlockRef) -> Self {
        Self {
            height: value.height,
            hash: value.hash,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolType {
    Transparent,
    Sapling,
    Orchard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRange {
    pub start: BlockRef,
    pub end: BlockRef,
    pub pool_types: Vec<PoolType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxFilter {
    pub block: Option<BlockRef>,
    pub index: Option<u64>,
    pub hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawTransactionStatus {
    InMempool,
    Mined { height: u64 },
    MinedOnStaleChain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTransaction {
    pub data: Vec<u8>,
    pub status: RawTransactionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentTransaction {
    pub txid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub version: String,
    pub vendor: String,
    pub taddr_support: bool,
    pub chain_name: String,
    pub sapling_activation_height: u64,
    pub consensus_branch_id: String,
    pub block_height: u64,
    pub git_commit: String,
    pub branch: String,
    pub build_date: String,
    pub build_user: String,
    pub estimated_height: u64,
    pub zcashd_build: String,
    pub zcashd_subversion: String,
    pub donation_address: String,
}

impl From<LightdInfo> for ServerInfo {
    fn from(info: LightdInfo) -> Self {
        Self {
            version: info.version,
            vendor: info.vendor,
            taddr_support: info.taddr_support,
            chain_name: info.chain_name,
            sapling_activation_height: info.sapling_activation_height,
            consensus_branch_id: info.consensus_branch_id,
            block_height: info.block_height,
            git_commit: info.git_commit,
            branch: info.branch,
            build_date: info.build_date,
            build_user: info.build_user,
            estimated_height: info.estimated_height,
            zcashd_build: info.zcashd_build,
            zcashd_subversion: info.zcashd_subversion,
            donation_address: info.donation_address,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransparentAddressBlockFilter {
    pub address: String,
    pub range: BlockRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressList {
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub value_zat: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolTxRequest {
    pub exclude_txid_suffixes: Vec<Vec<u8>>,
    pub pool_types: Vec<PoolType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeState {
    pub network: String,
    pub height: u64,
    pub hash: String,
    pub time: u32,
    pub sapling_tree: String,
    pub orchard_tree: String,
}

impl From<zcash_client_backend::proto::service::TreeState> for TreeState {
    fn from(value: zcash_client_backend::proto::service::TreeState) -> Self {
        Self {
            network: value.network,
            height: value.height,
            hash: value.hash,
            time: value.time,
            sapling_tree: value.sapling_tree,
            orchard_tree: value.orchard_tree,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShieldedProtocol {
    Sapling,
    Orchard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSubtreeRootsRequest {
    pub start_index: u32,
    pub shielded_protocol: ShieldedProtocol,
    pub max_entries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtreeRoot {
    pub root_hash: Vec<u8>,
    pub completing_block_hash: Vec<u8>,
    pub completing_block_height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetAddressUtxosRequest {
    pub addresses: Vec<String>,
    pub start_height: u64,
    pub max_entries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressUtxo {
    pub address: String,
    pub txid: Vec<u8>,
    pub index: i32,
    pub script: Vec<u8>,
    pub value_zat: i64,
    pub height: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingResponse {
    pub entry: i64,
    pub exit: i64,
}

// Compact representations from compact_formats.proto.
// You may decide to keep these crate-local, or to expose a separate
// low-level "compact" module if you do not want these in the top-level API.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainMetadata {
    pub sapling_commitment_tree_size: u32,
    pub orchard_commitment_tree_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactSaplingSpend {
    pub nf: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactSaplingSpends(pub Vec<CompactSaplingSpend>);

impl From<zcash_client_backend::proto::compact_formats::CompactSaplingSpend>
    for CompactSaplingSpend
{
    fn from(value: zcash_client_backend::proto::compact_formats::CompactSaplingSpend) -> Self {
        Self { nf: value.nf }
    }
}

impl From<Vec<zcash_client_backend::proto::compact_formats::CompactSaplingSpend>>
    for CompactSaplingSpends
{
    fn from(value: Vec<zcash_client_backend::proto::compact_formats::CompactSaplingSpend>) -> Self {
        Self(value.into_iter().map(Into::into).collect())
    }
}

impl From<Vec<CompactSaplingSpend>> for CompactSaplingSpends {
    fn from(value: Vec<CompactSaplingSpend>) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactSaplingOutput {
    pub cmu: Vec<u8>,
    pub ephemeral_key: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactSaplingOutputs(pub Vec<CompactSaplingOutput>);

impl From<zcash_client_backend::proto::compact_formats::CompactSaplingOutput>
    for CompactSaplingOutput
{
    fn from(value: zcash_client_backend::proto::compact_formats::CompactSaplingOutput) -> Self {
        Self {
            cmu: value.cmu,
            ephemeral_key: value.ephemeral_key,
            ciphertext: value.ciphertext,
        }
    }
}

impl From<Vec<zcash_client_backend::proto::compact_formats::CompactSaplingOutput>>
    for CompactSaplingOutputs
{
    fn from(
        value: Vec<zcash_client_backend::proto::compact_formats::CompactSaplingOutput>,
    ) -> Self {
        Self(value.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOrchardAction {
    pub nullifier: Vec<u8>,
    pub cmx: Vec<u8>,
    pub ephemeral_key: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOrchardActions(pub Vec<CompactOrchardAction>);

impl From<zcash_client_backend::proto::compact_formats::CompactOrchardAction>
    for CompactOrchardAction
{
    fn from(value: zcash_client_backend::proto::compact_formats::CompactOrchardAction) -> Self {
        Self {
            nullifier: value.nullifier,
            cmx: value.cmx,
            ephemeral_key: value.ephemeral_key,
            ciphertext: value.ciphertext,
        }
    }
}

impl From<Vec<zcash_client_backend::proto::compact_formats::CompactOrchardAction>>
    for CompactOrchardActions
{
    fn from(
        value: Vec<zcash_client_backend::proto::compact_formats::CompactOrchardAction>,
    ) -> Self {
        Self(value.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTx {
    pub index: u64,
    pub hash: Vec<u8>,
    pub fee: u32,
    pub spends: CompactSaplingSpends,
    pub outputs: CompactSaplingOutputs,
    pub actions: CompactOrchardActions,
    pub vin: CompactTransparentInputs,
    pub vout: CompactTransparentOutputs,
}

impl From<zcash_client_backend::proto::compact_formats::CompactTx> for CompactTx {
    fn from(value: zcash_client_backend::proto::compact_formats::CompactTx) -> Self {
        Self {
            index: value.index,
            hash: value.hash,
            fee: value.fee,
            spends: value.spends.into(),
            outputs: value.outputs.into(),
            actions: value.actions.into(),
            vin: value.vin,
            vout: value.vout,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTransparentInput {
    pub prevout_hash: Vec<u8>,
    pub prevout_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTransparentInputs(pub Vec<CompactTransparentInput>);

impl From<zcash_client_backend::proto::compact_formats::CompactTransparentInput>
    for CompactTransparentInput
{
    fn from(value: zcash_client_backend::proto::compact_formats::CompactTransparentInput) -> Self {
        Self {
            prevout_hash: value.prevout_hash,
            prevout_index: value.prevout_index,
        }
    }
}

impl From<Vec<zcash_client_backend::proto::compact_formats::CompactTransparentInput>>
    for CompactTransparentInputs
{
    fn from(
        value: Vec<zcash_client_backend::proto::compact_formats::CompactTransparentInput>,
    ) -> Self {
        Self(value.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTransparentOutput {
    pub value: u64,
    pub script: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactTransparentOutputs(pub Vec<CompactTransparentOutput>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactBlock {
    pub proto_version: u32,
    pub height: u64,
    pub hash: Vec<u8>,
    pub prev_hash: Vec<u8>,
    pub time: u32,
    pub header: Vec<u8>,
    pub vtx: Vec<CompactTx>,
    pub chain_metadata: Option<ChainMetadata>,
}

impl From<zcash_client_backend::proto::compact_formats::CompactBlock> for CompactBlock {
    fn from(value: zcash_client_backend::proto::compact_formats::CompactBlock) -> Self {
        Self {
            proto_version: value.proto_version,
            height: value.height,
            hash: value.hash,
            prev_hash: value.prev_hash,
            time: value.time,
            header: value.header,
            vtx: value.vtx,
            chain_metadata: value.chain_metadata,
        }
    }
}
