use zcash_protocol::consensus::BlockHeight;

#[derive(thiserror::Error, Debug)]
pub enum BlockHeightFromU64Error {
    #[error("BlockHeight {0} is too tall, over maximum u32.")]
    TooTall(u64),
}

pub fn block_height_from_u64(block_height: u64) -> Result<BlockHeight, BlockHeightFromU64Error> {
    u32::try_from(block_height)
        .map_err(|_e| BlockHeightFromU64Error::TooTall(block_height))
        .map(BlockHeight::from_u32)
}
