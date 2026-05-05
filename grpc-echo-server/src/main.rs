use lightwallet_protocol::{
    AddressList, Balance, BlockId, BlockRange, ChainSpec, CompactBlock, CompactTx, Empty,
    GetAddressUtxosArg, GetAddressUtxosReply, GetAddressUtxosReplyList, GetMempoolTxRequest,
    GetSubtreeRootsArg, LightdInfo, PingResponse, RawTransaction, SendResponse, SubtreeRoot,
    TransparentAddressBlockFilter, TreeState, TxFilter,
    compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer},
};
use tonic::{Request, Response, Status, Streaming, transport::Server};
use tracing::info;

struct EchoServer;

type Unimpl<T> = tokio_stream::Empty<Result<T, Status>>;

#[tonic::async_trait]
impl CompactTxStreamer for EchoServer {
    type GetBlockRangeStream = Unimpl<CompactBlock>;
    type GetBlockRangeNullifiersStream = Unimpl<CompactBlock>;
    type GetTaddressTxidsStream = Unimpl<RawTransaction>;
    type GetTaddressTransactionsStream = Unimpl<RawTransaction>;
    type GetMempoolTxStream = Unimpl<CompactTx>;
    type GetMempoolStreamStream = Unimpl<RawTransaction>;
    type GetSubtreeRootsStream = Unimpl<SubtreeRoot>;
    type GetAddressUtxosStreamStream = Unimpl<GetAddressUtxosReply>;

    async fn get_lightd_info(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<LightdInfo>, Status> {
        let peer = request.remote_addr();
        info!(peer = ?peer, "GetLightdInfo");
        let peer_str = peer.map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string());
        Ok(Response::new(LightdInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            vendor: format!("grpc-echo-server peer={}", peer_str),
            chain_name: "echo".to_string(),
            block_height: 1,
            ..Default::default()
        }))
    }

    async fn get_latest_block(
        &self,
        _: Request<ChainSpec>,
    ) -> Result<Response<BlockId>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_block(
        &self,
        _: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_block_nullifiers(
        &self,
        _: Request<BlockId>,
    ) -> Result<Response<CompactBlock>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_block_range(
        &self,
        _: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_block_range_nullifiers(
        &self,
        _: Request<BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_transaction(
        &self,
        _: Request<TxFilter>,
    ) -> Result<Response<RawTransaction>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn send_transaction(
        &self,
        _: Request<RawTransaction>,
    ) -> Result<Response<SendResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_taddress_txids(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_taddress_transactions(
        &self,
        _: Request<TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_taddress_balance(
        &self,
        _: Request<AddressList>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_taddress_balance_stream(
        &self,
        _: Request<Streaming<lightwallet_protocol::Address>>,
    ) -> Result<Response<Balance>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_mempool_tx(
        &self,
        _: Request<GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_mempool_stream(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_tree_state(
        &self,
        _: Request<BlockId>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_latest_tree_state(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<TreeState>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_subtree_roots(
        &self,
        _: Request<GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_address_utxos(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<GetAddressUtxosReplyList>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn get_address_utxos_stream(
        &self,
        _: Request<GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn ping(
        &self,
        _: Request<lightwallet_protocol::Duration>,
    ) -> Result<Response<PingResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let addr = std::env::var("ECHO_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8137".to_string())
        .parse()?;

    info!(%addr, "grpc-echo-server listening");

    Server::builder()
        .add_service(CompactTxStreamerServer::new(EchoServer))
        .serve(addr)
        .await?;

    Ok(())
}
