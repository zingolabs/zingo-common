//! Transparent address operations for the [`Indexer`] trait.
//!
//! These methods expose transparent (t-address) balance queries, transaction
//! history, and UTXO lookups. They are gated behind the
//! `globally-public-transparent` feature because transparent data is
//! publicly visible on-chain and using these methods leaks which
//! addresses belong to the caller.

use std::future::Future;

use lightwallet_protocol::{
    Address, AddressList, Balance, GetAddressUtxosArg, GetAddressUtxosReply,
    GetAddressUtxosReplyList, RawTransaction, TransparentAddressBlockFilter,
};

pub use super::error::transparent::*;
use super::{GrpcIndexer, Indexer};

/// Extension of [`Indexer`] for transparent address operations.
///
/// All methods in this trait expose globally-public transparent data.
/// Callers can depend on the following:
///
/// - Balance and UTXO results reflect only confirmed (mined) state.
/// - Streaming results are sorted by block height.
/// - Errors are partitioned per method so callers can distinguish
///   which operation failed.
pub trait TransparentIndexer: Indexer {
    type GetTaddressTxidsError: std::error::Error;
    type GetTaddressTransactionsError: std::error::Error;
    type GetTaddressBalanceError: std::error::Error;
    type GetTaddressBalanceStreamError: std::error::Error;
    type GetAddressUtxosError: std::error::Error;
    type GetAddressUtxosStreamError: std::error::Error;

    #[cfg(not(feature = "nym"))]
    #[deprecated(note = "use get_taddress_transactions instead")]
    fn get_taddress_txids(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> impl Future<Output = Result<tonic::Streaming<RawTransaction>, Self::GetTaddressTxidsError>>;
    #[cfg(feature = "nym")]
    #[deprecated(note = "use get_taddress_transactions instead")]
    fn get_taddress_txids(
        &self,
        filter: TransparentAddressBlockFilter,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<RawTransaction>, Self::GetTaddressTxidsError>>;

    #[cfg(not(feature = "nym"))]
    fn get_taddress_transactions(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> impl Future<Output = Result<tonic::Streaming<RawTransaction>, Self::GetTaddressTransactionsError>>;
    #[cfg(feature = "nym")]
    fn get_taddress_transactions(
        &self,
        filter: TransparentAddressBlockFilter,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<RawTransaction>, Self::GetTaddressTransactionsError>>;

    #[cfg(not(feature = "nym"))]
    fn get_taddress_balance(
        &self,
        addresses: AddressList,
    ) -> impl Future<Output = Result<Balance, Self::GetTaddressBalanceError>>;
    #[cfg(feature = "nym")]
    fn get_taddress_balance(
        &self,
        addresses: AddressList,
        proxied: bool,
    ) -> impl Future<Output = Result<Balance, Self::GetTaddressBalanceError>>;

    #[cfg(not(feature = "nym"))]
    fn get_taddress_balance_stream(
        &self,
        addresses: Vec<Address>,
    ) -> impl Future<Output = Result<Balance, Self::GetTaddressBalanceStreamError>>;
    #[cfg(feature = "nym")]
    fn get_taddress_balance_stream(
        &self,
        addresses: Vec<Address>,
        proxied: bool,
    ) -> impl Future<Output = Result<Balance, Self::GetTaddressBalanceStreamError>>;

    #[cfg(not(feature = "nym"))]
    fn get_address_utxos(
        &self,
        arg: GetAddressUtxosArg,
    ) -> impl Future<Output = Result<GetAddressUtxosReplyList, Self::GetAddressUtxosError>>;
    #[cfg(feature = "nym")]
    fn get_address_utxos(
        &self,
        arg: GetAddressUtxosArg,
        proxied: bool,
    ) -> impl Future<Output = Result<GetAddressUtxosReplyList, Self::GetAddressUtxosError>>;

    #[cfg(not(feature = "nym"))]
    fn get_address_utxos_stream(
        &self,
        arg: GetAddressUtxosArg,
    ) -> impl Future<
        Output = Result<tonic::Streaming<GetAddressUtxosReply>, Self::GetAddressUtxosStreamError>,
    >;
    #[cfg(feature = "nym")]
    fn get_address_utxos_stream(
        &self,
        arg: GetAddressUtxosArg,
        proxied: bool,
    ) -> impl Future<
        Output = Result<tonic::Streaming<GetAddressUtxosReply>, Self::GetAddressUtxosStreamError>,
    >;
}

impl TransparentIndexer for GrpcIndexer {
    type GetTaddressTxidsError = GetTaddressTxidsError;
    type GetTaddressTransactionsError = GetTaddressTransactionsError;
    type GetTaddressBalanceError = GetTaddressBalanceError;
    type GetTaddressBalanceStreamError = GetTaddressBalanceStreamError;
    type GetAddressUtxosError = GetAddressUtxosError;
    type GetAddressUtxosStreamError = GetAddressUtxosStreamError;

    #[allow(deprecated)]
    #[cfg(not(feature = "nym"))]
    async fn get_taddress_txids(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> Result<tonic::Streaming<RawTransaction>, GetTaddressTxidsError> {
        let (mut client, request) = self.stream_call(filter).await?;
        Ok(client.get_taddress_txids(request).await?.into_inner())
    }
    #[allow(deprecated)]
    #[cfg(feature = "nym")]
    async fn get_taddress_txids(
        &self,
        filter: TransparentAddressBlockFilter,
        proxied: bool,
    ) -> Result<tonic::Streaming<RawTransaction>, GetTaddressTxidsError> {
        let (mut client, request) = self.stream_call_routed(filter, proxied).await?;
        Ok(client.get_taddress_txids(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_taddress_transactions(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> Result<tonic::Streaming<RawTransaction>, GetTaddressTransactionsError> {
        let (mut client, request) = self.stream_call(filter).await?;
        Ok(client
            .get_taddress_transactions(request)
            .await?
            .into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_taddress_transactions(
        &self,
        filter: TransparentAddressBlockFilter,
        proxied: bool,
    ) -> Result<tonic::Streaming<RawTransaction>, GetTaddressTransactionsError> {
        let (mut client, request) = self.stream_call_routed(filter, proxied).await?;
        Ok(client
            .get_taddress_transactions(request)
            .await?
            .into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_taddress_balance(
        &self,
        addresses: AddressList,
    ) -> Result<Balance, GetTaddressBalanceError> {
        let (mut client, request) = self.time_boxed_call(addresses).await?;
        Ok(client.get_taddress_balance(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_taddress_balance(
        &self,
        addresses: AddressList,
        proxied: bool,
    ) -> Result<Balance, GetTaddressBalanceError> {
        let (mut client, request) = self.time_boxed_call_routed(addresses, proxied).await?;
        Ok(client.get_taddress_balance(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_taddress_balance_stream(
        &self,
        addresses: Vec<Address>,
    ) -> Result<Balance, GetTaddressBalanceStreamError> {
        let mut client = self.get_client().await?;
        let stream = tokio_stream::iter(addresses);
        Ok(client
            .get_taddress_balance_stream(stream)
            .await?
            .into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_taddress_balance_stream(
        &self,
        addresses: Vec<Address>,
        proxied: bool,
    ) -> Result<Balance, GetTaddressBalanceStreamError> {
        let mut client = self.get_client_routed(proxied).await?;
        let stream = tokio_stream::iter(addresses);
        Ok(client
            .get_taddress_balance_stream(stream)
            .await?
            .into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_address_utxos(
        &self,
        arg: GetAddressUtxosArg,
    ) -> Result<GetAddressUtxosReplyList, GetAddressUtxosError> {
        let (mut client, request) = self.time_boxed_call(arg).await?;
        Ok(client.get_address_utxos(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_address_utxos(
        &self,
        arg: GetAddressUtxosArg,
        proxied: bool,
    ) -> Result<GetAddressUtxosReplyList, GetAddressUtxosError> {
        let (mut client, request) = self.time_boxed_call_routed(arg, proxied).await?;
        Ok(client.get_address_utxos(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_address_utxos_stream(
        &self,
        arg: GetAddressUtxosArg,
    ) -> Result<tonic::Streaming<GetAddressUtxosReply>, GetAddressUtxosStreamError> {
        let (mut client, request) = self.stream_call(arg).await?;
        Ok(client.get_address_utxos_stream(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_address_utxos_stream(
        &self,
        arg: GetAddressUtxosArg,
        proxied: bool,
    ) -> Result<tonic::Streaming<GetAddressUtxosReply>, GetAddressUtxosStreamError> {
        let (mut client, request) = self.stream_call_routed(arg, proxied).await?;
        Ok(client.get_address_utxos_stream(request).await?.into_inner())
    }
}
