//! `zingo-netutils`
//!
//! This crate provides the [`IndexerClient`] trait for communicating with a Zcash
//! chain indexer, and [`GrpcIndexerClient`], a concrete implementation that
//! connects to a lightwalletd-compatible server via gRPC.

pub mod types;

use std::future::Future;
use std::time::Duration;

use futures_core::Stream;
use futures_core::stream::BoxStream;
use tonic::Request;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
use zcash_client_backend::proto::service::{BlockId, ChainSpec, Empty};

use crate::types::{
    Address, AddressList, AddressUtxo, Balance, BlockRange, BlockRef, CompactBlock, CompactTx,
    GetAddressUtxosRequest, GetSubtreeRootsRequest, MempoolTxRequest, PingResponse, RawTransaction,
    ServerInfo, SubtreeRoot, TransparentAddressBlockFilter, TreeState, TxFilter,
};

const DEFAULT_GRPC_TIMEOUT: Duration = Duration::from_secs(10);

/// The successful result of transaction submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentTransaction {
    pub txid: String,
}

/// Error type for [`GrpcIndexerClient`] construction and transport setup.
#[derive(Debug, thiserror::Error)]
pub enum GetClientError {
    #[error("bad uri: invalid scheme")]
    InvalidScheme,

    #[error("bad uri: invalid authority")]
    InvalidAuthority,

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Unified error type for indexer client operations.
///
/// The public trait uses one semantic error type rather than leaking per-RPC
/// tonic/protobuf details into callers.
#[derive(Debug, thiserror::Error)]
pub enum IndexerClientError {
    #[error(transparent)]
    GetClient(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("send rejected: {0}")]
    SendRejected(String),
}

fn client_tls_config() -> ClientTlsConfig {
    // Allow self-signed certs in tests.
    #[cfg(test)]
    {
        ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(
                std::fs::read("test-data/localhost.pem").expect("test file"),
            ))
            .with_webpki_roots()
    }
    #[cfg(not(test))]
    ClientTlsConfig::new().with_webpki_roots()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallTimeouts {
    pub get_info: Duration,
    pub get_latest_block: Duration,
    pub send_transaction: Duration,
    pub get_tree_state: Duration,
}

impl CallTimeouts {
    pub const fn new(all: Duration) -> Self {
        Self {
            get_info: all,
            get_latest_block: all,
            send_transaction: all,
            get_tree_state: all,
        }
    }

    pub const fn with_get_info(mut self, timeout: Duration) -> Self {
        self.get_info = timeout;
        self
    }

    pub const fn with_get_latest_block(mut self, timeout: Duration) -> Self {
        self.get_latest_block = timeout;
        self
    }

    pub const fn with_send_transaction(mut self, timeout: Duration) -> Self {
        self.send_transaction = timeout;
        self
    }

    pub const fn with_get_tree_state(mut self, timeout: Duration) -> Self {
        self.get_tree_state = timeout;
        self
    }
}

impl Default for CallTimeouts {
    fn default() -> Self {
        Self::new(DEFAULT_GRPC_TIMEOUT)
    }
}

/// Trait for communicating with a Zcash chain indexer.
///
/// This trait mirrors the canonical `CompactTxStreamer` service surface while
/// exposing crate-local semantic types instead of protobuf-generated transport
/// types. That keeps the rest of the codebase decoupled from the wire format
/// and makes mocking and testing easier.
pub trait IndexerClient {
    /// The error type returned by this client.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Stream of compact blocks.
    type BlockStream: Stream<Item = Result<CompactBlock, Self::Error>>;

    /// Stream of full raw transactions.
    type RawTransactionStream: Stream<Item = Result<RawTransaction, Self::Error>>;

    /// Stream of compact transactions.
    type CompactTxStream: Stream<Item = Result<CompactTx, Self::Error>>;

    /// Stream of subtree roots for a note commitment tree.
    type SubtreeRootStream: Stream<Item = Result<SubtreeRoot, Self::Error>>;

    /// Stream of transparent UTXOs.
    type AddressUtxoStream: Stream<Item = Result<AddressUtxo, Self::Error>>;

    /// Returns the block identifier of the block at the tip of the best chain.
    fn get_latest_block(&self) -> impl Future<Output = Result<BlockRef, Self::Error>>;

    /// Returns information about this lightwalletd instance and the state of
    /// the blockchain.
    fn get_lightd_info(&self) -> impl Future<Output = Result<ServerInfo, Self::Error>>;

    /// Returns the compact block corresponding to the given block identifier.
    ///
    /// Compact blocks contain the minimum block and transaction data needed by a
    /// wallet to detect relevant shielded activity, update witnesses, and, when
    /// provided by the server, detect transparent UTXOs relevant to the wallet.
    ///
    /// Compact transactions may include transparent inputs (`vin`) and outputs
    /// (`vout`) in addition to shielded data.
    fn get_block(&self, block: BlockRef)
    -> impl Future<Output = Result<CompactBlock, Self::Error>>;

    /// Returns a compact block containing only shielded nullifier information.
    ///
    /// Transparent transaction data, Sapling outputs, full Orchard action
    /// data, and commitment tree sizes are not included.
    ///
    /// Deprecated in the protocol; prefer [`Self::get_block_range`] with the
    /// appropriate pool filters.
    #[deprecated(note = "Protocol-deprecated; prefer get_block_range with pool filters")]
    fn get_block_nullifiers(
        &self,
        block: BlockRef,
    ) -> impl Future<Output = Result<CompactBlock, Self::Error>>;

    /// Returns a stream of consecutive compact blocks in the specified range.
    ///
    /// The range is inclusive of `range.end`. If `range.start <= range.end`,
    /// blocks are returned in increasing height order; otherwise they are
    /// returned in decreasing height order.
    ///
    /// Upstream protocol notes that if no pool types are specified, servers
    /// should default to the legacy behavior of returning only data relevant
    /// to the shielded Sapling and Orchard pools. Clients must verify server
    /// support before requesting pruned and/or transparent data via pool
    /// filters.
    fn get_block_range(
        &self,
        range: BlockRange,
    ) -> impl Future<Output = Result<Self::BlockStream, Self::Error>>;

    /// Returns a stream of compact blocks containing only shielded nullifier
    /// information.
    ///
    /// Transparent transaction data, Sapling outputs, full Orchard action
    /// data, and commitment tree sizes are not included. Implementations must
    /// ignore any transparent pool type in the request.
    ///
    /// Deprecated in the protocol; prefer [`Self::get_block_range`] with the
    /// appropriate pool filters.
    #[deprecated(note = "Protocol-deprecated; prefer get_block_range with pool filters")]
    fn get_block_range_nullifiers(
        &self,
        range: BlockRange,
    ) -> impl Future<Output = Result<Self::BlockStream, Self::Error>>;

    /// Returns the requested full, non-compact transaction.
    ///
    /// In the upstream protocol, this corresponds to the full transaction as
    /// returned by `zcashd`.
    fn get_transaction(
        &self,
        tx_filter: TxFilter,
    ) -> impl Future<Output = Result<RawTransaction, Self::Error>>;

    /// Submits the given transaction to the Zcash network.
    fn send_transaction(
        &self,
        tx: &[u8],
    ) -> impl Future<Output = Result<SentTransaction, Self::Error>>;

    /// Returns full transactions that match the given transparent address
    /// filter.
    ///
    /// Despite its historical name, the upstream RPC returns complete raw
    /// transactions, not transaction IDs.
    ///
    /// Deprecated in the protocol; prefer
    /// [`Self::get_taddress_transactions`].
    #[deprecated(note = "Protocol-deprecated; use get_taddress_transactions")]
    fn get_taddress_txids(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> impl Future<Output = Result<Self::RawTransactionStream, Self::Error>>;

    /// Returns the transactions corresponding to the given transparent address
    /// within the specified block range.
    ///
    /// Mempool transactions are not included.
    fn get_taddress_transactions(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> impl Future<Output = Result<Self::RawTransactionStream, Self::Error>>;

    /// Returns the balance for the given set of transparent addresses.
    fn get_taddress_balance(
        &self,
        addresses: AddressList,
    ) -> impl Future<Output = Result<Balance, Self::Error>>;

    /// Returns the balance for a streamed set of transparent addresses.
    fn get_taddress_balance_stream(
        &self,
        addresses: impl Stream<Item = Address>,
    ) -> impl Future<Output = Result<Balance, Self::Error>>;

    /// Returns a stream of compact transactions currently in the mempool.
    ///
    /// Results may be a few seconds out of date. If the excluded txid suffix
    /// list is empty, all transactions are returned; otherwise all
    /// non-excluded transactions are returned. Suffixes may be shortened to
    /// reduce bandwidth. If multiple mempool transactions match a given
    /// suffix, none of them are excluded.
    fn get_mempool_tx(
        &self,
        request: MempoolTxRequest,
    ) -> impl Future<Output = Result<Self::CompactTxStream, Self::Error>>;

    /// Returns a stream of current mempool transactions.
    ///
    /// The upstream server keeps the stream open while mempool transactions
    /// are available, and closes it when a new block is mined.
    fn get_mempool_stream(
        &self,
    ) -> impl Future<Output = Result<Self::RawTransactionStream, Self::Error>>;

    /// Returns the note commitment tree state corresponding to the given
    /// block.
    ///
    /// This is derived from the Zcash `z_gettreestate` RPC. The block may be
    /// specified by either height or hash, though upstream notes that support
    /// for selection by hash is not mandatory across all methods.
    fn get_tree_state(
        &self,
        block: BlockRef,
    ) -> impl Future<Output = Result<TreeState, Self::Error>>;

    /// Returns the note commitment tree state at the tip of the best chain.
    fn get_latest_tree_state(&self) -> impl Future<Output = Result<TreeState, Self::Error>>;

    /// Returns a stream of subtree roots for the specified shielded protocol.
    ///
    /// The upstream protocol defines this in terms of Sapling or Orchard note
    /// commitment tree subtrees.
    fn get_subtree_roots(
        &self,
        request: GetSubtreeRootsRequest,
    ) -> impl Future<Output = Result<Self::SubtreeRootStream, Self::Error>>;

    /// Returns the transparent UTXOs matching the given request.
    ///
    /// Upstream results are sorted by height, which makes it easy to issue a
    /// follow-up request that continues where the previous one left off.
    fn get_address_utxos(
        &self,
        request: GetAddressUtxosRequest,
    ) -> impl Future<Output = Result<Vec<AddressUtxo>, Self::Error>>;

    /// Returns a stream of transparent UTXOs matching the given request.
    fn get_address_utxos_stream(
        &self,
        request: GetAddressUtxosRequest,
    ) -> impl Future<Output = Result<Self::AddressUtxoStream, Self::Error>>;

    /// Testing-only RPC used to simulate delay and observe concurrency.
    ///
    /// On upstream `lightwalletd`, this requires `--ping-very-insecure` and
    /// should not be enabled in production.
    fn ping(&self, delay: Duration) -> impl Future<Output = Result<PingResponse, Self::Error>>;
}
/// gRPC-backed [`IndexerClient`] that connects to a lightwalletd-compatible
/// server.
#[derive(Clone)]
pub struct GrpcIndexerClient {
    uri: http::Uri,
    channel: Channel,
    call_timeouts: CallTimeouts,
}

impl std::fmt::Debug for GrpcIndexerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcIndexerClient")
            .field("uri", &self.uri)
            .finish_non_exhaustive()
    }
}

impl GrpcIndexerClient {
    pub fn new(uri: http::Uri) -> Result<Self, GetClientError> {
        let scheme = uri
            .scheme_str()
            .ok_or(GetClientError::InvalidScheme)?
            .to_string();
        if scheme != "http" && scheme != "https" {
            return Err(GetClientError::InvalidScheme);
        }

        let _authority = uri
            .authority()
            .ok_or(GetClientError::InvalidAuthority)?
            .clone();

        let endpoint = Endpoint::from_shared(uri.to_string())?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10));

        let endpoint = if scheme == "https" {
            endpoint.tls_config(client_tls_config())?
        } else {
            endpoint
        };

        let channel = endpoint.connect_lazy();

        Ok(Self {
            uri,
            channel,
            call_timeouts: CallTimeouts::default(),
        })
    }

    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeouts = CallTimeouts::new(timeout);
        self
    }

    pub fn with_call_timeouts(mut self, call_timeouts: CallTimeouts) -> Self {
        self.call_timeouts = call_timeouts;
        self
    }

    pub fn uri(&self) -> &http::Uri {
        &self.uri
    }

    fn client(&self) -> CompactTxStreamerClient<Channel> {
        CompactTxStreamerClient::new(self.channel.clone())
    }
}

impl IndexerClient for GrpcIndexerClient {
    type Error = IndexerClientError;

    type BlockStream = BoxStream<'static, Result<CompactBlock, Self::Error>>;
    type RawTransactionStream = BoxStream<'static, Result<RawTransaction, Self::Error>>;
    type CompactTxStream = BoxStream<'static, Result<CompactTx, Self::Error>>;
    type SubtreeRootStream = BoxStream<'static, Result<SubtreeRoot, Self::Error>>;
    type AddressUtxoStream = BoxStream<'static, Result<AddressUtxo, Self::Error>>;

    async fn get_lightd_info(&self) -> Result<ServerInfo, IndexerClientError> {
        let mut client = self.client();
        let mut request = Request::new(Empty {});
        request.set_timeout(self.call_timeouts.get_info);
        let response = client.get_lightd_info(request).await?;
        Ok(response.into_inner().into())
    }

    async fn get_latest_block(&self) -> Result<BlockRef, IndexerClientError> {
        let mut client = self.client();
        let mut request = Request::new(ChainSpec {});
        request.set_timeout(self.call_timeouts.get_latest_block);
        let response = client.get_latest_block(request).await?;
        Ok(response.into_inner().into())
    }

    async fn send_transaction(
        &self,
        tx_bytes: &[u8],
    ) -> Result<SentTransaction, IndexerClientError> {
        let mut client = self.client();
        let mut request = Request::new(zcash_client_backend::proto::service::RawTransaction {
            data: tx_bytes.to_vec(),
            height: 0,
        });
        request.set_timeout(self.call_timeouts.send_transaction);

        let response = client.send_transaction(request).await?;
        let send_response = response.into_inner();

        if send_response.error_code == 0 {
            let mut txid = send_response.error_message;
            if txid.starts_with('"') && txid.ends_with('"') && txid.len() >= 2 {
                txid = txid[1..txid.len() - 1].to_string();
            }
            Ok(SentTransaction { txid })
        } else {
            Err(IndexerClientError::SendRejected(format!(
                "{send_response:?}"
            )))
        }
    }

    async fn get_tree_state(&self, height: BlockRef) -> Result<TreeState, IndexerClientError> {
        let mut client = self.client();
        let mut request: Request<BlockId> = Request::new(height.into());
        request.set_timeout(self.call_timeouts.get_tree_state);

        let response = client.get_tree_state(request).await?;
        Ok(response.into_inner().into())
    }

    async fn get_block(&self, block: BlockRef) -> Result<CompactBlock, Self::Error> {
        let mut request: Request<BlockId> = Request::new(block.into());
        request.set_timeout(self.call_timeouts.get_tree_state);

        let response = self.client().get_block(request).await?;
        Ok(response.into_inner().into())
    }

    fn get_block_nullifiers(&self, block: BlockRef) -> Result<CompactBlock, Self::Error> {
        todo!()
    }

    fn get_block_range(&self, range: BlockRange) -> Result<Self::BlockStream, Self::Error> {
        todo!()
    }

    fn get_block_range_nullifiers(
        &self,
        range: BlockRange,
    ) -> Result<Self::BlockStream, Self::Error> {
        todo!()
    }

    fn get_transaction(&self, tx_filter: TxFilter) -> Result<RawTransaction, Self::Error> {
        todo!()
    }

    fn get_taddress_txids(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> Result<Self::RawTransactionStream, Self::Error> {
        todo!()
    }

    fn get_taddress_transactions(
        &self,
        filter: TransparentAddressBlockFilter,
    ) -> Result<Self::RawTransactionStream, Self::Error> {
        todo!()
    }

    fn get_taddress_balance(&self, addresses: AddressList) -> Result<Balance, Self::Error> {
        todo!()
    }

    fn get_taddress_balance_stream(
        &self,
        addresses: impl Stream<Item = Address>,
    ) -> Result<Balance, Self::Error> {
        todo!()
    }

    fn get_mempool_tx(
        &self,
        request: MempoolTxRequest,
    ) -> Result<Self::CompactTxStream, Self::Error> {
        todo!()
    }

    fn get_mempool_stream(&self) -> Result<Self::RawTransactionStream, Self::Error> {
        todo!()
    }

    fn get_latest_tree_state(&self) -> Result<TreeState, Self::Error> {
        todo!()
    }

    fn get_subtree_roots(
        &self,
        request: GetSubtreeRootsRequest,
    ) -> Result<Self::SubtreeRootStream, Self::Error> {
        todo!()
    }

    fn get_address_utxos(
        &self,
        request: GetAddressUtxosRequest,
    ) -> Result<Vec<AddressUtxo>, Self::Error> {
        todo!()
    }

    fn get_address_utxos_stream(
        &self,
        request: GetAddressUtxosRequest,
    ) -> Result<Self::AddressUtxoStream, Self::Error> {
        todo!()
    }

    fn ping(&self, delay: Duration) -> Result<PingResponse, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    //! Unit and integration-style tests for `zingo-netutils`.
    //!
    //! These tests focus on:
    //! - TLS test asset sanity (`test-data/localhost.pem` + `.key`)
    //! - Rustls plumbing (adding a local cert to a root store)
    //! - Connector correctness (scheme validation, HTTP/2 expectations)
    //! - Public semantic type mapping (`LightdInfo -> ServerInfo`, `BlockId -> BlockRef`)
    //!
    //! Notes:
    //! - Some tests spin up an in-process TLS server and use aggressive timeouts to
    //!   avoid hangs under nextest.
    //! - We explicitly install a rustls crypto provider to avoid
    //!   provider-selection panics in test binaries.

    use std::time::Duration;

    use http::{Request, Response};
    use hyper::{
        body::{Bytes, Incoming},
        service::service_fn,
    };
    use hyper_util::rt::TokioIo;
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};
    use tokio_rustls::{TlsAcceptor, rustls};
    use zcash_client_backend::proto::service::LightdInfo;

    use super::*;

    use tokio_rustls::rustls::RootCertStore;

    fn add_test_cert_to_roots(roots: &mut RootCertStore) {
        use tonic::transport::CertificateDer;

        const TEST_PEMFILE_PATH: &str = "test-data/localhost.pem";

        let Ok(fd) = std::fs::File::open(TEST_PEMFILE_PATH) else {
            eprintln!("Test TLS cert not found at {TEST_PEMFILE_PATH}, skipping");
            return;
        };

        let mut buf = std::io::BufReader::new(fd);
        let certs_bytes: Vec<tonic::transport::CertificateDer> = rustls_pemfile::certs(&mut buf)
            .filter_map(Result::ok)
            .collect();

        let certs: Vec<CertificateDer<'_>> = certs_bytes.into_iter().collect();
        roots.add_parsable_certificates(certs);
    }

    #[test]
    fn lightd_info_maps_to_server_info() {
        let info = LightdInfo {
            version: "1.2.3".to_string(),
            vendor: "zingo".to_string(),
            taddr_support: false,
            chain_name: "main".to_string(),
            sapling_activation_height: 419_200,
            consensus_branch_id: "76b809bb".to_string(),
            block_height: 2_345_678,
            git_commit: String::new(),
            branch: String::new(),
            build_date: String::new(),
            build_user: String::new(),
            estimated_height: 0,
            zcashd_build: String::new(),
            zcashd_subversion: String::new(),
            donation_address: String::new(),
        };

        let mapped = ServerInfo::from(info);

        assert_eq!(
            mapped,
            ServerInfo {
                chain_name: "main".to_string(),
                vendor: "zingo".to_string(),
                version: "1.2.3".to_string(),
                block_height: 2_345_678,
                sapling_activation_height: 419_200,
                consensus_branch_id: "76b809bb".to_string(),
                taddr_support: false,
                git_commit: String::new(),
                branch: String::new(),
                build_date: String::new(),
                build_user: String::new(),
                estimated_height: 0,
                zcashd_build: String::new(),
                zcashd_subversion: String::new(),
                donation_address: String::new()
            }
        );
    }

    #[test]
    fn block_id_maps_to_block_ref() {
        let block = BlockId {
            height: 123,
            hash: vec![1, 2, 3, 4],
        };

        let mapped = BlockRef::from(block);

        assert_eq!(
            mapped,
            BlockRef {
                height: 123,
                hash: vec![1, 2, 3, 4],
            }
        );
    }

    /// Ensures the committed localhost test certificate exists and is parseable as X.509.
    ///
    /// This catches:
    /// - missing file / wrong working directory assumptions
    /// - invalid PEM encoding
    /// - accidentally committing the wrong artifact (e.g., key instead of cert)
    #[test]
    fn localhost_cert_file_exists_and_is_parseable() {
        const CERT_PATH: &str = "test-data/localhost.pem";

        let pem = std::fs::read(CERT_PATH).expect("missing test-data/localhost.pem");

        let mut cursor = std::io::BufReader::new(pem.as_slice());
        let certs = rustls_pemfile::certs(&mut cursor)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert!(!certs.is_empty(), "no certs found in {CERT_PATH}");

        for cert in certs {
            let der = cert.as_ref();
            let parsed = x509_parser::parse_x509_certificate(der);
            assert!(
                parsed.is_ok(),
                "failed to parse a cert from {CERT_PATH} as X.509"
            );
        }
    }

    /// Guards against committing a CA certificate as the TLS server certificate.
    ///
    /// Rustls rejects certificates with CA constraints when used as an end-entity
    /// server certificate (e.g. `CaUsedAsEndEntity`), even if the cert is in the
    /// root store. This test ensures the committed localhost cert has `CA:FALSE`.
    #[test]
    fn localhost_cert_is_end_entity_not_ca() {
        let pem =
            std::fs::read("test-data/localhost.pem").expect("missing test-data/localhost.pem");
        let mut cursor = std::io::BufReader::new(pem.as_slice());

        let certs = rustls_pemfile::certs(&mut cursor)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert!(!certs.is_empty(), "no certs found in localhost.pem");

        let der = certs[0].as_ref();
        let parsed = x509_parser::parse_x509_certificate(der).expect("failed to parse X.509");
        let x509 = parsed.1;

        let constraints = x509
            .basic_constraints()
            .expect("missing basic constraints extension");

        assert!(
            !constraints.unwrap().value.ca,
            "localhost.pem must be CA:FALSE"
        );
    }

    /// Loads a rustls `ServerConfig` for a local TLS server using the committed
    /// test certificate and private key.
    ///
    /// The cert/key pair is *test-only* and is stored under `test-data/`.
    /// This is used to verify that the client-side root-store injection
    /// (`add_test_cert_to_roots`) actually enables successful TLS handshakes.
    fn load_test_server_config() -> std::sync::Arc<rustls::ServerConfig> {
        let cert_pem =
            std::fs::read("test-data/localhost.pem").expect("missing test-data/localhost.pem");
        let key_pem =
            std::fs::read("test-data/localhost.key").expect("missing test-data/localhost.key");

        let mut cert_cursor = std::io::BufReader::new(cert_pem.as_slice());
        let mut key_cursor = std::io::BufReader::new(key_pem.as_slice());

        let certs = rustls_pemfile::certs(&mut cert_cursor)
            .filter_map(Result::ok)
            .map(rustls::pki_types::CertificateDer::from)
            .collect::<Vec<_>>();

        let key = rustls_pemfile::private_key(&mut key_cursor)
            .expect("failed to read private key")
            .expect("no private key found");

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("bad cert or key");

        std::sync::Arc::new(config)
    }

    /// Smoke test: adding the committed localhost cert to a rustls root store enables
    /// a client to complete a TLS handshake and perform an HTTP request.
    #[tokio::test]
    async fn add_test_cert_to_roots_enables_tls_handshake() {
        use http_body_util::Full;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        use tokio::net::TcpListener;
        use tokio_rustls::TlsAcceptor;
        use tokio_rustls::rustls;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr = listener.local_addr().expect("local_addr failed");

        let tls_config = load_test_server_config();
        let acceptor = TlsAcceptor::from(tls_config);

        let ready = oneshot::channel::<()>();
        let ready_tx = ready.0;
        let ready_rx = ready.1;

        let server_task = tokio::spawn(async move {
            let _ = ready_tx.send(());

            let accept_res = timeout(Duration::from_secs(3), listener.accept()).await;
            let (socket, _) = accept_res
                .expect("server accept timed out")
                .expect("accept failed");

            let tls_stream = timeout(Duration::from_secs(3), acceptor.accept(socket))
                .await
                .expect("tls accept timed out")
                .expect("tls accept failed");

            let io = TokioIo::new(tls_stream);

            let svc = service_fn(|mut req: http::Request<hyper::body::Incoming>| async move {
                use http_body_util::BodyExt;

                while let Some(frame) = req.body_mut().frame().await {
                    if frame.is_err() {
                        break;
                    }
                }

                let mut resp = http::Response::new(Full::new(Bytes::from_static(b"ok")));
                resp.headers_mut().insert(
                    http::header::CONNECTION,
                    http::HeaderValue::from_static("close"),
                );
                Ok::<_, hyper::Error>(resp)
            });

            timeout(
                Duration::from_secs(3),
                hyper::server::conn::http1::Builder::new()
                    .keep_alive(false)
                    .serve_connection(io, svc),
            )
            .await
            .expect("serve_connection timed out")
            .expect("serve_connection failed");
        });

        let _ = timeout(Duration::from_secs(1), ready_rx)
            .await
            .expect("server ready signal timed out")
            .expect("server dropped before ready");

        // Build client root store and add the test cert.
        let mut roots = rustls::RootCertStore::empty();
        add_test_cert_to_roots(&mut roots);

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        // This MUST allow http1 since the server uses hyper http1 builder.
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(client_config)
            .https_only()
            .enable_http1()
            .build();

        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(https);

        let uri: http::Uri = format!("https://127.0.0.1:{}/", addr.port())
            .parse()
            .expect("bad uri");

        let req = http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(Full::<Bytes>::new(Bytes::new()))
            .expect("request build failed");

        let res = timeout(Duration::from_secs(3), client.request(req))
            .await
            .expect("client request timed out")
            .expect("TLS handshake or request failed");

        assert!(res.status().is_success());

        timeout(Duration::from_secs(3), server_task)
            .await
            .expect("server task timed out")
            .expect("server task failed");
    }

    /// Validates that the connector rejects non-HTTP(S) URIs.
    ///
    /// This test is intended to fail until production code checks for:
    /// - `http` and `https` schemes only
    /// and rejects everything else (e.g. `ftp`).
    #[test]
    fn rejects_non_http_schemes() {
        let uri: http::Uri = "ftp://example.com:1234".parse().unwrap();
        let res = GrpcIndexerClient::new(uri);

        assert!(
            res.is_err(),
            "expected GrpcIndexerClient::new() to reject non-http(s) schemes, but got Ok"
        );
    }

    /// A gRPC (HTTP/2) client must not succeed against an HTTP/1.1-only TLS server.
    #[tokio::test]
    async fn https_connector_must_not_downgrade_to_http1() {
        use http_body_util::Full;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr = listener.local_addr().expect("local_addr failed");

        let tls_config = load_test_server_config();
        let acceptor = TlsAcceptor::from(tls_config);

        let server_task = tokio::spawn(async move {
            let accept_res = timeout(Duration::from_secs(3), listener.accept()).await;
            let (socket, _) = accept_res
                .expect("server accept timed out")
                .expect("accept failed");

            let tls_stream = acceptor.accept(socket).await.expect("tls accept failed");
            let io = TokioIo::new(tls_stream);

            let svc = service_fn(|_req: Request<Incoming>| async move {
                Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from_static(b"ok"))))
            });

            // This may error with VersionH2 if the client sends an h2 preface, or it may
            // simply never be reached if ALPN fails earlier. Either is fine for this test.
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        });

        let base = format!("https://127.0.0.1:{}", addr.port());
        let uri = base.parse::<http::Uri>().expect("bad base uri");

        let endpoint = tonic::transport::Endpoint::from_shared(uri.to_string())
            .expect("endpoint")
            .tcp_nodelay(true);

        let connect_res = endpoint
            .tls_config(client_tls_config())
            .expect("tls_config failed")
            .connect()
            .await;

        // A gRPC (HTTP/2) client must not succeed against an HTTP/1.1-only TLS server.
        assert!(
            connect_res.is_err(),
            "expected connect to fail (no downgrade to HTTP/1.1), but it succeeded"
        );

        server_task.abort();
    }

    #[tokio::test]
    async fn connects_to_public_mainnet_indexer_and_gets_info() {
        let endpoint = "https://zec.rocks:443".to_string();
        let uri: http::Uri = endpoint.parse().expect("bad mainnet indexer URI");

        let response = GrpcIndexerClient::new(uri)
            .expect("URI to be valid")
            .get_lightd_info()
            .await
            .expect("to get info");

        assert!(
            !response.chain_name.is_empty(),
            "chain_name should not be empty"
        );
        assert!(
            response.block_height > 0,
            "block_height should be > 0, got {}",
            response.block_height
        );

        let chain = response.chain_name.to_ascii_lowercase();
        assert!(
            chain.contains("main"),
            "expected a mainnet server, got chain_name={:?}",
            response.chain_name
        );
    }
}
