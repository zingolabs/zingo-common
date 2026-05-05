//! A complete [`Indexer`] abstraction for communicating with Zcash chain
//! indexers (`lightwalletd` / `zainod`).
//!
//! # Organizing principle
//!
//! The [`Indexer`] trait is the sole interface a Zcash wallet or tool needs
//! to query, sync, and broadcast against a chain indexer. It is
//! implementation-agnostic: production code uses the provided [`GrpcIndexer`]
//! (gRPC over tonic), while tests can supply a mock implementor with no
//! network dependency.
//!
//! All proto types come from
//! [`lightwallet-protocol`](https://crates.io/crates/lightwallet-protocol)
//! and are re-exported via `pub use lightwallet_protocol` so consumers do
//! not need an additional dependency.
//!
//! # Feature gates
//!
//! All features are **off by default**.
//!
//! | Feature | What it enables |
//! |---|---|
//! | `globally-public-transparent` | [`TransparentIndexer`] sub-trait for t-address balance, transaction history, and UTXO queries. Pulls in `tokio-stream`. |
//! | `ping-very-insecure` | [`Indexer::ping`] method. Name mirrors the lightwalletd `--ping-very-insecure` CLI flag. Testing only. |
//! | `back_compatible` | [`GrpcIndexer::get_zcb_client`] returning `zcash_client_backend`'s `CompactTxStreamerClient` for pepper-sync compatibility. |
//! | `nym` | Route gRPC traffic through the [Nym mixnet](https://nymtech.net/) via an embedded SOCKS5 proxy. Adds `proxied: bool` parameter to all trait methods and exposes [`NymProxy`] for proxy lifecycle management. See [`GrpcIndexer::with_nym`] and [`GrpcIndexer::with_socks_proxy`]. |
//!
//! **Note:** Build docs with `--all-features` so intra-doc links to
//! feature-gated items resolve:
//! ```text
//! RUSTDOCFLAGS="-D warnings" cargo doc --all-features --document-private-items
//! ```
//!
//! # Backwards compatibility
//!
//! Code that needs a raw `CompactTxStreamerClient<Channel>` (e.g.
//! pepper-sync) can call [`GrpcIndexer::get_client`] (which respects
//! the `proxied` parameter when the `nym` feature is enabled), or
//! enable the `back_compatible` feature for
//! [`GrpcIndexer::get_zcb_client`] which returns
//! `zcash_client_backend`'s client type as a migration bridge.

use std::future::Future;
use std::time::Duration;

use tonic::Request;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[cfg(feature = "nym")]
use std::pin::Pin;
#[cfg(feature = "nym")]
use std::task::{Context, Poll};

pub use lightwallet_protocol;

use lightwallet_protocol::{
    BlockId, BlockRange, ChainSpec, CompactBlock, CompactTx, CompactTxStreamerClient, Empty,
    GetMempoolTxRequest, GetSubtreeRootsArg, LightdInfo, RawTransaction, SubtreeRoot, TreeState,
    TxFilter,
};

#[cfg(feature = "ping-very-insecure")]
use lightwallet_protocol::{Duration as ProtoDuration, PingResponse};

pub mod error;
pub use error::*;

#[cfg(feature = "globally-public-transparent")]
mod globally_public;
#[cfg(feature = "globally-public-transparent")]
pub use globally_public::TransparentIndexer;

#[cfg(feature = "nym")]
mod nym_proxy;
#[cfg(feature = "nym")]
pub use nym_proxy::NymProxy;

fn client_tls_config() -> ClientTlsConfig {
    // Allow self-signed certs in tests
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

/// Build a raw `rustls::ClientConfig` for TLS-over-SOCKS connections.
///
/// Tonic's `ClientTlsConfig` applies only to its built-in connector. When
/// routing through a custom SOCKS5 connector we must layer TLS manually
/// using `tokio-rustls`, so we need the raw `rustls::ClientConfig`.
#[cfg(feature = "nym")]
fn socks_rustls_client_config() -> tokio_rustls::rustls::ClientConfig {
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    // gRPC requires HTTP/2. ALPN must advertise "h2" so the server
    // selects the correct protocol during the TLS handshake.
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

/// Transport wrapper for SOCKS5-routed connections.
///
/// Wraps either a plain TCP stream (for `http://` targets) or a TLS
/// stream layered over TCP (for `https://` targets). Both inner types
/// are `Unpin`, so `AsyncRead`/`AsyncWrite` can be implemented safely
/// without `pin-project`.
#[cfg(feature = "nym")]
enum SocksIo {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>),
}

#[cfg(feature = "nym")]
impl tokio::io::AsyncRead for SocksIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SocksIo::Plain(s) => Pin::new(s).poll_read(cx, buf),
            SocksIo::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

#[cfg(feature = "nym")]
impl tokio::io::AsyncWrite for SocksIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            SocksIo::Plain(s) => Pin::new(s).poll_write(cx, buf),
            SocksIo::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SocksIo::Plain(s) => Pin::new(s).poll_flush(cx),
            SocksIo::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SocksIo::Plain(s) => Pin::new(s).poll_shutdown(cx),
            SocksIo::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

const DEFAULT_GRPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Nym adds 3-10s latency per mix-node hop. Use a longer timeout for
/// Nym-routed requests to avoid spurious timeouts.
#[cfg(feature = "nym")]
const DEFAULT_NYM_GRPC_TIMEOUT: Duration = Duration::from_secs(60);

/// Trait for communicating with a Zcash chain indexer.
///
/// Implementors provide access to a lightwalletd-compatible server.
/// Callers can depend on the following guarantees:
///
/// - Each method opens a fresh connection (or reuses a pooled one) — no
///   persistent session state is assumed between calls.
/// - Errors are partitioned per method so callers can handle connection
///   failures separately from server-side errors.
/// - All methods are safe to call concurrently from multiple tasks.
pub trait Indexer {
    type GetInfoError: std::error::Error;
    type GetLatestBlockError: std::error::Error;
    type SendTransactionError: std::error::Error;
    type GetTreeStateError: std::error::Error;
    type GetBlockError: std::error::Error;
    type GetBlockNullifiersError: std::error::Error;
    type GetBlockRangeError: std::error::Error;
    type GetBlockRangeNullifiersError: std::error::Error;
    type GetTransactionError: std::error::Error;
    type GetMempoolTxError: std::error::Error;
    type GetMempoolStreamError: std::error::Error;
    type GetLatestTreeStateError: std::error::Error;
    type GetSubtreeRootsError: std::error::Error;

    #[cfg(feature = "ping-very-insecure")]
    type PingError: std::error::Error;

    /// Return server metadata (chain name, block height, version, etc.).
    #[cfg(not(feature = "nym"))]
    fn get_info(&self) -> impl Future<Output = Result<LightdInfo, Self::GetInfoError>>;
    #[cfg(feature = "nym")]
    fn get_info(
        &self,
        proxied: bool,
    ) -> impl Future<Output = Result<LightdInfo, Self::GetInfoError>>;

    /// Return the height and hash of the chain tip.
    #[cfg(not(feature = "nym"))]
    fn get_latest_block(&self) -> impl Future<Output = Result<BlockId, Self::GetLatestBlockError>>;
    #[cfg(feature = "nym")]
    fn get_latest_block(
        &self,
        proxied: bool,
    ) -> impl Future<Output = Result<BlockId, Self::GetLatestBlockError>>;

    /// Submit a raw transaction to the network.
    #[cfg(not(feature = "nym"))]
    fn send_transaction(
        &self,
        tx_bytes: Box<[u8]>,
    ) -> impl Future<Output = Result<String, Self::SendTransactionError>>;
    #[cfg(feature = "nym")]
    fn send_transaction(
        &self,
        tx_bytes: Box<[u8]>,
        proxied: bool,
    ) -> impl Future<Output = Result<String, Self::SendTransactionError>>;

    /// Fetch the note commitment tree state for the given block.
    #[cfg(not(feature = "nym"))]
    fn get_tree_state(
        &self,
        block_id: BlockId,
    ) -> impl Future<Output = Result<TreeState, Self::GetTreeStateError>>;
    #[cfg(feature = "nym")]
    fn get_tree_state(
        &self,
        block_id: BlockId,
        proxied: bool,
    ) -> impl Future<Output = Result<TreeState, Self::GetTreeStateError>>;

    /// Return the compact block at the given height.
    #[cfg(not(feature = "nym"))]
    fn get_block(
        &self,
        block_id: BlockId,
    ) -> impl Future<Output = Result<CompactBlock, Self::GetBlockError>>;
    #[cfg(feature = "nym")]
    fn get_block(
        &self,
        block_id: BlockId,
        proxied: bool,
    ) -> impl Future<Output = Result<CompactBlock, Self::GetBlockError>>;

    /// Return the compact block at the given height, containing only nullifiers.
    #[cfg(not(feature = "nym"))]
    #[deprecated(note = "use get_block instead")]
    fn get_block_nullifiers(
        &self,
        block_id: BlockId,
    ) -> impl Future<Output = Result<CompactBlock, Self::GetBlockNullifiersError>>;
    #[cfg(feature = "nym")]
    #[deprecated(note = "use get_block instead")]
    fn get_block_nullifiers(
        &self,
        block_id: BlockId,
        proxied: bool,
    ) -> impl Future<Output = Result<CompactBlock, Self::GetBlockNullifiersError>>;

    /// Return a stream of consecutive compact blocks for the given range.
    #[cfg(not(feature = "nym"))]
    fn get_block_range(
        &self,
        range: BlockRange,
    ) -> impl Future<Output = Result<tonic::Streaming<CompactBlock>, Self::GetBlockRangeError>>;
    #[cfg(feature = "nym")]
    fn get_block_range(
        &self,
        range: BlockRange,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<CompactBlock>, Self::GetBlockRangeError>>;

    /// Return a stream of consecutive compact blocks (nullifiers only) for the given range.
    #[cfg(not(feature = "nym"))]
    #[deprecated(note = "use get_block_range instead")]
    fn get_block_range_nullifiers(
        &self,
        range: BlockRange,
    ) -> impl Future<Output = Result<tonic::Streaming<CompactBlock>, Self::GetBlockRangeNullifiersError>>;
    #[cfg(feature = "nym")]
    #[deprecated(note = "use get_block_range instead")]
    fn get_block_range_nullifiers(
        &self,
        range: BlockRange,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<CompactBlock>, Self::GetBlockRangeNullifiersError>>;

    /// Return the full serialized transaction matching the given filter.
    #[cfg(not(feature = "nym"))]
    fn get_transaction(
        &self,
        filter: TxFilter,
    ) -> impl Future<Output = Result<RawTransaction, Self::GetTransactionError>>;
    #[cfg(feature = "nym")]
    fn get_transaction(
        &self,
        filter: TxFilter,
        proxied: bool,
    ) -> impl Future<Output = Result<RawTransaction, Self::GetTransactionError>>;

    /// Return a stream of compact transactions currently in the mempool.
    #[cfg(not(feature = "nym"))]
    fn get_mempool_tx(
        &self,
        request: GetMempoolTxRequest,
    ) -> impl Future<Output = Result<tonic::Streaming<CompactTx>, Self::GetMempoolTxError>>;
    #[cfg(feature = "nym")]
    fn get_mempool_tx(
        &self,
        request: GetMempoolTxRequest,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<CompactTx>, Self::GetMempoolTxError>>;

    /// Return a stream of raw mempool transactions.
    #[cfg(not(feature = "nym"))]
    fn get_mempool_stream(
        &self,
    ) -> impl Future<Output = Result<tonic::Streaming<RawTransaction>, Self::GetMempoolStreamError>>;
    #[cfg(feature = "nym")]
    fn get_mempool_stream(
        &self,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<RawTransaction>, Self::GetMempoolStreamError>>;

    /// Return the note commitment tree state at the chain tip.
    #[cfg(not(feature = "nym"))]
    fn get_latest_tree_state(
        &self,
    ) -> impl Future<Output = Result<TreeState, Self::GetLatestTreeStateError>>;
    #[cfg(feature = "nym")]
    fn get_latest_tree_state(
        &self,
        proxied: bool,
    ) -> impl Future<Output = Result<TreeState, Self::GetLatestTreeStateError>>;

    /// Return a stream of subtree roots for the given shielded protocol.
    #[cfg(not(feature = "nym"))]
    fn get_subtree_roots(
        &self,
        arg: GetSubtreeRootsArg,
    ) -> impl Future<Output = Result<tonic::Streaming<SubtreeRoot>, Self::GetSubtreeRootsError>>;
    #[cfg(feature = "nym")]
    fn get_subtree_roots(
        &self,
        arg: GetSubtreeRootsArg,
        proxied: bool,
    ) -> impl Future<Output = Result<tonic::Streaming<SubtreeRoot>, Self::GetSubtreeRootsError>>;

    /// Simulate server latency for testing.
    #[cfg(all(feature = "ping-very-insecure", not(feature = "nym")))]
    fn ping(
        &self,
        duration: ProtoDuration,
    ) -> impl Future<Output = Result<PingResponse, Self::PingError>>;
    #[cfg(all(feature = "ping-very-insecure", feature = "nym"))]
    fn ping(
        &self,
        duration: ProtoDuration,
        proxied: bool,
    ) -> impl Future<Output = Result<PingResponse, Self::PingError>>;
}

/// gRPC-backed [`Indexer`] that connects to a lightwalletd server.
///
/// # Proxy routing
///
/// When the `nym` feature is enabled, every trait method gains a
/// `proxied: bool` parameter. Three usage paths are supported:
///
/// 1. **No proxy** — construct with [`new`](Self::new) and pass
///    `proxied: false`.
/// 2. **Built-in Nym proxy** — call [`with_nym()`](Self::with_nym)
///    after construction. The proxy is created, validated, and owned
///    by `GrpcIndexer`. On request failure it reconnects automatically.
/// 3. **Manual SOCKS5** — call [`with_socks_proxy()`](Self::with_socks_proxy)
///    with an external proxy address. No automatic reconnection.
///
/// Passing `proxied: true` without configuring a proxy returns
/// [`GetClientError::NoProxy`].
#[derive(Clone)]
pub struct GrpcIndexer {
    uri: http::Uri,
    scheme: String,
    authority: http::uri::Authority,
    endpoint: Endpoint,
    #[cfg(feature = "nym")]
    socks_proxy: Option<String>,
    #[cfg(feature = "nym")]
    nym_proxy: Option<std::sync::Arc<tokio::sync::Mutex<NymProxy>>>,
}

impl std::fmt::Debug for GrpcIndexer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcIndexer")
            .field("scheme", &self.scheme)
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

impl GrpcIndexer {
    pub fn new(uri: http::Uri) -> Result<Self, GetClientError> {
        let scheme = uri
            .scheme_str()
            .ok_or(GetClientError::InvalidScheme)?
            .to_string();
        if scheme != "http" && scheme != "https" {
            return Err(GetClientError::InvalidScheme);
        }
        let authority = uri
            .authority()
            .ok_or(GetClientError::InvalidAuthority)?
            .clone();

        let endpoint = Endpoint::from_shared(uri.to_string())?.tcp_nodelay(true);
        let endpoint = if scheme == "https" {
            endpoint.tls_config(client_tls_config())?
        } else {
            endpoint
        };

        Ok(Self {
            uri,
            scheme,
            authority,
            endpoint,
            #[cfg(feature = "nym")]
            socks_proxy: None,
            #[cfg(feature = "nym")]
            nym_proxy: None,
        })
    }

    pub fn uri(&self) -> &http::Uri {
        &self.uri
    }

    /// Set a SOCKS5 proxy address for proxied connections.
    ///
    /// Pass the address returned by [`NymProxy::socks5_addr`] or any
    /// other SOCKS5 proxy. When `proxied: true` is passed to trait
    /// methods, connections are routed through this proxy.
    ///
    /// The caller manages the proxy lifecycle externally — no automatic
    /// reconnection is provided for manually configured proxies.
    #[cfg(feature = "nym")]
    pub fn with_socks_proxy(mut self, addr: &str) -> Self {
        self.socks_proxy = Some(addr.to_string());
        self
    }

    /// Create, validate, and own an embedded Nym SOCKS5 proxy.
    ///
    /// This is the recommended way to use Nym routing. It:
    /// 1. Starts a [`NymProxy`] (auto-discovers exit gateways, retries).
    /// 2. Validates end-to-end connectivity by sending a
    ///    `get_lightd_info` gRPC probe through the proxy.
    /// 3. Stores the proxy internally — all clones of this `GrpcIndexer`
    ///    share the same proxy via `Arc<Mutex<_>>`.
    ///
    /// On request failure, [`GrpcIndexer`] calls
    /// [`NymProxy::reconnect`] and retries once automatically.
    ///
    /// # Errors
    ///
    /// Returns [`GetClientError::NymStart`] if no working exit gateway
    /// can be found, or if the gRPC probe fails through all attempted
    /// gateways.
    #[cfg(feature = "nym")]
    pub async fn with_nym(mut self) -> Result<Self, GetClientError> {
        const MAX_WITH_NYM_ATTEMPTS: usize = 5;
        const PROBE_TIMEOUT: Duration = DEFAULT_NYM_GRPC_TIMEOUT;

        let mut last_err = None;
        for _attempt in 0..MAX_WITH_NYM_ATTEMPTS {
            let proxy = match NymProxy::start().await {
                Ok(p) => p,
                Err(e) => {
                    last_err = Some(GetClientError::NymStart(Box::new(e)));
                    continue;
                }
            };
            let addr = proxy.socks5_addr();

            // Probe: send a get_lightd_info request through the proxy
            // to verify end-to-end connectivity.
            match tokio::time::timeout(PROBE_TIMEOUT, self.connect_channel_via_socks_proxy(&addr))
                .await
            {
                Ok(Ok(channel)) => {
                    // Verify gRPC works by sending a real request.
                    let mut client = CompactTxStreamerClient::new(channel);
                    let mut request = Request::new(Empty {});
                    request.set_timeout(PROBE_TIMEOUT);
                    match client.get_lightd_info(request).await {
                        Ok(resp) => {
                            let info = resp.into_inner();
                            if info.block_height > 0 {
                                self.nym_proxy =
                                    Some(std::sync::Arc::new(tokio::sync::Mutex::new(proxy)));
                                return Ok(self);
                            }
                            last_err = Some(GetClientError::NymStart(Box::new(
                                NymProxyError::ConnectivityCheck(
                                    "probe returned block_height=0".into(),
                                ),
                            )));
                        }
                        Err(e) => {
                            last_err = Some(GetClientError::NymStart(Box::new(
                                NymProxyError::ConnectivityCheck(e.to_string()),
                            )));
                        }
                    }
                }
                Ok(Err(e)) => {
                    last_err = Some(e);
                }
                Err(_timeout) => {
                    last_err = Some(GetClientError::NymStart(Box::new(
                        NymProxyError::ConnectivityCheck("probe timed out".into()),
                    )));
                }
            }
            proxy.disconnect().await;
        }

        Err(last_err.unwrap_or(GetClientError::NymStart(Box::new(
            NymProxyError::NoProvider,
        ))))
    }

    /// Connect to the pre-configured endpoint and return a gRPC client.
    ///
    /// When the `nym` feature is enabled, pass `proxied: true` to route
    /// through the configured SOCKS5 proxy, or `false` for a direct
    /// connection. Without the `nym` feature this always connects
    /// directly.
    ///
    /// # Privacy
    ///
    /// Calling this with `proxied: false` (or without the `nym` feature)
    /// connects directly to the indexer, exposing the caller's IP.
    #[cfg(not(feature = "nym"))]
    pub async fn get_client(&self) -> Result<CompactTxStreamerClient<Channel>, GetClientError> {
        let channel = self.endpoint.connect().await?;
        Ok(CompactTxStreamerClient::new(channel))
    }

    /// Connect to the pre-configured endpoint and return a gRPC client.
    ///
    /// Pass `proxied: true` to route through the configured SOCKS5
    /// proxy, or `false` for a direct connection.
    ///
    /// # Privacy
    ///
    /// Calling this with `proxied: false` connects directly to the
    /// indexer, exposing the caller's IP.
    #[cfg(feature = "nym")]
    pub async fn get_client(
        &self,
        proxied: bool,
    ) -> Result<CompactTxStreamerClient<Channel>, GetClientError> {
        let channel = self.connect_channel(proxied).await?;
        Ok(CompactTxStreamerClient::new(channel))
    }

    /// Return the effective target port for the configured URI.
    ///
    /// Uses the explicit authority port when present. Otherwise, falls
    /// back to the scheme default: `8137` for `https` and `80` for
    /// `http`.
    #[cfg(feature = "nym")]
    fn default_target_port(&self) -> u16 {
        self.authority
            .port_u16()
            .unwrap_or_else(|| match self.scheme.as_str() {
                "https" => 8137,
                "http" => 80,
                _ => unreachable!("GrpcIndexer::new validates the scheme"),
            })
    }

    /// Connect a gRPC channel through the provided SOCKS5 proxy address.
    ///
    /// This performs a single connection attempt through the given local
    /// SOCKS5 proxy. For `https` targets it also layers TLS manually
    /// over the SOCKS5 tunnel. Proxy reset and retry policy are handled
    /// by [`connect_channel`](Self::connect_channel), so this helper
    /// stays off the hot path after a successful proxy startup.
    #[cfg(feature = "nym")]
    async fn connect_channel_via_socks_proxy(
        &self,
        proxy_addr: &str,
    ) -> Result<Channel, GetClientError> {
        let target_host = self.authority.host().to_string();
        let target_port = self.default_target_port();
        let is_tls = self.scheme == "https";

        let proxy = proxy_addr.to_string();
        let authority = self.authority.clone();

        let connector = tower::service_fn(move |_uri: http::Uri| {
            let proxy = proxy.clone();
            let target_host = target_host.clone();
            let target_port = target_port;
            let is_tls = is_tls;
            async move {
                let tcp = tokio_socks::tcp::Socks5Stream::connect(
                    &*proxy,
                    (target_host.as_str(), target_port),
                )
                .await
                .map_err(std::io::Error::other)?;

                let io = if is_tls {
                    let tls_config = socks_rustls_client_config();
                    let connector =
                        tokio_rustls::TlsConnector::from(std::sync::Arc::new(tls_config));
                    let server_name =
                        tokio_rustls::rustls::pki_types::ServerName::try_from(target_host.clone())
                            .map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
                            })?;
                    let tls_stream = connector.connect(server_name, tcp.into_inner()).await?;
                    SocksIo::Tls(Box::new(tls_stream))
                } else {
                    SocksIo::Plain(tcp.into_inner())
                };

                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(io))
            }
        });

        // Use http:// regardless of actual scheme — TLS is handled
        // manually inside the connector (layered over SOCKS5), so tonic
        // must not attempt its own TLS negotiation.
        let endpoint = Endpoint::from_shared(format!("http://{authority}"))?;
        Ok(endpoint.connect_with_connector(connector).await?)
    }

    /// Connect and return a raw `Channel`, routing through the proxy
    /// if `proxied` is true.
    ///
    /// When an owned [`NymProxy`] is present (via [`with_nym`](Self::with_nym)),
    /// a failed connection attempt triggers a one-time recovery:
    /// [`NymProxy::reconnect`] is called and the connection retried once.
    /// Manual SOCKS5 proxies (via [`with_socks_proxy`](Self::with_socks_proxy))
    /// do not auto-recover.
    ///
    /// # Privacy guarantees (when `proxied` is true)
    ///
    /// - **DNS**: The target domain is sent as SOCKS5 ATYP=0x03, so DNS
    ///   resolution happens at the exit gateway, not locally.
    /// - **TLS**: The TLS handshake (including SNI) occurs inside the
    ///   SOCKS5 tunnel after CONNECT completes — it is not visible to
    ///   local network observers.
    /// - **IP**: The indexer server sees the exit gateway's IP, not the
    ///   caller's.
    #[cfg(feature = "nym")]
    async fn connect_channel(&self, proxied: bool) -> Result<Channel, GetClientError> {
        if proxied {
            if let Some(ref nym_proxy) = self.nym_proxy {
                // Hold the mutex for the full connection. The Nym Socks5MixnetClient
                // panics in background cleanup tasks when two concurrent SOCKS5
                // connections close at the same time (TrySendError::Disconnected).
                // Serializing here prevents concurrent use of the same client instance.
                let mut guard = nym_proxy.lock().await;
                let proxy_addr = guard.socks5_addr();
                match self.connect_channel_via_socks_proxy(&proxy_addr).await {
                    Ok(channel) => Ok(channel),
                    Err(_) => {
                        guard
                            .reconnect()
                            .await
                            .map_err(|e| GetClientError::NymStart(Box::new(e)))?;
                        let new_addr = guard.socks5_addr();
                        self.connect_channel_via_socks_proxy(&new_addr).await
                    }
                }
            } else if let Some(ref addr) = self.socks_proxy {
                self.connect_channel_via_socks_proxy(addr).await
            } else {
                Err(GetClientError::NoProxy)
            }
        } else {
            Ok(self.endpoint.connect().await?)
        }
    }

    /// Get a gRPC client, routing through the proxy if `proxied` is true.
    #[cfg(feature = "nym")]
    async fn get_client_routed(
        &self,
        proxied: bool,
    ) -> Result<CompactTxStreamerClient<Channel>, GetClientError> {
        let channel = self.connect_channel(proxied).await?;
        Ok(CompactTxStreamerClient::new(channel))
    }

    #[cfg(not(feature = "nym"))]
    async fn time_boxed_call<T>(
        &self,
        payload: T,
    ) -> Result<(CompactTxStreamerClient<Channel>, Request<T>), GetClientError> {
        let client = self.get_client().await?;
        let mut request = Request::new(payload);
        request.set_timeout(DEFAULT_GRPC_TIMEOUT);
        Ok((client, request))
    }

    #[cfg(feature = "nym")]
    async fn time_boxed_call_routed<T>(
        &self,
        payload: T,
        proxied: bool,
    ) -> Result<(CompactTxStreamerClient<Channel>, Request<T>), GetClientError> {
        let client = self.get_client_routed(proxied).await?;
        let mut request = Request::new(payload);
        let timeout = if proxied {
            DEFAULT_NYM_GRPC_TIMEOUT
        } else {
            DEFAULT_GRPC_TIMEOUT
        };
        request.set_timeout(timeout);
        Ok((client, request))
    }

    #[cfg(not(feature = "nym"))]
    async fn stream_call<T>(
        &self,
        payload: T,
    ) -> Result<(CompactTxStreamerClient<Channel>, Request<T>), GetClientError> {
        let client = self.get_client().await?;
        Ok((client, Request::new(payload)))
    }

    #[cfg(feature = "nym")]
    async fn stream_call_routed<T>(
        &self,
        payload: T,
        proxied: bool,
    ) -> Result<(CompactTxStreamerClient<Channel>, Request<T>), GetClientError> {
        let client = self.get_client_routed(proxied).await?;
        Ok((client, Request::new(payload)))
    }
}

#[cfg(feature = "back_compatible")]
impl GrpcIndexer {
    /// Return a gRPC client using `zcash_client_backend`'s generated types,
    /// for compatibility with code that expects that crate's
    /// `CompactTxStreamerClient` (e.g. pepper-sync).
    #[cfg(not(feature = "nym"))]
    pub async fn get_zcb_client(
        &self,
    ) -> Result<
        zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient<
            Channel,
        >,
        GetClientError,
    > {
        let channel = self.endpoint.connect().await?;
        Ok(
            zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient::new(channel),
        )
    }

    #[cfg(feature = "nym")]
    pub async fn get_zcb_client(
        &self,
        proxied: bool,
    ) -> Result<
        zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient<
            Channel,
        >,
        GetClientError,
    > {
        let channel = self.connect_channel(proxied).await?;
        Ok(
            zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient::new(channel),
        )
    }
}

impl Indexer for GrpcIndexer {
    type GetInfoError = GetInfoError;
    type GetLatestBlockError = GetLatestBlockError;
    type SendTransactionError = SendTransactionError;
    type GetTreeStateError = GetTreeStateError;
    type GetBlockError = GetBlockError;
    type GetBlockNullifiersError = GetBlockNullifiersError;
    type GetBlockRangeError = GetBlockRangeError;
    type GetBlockRangeNullifiersError = GetBlockRangeNullifiersError;
    type GetTransactionError = GetTransactionError;
    type GetMempoolTxError = GetMempoolTxError;
    type GetMempoolStreamError = GetMempoolStreamError;
    type GetLatestTreeStateError = GetLatestTreeStateError;
    type GetSubtreeRootsError = GetSubtreeRootsError;
    #[cfg(feature = "ping-very-insecure")]
    type PingError = PingError;

    #[cfg(not(feature = "nym"))]
    async fn get_info(&self) -> Result<LightdInfo, GetInfoError> {
        let (mut client, request) = self.time_boxed_call(Empty {}).await?;
        Ok(client.get_lightd_info(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_info(&self, proxied: bool) -> Result<LightdInfo, GetInfoError> {
        let (mut client, request) = self.time_boxed_call_routed(Empty {}, proxied).await?;
        Ok(client.get_lightd_info(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_latest_block(&self) -> Result<BlockId, GetLatestBlockError> {
        let (mut client, request) = self.time_boxed_call(ChainSpec {}).await?;
        Ok(client.get_latest_block(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_latest_block(&self, proxied: bool) -> Result<BlockId, GetLatestBlockError> {
        let (mut client, request) = self.time_boxed_call_routed(ChainSpec {}, proxied).await?;
        Ok(client.get_latest_block(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn send_transaction(&self, tx_bytes: Box<[u8]>) -> Result<String, SendTransactionError> {
        let (mut client, request) = self
            .time_boxed_call(RawTransaction {
                data: tx_bytes.to_vec(),
                height: 0,
            })
            .await?;
        let sendresponse = client.send_transaction(request).await?.into_inner();
        if sendresponse.error_code == 0 {
            let mut transaction_id = sendresponse.error_message;
            if transaction_id.starts_with('\"') && transaction_id.ends_with('\"') {
                transaction_id = transaction_id[1..transaction_id.len() - 1].to_string();
            }
            Ok(transaction_id)
        } else {
            Err(SendTransactionError::SendRejected(format!(
                "{sendresponse:?}"
            )))
        }
    }
    #[cfg(feature = "nym")]
    async fn send_transaction(
        &self,
        tx_bytes: Box<[u8]>,
        proxied: bool,
    ) -> Result<String, SendTransactionError> {
        let (mut client, request) = self
            .time_boxed_call_routed(
                RawTransaction {
                    data: tx_bytes.to_vec(),
                    height: 0,
                },
                proxied,
            )
            .await?;
        let sendresponse = client.send_transaction(request).await?.into_inner();
        if sendresponse.error_code == 0 {
            let mut transaction_id = sendresponse.error_message;
            if transaction_id.starts_with('\"') && transaction_id.ends_with('\"') {
                transaction_id = transaction_id[1..transaction_id.len() - 1].to_string();
            }
            Ok(transaction_id)
        } else {
            Err(SendTransactionError::SendRejected(format!(
                "{sendresponse:?}"
            )))
        }
    }

    #[cfg(not(feature = "nym"))]
    async fn get_tree_state(&self, block_id: BlockId) -> Result<TreeState, GetTreeStateError> {
        let (mut client, request) = self.time_boxed_call(block_id).await?;
        Ok(client.get_tree_state(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_tree_state(
        &self,
        block_id: BlockId,
        proxied: bool,
    ) -> Result<TreeState, GetTreeStateError> {
        let (mut client, request) = self.time_boxed_call_routed(block_id, proxied).await?;
        Ok(client.get_tree_state(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_block(&self, block_id: BlockId) -> Result<CompactBlock, GetBlockError> {
        let (mut client, request) = self.time_boxed_call(block_id).await?;
        Ok(client.get_block(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_block(
        &self,
        block_id: BlockId,
        proxied: bool,
    ) -> Result<CompactBlock, GetBlockError> {
        let (mut client, request) = self.time_boxed_call_routed(block_id, proxied).await?;
        Ok(client.get_block(request).await?.into_inner())
    }

    #[allow(deprecated)]
    #[cfg(not(feature = "nym"))]
    async fn get_block_nullifiers(
        &self,
        block_id: BlockId,
    ) -> Result<CompactBlock, GetBlockNullifiersError> {
        let (mut client, request) = self.time_boxed_call(block_id).await?;
        Ok(client.get_block_nullifiers(request).await?.into_inner())
    }
    #[allow(deprecated)]
    #[cfg(feature = "nym")]
    async fn get_block_nullifiers(
        &self,
        block_id: BlockId,
        proxied: bool,
    ) -> Result<CompactBlock, GetBlockNullifiersError> {
        let (mut client, request) = self.time_boxed_call_routed(block_id, proxied).await?;
        Ok(client.get_block_nullifiers(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_block_range(
        &self,
        range: BlockRange,
    ) -> Result<tonic::Streaming<CompactBlock>, GetBlockRangeError> {
        let (mut client, request) = self.stream_call(range).await?;
        Ok(client.get_block_range(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_block_range(
        &self,
        range: BlockRange,
        proxied: bool,
    ) -> Result<tonic::Streaming<CompactBlock>, GetBlockRangeError> {
        let (mut client, request) = self.stream_call_routed(range, proxied).await?;
        Ok(client.get_block_range(request).await?.into_inner())
    }

    #[allow(deprecated)]
    #[cfg(not(feature = "nym"))]
    async fn get_block_range_nullifiers(
        &self,
        range: BlockRange,
    ) -> Result<tonic::Streaming<CompactBlock>, GetBlockRangeNullifiersError> {
        let (mut client, request) = self.stream_call(range).await?;
        Ok(client
            .get_block_range_nullifiers(request)
            .await?
            .into_inner())
    }
    #[allow(deprecated)]
    #[cfg(feature = "nym")]
    async fn get_block_range_nullifiers(
        &self,
        range: BlockRange,
        proxied: bool,
    ) -> Result<tonic::Streaming<CompactBlock>, GetBlockRangeNullifiersError> {
        let (mut client, request) = self.stream_call_routed(range, proxied).await?;
        Ok(client
            .get_block_range_nullifiers(request)
            .await?
            .into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_transaction(
        &self,
        filter: TxFilter,
    ) -> Result<RawTransaction, GetTransactionError> {
        let (mut client, request) = self.time_boxed_call(filter).await?;
        Ok(client.get_transaction(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_transaction(
        &self,
        filter: TxFilter,
        proxied: bool,
    ) -> Result<RawTransaction, GetTransactionError> {
        let (mut client, request) = self.time_boxed_call_routed(filter, proxied).await?;
        Ok(client.get_transaction(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_mempool_tx(
        &self,
        request: GetMempoolTxRequest,
    ) -> Result<tonic::Streaming<CompactTx>, GetMempoolTxError> {
        let (mut client, request) = self.stream_call(request).await?;
        Ok(client.get_mempool_tx(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_mempool_tx(
        &self,
        request: GetMempoolTxRequest,
        proxied: bool,
    ) -> Result<tonic::Streaming<CompactTx>, GetMempoolTxError> {
        let (mut client, request) = self.stream_call_routed(request, proxied).await?;
        Ok(client.get_mempool_tx(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_mempool_stream(
        &self,
    ) -> Result<tonic::Streaming<RawTransaction>, GetMempoolStreamError> {
        let (mut client, request) = self.stream_call(Empty {}).await?;
        Ok(client.get_mempool_stream(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_mempool_stream(
        &self,
        proxied: bool,
    ) -> Result<tonic::Streaming<RawTransaction>, GetMempoolStreamError> {
        let (mut client, request) = self.stream_call_routed(Empty {}, proxied).await?;
        Ok(client.get_mempool_stream(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_latest_tree_state(&self) -> Result<TreeState, GetLatestTreeStateError> {
        let (mut client, request) = self.time_boxed_call(Empty {}).await?;
        Ok(client.get_latest_tree_state(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_latest_tree_state(
        &self,
        proxied: bool,
    ) -> Result<TreeState, GetLatestTreeStateError> {
        let (mut client, request) = self.time_boxed_call_routed(Empty {}, proxied).await?;
        Ok(client.get_latest_tree_state(request).await?.into_inner())
    }

    #[cfg(not(feature = "nym"))]
    async fn get_subtree_roots(
        &self,
        arg: GetSubtreeRootsArg,
    ) -> Result<tonic::Streaming<SubtreeRoot>, GetSubtreeRootsError> {
        let (mut client, request) = self.stream_call(arg).await?;
        Ok(client.get_subtree_roots(request).await?.into_inner())
    }
    #[cfg(feature = "nym")]
    async fn get_subtree_roots(
        &self,
        arg: GetSubtreeRootsArg,
        proxied: bool,
    ) -> Result<tonic::Streaming<SubtreeRoot>, GetSubtreeRootsError> {
        let (mut client, request) = self.stream_call_routed(arg, proxied).await?;
        Ok(client.get_subtree_roots(request).await?.into_inner())
    }

    #[cfg(all(feature = "ping-very-insecure", not(feature = "nym")))]
    async fn ping(&self, duration: ProtoDuration) -> Result<PingResponse, PingError> {
        let (mut client, request) = self.time_boxed_call(duration).await?;
        Ok(client.ping(request).await?.into_inner())
    }
    #[cfg(all(feature = "ping-very-insecure", feature = "nym"))]
    async fn ping(
        &self,
        duration: ProtoDuration,
        proxied: bool,
    ) -> Result<PingResponse, PingError> {
        let (mut client, request) = self.time_boxed_call_routed(duration, proxied).await?;
        Ok(client.ping(request).await?.into_inner())
    }
}

#[cfg(test)]
mod proto_agreement;

#[cfg(test)]
mod tests {
    //! Unit and integration-style tests for `zingo-netutils`.
    //!
    //! These tests focus on:
    //! - TLS test asset sanity (`test-data/localhost.pem` + `.key`)
    //! - Rustls plumbing (adding a local cert to a root store)
    //! - Connector correctness (scheme validation, HTTP/2 expectations)
    //! - URI rewrite behavior (no panics; returns structured errors)
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

    use super::*;

    use tokio_rustls::rustls::RootCertStore;

    /// Install the ring crypto provider for tests. This is needed when the
    /// `nym` feature is enabled because nym's dependencies pull in both
    /// `ring` and `aws-lc-rs`, which prevents automatic provider selection.
    fn ensure_crypto_provider() {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    }

    fn add_test_cert_to_roots(roots: &mut RootCertStore) {
        use tonic::transport::CertificateDer;
        eprintln!("Adding test cert to roots");

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
    ///
    /// Implementation notes:
    /// - Uses a local TLS server with the committed cert/key.
    /// - Uses strict timeouts to prevent hangs under nextest.
    /// - Explicitly drains the request body and disables keep-alive so that
    ///   `serve_connection` terminates deterministically.
    /// - Installs the rustls crypto provider to avoid provider
    ///   selection panics in test binaries.
    #[tokio::test]
    async fn add_test_cert_to_roots_enables_tls_handshake() {
        ensure_crypto_provider();
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

        timeout(Duration::from_secs(1), ready_rx)
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
    /// This test is intended to fail until production code checks for
    /// `http` and `https` schemes only, rejecting everything else (e.g. `ftp`).
    #[test]
    fn rejects_non_http_schemes() {
        let uri: http::Uri = "ftp://example.com:1234".parse().unwrap();
        let res = GrpcIndexer::new(uri);

        assert!(
            res.is_err(),
            "expected GrpcIndexer::new() to reject non-http(s) schemes, but got Ok"
        );
    }

    /// Demonstrates the HTTPS downgrade hazard: the underlying client can successfully
    /// talk to an HTTP/1.1-only TLS server if the HTTPS branch does not enforce HTTP/2.
    ///
    /// This is intentionally written as a “should be HTTP/2” test so it fails until
    /// the HTTPS client is constructed with `http2_only(true)`.
    #[tokio::test]
    async fn https_connector_must_not_downgrade_to_http1() {
        ensure_crypto_provider();
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
        ensure_crypto_provider();
        let endpoint =
            "https://zec.rocks:443".to_string();

        let uri: http::Uri = endpoint.parse().expect("bad mainnet indexer URI");

        let response = GrpcIndexer::new(uri)
            .expect("URI to be valid.")
            .get_info(
                #[cfg(feature = "nym")]
                false,
            )
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

    /// The proto spec says:
    ///   "If range.start <= range.end, blocks are returned increasing height order;
    ///    otherwise blocks are returned in decreasing height order."
    ///
    /// Our doc for `get_block_range` currently claims ascending-only.
    /// This test requests a descending range (start > end) and asserts
    /// the server returns blocks in decreasing height order.
    #[tokio::test]
    async fn get_block_range_supports_descending_order() {
        ensure_crypto_provider();
        use tokio_stream::StreamExt;

        let uri: http::Uri =
            "https://zec.rocks:443"
                .parse()
                .unwrap();
        let indexer = GrpcIndexer::new(uri).expect("valid URI");

        let tip = indexer
            .get_latest_block(
                #[cfg(feature = "nym")]
                false,
            )
            .await
            .expect("get_latest_block");
        let start_height = tip.height;
        let end_height = start_height.saturating_sub(4);

        // start > end → proto says descending order
        let range = BlockRange {
            start: Some(BlockId {
                height: start_height,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: end_height,
                hash: vec![],
            }),
            pool_types: vec![],
        };

        let mut stream = indexer
            .get_block_range(
                range,
                #[cfg(feature = "nym")]
                false,
            )
            .await
            .expect("get_block_range");

        let mut heights = Vec::new();
        while let Some(block) = stream.next().await {
            let block = block.expect("stream item");
            heights.push(block.height);
        }

        assert!(
            !heights.is_empty(),
            "expected at least one block in the descending range",
        );

        // The proto guarantees descending order when start > end.
        // If this assertion fails, the server does not support descending ranges.
        for window in heights.windows(2) {
            assert!(
                window[0] > window[1],
                "expected descending order, but got heights: {heights:?}",
            );
        }
    }

    // ── Nym integration tests ───────────────────────────────────────
    // Run with: cargo test -p zingo-netutils --features nym

    /// Helper: create a `GrpcIndexer` with a validated built-in Nym proxy.
    #[cfg(feature = "nym")]
    async fn nym_test_indexer() -> GrpcIndexer {
        ensure_crypto_provider();
        // Must use a publicly routable server — Nym exit gateways are on the
        // public internet and cannot reach Tailscale (*.ts.net) addresses.
        let uri: http::Uri = "https://zec.rocks:443".parse().unwrap();
        GrpcIndexer::new(uri)
            .expect("valid URI")
            .with_nym()
            .await
            .expect("with_nym should start and validate proxy")
    }

    /// Verify that `proxied: false` works as a normal direct connection
    /// without touching the proxy.
    #[cfg(feature = "nym")]
    #[tokio::test]
    async fn proxied_false_bypasses_proxy() {
        ensure_crypto_provider();
        let uri: http::Uri =
            "https://zec.rocks:443"
                .parse()
                .unwrap();
        let indexer = GrpcIndexer::new(uri).expect("valid URI");

        let info = indexer.get_info(false).await.expect("get_info(false)");
        assert!(!info.chain_name.is_empty());
        assert!(info.block_height > 0);

        // No proxy was configured, so nym_proxy should be None.
        assert!(
            indexer.nym_proxy.is_none(),
            "nym_proxy should be None when no proxy is configured"
        );
    }

    /// Verify that `proxied: true` without a configured proxy returns
    /// `GetClientError::NoProxy`.
    #[cfg(feature = "nym")]
    #[tokio::test]
    async fn proxied_true_without_proxy_errors() {
        ensure_crypto_provider();
        let uri: http::Uri =
            "https://zec.rocks:443"
                .parse()
                .unwrap();
        let indexer = GrpcIndexer::new(uri).expect("valid URI");

        let err = indexer
            .get_info(true)
            .await
            .expect_err("should fail without proxy");
        assert!(
            err.to_string().contains("no proxy configured"),
            "expected NoProxy error, got: {err}"
        );
    }

    /// Simple get_info test using manual `NymProxy::start()` +
    /// `with_socks_proxy()`.
    #[cfg(feature = "nym")]
    #[tokio::test(flavor = "multi_thread")]
    async fn get_info_via_manual_socks_proxy() {
        ensure_crypto_provider();

        // Manual setup with retry (Nym gateways are flaky).
        let mut proxy = None;
        let mut indexer = None;
        for attempt in 0..5 {
            let p = match NymProxy::start().await {
                Ok(p) => p,
                Err(_) if attempt < 4 => continue,
                Err(e) => panic!("NymProxy::start failed after 5 attempts: {e}"),
            };
            let addr = p.socks5_addr();
            // Must use a publicly routable server — Nym exit gateways are on the
            // public internet and cannot reach Tailscale (*.ts.net) addresses.
            let uri: http::Uri = "https://zec.rocks:443".parse().unwrap();
            let idx = GrpcIndexer::new(uri)
                .expect("valid URI")
                .with_socks_proxy(&addr);

            match tokio::time::timeout(Duration::from_secs(60), idx.get_info(true)).await {
                Ok(Ok(info)) if info.block_height > 0 => {
                    proxy = Some(p);
                    indexer = Some(idx);
                    break;
                }
                _ if attempt < 4 => {
                    p.disconnect().await;
                    continue;
                }
                Ok(Err(e)) => panic!("get_info failed after 5 attempts: {e}"),
                _ => panic!("get_info timed out on all 5 attempts"),
            }
        }
        let proxy = proxy.unwrap();
        let indexer = indexer.unwrap();

        let info = indexer
            .get_info(true)
            .await
            .expect("get_info via manual proxy");
        assert!(!info.chain_name.is_empty());
        assert!(info.block_height > 0);

        proxy.disconnect().await;
    }

    /// Simple get_info test using the `with_nym()` builder.
    #[cfg(feature = "nym")]
    #[tokio::test(flavor = "multi_thread")]
    async fn get_info_via_with_nym() {
        let indexer = nym_test_indexer().await;
        let info = indexer.get_info(true).await.expect("get_info via with_nym");
        let chain = info.chain_name.to_ascii_lowercase();
        assert!(
            chain.contains("main"),
            "expected mainnet, got chain_name={:?}",
            info.chain_name
        );
        assert!(info.block_height > 0);
    }

    /// Mirror of `connects_to_public_mainnet_indexer_and_gets_info`
    /// but routed over Nym.
    #[cfg(feature = "nym")]
    #[tokio::test(flavor = "multi_thread")]
    async fn get_latest_block_over_nym() {
        let indexer = nym_test_indexer().await;
        let block = indexer
            .get_latest_block(true)
            .await
            .expect("get_latest_block via nym");
        assert!(block.height > 0);
    }

    /// Mirror of `get_block_range_supports_descending_order` but
    /// routed over Nym.
    #[cfg(feature = "nym")]
    #[tokio::test(flavor = "multi_thread")]
    async fn get_block_range_over_nym() {
        use tokio_stream::StreamExt;

        let indexer = nym_test_indexer().await;
        let range = BlockRange {
            start: Some(BlockId {
                height: 2_000_000,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: 2_000_002,
                hash: vec![],
            }),
            pool_types: vec![],
        };

        let mut stream = indexer
            .get_block_range(range, true)
            .await
            .expect("get_block_range via nym");

        let mut count = 0;
        while let Some(block) = stream.next().await {
            let _ = block.expect("stream item");
            count += 1;
        }
        assert!(count > 0, "expected at least one block");
    }

    #[tokio::test]
    #[ignore = "requires Docker image 'grpc-echo-server'; build with: docker build -t grpc-echo-server -f grpc-echo-server/Dockerfile ."]
    async fn echo_server_records_peer_address() {
        // Start the container, publishing the internal 8137 port to an ephemeral host port.
        let output = std::process::Command::new("docker")
            .args(["run", "-d", "-p", "0:8137", "grpc-echo-server"])
            .output()
            .expect("failed to launch docker");
        assert!(
            output.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // RAII guard: removes the container when it drops out of scope.
        struct ContainerGuard(String);
        impl Drop for ContainerGuard {
            fn drop(&mut self) {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", &self.0])
                    .output();
            }
        }
        let _guard = ContainerGuard(container_id.clone());

        // Discover the host port Docker assigned.
        let port_output = std::process::Command::new("docker")
            .args(["port", &container_id, "8137"])
            .output()
            .expect("docker port failed");
        let port_str = String::from_utf8_lossy(&port_output.stdout);
        // Output is like "0.0.0.0:PORT\n" or "[::]:PORT\n"; take the last colon segment.
        let port: u16 = port_str
            .trim()
            .rsplit(':')
            .next()
            .expect("no colon in docker port output")
            .parse()
            .expect("port is not a number");

        let uri: http::Uri = format!("http://127.0.0.1:{port}")
            .parse()
            .expect("bad URI");

        // Wait for the server to be ready (up to 10 attempts, 200ms apart).
        let indexer = GrpcIndexer::new(uri).expect("URI to be valid");
        let mut info = None;
        for _ in 0..10 {
            match indexer
                .get_info(
                    #[cfg(feature = "nym")]
                    false,
                )
                .await
            {
                Ok(i) => {
                    info = Some(i);
                    break;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        let info = info.expect("server never became ready within 10 retries");

        assert_eq!(info.chain_name, "echo", "unexpected chain_name");
        assert!(
            info.vendor.contains("peer="),
            "vendor field should contain 'peer=', got: {:?}",
            info.vendor
        );

        // Extract and print the peer address the server observed.
        let peer = info
            .vendor
            .split("peer=")
            .nth(1)
            .unwrap_or("<not found>");
        println!("Echo server observed client peer address: {peer}");
    }

    /// Verify that cloned indexers share the same Nym proxy.
    #[cfg(feature = "nym")]
    #[tokio::test(flavor = "multi_thread")]
    async fn clone_shares_proxy() {
        let indexer = nym_test_indexer().await;
        let clone = indexer.clone();

        // Both should succeed using the same proxy.
        let (r1, r2) = tokio::join!(indexer.get_info(true), clone.get_info(true));
        r1.expect("original get_info via nym");
        r2.expect("clone get_info via nym");

        // Both point to the same Arc.
        assert!(std::sync::Arc::ptr_eq(
            indexer.nym_proxy.as_ref().unwrap(),
            clone.nym_proxy.as_ref().unwrap(),
        ));
    }
}
