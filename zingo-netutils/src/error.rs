//! Error types for the [`Indexer`](super::Indexer) and
//! `TransparentIndexer` traits.
//!
//! Each trait method has a dedicated error enum so callers can
//! distinguish connection failures ([`GetClientError`]) from
//! server-side gRPC errors ([`tonic::Status`]).

/// Connection-level error returned by [`GrpcIndexer::new`](super::GrpcIndexer::new)
/// and the connection phase of every trait method.
///
/// Callers can depend on:
/// - `InvalidScheme` and `InvalidAuthority` are deterministic — retrying
///   with the same URI will always fail.
/// - `Transport` wraps a [`tonic::transport::Error`] and may be transient
///   (e.g. DNS resolution, TCP connect timeout). Retrying may succeed.
///
/// ```
/// use zingo_netutils::GetClientError;
///
/// let e = GetClientError::InvalidScheme;
/// assert_eq!(e.to_string(), "bad uri: invalid scheme");
///
/// let e = GetClientError::InvalidAuthority;
/// assert_eq!(e.to_string(), "bad uri: invalid authority");
///
/// // Transport variant accepts From<tonic::transport::Error>
/// let _: fn(tonic::transport::Error) -> GetClientError = GetClientError::from;
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetClientError {
    #[error("bad uri: invalid scheme")]
    InvalidScheme,

    #[error("bad uri: invalid authority")]
    InvalidAuthority,

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),

    #[cfg(feature = "nym")]
    #[error("SOCKS5 proxy error: {0}")]
    SocksProxy(std::io::Error),

    #[cfg(feature = "nym")]
    #[error("failed to start Nym proxy: {0}")]
    NymStart(Box<NymProxyError>),

    #[cfg(feature = "nym")]
    #[error(
        "proxied request but no proxy configured — call with_nym() or with_socks_proxy() first"
    )]
    NoProxy,
}

/// Error from [`NymProxy`](super::NymProxy) lifecycle operations.
#[cfg(feature = "nym")]
#[derive(Debug, thiserror::Error)]
pub enum NymProxyError {
    /// Failed to build the Nym mixnet client.
    #[error("failed to build Nym client: {0}")]
    Build(Box<nym_sdk::Error>),

    /// Failed to connect to the Nym mixnet.
    #[error("failed to connect to Nym mixnet: {0}")]
    Connect(Box<nym_sdk::Error>),

    /// Failed to query the Nym API for service providers.
    #[error("Nym API query failed: {0}")]
    DiscoveryApi(String),

    /// No public exit gateway could be discovered.
    #[error("no public Nym exit gateway found")]
    NoProvider,

    /// End-to-end connectivity check through the SOCKS5 tunnel failed.
    #[error("connectivity check failed: {0}")]
    ConnectivityCheck(String),
}

#[cfg(test)]
mod get_client_error_tests {
    use super::*;

    #[test]
    fn transport_from_conversion() {
        // Verify the From impl exists at compile time.
        let _: fn(tonic::transport::Error) -> GetClientError = GetClientError::from;
    }

    #[cfg(feature = "nym")]
    #[test]
    fn socks_proxy_error_display() {
        let e = GetClientError::SocksProxy(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));
        assert!(e.to_string().contains("SOCKS5 proxy error"));
        assert!(e.to_string().contains("connection refused"));
    }
}

/// Error from the `get_info` (`GetLightdInfo`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetLightdInfoError` means the server received the request but
///   returned a gRPC status (e.g. `Unavailable`, `Internal`).
///
/// ```
/// use zingo_netutils::{GetClientError, GetInfoError};
///
/// let e = GetInfoError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetInfoError::GetClientError(_)));
///
/// let e = GetInfoError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetInfoError::GetLightdInfoError(_)));
/// assert!(e.to_string().contains("oops"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetInfoError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetLightdInfoError(#[from] tonic::Status),
}

/// Error from the `get_latest_block` (`GetLatestBlock`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetLatestBlockError` means the server returned a gRPC status.
///
/// ```
/// use zingo_netutils::{GetClientError, GetLatestBlockError};
///
/// let e = GetLatestBlockError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetLatestBlockError::GetClientError(_)));
///
/// let e = GetLatestBlockError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetLatestBlockError::GetLatestBlockError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetLatestBlockError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetLatestBlockError(#[from] tonic::Status),
}

/// Error from the `send_transaction` (`SendTransaction`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `SendTransactionError` means the server returned a gRPC status
///   before evaluating the transaction.
/// - `SendRejected` means the server received and evaluated the
///   transaction but rejected it (e.g. duplicate, invalid). The string
///   contains the server's rejection reason. This is **not** retryable
///   with the same transaction bytes.
///
/// ```
/// use zingo_netutils::{GetClientError, SendTransactionError};
///
/// let e = SendTransactionError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, SendTransactionError::GetClientError(_)));
///
/// let e = SendTransactionError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, SendTransactionError::SendTransactionError(_)));
///
/// let e = SendTransactionError::SendRejected("duplicate".into());
/// assert!(matches!(e, SendTransactionError::SendRejected(_)));
/// assert_eq!(e.to_string(), "send rejected: duplicate");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum SendTransactionError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    SendTransactionError(#[from] tonic::Status),

    #[error("send rejected: {0}")]
    SendRejected(String),
}

/// Error from the `get_tree_state` (`GetTreeState`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetTreeStateError` means the server returned a gRPC status
///   (e.g. the requested block does not exist).
///
/// ```
/// use zingo_netutils::{GetClientError, GetTreeStateError};
///
/// let e = GetTreeStateError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetTreeStateError::GetClientError(_)));
///
/// let e = GetTreeStateError::from(tonic::Status::not_found("no such block"));
/// assert!(matches!(e, GetTreeStateError::GetTreeStateError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetTreeStateError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetTreeStateError(#[from] tonic::Status),
}

/// Error from the `get_block` (`GetBlock`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetBlockError` means the server returned a gRPC status
///   (e.g. the requested block does not exist).
///
/// ```
/// use zingo_netutils::{GetClientError, GetBlockError};
///
/// let e = GetBlockError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetBlockError::GetClientError(_)));
///
/// let e = GetBlockError::from(tonic::Status::not_found("no such block"));
/// assert!(matches!(e, GetBlockError::GetBlockError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetBlockError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetBlockError(#[from] tonic::Status),
}

/// Error from the deprecated `get_block_nullifiers` (`GetBlockNullifiers`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetBlockNullifiersError` means the server returned a gRPC status.
///
/// ```
/// use zingo_netutils::{GetClientError, GetBlockNullifiersError};
///
/// let e = GetBlockNullifiersError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetBlockNullifiersError::GetClientError(_)));
///
/// let e = GetBlockNullifiersError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetBlockNullifiersError::GetBlockNullifiersError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetBlockNullifiersError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetBlockNullifiersError(#[from] tonic::Status),
}

/// Error from the `get_block_range` (`GetBlockRange`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetBlockRangeError` means the server returned a gRPC status
///   before or instead of streaming blocks.
///
/// ```
/// use zingo_netutils::{GetClientError, GetBlockRangeError};
///
/// let e = GetBlockRangeError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetBlockRangeError::GetClientError(_)));
///
/// let e = GetBlockRangeError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetBlockRangeError::GetBlockRangeError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetBlockRangeError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetBlockRangeError(#[from] tonic::Status),
}

/// Error from the deprecated `get_block_range_nullifiers` (`GetBlockRangeNullifiers`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetBlockRangeNullifiersError` means the server returned a gRPC status.
///
/// ```
/// use zingo_netutils::{GetClientError, GetBlockRangeNullifiersError};
///
/// let e = GetBlockRangeNullifiersError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetBlockRangeNullifiersError::GetClientError(_)));
///
/// let e = GetBlockRangeNullifiersError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetBlockRangeNullifiersError::GetBlockRangeNullifiersError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetBlockRangeNullifiersError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetBlockRangeNullifiersError(#[from] tonic::Status),
}

/// Error from the `get_transaction` (`GetTransaction`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetTransactionError` means the server returned a gRPC status
///   (e.g. the transaction was not found).
///
/// ```
/// use zingo_netutils::{GetClientError, GetTransactionError};
///
/// let e = GetTransactionError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetTransactionError::GetClientError(_)));
///
/// let e = GetTransactionError::from(tonic::Status::not_found("no such tx"));
/// assert!(matches!(e, GetTransactionError::GetTransactionError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetTransactionError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetTransactionError(#[from] tonic::Status),
}

/// Error from the `get_mempool_tx` (`GetMempoolTx`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetMempoolTxError` means the server returned a gRPC status
///   before or instead of streaming mempool transactions.
///
/// ```
/// use zingo_netutils::{GetClientError, GetMempoolTxError};
///
/// let e = GetMempoolTxError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetMempoolTxError::GetClientError(_)));
///
/// let e = GetMempoolTxError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetMempoolTxError::GetMempoolTxError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetMempoolTxError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetMempoolTxError(#[from] tonic::Status),
}

/// Error from the `get_mempool_stream` (`GetMempoolStream`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetMempoolStreamError` means the server returned a gRPC status
///   before or instead of opening the stream.
///
/// ```
/// use zingo_netutils::{GetClientError, GetMempoolStreamError};
///
/// let e = GetMempoolStreamError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetMempoolStreamError::GetClientError(_)));
///
/// let e = GetMempoolStreamError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetMempoolStreamError::GetMempoolStreamError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetMempoolStreamError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetMempoolStreamError(#[from] tonic::Status),
}

/// Error from the `get_latest_tree_state` (`GetLatestTreeState`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetLatestTreeStateError` means the server returned a gRPC status.
///
/// ```
/// use zingo_netutils::{GetClientError, GetLatestTreeStateError};
///
/// let e = GetLatestTreeStateError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetLatestTreeStateError::GetClientError(_)));
///
/// let e = GetLatestTreeStateError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetLatestTreeStateError::GetLatestTreeStateError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetLatestTreeStateError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetLatestTreeStateError(#[from] tonic::Status),
}

/// Error from the `get_subtree_roots` (`GetSubtreeRoots`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `GetSubtreeRootsError` means the server returned a gRPC status
///   before or instead of streaming subtree roots.
///
/// ```
/// use zingo_netutils::{GetClientError, GetSubtreeRootsError};
///
/// let e = GetSubtreeRootsError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, GetSubtreeRootsError::GetClientError(_)));
///
/// let e = GetSubtreeRootsError::from(tonic::Status::internal("oops"));
/// assert!(matches!(e, GetSubtreeRootsError::GetSubtreeRootsError(_)));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum GetSubtreeRootsError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    GetSubtreeRootsError(#[from] tonic::Status),
}

/// Error from the `ping` (`Ping`) RPC.
///
/// Callers can depend on:
/// - `GetClientError` means the connection was never established.
/// - `PingError` means the server returned a gRPC status (e.g. the
///   server was not started with `--ping-very-insecure`).
///
/// ```
/// # #[cfg(feature = "ping-very-insecure")]
/// # {
/// use zingo_netutils::{GetClientError, PingError};
///
/// let e = PingError::from(GetClientError::InvalidScheme);
/// assert!(matches!(e, PingError::GetClientError(_)));
///
/// let e = PingError::from(tonic::Status::permission_denied("not enabled"));
/// assert!(matches!(e, PingError::PingError(_)));
/// # }
/// ```
#[cfg(feature = "ping-very-insecure")]
#[derive(Debug, thiserror::Error)]
pub enum PingError {
    #[error(transparent)]
    GetClientError(#[from] GetClientError),

    #[error("gRPC error: {0}")]
    PingError(#[from] tonic::Status),
}

/// Helper to construct a `tonic::Status` for testing.
#[cfg(test)]
fn test_status() -> tonic::Status {
    tonic::Status::internal("test error")
}

/// Helper to construct a `GetClientError` for testing.
#[cfg(test)]
fn test_client_error() -> GetClientError {
    GetClientError::InvalidScheme
}

/// Macro that generates a test module for a standard 2-variant error enum
/// (GetClientError + tonic::Status).
#[cfg(test)]
macro_rules! two_variant_error_tests {
    ($mod_name:ident, $error_type:ident, $status_variant:ident) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn from_get_client_error() {
                let inner = test_client_error();
                let e = $error_type::from(inner);
                assert!(matches!(e, $error_type::GetClientError(_)));
                assert!(e.to_string().contains("invalid scheme"));
            }

            #[test]
            fn from_status() {
                let status = test_status();
                let e = $error_type::from(status);
                assert!(matches!(e, $error_type::$status_variant(_)));
                assert!(e.to_string().contains("test error"));
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    two_variant_error_tests!(get_info, GetInfoError, GetLightdInfoError);
    two_variant_error_tests!(get_latest_block, GetLatestBlockError, GetLatestBlockError);
    two_variant_error_tests!(get_tree_state, GetTreeStateError, GetTreeStateError);
    two_variant_error_tests!(get_block, GetBlockError, GetBlockError);
    two_variant_error_tests!(
        get_block_nullifiers,
        GetBlockNullifiersError,
        GetBlockNullifiersError
    );
    two_variant_error_tests!(get_block_range, GetBlockRangeError, GetBlockRangeError);
    two_variant_error_tests!(
        get_block_range_nullifiers,
        GetBlockRangeNullifiersError,
        GetBlockRangeNullifiersError
    );
    two_variant_error_tests!(get_transaction, GetTransactionError, GetTransactionError);
    two_variant_error_tests!(get_mempool_tx, GetMempoolTxError, GetMempoolTxError);
    two_variant_error_tests!(
        get_mempool_stream,
        GetMempoolStreamError,
        GetMempoolStreamError
    );
    two_variant_error_tests!(
        get_latest_tree_state,
        GetLatestTreeStateError,
        GetLatestTreeStateError
    );
    two_variant_error_tests!(
        get_subtree_roots,
        GetSubtreeRootsError,
        GetSubtreeRootsError
    );

    mod send_transaction {
        use super::*;

        #[test]
        fn from_get_client_error() {
            let inner = test_client_error();
            let e = SendTransactionError::from(inner);
            assert!(matches!(e, SendTransactionError::GetClientError(_)));
        }

        #[test]
        fn from_status() {
            let status = test_status();
            let e = SendTransactionError::from(status);
            assert!(matches!(e, SendTransactionError::SendTransactionError(_)));
        }

        #[test]
        fn send_rejected() {
            let e = SendTransactionError::SendRejected("bad tx".into());
            assert!(matches!(e, SendTransactionError::SendRejected(_)));
            assert_eq!(e.to_string(), "send rejected: bad tx");
        }
    }

    #[cfg(feature = "ping-very-insecure")]
    two_variant_error_tests!(ping, PingError, PingError);
}

// ── TransparentIndexer errors ───────────────────────────────────────

#[cfg(feature = "globally-public-transparent")]
pub mod transparent {
    use super::GetClientError;

    /// Error from the deprecated `get_taddress_txids` (`GetTaddressTxids`) RPC.
    ///
    /// Callers can depend on:
    /// - `GetClientError` means the connection was never established.
    /// - `GetTaddressTxidsError` means the server returned a gRPC status.
    ///
    /// ```
    /// # #[cfg(feature = "globally-public-transparent")]
    /// # {
    /// use zingo_netutils::error::transparent::GetTaddressTxidsError;
    /// use zingo_netutils::GetClientError;
    ///
    /// let e = GetTaddressTxidsError::from(GetClientError::InvalidScheme);
    /// assert!(matches!(e, GetTaddressTxidsError::GetClientError(_)));
    ///
    /// let e = GetTaddressTxidsError::from(tonic::Status::internal("oops"));
    /// assert!(matches!(e, GetTaddressTxidsError::GetTaddressTxidsError(_)));
    /// # }
    /// ```
    #[derive(Debug, thiserror::Error)]
    pub enum GetTaddressTxidsError {
        #[error(transparent)]
        GetClientError(#[from] GetClientError),

        #[error("gRPC error: {0}")]
        GetTaddressTxidsError(#[from] tonic::Status),
    }

    /// Error from the `get_taddress_transactions` (`GetTaddressTransactions`) RPC.
    ///
    /// Callers can depend on:
    /// - `GetClientError` means the connection was never established.
    /// - `GetTaddressTransactionsError` means the server returned a gRPC status.
    ///
    /// ```
    /// # #[cfg(feature = "globally-public-transparent")]
    /// # {
    /// use zingo_netutils::error::transparent::GetTaddressTransactionsError;
    /// use zingo_netutils::GetClientError;
    ///
    /// let e = GetTaddressTransactionsError::from(GetClientError::InvalidScheme);
    /// assert!(matches!(e, GetTaddressTransactionsError::GetClientError(_)));
    ///
    /// let e = GetTaddressTransactionsError::from(tonic::Status::internal("oops"));
    /// assert!(matches!(e, GetTaddressTransactionsError::GetTaddressTransactionsError(_)));
    /// # }
    /// ```
    #[derive(Debug, thiserror::Error)]
    pub enum GetTaddressTransactionsError {
        #[error(transparent)]
        GetClientError(#[from] GetClientError),

        #[error("gRPC error: {0}")]
        GetTaddressTransactionsError(#[from] tonic::Status),
    }

    /// Error from the `get_taddress_balance` (`GetTaddressBalance`) RPC.
    ///
    /// Callers can depend on:
    /// - `GetClientError` means the connection was never established.
    /// - `GetTaddressBalanceError` means the server returned a gRPC status.
    ///
    /// ```
    /// # #[cfg(feature = "globally-public-transparent")]
    /// # {
    /// use zingo_netutils::error::transparent::GetTaddressBalanceError;
    /// use zingo_netutils::GetClientError;
    ///
    /// let e = GetTaddressBalanceError::from(GetClientError::InvalidScheme);
    /// assert!(matches!(e, GetTaddressBalanceError::GetClientError(_)));
    ///
    /// let e = GetTaddressBalanceError::from(tonic::Status::internal("oops"));
    /// assert!(matches!(e, GetTaddressBalanceError::GetTaddressBalanceError(_)));
    /// # }
    /// ```
    #[derive(Debug, thiserror::Error)]
    pub enum GetTaddressBalanceError {
        #[error(transparent)]
        GetClientError(#[from] GetClientError),

        #[error("gRPC error: {0}")]
        GetTaddressBalanceError(#[from] tonic::Status),
    }

    /// Error from the `get_taddress_balance_stream` (`GetTaddressBalanceStream`) RPC.
    ///
    /// Callers can depend on:
    /// - `GetClientError` means the connection was never established.
    /// - `GetTaddressBalanceStreamError` means the server returned a gRPC status.
    ///
    /// ```
    /// # #[cfg(feature = "globally-public-transparent")]
    /// # {
    /// use zingo_netutils::error::transparent::GetTaddressBalanceStreamError;
    /// use zingo_netutils::GetClientError;
    ///
    /// let e = GetTaddressBalanceStreamError::from(GetClientError::InvalidScheme);
    /// assert!(matches!(e, GetTaddressBalanceStreamError::GetClientError(_)));
    ///
    /// let e = GetTaddressBalanceStreamError::from(tonic::Status::internal("oops"));
    /// assert!(matches!(e, GetTaddressBalanceStreamError::GetTaddressBalanceStreamError(_)));
    /// # }
    /// ```
    #[derive(Debug, thiserror::Error)]
    pub enum GetTaddressBalanceStreamError {
        #[error(transparent)]
        GetClientError(#[from] GetClientError),

        #[error("gRPC error: {0}")]
        GetTaddressBalanceStreamError(#[from] tonic::Status),
    }

    /// Error from the `get_address_utxos` (`GetAddressUtxos`) RPC.
    ///
    /// Callers can depend on:
    /// - `GetClientError` means the connection was never established.
    /// - `GetAddressUtxosError` means the server returned a gRPC status.
    ///
    /// ```
    /// # #[cfg(feature = "globally-public-transparent")]
    /// # {
    /// use zingo_netutils::error::transparent::GetAddressUtxosError;
    /// use zingo_netutils::GetClientError;
    ///
    /// let e = GetAddressUtxosError::from(GetClientError::InvalidScheme);
    /// assert!(matches!(e, GetAddressUtxosError::GetClientError(_)));
    ///
    /// let e = GetAddressUtxosError::from(tonic::Status::internal("oops"));
    /// assert!(matches!(e, GetAddressUtxosError::GetAddressUtxosError(_)));
    /// # }
    /// ```
    #[derive(Debug, thiserror::Error)]
    pub enum GetAddressUtxosError {
        #[error(transparent)]
        GetClientError(#[from] GetClientError),

        #[error("gRPC error: {0}")]
        GetAddressUtxosError(#[from] tonic::Status),
    }

    /// Error from the `get_address_utxos_stream` (`GetAddressUtxosStream`) RPC.
    ///
    /// Callers can depend on:
    /// - `GetClientError` means the connection was never established.
    /// - `GetAddressUtxosStreamError` means the server returned a gRPC status
    ///   before or instead of streaming UTXOs.
    ///
    /// ```
    /// # #[cfg(feature = "globally-public-transparent")]
    /// # {
    /// use zingo_netutils::error::transparent::GetAddressUtxosStreamError;
    /// use zingo_netutils::GetClientError;
    ///
    /// let e = GetAddressUtxosStreamError::from(GetClientError::InvalidScheme);
    /// assert!(matches!(e, GetAddressUtxosStreamError::GetClientError(_)));
    ///
    /// let e = GetAddressUtxosStreamError::from(tonic::Status::internal("oops"));
    /// assert!(matches!(e, GetAddressUtxosStreamError::GetAddressUtxosStreamError(_)));
    /// # }
    /// ```
    #[derive(Debug, thiserror::Error)]
    pub enum GetAddressUtxosStreamError {
        #[error(transparent)]
        GetClientError(#[from] GetClientError),

        #[error("gRPC error: {0}")]
        GetAddressUtxosStreamError(#[from] tonic::Status),
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn test_status() -> tonic::Status {
            tonic::Status::internal("test error")
        }

        fn test_client_error() -> GetClientError {
            GetClientError::InvalidScheme
        }

        macro_rules! two_variant_error_tests {
            ($mod_name:ident, $error_type:ident, $status_variant:ident) => {
                mod $mod_name {
                    use super::*;

                    #[test]
                    fn from_get_client_error() {
                        let inner = test_client_error();
                        let e = $error_type::from(inner);
                        assert!(matches!(e, $error_type::GetClientError(_)));
                        assert!(e.to_string().contains("invalid scheme"));
                    }

                    #[test]
                    fn from_status() {
                        let status = test_status();
                        let e = $error_type::from(status);
                        assert!(matches!(e, $error_type::$status_variant(_)));
                        assert!(e.to_string().contains("test error"));
                    }
                }
            };
        }

        two_variant_error_tests!(
            get_taddress_txids,
            GetTaddressTxidsError,
            GetTaddressTxidsError
        );
        two_variant_error_tests!(
            get_taddress_transactions,
            GetTaddressTransactionsError,
            GetTaddressTransactionsError
        );
        two_variant_error_tests!(
            get_taddress_balance,
            GetTaddressBalanceError,
            GetTaddressBalanceError
        );
        two_variant_error_tests!(
            get_taddress_balance_stream,
            GetTaddressBalanceStreamError,
            GetTaddressBalanceStreamError
        );
        two_variant_error_tests!(
            get_address_utxos,
            GetAddressUtxosError,
            GetAddressUtxosError
        );
        two_variant_error_tests!(
            get_address_utxos_stream,
            GetAddressUtxosStreamError,
            GetAddressUtxosStreamError
        );
    }
}
