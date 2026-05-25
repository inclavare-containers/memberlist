use std::{borrow::Cow, sync::Arc};

use memberlist_proto::{MessageType, ProtoEncoderError};
use nodecraft::Node;
use smallvec_wrapper::OneOrMore;
use smol_str::SmolStr;

use crate::{
  delegate::{Delegate, DelegateError},
  proto::{DecodeError, EncodeError, ErrorResponse},
  transport::Transport,
};

#[cfg(any(
  feature = "crc32",
  feature = "xxhash64",
  feature = "xxhash32",
  feature = "xxhash3",
  feature = "murmur3",
))]
use crate::proto::ChecksumError;

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "snappy",
  feature = "brotli",
))]
use crate::proto::CompressionError;

#[cfg(feature = "encryption")]
pub use crate::proto::EncryptionError;

pub use crate::transport::TransportError;

/// Error type for the [`Memberlist`](crate::Memberlist)
#[derive(thiserror::Error)]
pub enum Error<T: Transport, D: Delegate> {
  /// Returns when the node is not running.
  #[error("node is not running, please bootstrap first")]
  NotRunning,
  /// Returns when timeout waiting for update broadcast.
  #[error("timeout waiting for update broadcast")]
  UpdateTimeout,
  /// Returns when timeout waiting for leave broadcast.
  #[error("timeout waiting for leave broadcast")]
  LeaveTimeout,
  /// Returns when lost connection with a peer.
  #[error("no response from node {0}")]
  Lost(Node<T::Id, T::ResolvedAddress>),
  /// Delegate error
  #[error(transparent)]
  Delegate(#[from] DelegateError<D>),
  /// Transport error
  #[error(transparent)]
  Transport(T::Error),
  /// Returned when a message is received with an unexpected type.
  #[error("unexpected message: expected {expected}, got {got}")]
  UnexpectedMessage {
    /// The expected message type.
    expected: MessageType,
    /// The actual message type.
    got: MessageType,
  },
  /// Returned when receive a unknown message type value
  #[error("unknown message type value {0}")]
  UnknownMessageType(u8),
  /// Returned when the sequence number of [`Ack`](crate::proto::Ack) is not
  /// match the sequence number of [`Ping`](crate::proto::Ping).
  #[error("sequence number mismatch: ping({ping}), ack({ack})")]
  SequenceNumberMismatch {
    /// The sequence number of [`Ping`](crate::proto::Ping).
    ping: u32,
    /// The sequence number of [`Ack`](crate::proto::Ack).
    ack: u32,
  },
  /// Failed to encode message
  #[error(transparent)]
  Encode(#[from] EncodeError),
  /// Failed to decode message
  #[error(transparent)]
  Decode(#[from] DecodeError),
  /// Returned when a remote error is received.
  #[error("remote error: {0}")]
  Remote(SmolStr),
  /// Multiple errors
  #[error("errors:\n{}", format_multiple_errors(.0))]
  Multiple(Arc<[Self]>),
  /// Returned when a custom error is created by users.
  #[error("{0}")]
  Other(Cow<'static, str>),
  /// Encryption error
  #[error(transparent)]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  Encryption(#[from] EncryptionError),
  /// Compressor error
  #[error(transparent)]
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "snappy",
      feature = "brotli",
    )))
  )]
  Compression(#[from] CompressionError),
  /// Checksum error
  #[error(transparent)]
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash64",
    feature = "xxhash32",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash64",
      feature = "xxhash32",
      feature = "xxhash3",
      feature = "murmur3",
    )))
  )]
  Checksum(#[from] ChecksumError),
}

impl<T: Transport, D: Delegate> core::fmt::Debug for Error<T, D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{self}")
  }
}

impl<T: Transport, D: Delegate> From<ProtoEncoderError> for Error<T, D> {
  fn from(value: ProtoEncoderError) -> Self {
    match value {
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "snappy",
        feature = "brotli",
      ))]
      ProtoEncoderError::Compress(e) => Self::Compression(e),
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash64",
        feature = "xxhash32",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      ProtoEncoderError::Checksum(e) => Self::Checksum(e),
      #[cfg(feature = "encryption")]
      ProtoEncoderError::Encrypt(e) => Self::Encryption(e),
      ProtoEncoderError::Encode(e) => Self::Encode(e),
    }
  }
}

impl<T: Transport, D: Delegate> FromIterator<Error<T, D>> for Option<Error<T, D>> {
  fn from_iter<I: IntoIterator<Item = Error<T, D>>>(iter: I) -> Self {
    let errors = iter.into_iter().collect::<OneOrMore<_>>();
    let num_errs = errors.len();
    match num_errs {
      0 => None,
      _ => Some(match errors.into_either() {
        either::Either::Left([e]) => e,
        either::Either::Right(e) => Error::Multiple(e.into_vec().into()),
      }),
    }
  }
}

impl<T: Transport, D: Delegate> Error<T, D> {
  /// Returns an iterator over the errors.
  #[auto_enums::auto_enum(Iterator)]
  #[inline]
  pub fn iter(&self) -> impl Iterator<Item = &Self> {
    match self {
      Self::Multiple(errors) => errors.iter(),
      _ => std::iter::once(self),
    }
  }

  /// Creates a new error with the given delegate error.
  #[inline]
  pub fn delegate(e: DelegateError<D>) -> Self {
    Self::Delegate(e)
  }

  /// Creates a [`Error::SequenceNumberMismatch`] error.
  #[inline]
  pub fn sequence_number_mismatch(ping: u32, ack: u32) -> Self {
    Self::SequenceNumberMismatch { ping, ack }
  }

  /// Creates a [`Error::UnexpectedMessage`] error.
  #[inline]
  pub fn unexpected_message(expected: MessageType, got: MessageType) -> Self {
    Self::UnexpectedMessage { expected, got }
  }

  /// Creates a new error with the given transport error.
  #[inline]
  pub fn transport(e: T::Error) -> Self {
    Self::Transport(e)
  }

  /// Creates a new error with the given remote error.
  #[inline]
  pub fn remote(e: ErrorResponse) -> Self {
    Self::Remote(e.into())
  }

  /// Creates a new error with the given message.
  #[inline]
  pub fn custom(e: std::borrow::Cow<'static, str>) -> Self {
    Self::Other(e)
  }

  #[inline]
  pub(crate) async fn try_from_stream<S>(stream: S) -> Result<(), Self>
  where
    S: futures::stream::Stream<Item = Self>,
  {
    use futures::stream::StreamExt;

    Self::try_from_one_or_more(stream.collect::<OneOrMore<_>>().await)
  }

  #[inline]
  pub(crate) fn try_from_one_or_more(errs: OneOrMore<Self>) -> Result<(), Self> {
    let num = errs.len();
    match num {
      0 => Ok(()),
      _ => Err(match errs.into_either() {
        either::Either::Left([e]) => e,
        either::Either::Right(e) => Self::Multiple(e.into_vec().into()),
      }),
    }
  }

  #[inline]
  pub(crate) fn is_remote_failure(&self) -> bool {
    match self {
      Self::Transport(e) => e.is_remote_failure(),
      Self::Multiple(errors) => errors.iter().any(Self::is_remote_failure),
      _ => false,
    }
  }
}

fn format_multiple_errors<T: Transport, D: Delegate>(errors: &[Error<T, D>]) -> String {
  errors
    .iter()
    .enumerate()
    .map(|(i, err)| format!("  {}. {}", i + 1, err))
    .collect::<Vec<_>>()
    .join("\n")
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use futures::stream;
  use memberlist_proto::MessageType;
  use nodecraft::resolver::socket_addr::SocketAddrResolver;
  use smallvec_wrapper::OneOrMore;
  use smol_str::SmolStr;

  use super::*;
  use crate::{delegate::VoidDelegate, transport::unimplemented::UnimplementedTransport};

  type TestTransport = UnimplementedTransport<
    SmolStr,
    SocketAddrResolver<agnostic::tokio::TokioRuntime>,
    agnostic::tokio::TokioRuntime,
  >;
  type TestDelegate = VoidDelegate<SmolStr, SocketAddr>;
  type TestError = Error<TestTransport, TestDelegate>;

  #[test]
  fn from_iter_preserves_none_single_and_multiple() {
    let none = Vec::<TestError>::new().into_iter().collect::<Option<_>>();
    assert!(none.is_none());

    let single = [TestError::custom("single".into())]
      .into_iter()
      .collect::<Option<_>>()
      .unwrap();
    assert_eq!(single.to_string(), "single");
    assert_eq!(single.iter().count(), 1);

    let multiple = [
      TestError::custom("first".into()),
      TestError::sequence_number_mismatch(7, 9),
    ]
    .into_iter()
    .collect::<Option<_>>()
    .unwrap();
    assert_eq!(multiple.iter().count(), 2);
    assert_eq!(
      multiple.to_string(),
      "errors:\n  1. first\n  2. sequence number mismatch: ping(7), ack(9)"
    );
  }

  #[test]
  fn constructors_and_debug_use_display_messages() {
    let unexpected = TestError::unexpected_message(MessageType::Ack, MessageType::Ping);
    assert_eq!(
      format!("{unexpected:?}"),
      "unexpected message: expected Ack, got Ping"
    );

    let remote = TestError::remote(ErrorResponse::new("remote failed"));
    assert_eq!(remote.to_string(), "remote error: remote failed");

    let unknown = TestError::UnknownMessageType(255);
    assert_eq!(unknown.to_string(), "unknown message type value 255");
  }

  #[test]
  fn try_from_one_or_more_returns_ok_single_or_multiple() {
    let empty = OneOrMore::<TestError>::new();
    assert!(TestError::try_from_one_or_more(empty).is_ok());

    let mut single = OneOrMore::new();
    single.push(TestError::custom("only".into()));
    assert_eq!(
      TestError::try_from_one_or_more(single)
        .unwrap_err()
        .to_string(),
      "only"
    );

    let mut multiple = OneOrMore::new();
    multiple.push(TestError::custom("one".into()));
    multiple.push(TestError::custom("two".into()));
    assert_eq!(
      TestError::try_from_one_or_more(multiple)
        .unwrap_err()
        .to_string(),
      "errors:\n  1. one\n  2. two"
    );
  }

  #[tokio::test]
  async fn try_from_stream_collects_errors() {
    assert!(TestError::try_from_stream(stream::empty()).await.is_ok());

    let err = TestError::try_from_stream(stream::iter([
      TestError::custom("stream one".into()),
      TestError::custom("stream two".into()),
    ]))
    .await
    .unwrap_err();

    assert_eq!(err.iter().count(), 2);
    assert_eq!(err.to_string(), "errors:\n  1. stream one\n  2. stream two");
  }
}
