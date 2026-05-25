use std::marker::PhantomData;

use nodecraft::Id;

use crate::transport::TransportError;

use super::*;

/// An error type for the [`UnimplementedTransport`].
#[derive(Debug, thiserror::Error)]
#[error("error for unimplemented transport")]
pub struct UnimplementedTransportError;

impl TransportError for UnimplementedTransportError {
  fn is_remote_failure(&self) -> bool {
    unimplemented!()
  }

  fn custom(_err: std::borrow::Cow<'static, str>) -> Self {
    unimplemented!()
  }
}

impl From<std::io::Error> for UnimplementedTransportError {
  fn from(_: std::io::Error) -> Self {
    unimplemented!()
  }
}

/// Unimplemented reader for testing purposes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnimplementedReader;

impl ProtoReader for UnimplementedReader {
  async fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn read_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn peek(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn peek_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }
}

/// Unimplemented writer for testing purposes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnimplementedWriter;

impl ProtoWriter for UnimplementedWriter {
  async fn write_all(&mut self, _: &[u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn close(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }
}

/// An unimplemented connection for testing purposes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnimplementedConnection;

impl Connection for UnimplementedConnection {
  type Reader = UnimplementedReader;
  type Writer = UnimplementedWriter;

  fn split(self) -> (Self::Reader, Self::Writer) {
    unimplemented!()
  }

  async fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn read_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn peek(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn peek_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn write_all(&mut self, _: &[u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn close(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }
}

/// A stream that does not implement any of the required methods.
/// Which can be only used for testing purposes.
pub struct UnimplementedStream<R>(PhantomData<R>);

/// A transport that does not implement any of the required methods.
/// Which can be only used for testing purposes.
pub struct UnimplementedTransport<I, A, R>(PhantomData<(I, A, R)>);

impl<I, A, R> Transport for UnimplementedTransport<I, A, R>
where
  I: Id + Data + Send + Sync + 'static,
  A: AddressResolver<Runtime = R>,
  A::Address: Send + Sync + 'static,
  A::ResolvedAddress: Data + Send + Sync + 'static,
  R: RuntimeLite,
{
  type Error = UnimplementedTransportError;

  type Id = I;

  type Address = A::Address;

  type ResolvedAddress = A::ResolvedAddress;

  type Resolver = A;

  type Connection = UnimplementedConnection;

  type Runtime = R;

  type Options = ();

  async fn new(_: Self::Options) -> Result<Self, Self::Error> {
    unimplemented!()
  }

  async fn resolve(
    &self,
    _: &<Self::Resolver as AddressResolver>::Address,
  ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
    unimplemented!()
  }

  fn local_id(&self) -> &Self::Id {
    unimplemented!()
  }

  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    unimplemented!()
  }

  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    unimplemented!()
  }

  fn max_packet_size(&self) -> usize {
    unimplemented!()
  }

  fn header_overhead(&self) -> usize {
    unimplemented!()
  }

  fn blocked_address(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error> {
    unimplemented!()
  }

  async fn send_to(
    &self,
    _: &Self::ResolvedAddress,
    _: Payload,
  ) -> Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error> {
    unimplemented!()
  }

  async fn open(
    &self,
    _: &Self::ResolvedAddress,
    _: <Self::Runtime as RuntimeLite>::Instant,
  ) -> Result<Self::Connection, Self::Error> {
    unimplemented!()
  }

  fn packet(
    &self,
  ) -> crate::transport::PacketSubscriber<
    <Self::Resolver as AddressResolver>::ResolvedAddress,
    <Self::Runtime as RuntimeLite>::Instant,
  > {
    unimplemented!()
  }

  fn stream(&self) -> StreamSubscriber<Self::ResolvedAddress, Self::Connection> {
    unimplemented!()
  }

  fn packet_reliable(&self) -> bool {
    unimplemented!()
  }

  fn packet_secure(&self) -> bool {
    unimplemented!()
  }

  fn stream_secure(&self) -> bool {
    unimplemented!()
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use std::{borrow::Cow, io, marker::PhantomData, net::SocketAddr, panic::AssertUnwindSafe};

  use futures::FutureExt;
  use nodecraft::resolver::socket_addr::SocketAddrResolver;
  use smol_str::SmolStr;

  use super::*;

  type Runtime = agnostic_lite::tokio::TokioRuntime;
  type Resolver = SocketAddrResolver<Runtime>;
  type TestTransport = UnimplementedTransport<SmolStr, Resolver, Runtime>;

  fn transport() -> TestTransport {
    UnimplementedTransport(PhantomData)
  }

  #[test]
  fn unimplemented_transport_error_methods_panic() {
    assert!(std::panic::catch_unwind(|| UnimplementedTransportError.is_remote_failure()).is_err());
    assert!(
      std::panic::catch_unwind(|| UnimplementedTransportError::custom(Cow::Borrowed("err")))
        .is_err()
    );
    assert!(
      std::panic::catch_unwind(|| UnimplementedTransportError::from(io::Error::other("err")))
        .is_err()
    );
  }

  #[tokio::test]
  async fn unimplemented_reader_writer_and_connection_methods_panic() {
    let mut reader = UnimplementedReader;
    let mut writer = UnimplementedWriter;
    let mut conn = UnimplementedConnection;
    let mut buf = [0; 1];

    assert!(
      AssertUnwindSafe(reader.read(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(reader.read_exact(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(reader.peek(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(reader.peek_exact(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );

    assert!(
      AssertUnwindSafe(writer.write_all(&buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(writer.flush())
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(writer.close())
        .catch_unwind()
        .await
        .is_err()
    );

    assert!(std::panic::catch_unwind(|| conn.split()).is_err());
    assert!(
      AssertUnwindSafe(conn.read(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(conn.read_exact(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(conn.peek(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(conn.peek_exact(&mut buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(conn.write_all(&buf))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(AssertUnwindSafe(conn.flush()).catch_unwind().await.is_err());
    assert!(AssertUnwindSafe(conn.close()).catch_unwind().await.is_err());
  }

  #[tokio::test]
  async fn unimplemented_transport_methods_panic() {
    let transport = transport();
    let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
    let deadline = Runtime::now();

    assert!(
      AssertUnwindSafe(TestTransport::new(()))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(transport.resolve(&addr))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(std::panic::catch_unwind(|| transport.local_id()).is_err());
    assert!(std::panic::catch_unwind(|| transport.local_address()).is_err());
    assert!(std::panic::catch_unwind(|| transport.advertise_address()).is_err());
    assert!(std::panic::catch_unwind(|| transport.max_packet_size()).is_err());
    assert!(std::panic::catch_unwind(|| transport.header_overhead()).is_err());
    assert!(std::panic::catch_unwind(|| transport.blocked_address(&addr)).is_err());
    assert!(
      AssertUnwindSafe(transport.send_to(&addr, Payload::new(0, 0)))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(
      AssertUnwindSafe(transport.open(&addr, deadline))
        .catch_unwind()
        .await
        .is_err()
    );
    assert!(std::panic::catch_unwind(|| transport.packet()).is_err());
    assert!(std::panic::catch_unwind(|| transport.stream()).is_err());
    assert!(std::panic::catch_unwind(|| transport.packet_reliable()).is_err());
    assert!(std::panic::catch_unwind(|| transport.packet_secure()).is_err());
    assert!(std::panic::catch_unwind(|| transport.stream_secure()).is_err());
    assert!(
      AssertUnwindSafe(transport.shutdown())
        .catch_unwind()
        .await
        .is_err()
    );
  }
}
