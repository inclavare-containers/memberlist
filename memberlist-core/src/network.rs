use core::sync::atomic::Ordering;

use crate::transport::Connection;

use super::{base::Memberlist, delegate::Delegate, error::Error, proto::*, transport::Transport};

use agnostic_lite::RuntimeLite;
use bytes::{Buf, Bytes};
use futures::{
  future::FutureExt,
  stream::{FuturesUnordered, Stream},
};
mod packet;
mod stream;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  pub(crate) async fn send_ping_and_wait_for_ack(
    &self,
    target: &T::ResolvedAddress,
    ping: Ping<T::Id, T::ResolvedAddress>,
    deadline: <T::Runtime as RuntimeLite>::Instant,
  ) -> Result<bool, Error<T, D>> {
    let conn = match self.inner.transport.open(target, deadline).await {
      Ok(conn) => conn,
      Err(_) => {
        // If the node is actually dead we expect this to fail, so we
        // shouldn't spam the logs with it. After this point, errors
        // with the connection are real, unexpected errors and should
        // get propagated up.
        return Ok(false);
      }
    };

    let ping_sequence_number = ping.sequence_number();
    let (mut reader, mut writer) = conn.split();
    let res = <T::Runtime as RuntimeLite>::timeout_at(deadline, async {
      self
        .send_message(&mut writer, [Message::Ping(ping)])
        .await?;
      self.read_message(target, &mut reader).await
    })
    .await;

    let mut msg = match res {
      Ok(Ok(msg)) => msg,
      Ok(Err(e)) => return Err(e),
      Err(e) => return Err(Error::transport(std::io::Error::from(e).into())),
    };

    if msg.is_empty() {
      return Err(Error::custom("receive empty message".into()));
    }
    let mt = match MessageType::try_from(msg[0]) {
      Ok(mt) => mt,
      Err(val) => return Err(Error::UnknownMessageType(val)),
    };
    msg.advance(1);

    if let MessageType::Ack = mt {
      // The Ack is length-delimited in the message protocol. After advancing
      // past the message type tag, the buffer starts with a varint length
      // prefix. Decode and skip it so decode_sequence_number reads the raw
      // Ack fields.
      let (len_bytes, ack_len) = varing::decode_u32_varint(&msg)
        .map_err(|e| Error::custom(format!("decode ack length: {}", e).into()))?;
      let ack_len = usize::try_from(ack_len)
        .map_err(|_| Error::custom("decode ack length: length does not fit in usize".into()))?;
      let start = len_bytes.get();
      let end = start.checked_add(ack_len).ok_or_else(|| {
        Error::custom("decode ack length: overflow computing ack payload range".into())
      })?;
      let ack_data = msg.get(start..end).ok_or_else(|| {
        Error::custom("decode ack length: ack payload exceeds message buffer".into())
      })?;
      let seqn = match Ack::decode_sequence_number(ack_data) {
        Ok(seqn) => seqn.1,
        Err(e) => return Err(e.into()),
      };

      if seqn != ping_sequence_number {
        return Err(Error::sequence_number_mismatch(ping_sequence_number, seqn));
      }

      Ok(true)
    } else {
      Err(Error::unexpected_message(MessageType::Ack, mt))
    }
  }

  /// Returns an messages processor to encode/compress/encrypt messages
  pub(crate) fn unreliable_encoder<'a, M>(
    &'a self,
    packets: M,
  ) -> ProtoEncoder<T::Id, T::ResolvedAddress, M>
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'a,
  {
    #[allow(unused_mut)]
    let mut encoder = ProtoEncoder::new(self.inner.transport.max_packet_size())
      .with_messages(packets)
      .with_label(self.inner.opts.label().clone())
      .with_overhead(self.inner.transport.header_overhead());

    #[cfg(checksum)]
    if !self.inner.transport.packet_reliable() {
      encoder.maybe_checksum(self.inner.opts.checksum_algo());
    }

    #[cfg(feature = "encryption")]
    if !self.inner.transport.packet_secure() && self.encryption_enabled() {
      encoder.set_encryption(
        self.inner.opts.encryption_algo().unwrap(),
        self.inner.keyring.as_ref().unwrap().primary_key(),
      );
    }

    #[cfg(compression)]
    encoder.maybe_compression(self.inner.opts.compress_algo());

    encoder
  }

  /// Returns an messages processor to encode/compress/encrypt messages
  pub(crate) fn reliable_encoder<'a, M>(
    &'a self,
    packets: M,
  ) -> ProtoEncoder<T::Id, T::ResolvedAddress, M>
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'a,
  {
    #[allow(unused_mut)]
    let mut encoder = ProtoEncoder::new(self.inner.transport.max_packet_size())
      .with_messages(packets)
      .with_label(self.inner.opts.label().clone())
      .with_overhead(self.inner.transport.header_overhead());

    #[cfg(feature = "encryption")]
    if !self.inner.transport.stream_secure() && self.encryption_enabled() {
      encoder.set_encryption(
        self.inner.opts.encryption_algo().unwrap(),
        self.inner.keyring.as_ref().unwrap().primary_key(),
      );
    }

    #[cfg(compression)]
    encoder.maybe_compression(self.inner.opts.compress_algo());

    encoder
  }

  #[auto_enums::auto_enum(futures03::Stream)]
  pub(crate) async fn transport_send_packets<'a, M>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    msgs: M,
  ) -> impl Stream<Item = Result<(), Error<T, D>>> + Send + 'a
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'static,
  {
    let encoder = self.unreliable_encoder(msgs);
    match encoder.hint() {
      Err(e) => futures::stream::once(async { Err(e.into()) }),
      Ok(hint) => {
        #[cfg(not(offload))]
        {
          let _ = hint;
          FuturesUnordered::from_iter(encoder.encode().map(|res| match res {
            Ok(payload) => futures::future::Either::Left(self.raw_send_packet(addr, payload)),
            Err(e) => futures::future::Either::Right(Self::to_async_err(e.into())),
          }))
        }

        #[cfg(offload)]
        {
          match hint.should_offload(self.inner.opts.offload_size) {
            false => FuturesUnordered::from_iter(encoder.encode().map(|res| match res {
              Ok(payload) => futures::future::Either::Left(self.raw_send_packet(addr, payload)),
              Err(e) => futures::future::Either::Right(Self::to_async_err(e.into())),
            })),
            true => {
              #[cfg(not(feature = "rayon"))]
              {
                let payloads = encoder.blocking_encode::<T::Runtime>().await;
                FuturesUnordered::from_iter(payloads.into_iter().map(|res| match res {
                  Ok(payload) => futures::future::Either::Left(self.raw_send_packet(addr, payload)),
                  Err(e) => futures::future::Either::Right(Self::to_async_err(e.into())),
                }))
              }

              #[cfg(feature = "rayon")]
              {
                use rayon::iter::ParallelIterator;

                let payloads = encoder
                  .rayon_encode()
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  })
                  .collect::<Vec<_>>();

                FuturesUnordered::from_iter(payloads.into_iter().map(|payload| {
                  futures::future::Either::Left(self.raw_send_packet(addr, payload))
                }))
              }
            }
          }
        }
      }
    }
  }

  pub(crate) async fn send_message<'a, M>(
    &'a self,
    conn: &'a mut <T::Connection as Connection>::Writer,
    msgs: M,
  ) -> Result<(), Error<T, D>>
  where
    M: AsRef<[Message<T::Id, T::ResolvedAddress>]> + Send + Sync + 'static,
  {
    let encoder = self.reliable_encoder(msgs);

    match encoder.hint() {
      Err(e) => Err(e.into()),
      Ok(hint) => {
        #[cfg(not(offload))]
        {
          let _ = hint;
          let mut errs = OneOrMore::new();
          for res in encoder.encode() {
            match res {
              Ok(payload) => match self.raw_send_message(conn, payload).await {
                Ok(()) => {}
                Err(e) => errs.push(e),
              },
              Err(e) => errs.push(e.into()),
            }
          }

          Error::try_from_one_or_more(errs)
        }

        #[cfg(offload)]
        {
          match hint.should_offload(self.inner.opts.offload_size) {
            false => {
              let mut errs = OneOrMore::new();
              for res in encoder.encode() {
                match res {
                  Ok(payload) => match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  },
                  Err(e) => errs.push(e.into()),
                }
              }

              Error::try_from_one_or_more(errs)
            }
            true => {
              #[cfg(not(feature = "rayon"))]
              {
                let mut errs = OneOrMore::new();
                let payloads = encoder
                  .blocking_encode::<T::Runtime>()
                  .await
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  });

                for payload in payloads {
                  match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  }
                }

                Error::try_from_one_or_more(errs)
              }

              #[cfg(feature = "rayon")]
              {
                use rayon::iter::ParallelIterator;

                let payloads = encoder
                  .rayon_encode()
                  .filter_map(|res| match res {
                    Ok(payload) => Some(payload),
                    Err(e) => {
                      tracing::error!(err = %e, "memberlist.pakcet: failed to process packet");
                      None
                    }
                  })
                  .collect::<Vec<_>>();

                let mut errs = OneOrMore::new();
                for payload in payloads {
                  match self.raw_send_message(conn, payload).await {
                    Ok(()) => {}
                    Err(e) => errs.push(e),
                  }
                }

                Error::try_from_one_or_more(errs)
              }
            }
          }
        }
      }
    }
  }

  pub(crate) async fn read_message(
    &self,
    from: &T::ResolvedAddress,
    reader: &mut <T::Connection as Connection>::Reader,
  ) -> Result<Bytes, Error<T, D>> {
    self
      .inner
      .transport
      .read(from, reader)
      .await
      .map_err(Error::transport)?;

    let mut decoder = ProtoDecoder::new();

    #[cfg(offload)]
    decoder.with_offload_size(self.inner.opts.offload_size);

    #[cfg(feature = "encryption")]
    if self.encryption_enabled() {
      decoder
        .with_encryption(triomphe::Arc::from_iter(
          self.inner.keyring.as_ref().unwrap().keys(),
        ))
        .with_verify_incoming(self.inner.opts.gossip_verify_incoming);
    }

    if !self.inner.opts.skip_inbound_label_check {
      decoder.with_label(self.inner.opts.label().clone());
    }

    decoder
      .decode_from_reader::<_, T::Runtime>(reader)
      .await
      .map_err(|e| Error::transport(e.into()))
  }

  async fn raw_send_packet<'a>(
    &'a self,
    addr: &'a T::ResolvedAddress,
    payload: Payload,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .send_to(addr, payload)
      .await
      .map(|(_sent, _)| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!(
            "memberlist.packet.sent",
            self.inner.opts.metric_labels.iter()
          )
          .increment(_sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  async fn raw_send_message<'a>(
    &'a self,
    conn: &'a mut <T::Connection as Connection>::Writer,
    payload: Payload,
  ) -> Result<(), Error<T, D>> {
    self
      .inner
      .transport
      .write(conn, payload)
      .await
      .map(|_sent| {
        #[cfg(feature = "metrics")]
        {
          metrics::counter!(
            "memberlist.stream.sent",
            self.inner.opts.metric_labels.iter()
          )
          .increment(_sent as u64);
        }
      })
      .map_err(Error::transport)
  }

  async fn to_async_err(e: Error<T, D>) -> Result<(), Error<T, D>>
  where
    T: Transport,
    D: Delegate,
  {
    Err(e)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    borrow::Cow,
    io,
    net::SocketAddr,
    sync::{
      Arc,
      atomic::{AtomicBool, AtomicUsize, Ordering},
    },
  };

  use bytes::Bytes;
  use futures::{StreamExt, io::Cursor};
  use nodecraft::resolver::socket_addr::SocketAddrResolver;
  use smol_str::SmolStr;

  use crate::{
    Options,
    delegate::VoidDelegate,
    proto::{Payload, ProtoWriter},
    tests::get_memberlist,
    transport::{
      Connection, PacketSubscriber, StreamSubscriber, TransportError, packet_stream,
      promised_stream, unimplemented::UnimplementedReader,
    },
  };

  use super::*;

  type Runtime = agnostic_lite::tokio::TokioRuntime;
  type Resolver = SocketAddrResolver<Runtime>;
  type TestDelegate = VoidDelegate<SmolStr, SocketAddr>;

  #[derive(Debug, thiserror::Error)]
  #[error("{0}")]
  struct TestError(Cow<'static, str>);

  impl From<io::Error> for TestError {
    fn from(err: io::Error) -> Self {
      Self(Cow::Owned(err.to_string()))
    }
  }

  impl TransportError for TestError {
    fn is_remote_failure(&self) -> bool {
      true
    }

    fn custom(err: Cow<'static, str>) -> Self {
      Self(err)
    }
  }

  #[derive(Default)]
  struct TestConnection {
    reader: UnimplementedReader,
    writer: Cursor<Vec<u8>>,
  }

  impl Connection for TestConnection {
    type Reader = UnimplementedReader;
    type Writer = Cursor<Vec<u8>>;

    fn split(self) -> (Self::Reader, Self::Writer) {
      (self.reader, self.writer)
    }

    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      let _ = buf;
      unimplemented!()
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
      let _ = buf;
      unimplemented!()
    }

    async fn peek(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      let _ = buf;
      unimplemented!()
    }

    async fn peek_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
      let _ = buf;
      unimplemented!()
    }

    async fn write_all(&mut self, payload: &[u8]) -> io::Result<()> {
      self.writer.write_all(payload).await
    }

    async fn flush(&mut self) -> io::Result<()> {
      self.writer.flush().await
    }

    async fn close(&mut self) -> io::Result<()> {
      self.writer.close().await
    }
  }

  struct TestTransport {
    id: SmolStr,
    addr: SocketAddr,
    fail_send: Arc<AtomicBool>,
    sent_packets: Arc<AtomicUsize>,
  }

  impl TestTransport {
    fn new(fail_send: Arc<AtomicBool>, sent_packets: Arc<AtomicUsize>) -> Self {
      Self {
        id: "local".into(),
        addr: SocketAddr::from(([127, 0, 0, 1], 7946)),
        fail_send,
        sent_packets,
      }
    }
  }

  impl Transport for TestTransport {
    type Error = TestError;
    type Id = SmolStr;
    type Address = SocketAddr;
    type ResolvedAddress = SocketAddr;
    type Resolver = Resolver;
    type Connection = TestConnection;
    type Runtime = Runtime;
    type Options = ();

    async fn new(_: Self::Options) -> Result<Self, Self::Error> {
      Ok(Self::new(
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
      ))
    }

    async fn resolve(&self, addr: &Self::Address) -> Result<Self::ResolvedAddress, Self::Error> {
      Ok(*addr)
    }

    fn local_id(&self) -> &Self::Id {
      &self.id
    }

    fn local_address(&self) -> &Self::Address {
      &self.addr
    }

    fn advertise_address(&self) -> &Self::ResolvedAddress {
      &self.addr
    }

    fn max_packet_size(&self) -> usize {
      256
    }

    fn header_overhead(&self) -> usize {
      3
    }

    fn blocked_address(&self, _: &Self::ResolvedAddress) -> Result<(), Self::Error> {
      Ok(())
    }

    async fn send_to(
      &self,
      _: &Self::ResolvedAddress,
      packet: Payload,
    ) -> Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error> {
      if self.fail_send.load(Ordering::SeqCst) {
        return Err(TestError::custom(Cow::Borrowed("send failed")));
      }

      self.sent_packets.fetch_add(1, Ordering::SeqCst);
      Ok((packet.as_slice().len(), Runtime::now()))
    }

    async fn open(
      &self,
      _: &Self::ResolvedAddress,
      _: <Self::Runtime as RuntimeLite>::Instant,
    ) -> Result<Self::Connection, Self::Error> {
      Ok(TestConnection::default())
    }

    fn packet(
      &self,
    ) -> PacketSubscriber<Self::ResolvedAddress, <Self::Runtime as RuntimeLite>::Instant> {
      let (producer, subscriber) = packet_stream::<Self>();
      producer.close();
      subscriber
    }

    fn stream(&self) -> StreamSubscriber<Self::ResolvedAddress, Self::Connection> {
      let (producer, subscriber) = promised_stream::<Self>();
      producer.close();
      subscriber
    }

    fn packet_reliable(&self) -> bool {
      false
    }

    fn packet_secure(&self) -> bool {
      false
    }

    fn stream_secure(&self) -> bool {
      false
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
      Ok(())
    }
  }

  async fn test_memberlist(
    fail_send: Arc<AtomicBool>,
    sent_packets: Arc<AtomicUsize>,
  ) -> Memberlist<TestTransport, TestDelegate> {
    get_memberlist(
      TestTransport::new(fail_send, sent_packets),
      TestDelegate::default(),
      Options::default(),
    )
    .await
    .unwrap()
  }

  #[tokio::test]
  async fn encoders_use_transport_limits_and_options() {
    let memberlist = test_memberlist(
      Arc::new(AtomicBool::new(false)),
      Arc::new(AtomicUsize::new(0)),
    )
    .await;
    let messages = [Message::UserData(Bytes::from_static(b"payload"))];

    let unreliable = memberlist.unreliable_encoder(messages.clone());
    assert_eq!(unreliable.overhead(), 3);
    assert_eq!(unreliable.messages().as_ref().len(), 1);
    assert!(unreliable.hint().is_ok());

    let reliable = memberlist.reliable_encoder(messages);
    assert_eq!(reliable.overhead(), 3);
    assert_eq!(reliable.messages().as_ref().len(), 1);
    assert!(reliable.hint().is_ok());

    memberlist.shutdown().await.unwrap();
  }

  #[tokio::test]
  async fn transport_send_packets_forwards_successes_and_errors() {
    let fail_send = Arc::new(AtomicBool::new(false));
    let sent_packets = Arc::new(AtomicUsize::new(0));
    let memberlist = test_memberlist(fail_send.clone(), sent_packets.clone()).await;
    let target = SocketAddr::from(([127, 0, 0, 1], 9000));

    let stream = memberlist
      .transport_send_packets(&target, [Message::UserData(Bytes::from_static(b"ok"))])
      .await;
    let results = stream.collect::<Vec<_>>().await;
    assert_eq!(results.len(), 1);
    assert!(results.into_iter().all(|res| res.is_ok()));
    assert_eq!(sent_packets.load(Ordering::SeqCst), 1);

    fail_send.store(true, Ordering::SeqCst);
    let stream = memberlist
      .transport_send_packets(&target, [Message::UserData(Bytes::from_static(b"fail"))])
      .await;
    let results = stream.collect::<Vec<_>>().await;
    assert_eq!(results.len(), 1);
    assert!(matches!(results.into_iter().next(), Some(Err(_))));

    memberlist.shutdown().await.unwrap();
  }

  #[tokio::test]
  async fn send_message_writes_encoded_payloads_to_connection() {
    let memberlist = test_memberlist(
      Arc::new(AtomicBool::new(false)),
      Arc::new(AtomicUsize::new(0)),
    )
    .await;
    let (_, mut writer) = TestConnection::default().split();

    memberlist
      .send_message(
        &mut writer,
        [Message::UserData(Bytes::from_static(b"stream"))],
      )
      .await
      .unwrap();

    assert!(writer.position() > 0);
    memberlist.shutdown().await.unwrap();
  }
}
