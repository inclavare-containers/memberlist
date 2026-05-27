#![allow(missing_docs, warnings)]

use std::{net::SocketAddr, sync::Arc};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use memberlist_core::{
  proto::{Label, Message},
  tests::AnyError,
  transport::Transport,
};
use smol_str::SmolStr;

use crate::{Listener, StreamLayer};

/// A helper function to create TLS stream layer for testing
#[cfg(feature = "tls")]
pub async fn tls_stream_layer<R: Runtime>() -> crate::tls::TlsOptions {
  use crate::tls::{NoopCertificateVerifier, TlsOptions, rustls};
  use rustls::pki_types::{CertificateDer, PrivateKeyDer};

  let certs = test_cert_gen::gen_keys();

  let cfg = rustls::ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(
      vec![CertificateDer::from(
        certs.server.cert_and_key.cert.get_der().to_vec(),
      )],
      #[cfg(target_os = "linux")]
      PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        certs.server.cert_and_key.key.get_der().to_vec(),
      )),
      #[cfg(not(target_os = "linux"))]
      PrivateKeyDer::from(rustls::pki_types::PrivatePkcs1KeyDer::from(
        certs.server.cert_and_key.key.get_der().to_vec(),
      )),
    )
    .expect("bad certificate/key");
  let acceptor = futures_rustls::TlsAcceptor::from(Arc::new(cfg));

  let cfg = rustls::ClientConfig::builder()
    .dangerous()
    .with_custom_certificate_verifier(NoopCertificateVerifier::new())
    .with_no_client_auth();
  let connector = futures_rustls::TlsConnector::from(Arc::new(cfg));
  TlsOptions::new(
    rustls::pki_types::ServerName::IpAddress(
      "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
    ),
    acceptor,
    connector,
  )
}

#[cfg(all(test, feature = "tokio", feature = "tcp"))]
mod unit_tests {
  use std::{
    borrow::Cow,
    io,
    net::SocketAddr,
    sync::{
      Arc,
      atomic::{AtomicBool, Ordering},
    },
    time::Duration,
  };

  use agnostic::{
    Runtime, RuntimeLite,
    net::{Net, UdpSocket},
    tokio::TokioRuntime,
  };
  use memberlist_core::{
    proto::{CIDRsPolicy, Payload},
    transport::{Connection as _, Transport as _, TransportError as _},
  };
  use nodecraft::resolver::socket_addr::SocketAddrResolver;
  use smol_str::SmolStr;

  use crate::{
    NetTransport, NetTransportError, NetTransportOptions, PacketProcessor, PromisedProcessor,
    StreamLayer as _,
    stream_layer::{Listener as _, PromisedStream as _, tcp::Tcp},
  };

  type TestResolver = SocketAddrResolver<TokioRuntime>;
  type TestLayer = Tcp<TokioRuntime>;
  type TestTransport = NetTransport<SmolStr, TestResolver, TestLayer, TokioRuntime>;

  fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
  }

  fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  async fn recv_with_timeout<F, T>(future: F) -> T
  where
    F: core::future::Future<Output = T> + Send,
  {
    <TokioRuntime as RuntimeLite>::timeout(Duration::from_secs(1), future)
      .await
      .expect("timed out waiting for loopback event")
  }

  #[test]
  fn options_defaults_clone_and_into_parts() {
    let _default_opts =
      <TestTransport as memberlist_core::transport::Transport>::Options::new("node-default".into());
    let _resolver_opts =
      <TestTransport as memberlist_core::transport::Transport>::Options::with_resolver_options(
        "node-resolver".into(),
        (),
      );

    let opts =
      NetTransportOptions::<SmolStr, TestResolver, TestLayer>::with_resolver_options_and_stream_layer_options(
        "node-a".into(),
        (),
        (),
      )
      .with_cidrs_policy(CIDRsPolicy::block_all())
      .with_max_packet_size(512)
      .with_recv_buffer_size(1024)
      .with_advertise_address(loopback(9000));

    let mut cloned = opts.clone();
    cloned.add_bind_address(loopback(0));
    cloned.add_bind_address(loopback(0));

    assert_eq!(cloned.id(), "node-a");
    assert_eq!(cloned.bind_addresses().len(), 1);
    assert_eq!(cloned.advertise_address(), Some(&loopback(9000)));
    assert!(cloned.cidrs_policy().is_block_all());
    assert_eq!(cloned.max_packet_size(), 512);
    assert_eq!(cloned.recv_buffer_size(), 1024);

    let (_resolver_opts, _stream_layer_opts, _inner): ((), (), _) = cloned.into();
  }

  #[test]
  fn transport_error_classifies_remote_failures() {
    let remote_kinds = [
      io::ErrorKind::ConnectionRefused,
      io::ErrorKind::ConnectionReset,
      io::ErrorKind::ConnectionAborted,
      io::ErrorKind::BrokenPipe,
      io::ErrorKind::TimedOut,
      io::ErrorKind::NotConnected,
    ];

    for kind in remote_kinds {
      let err = NetTransportError::<TestResolver>::Io(io::Error::from(kind));
      assert!(err.is_remote_failure(), "{kind:?}");
    }

    assert!(
      !NetTransportError::<TestResolver>::Io(io::Error::from(io::ErrorKind::Other))
        .is_remote_failure()
    );
    assert!(!NetTransportError::<TestResolver>::NoPrivateIP.is_remote_failure());
    assert!(!NetTransportError::<TestResolver>::EmptyBindAddresses.is_remote_failure());
    assert!(
      !NetTransportError::<TestResolver>::Custom(Cow::Borrowed("custom")).is_remote_failure()
    );
    assert!(matches!(
      NetTransportError::<TestResolver>::custom(Cow::Borrowed("via-trait")),
      NetTransportError::Custom(Cow::Borrowed("via-trait"))
    ));
    assert_eq!(
      format!(
        "{:?}",
        NetTransportError::<TestResolver>::PacketTooLarge(70000)
      ),
      "packet too large, the maximum packet can be sent is 65535, got 70000"
    );
  }

  #[test]
  fn advertise_address_index_prefers_specific_addresses() {
    let unspecified_v4 = SocketAddr::from(([0, 0, 0, 0], 1));
    let specific_v4 = loopback(2);
    let unspecified_v6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 3));
    let specific_v6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 4));

    assert_eq!(
      TestTransport::find_advertise_addr_index(&[unspecified_v4, specific_v4]),
      1
    );
    assert_eq!(
      TestTransport::find_advertise_addr_index(&[unspecified_v4, unspecified_v6]),
      0
    );
    assert_eq!(
      TestTransport::find_advertise_addr_index(&[unspecified_v4, specific_v6]),
      1
    );
  }

  #[test]
  fn empty_bind_addresses_are_rejected() {
    rt().block_on(async {
      let opts = NetTransportOptions::<SmolStr, TestResolver, TestLayer>::with_stream_layer_options(
        "empty".into(),
        (),
      );

      let err = match TestTransport::new(opts).await {
        Ok(_) => panic!("transport creation should fail without bind addresses"),
        Err(err) => err,
      };
      assert!(matches!(err, NetTransportError::EmptyBindAddresses));
    });
  }

  #[test]
  fn new_transport_exposes_local_metadata_and_blocks_disallowed_ips() {
    rt().block_on(async {
      let advertise = loopback(9191);
      let mut opts =
        NetTransportOptions::<SmolStr, TestResolver, TestLayer>::with_stream_layer_options(
          "metadata".into(),
          (),
        )
        .with_max_packet_size(1024)
        .with_recv_buffer_size(1024)
        .with_cidrs_policy(CIDRsPolicy::block_all())
        .with_advertise_address(advertise);
      opts.add_bind_address(loopback(0));

      let transport = TestTransport::new(opts).await.unwrap();

      assert_eq!(transport.local_id(), "metadata");
      assert_eq!(transport.header_overhead(), 0);
      assert!(!transport.packet_reliable());
      assert!(!transport.packet_secure());
      assert!(!transport.stream_secure());
      assert_eq!(transport.max_packet_size(), 1024);
      assert_eq!(*transport.advertise_address(), advertise);
      assert_eq!(transport.local_address().ip(), loopback(0).ip());
      assert!(matches!(
        transport.blocked_address(&loopback(8080)),
        Err(NetTransportError::BlockedIp(_))
      ));

      transport.shutdown().await.unwrap();
      transport.shutdown().await.unwrap();
    });
  }

  #[test]
  fn send_to_delivers_udp_payload_and_errors_after_shutdown() {
    rt().block_on(async {
      let mut opts =
        NetTransportOptions::<SmolStr, TestResolver, TestLayer>::with_stream_layer_options(
          "send-to".into(),
          (),
        )
        .with_recv_buffer_size(1024);
      opts.add_bind_address(loopback(0));
      let transport = TestTransport::new(opts).await.unwrap();

      let receiver = <<TokioRuntime as Runtime>::Net as Net>::UdpSocket::bind(loopback(0))
        .await
        .unwrap();
      let receiver_addr = receiver.local_addr().unwrap();

      let mut payload = Payload::new(0, 8);
      payload.data_mut().copy_from_slice(b"send-udp");
      let (sent, _) = transport.send_to(&receiver_addr, payload).await.unwrap();
      assert_eq!(sent, 8);

      let mut buf = [0; 16];
      let (received, from) = recv_with_timeout(receiver.recv_from(&mut buf))
        .await
        .unwrap();
      assert_eq!(&buf[..received], b"send-udp");
      assert_eq!(from.ip(), loopback(0).ip());

      transport.shutdown().await.unwrap();

      let mut payload = Payload::new(0, 5);
      payload.data_mut().copy_from_slice(b"after");
      let err = transport
        .send_to(&receiver_addr, payload)
        .await
        .unwrap_err();
      assert!(matches!(
        err,
        NetTransportError::Io(ref io_err)
          if io_err.kind() == io::ErrorKind::ConnectionAborted
      ));
    });
  }

  #[test]
  fn packet_processor_forwards_non_empty_udp_packets() {
    rt().block_on(async {
      let socket = <<TokioRuntime as Runtime>::Net as Net>::UdpSocket::bind(loopback(0))
        .await
        .unwrap();
      let local_addr = socket.local_addr().unwrap();
      let sender = <<TokioRuntime as Runtime>::Net as Net>::UdpSocket::bind(loopback(0))
        .await
        .unwrap();
      let sender_addr = sender.local_addr().unwrap();
      let (packet_tx, packet_rx) = memberlist_core::transport::packet_stream::<TestTransport>();
      let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
      let shutdown = Arc::new(AtomicBool::new(false));

      let task = <TokioRuntime as RuntimeLite>::spawn(
        PacketProcessor::<TestResolver, TestTransport> {
          packet_tx,
          socket: Arc::new(socket),
          local_addr,
          shutdown: shutdown.clone(),
          shutdown_rx,
        }
        .run(),
      );

      sender.send_to(&[], local_addr).await.unwrap();
      sender.send_to(b"packet-body", local_addr).await.unwrap();

      let packet = recv_with_timeout(packet_rx.recv()).await.unwrap();
      let (from, _, payload) = packet.into_components();
      assert_eq!(from, sender_addr);
      assert_eq!(payload.as_ref(), b"packet-body");
      assert!(packet_rx.is_empty());

      shutdown.store(true, Ordering::SeqCst);
      shutdown_tx.close();
      task.await.unwrap();
    });
  }

  #[test]
  fn promised_processor_forwards_accepted_streams() {
    rt().block_on(async {
      let layer = <TestLayer as crate::StreamLayer>::new(()).await.unwrap();
      let listener = layer.bind(loopback(0)).await.unwrap();
      let local_addr = listener.local_addr();
      let (stream_tx, stream_rx) = memberlist_core::transport::promised_stream::<TestTransport>();
      let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

      let task = <TokioRuntime as RuntimeLite>::spawn(
        PromisedProcessor::<TestResolver, TestTransport, TestLayer> {
          stream_tx,
          ln: Arc::new(listener),
          local_addr,
          shutdown_rx,
        }
        .run(),
      );

      let mut client_stream = layer.connect(local_addr).await.unwrap();
      let client_addr = client_stream.local_addr();
      let (remote_addr, mut server_stream) = recv_with_timeout(stream_rx.recv()).await.unwrap();
      assert_eq!(remote_addr, client_addr);
      assert_eq!(server_stream.peer_addr(), client_addr);

      client_stream.write_all(b"accepted").await.unwrap();
      client_stream.flush().await.unwrap();
      let mut buf = [0; 8];
      server_stream.read_exact(&mut buf).await.unwrap();
      assert_eq!(&buf, b"accepted");

      shutdown_tx.close();
      task.await.unwrap();
    });
  }

  #[test]
  fn tcp_stream_layer_accepts_and_exchanges_bytes() {
    rt().block_on(async {
      let layer = <TestLayer as crate::StreamLayer>::new(()).await.unwrap();
      let listener = layer.bind(loopback(0)).await.unwrap();
      let local_addr = listener.local_addr();

      let _copied = Tcp::<TokioRuntime>::new().clone();
      let _defaulted = Tcp::<TokioRuntime>::default();
      assert!(!TestLayer::is_secure());

      let accept = listener.accept();
      let connect = layer.connect(local_addr);
      let (accepted, connected) = futures::future::join(accept, connect).await;
      let (mut server_stream, peer_addr) = accepted.unwrap();
      let mut client_stream = connected.unwrap();

      assert_eq!(server_stream.local_addr(), local_addr);
      assert_eq!(server_stream.peer_addr(), peer_addr);
      assert_eq!(client_stream.peer_addr(), local_addr);

      client_stream.write_all(b"ping").await.unwrap();
      client_stream.flush().await.unwrap();
      let mut peeked = [0; 2];
      assert_eq!(server_stream.peek(&mut peeked).await.unwrap(), 2);
      assert_eq!(&peeked, b"pi");

      let mut read = [0; 4];
      server_stream.read_exact(&mut read).await.unwrap();
      assert_eq!(&read, b"ping");

      server_stream.write_all(b"pong").await.unwrap();
      server_stream.flush().await.unwrap();
      let mut response = [0; 4];
      client_stream.peek_exact(&mut response).await.unwrap();
      assert_eq!(&response, b"pong");
      client_stream.read_exact(&mut response).await.unwrap();
      assert_eq!(&response, b"pong");

      client_stream.close().await.unwrap();
      listener.shutdown().await.unwrap();
    });
  }
}
