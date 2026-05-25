use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use futures::FutureExt;
use smol_str::SmolStr;

use crate::{
  broadcast::{Broadcast, MemberlistBroadcast},
  proto::Message,
};

use super::*;

impl NodeCalculator for usize {
  async fn num_nodes(&self) -> usize {
    *self
  }
}

#[derive(Debug)]
struct TestBroadcast {
  key: &'static str,
  id: Option<&'static str>,
  invalidates: Option<&'static str>,
  msg: Bytes,
  unique: bool,
  notify: Option<async_channel::Sender<()>>,
}

impl Broadcast for TestBroadcast {
  type Id = &'static str;
  type Message = Bytes;

  fn id(&self) -> Option<&Self::Id> {
    self.id.as_ref()
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.invalidates == Some(other.key)
  }

  fn message(&self) -> &Self::Message {
    &self.msg
  }

  fn encoded_len(msg: &Self::Message) -> usize {
    msg.len()
  }

  async fn finished(&self) {
    if let Some(tx) = &self.notify {
      let _ = tx.send(()).await;
    }
  }

  fn is_unique(&self) -> bool {
    self.unique
  }
}

fn test_broadcast(key: &'static str, msg: impl Into<Bytes>) -> TestBroadcast {
  TestBroadcast {
    key,
    id: None,
    invalidates: None,
    msg: msg.into(),
    unique: false,
    notify: None,
  }
}

#[test]
fn test_limited_broadcast_less() {
  struct Case {
    name: &'static str,
    a: Arc<LimitedBroadcast<MemberlistBroadcast<SmolStr, SocketAddr>>>,
    b: Arc<LimitedBroadcast<MemberlistBroadcast<SmolStr, SocketAddr>>>,
  }

  let cases = [
    Case {
      name: "diff-transmits",
      a: LimitedBroadcast {
        transmits: 0,
        msg_len: 10,
        id: 100,
        broadcast: Arc::new(MemberlistBroadcast::<SmolStr, SocketAddr> {
          node: "diff-transmits-a".into(),
          msg: Message::UserData(Bytes::from([0; 10].as_slice())),
          notify: None,
        }),
      }
      .into(),
      b: LimitedBroadcast {
        transmits: 1,
        msg_len: 10,
        id: 100,
        broadcast: Arc::new(MemberlistBroadcast::<SmolStr, SocketAddr> {
          node: "diff-transmits-b".into(),
          msg: Message::UserData(Bytes::from([0; 10].as_slice())),
          notify: None,
        }),
      }
      .into(),
    },
    Case {
      name: "same-transmits--diff-len",
      a: LimitedBroadcast {
        transmits: 0,
        msg_len: 12,
        id: 100,
        broadcast: Arc::new(MemberlistBroadcast::<SmolStr, SocketAddr> {
          node: "same-transmits--diff-len-a".into(),
          msg: Message::UserData(Bytes::from([0; 12].as_slice())),
          notify: None,
        }),
      }
      .into(),
      b: LimitedBroadcast {
        transmits: 0,
        msg_len: 10,
        id: 100,
        broadcast: Arc::new(MemberlistBroadcast::<SmolStr, SocketAddr> {
          node: "same-transmits--diff-len-b".into(),
          msg: Message::UserData(Bytes::from([0; 10].as_slice())),
          notify: None,
        }),
      }
      .into(),
    },
    Case {
      name: "same-transmits--same-len--diff-id",
      a: LimitedBroadcast {
        transmits: 0,
        msg_len: 12,
        id: 100,
        broadcast: Arc::new(MemberlistBroadcast::<SmolStr, SocketAddr> {
          node: "same-transmits--same-len--diff-id-a".into(),
          msg: Message::UserData(Bytes::from([0; 12].as_slice())),
          notify: None,
        }),
      }
      .into(),
      b: LimitedBroadcast {
        transmits: 0,
        msg_len: 12,
        id: 90,
        broadcast: Arc::new(MemberlistBroadcast::<SmolStr, SocketAddr> {
          node: "same-transmits--same-len--diff-id-b".into(),
          msg: Message::UserData(Bytes::from([0; 12].as_slice())),
          notify: None,
        }),
      }
      .into(),
    },
  ];

  for c in cases {
    assert!(c.a < c.b, "case: {}", c.name);

    #[allow(clippy::all)]
    let mut tree = BTreeSet::new();
    tree.insert(c.b.clone());
    tree.insert(c.a.clone());

    let min = tree.iter().min().unwrap();
    assert_eq!(min.transmits, c.a.transmits, "case: {}", c.name);
    assert_eq!(min.msg_len, c.a.msg_len, "case: {}", c.name);
    assert_eq!(min.id, c.a.id, "case: {}", c.name);

    let max = tree.iter().max().unwrap();
    assert_eq!(max.transmits, c.b.transmits, "case: {}", c.name);
    assert_eq!(max.msg_len, c.b.msg_len, "case: {}", c.name);
    assert_eq!(max.id, c.b.id, "case: {}", c.name);
  }
}

#[tokio::test]
async fn test_transmit_limited_queue() {
  let q = TransmitLimitedQueue::new(1, 1);
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "test".into(),
    msg: Message::UserData(Bytes::new()),
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "foo".into(),
    msg: Message::UserData(Bytes::new()),
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "bar".into(),
    msg: Message::UserData(Bytes::new()),
    notify: None,
  })
  .await;

  assert_eq!(q.num_queued().await, 3);

  let dump = q.ordered_view(true).await;

  assert_eq!(dump.len(), 3);
  assert_eq!(dump[0].broadcast.node.as_str(), "test");
  assert_eq!(dump[1].broadcast.node.as_str(), "foo");
  assert_eq!(dump[2].broadcast.node.as_str(), "bar");

  // Should invalidate previous message
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "test".into(),
    msg: Message::UserData(Bytes::new()),
    notify: None,
  })
  .await;

  assert_eq!(q.num_queued().await, 3);
  let dump = q.ordered_view(true).await;

  assert_eq!(dump.len(), 3);
  assert_eq!(dump[0].broadcast.node.as_str(), "foo");
  assert_eq!(dump[1].broadcast.node.as_str(), "bar");
  assert_eq!(dump[2].broadcast.node.as_str(), "test");
}

#[tokio::test]
async fn test_transmit_limited_get_broadcasts() {
  let q = TransmitLimitedQueue::new(3, 10);

  // 18 bytes per message
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "test".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"1. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "foo".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"2. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "bar".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"3. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "baz".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"4. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;

  let all = q.get_broadcasts(100).await;
  assert_eq!(all.len(), 4);

  let all = q.get_broadcasts(80).await;
  assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn test_transmit_limited_get_broadcasts_limit() {
  let q = TransmitLimitedQueue::new(1, 10);

  assert_eq!(0, q.inner.lock().await.id_gen);
  assert_eq!(
    2,
    retransmit_limit(q.retransmit_mult, q.num_nodes.num_nodes().await)
  );

  // 18 bytes per message
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "test".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"1. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "foo".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"2. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "bar".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"3. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "baz".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"4. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;

  assert_eq!(4, q.inner.lock().await.id_gen);

  // 3 byte overhead, should only get 3 messages back
  let partial = q.get_broadcasts(80).await;
  assert_eq!(partial.len(), 3);

  assert_eq!(
    4,
    q.inner.lock().await.id_gen,
    "id generator doesn't reset until empty"
  );

  let partial = q.get_broadcasts(80).await;
  assert_eq!(partial.len(), 3);
  assert_eq!(
    4,
    q.inner.lock().await.id_gen,
    "id generator doesn't reset until empty"
  );

  // Only two not expired
  let partial = q.get_broadcasts(80).await;
  assert_eq!(partial.len(), 2);
  assert_eq!(
    0,
    q.inner.lock().await.id_gen,
    "id generator resets on empty"
  );

  // Should get nothing
  let partial = q.get_broadcasts(80).await;
  assert_eq!(partial.len(), 0);
  assert_eq!(
    0,
    q.inner.lock().await.id_gen,
    "id generator resets on empty"
  );
}

#[tokio::test]
async fn test_transmit_limited_prune() {
  let q = TransmitLimitedQueue::new(1, 10);
  let (tx1, rx1) = async_channel::bounded(1);
  let (tx2, rx2) = async_channel::bounded(1);

  // 18 bytes per message
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "test".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"1. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: Some(tx1),
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "foo".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"2. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: Some(tx2),
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "bar".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"3. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;
  q.queue_broadcast(MemberlistBroadcast::<SmolStr, SocketAddr> {
    node: "baz".into(),
    msg: {
      let mut msg = BytesMut::new();
      msg.put_slice(b"4. this is a test.");
      Message::UserData(msg.freeze())
    },
    notify: None,
  })
  .await;

  // keep only 2
  q.prune(2).await;

  assert_eq!(2, q.num_queued().await);

  // Should notify the first two
  futures::select! {
    _ = rx1.recv().fuse() => {},
    default => panic!("expected invalidation"),
  }

  futures::select! {
    _ = rx2.recv().fuse() => {},
    default => panic!("expected invalidation"),
  }

  let dump = q.ordered_view(true).await;
  assert_eq!(dump[0].broadcast.id().unwrap().as_str(), "bar");
  assert_eq!(dump[1].broadcast.id().unwrap().as_str(), "baz");
}

#[tokio::test]
async fn test_transmit_limited_ordering() {
  let q = TransmitLimitedQueue::new(1, 10);
  let insert = |name: &str, transmits: usize| {
    q.queue_broadcast_in(
      MemberlistBroadcast::<SmolStr, SocketAddr> {
        node: name.into(),
        msg: Message::UserData(Bytes::new()),
        notify: None,
      },
      transmits,
    )
  };

  insert("node0", 0).await;
  insert("node1", 10).await;
  insert("node2", 3).await;
  insert("node3", 4).await;
  insert("node4", 7).await;

  let dump = q.ordered_view(true).await;
  assert_eq!(dump[0].transmits, 10);
  assert_eq!(dump[1].transmits, 7);
  assert_eq!(dump[2].transmits, 4);
  assert_eq!(dump[3].transmits, 3);
  assert_eq!(dump[4].transmits, 0);
}

#[tokio::test]
async fn test_transmit_limited_reset_notifies_and_clears_id_gen() {
  let q = TransmitLimitedQueue::new(2, 10);
  let (tx1, rx1) = async_channel::bounded(1);
  let (tx2, rx2) = async_channel::bounded(1);

  q.queue_broadcast(TestBroadcast {
    notify: Some(tx1),
    ..test_broadcast("first", Bytes::from_static(b"first"))
  })
  .await;
  q.queue_broadcast(TestBroadcast {
    notify: Some(tx2),
    ..test_broadcast("second", Bytes::from_static(b"second"))
  })
  .await;
  assert_eq!(q.num_queued().await, 2);
  assert_eq!(q.inner.lock().await.id_gen, 1);

  q.reset().await;

  assert_eq!(q.num_queued().await, 0);
  assert_eq!(q.inner.lock().await.id_gen, 0);
  futures::select! {
    _ = rx1.recv().fuse() => {},
    default => panic!("expected first reset notification"),
  }
  futures::select! {
    _ = rx2.recv().fuse() => {},
    default => panic!("expected second reset notification"),
  }
}

#[tokio::test]
async fn test_transmit_limited_limit_smaller_than_message_keeps_item() {
  let q = TransmitLimitedQueue::new(2, 10);
  let (tx, rx) = async_channel::bounded(1);

  q.queue_broadcast(TestBroadcast {
    notify: Some(tx),
    ..test_broadcast("too-large", Bytes::from_static(b"1234"))
  })
  .await;

  assert!(q.get_broadcasts(3).await.is_empty());
  assert_eq!(q.num_queued().await, 1);
  futures::select! {
    _ = rx.recv().fuse() => panic!("message should not finish when it cannot fit"),
    default => {},
  }

  let sent = q.get_broadcasts(4).await;
  assert_eq!(sent.len(), 1);
  assert_eq!(sent[0], Bytes::from_static(b"1234"));
  assert_eq!(q.num_queued().await, 1);
}

#[tokio::test]
async fn test_transmit_limited_no_id_broadcast_invalidates_by_scan() {
  let q = TransmitLimitedQueue::new(2, 10);
  let (tx, rx) = async_channel::bounded(1);

  q.queue_broadcast(TestBroadcast {
    notify: Some(tx),
    ..test_broadcast("stale", Bytes::from_static(b"old"))
  })
  .await;
  q.queue_broadcast(test_broadcast("other", Bytes::from_static(b"keep")))
    .await;
  q.queue_broadcast(TestBroadcast {
    invalidates: Some("stale"),
    ..test_broadcast("replacement", Bytes::from_static(b"new"))
  })
  .await;

  futures::select! {
    _ = rx.recv().fuse() => {},
    default => panic!("expected invalidated broadcast notification"),
  }
  let mut keys = q
    .ordered_view(true)
    .await
    .iter()
    .map(|item| item.broadcast.key)
    .collect::<Vec<_>>();
  keys.sort_unstable();
  assert_eq!(keys, vec!["other", "replacement"]);
}

#[tokio::test]
async fn test_transmit_limited_unique_no_id_broadcast_skips_invalidation_scan() {
  let q = TransmitLimitedQueue::new(2, 10);
  let (tx, rx) = async_channel::bounded(1);

  q.queue_broadcast(TestBroadcast {
    notify: Some(tx),
    ..test_broadcast("existing", Bytes::from_static(b"old"))
  })
  .await;
  q.queue_broadcast(TestBroadcast {
    invalidates: Some("existing"),
    unique: true,
    ..test_broadcast("unique", Bytes::from_static(b"newer"))
  })
  .await;

  futures::select! {
    _ = rx.recv().fuse() => panic!("unique broadcast should not scan for invalidations"),
    default => {},
  }
  assert_eq!(q.num_queued().await, 2);
}
