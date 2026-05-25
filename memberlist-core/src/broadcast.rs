use crate::{
  base::Memberlist,
  delegate::Delegate,
  error::Error,
  proto::{Data, Message, TinyVec},
  transport::Transport,
};
use async_channel::Sender;

use nodecraft::CheapClone;

/// Something that can be broadcasted via gossip to
/// the memberlist cluster.
pub trait Broadcast: core::fmt::Debug + Send + Sync + 'static {
  /// The id type
  type Id: Clone + Eq + core::hash::Hash + core::fmt::Debug + core::fmt::Display;
  /// The message type
  type Message: Clone + core::fmt::Debug + Send + Sync + 'static;

  /// An optional extension of the Broadcast trait that
  /// gives each message a unique id and that is used to optimize
  fn id(&self) -> Option<&Self::Id>;

  /// Checks if enqueuing the current broadcast
  /// invalidates a previous broadcast
  fn invalidates(&self, other: &Self) -> bool;

  /// Returns the message
  fn message(&self) -> &Self::Message;

  /// Returns the encoded length of the message
  fn encoded_len(msg: &Self::Message) -> usize;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  fn finished(&self) -> impl std::future::Future<Output = ()> + Send;

  /// Indicates that each message is
  /// intrinsically unique and there is no need to scan the broadcast queue for
  /// duplicates.
  ///
  /// You should ensure that `invalidates` always returns false if implementing
  /// this.
  fn is_unique(&self) -> bool {
    false
  }
}

#[viewit::viewit]
pub(crate) struct MemberlistBroadcast<I, A> {
  node: I,
  msg: Message<I, A>,
  notify: Option<async_channel::Sender<()>>,
}

impl<I: core::fmt::Debug, A: core::fmt::Debug> core::fmt::Debug for MemberlistBroadcast<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("node", &self.node)
      .field("msg", &self.msg)
      .finish()
  }
}

impl<I, A> Broadcast for MemberlistBroadcast<I, A>
where
  I: CheapClone
    + Data
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: CheapClone
    + Data
    + Eq
    + core::hash::Hash
    + core::fmt::Display
    + core::fmt::Debug
    + Send
    + Sync
    + 'static,
{
  type Id = I;
  type Message = Message<I, A>;

  fn id(&self) -> Option<&Self::Id> {
    Some(&self.node)
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node.eq(&other.node)
  }

  fn message(&self) -> &Self::Message {
    &self.msg
  }

  fn encoded_len(msg: &Self::Message) -> usize {
    msg.encoded_len_with_length_delimited()
  }

  async fn finished(&self) {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()).await {
        tracing::error!("memberlist: broadcast failed to notify: {}", e);
      }
    }
  }

  fn is_unique(&self) -> bool {
    false
  }
}

impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  #[inline]
  pub(crate) async fn broadcast_notify(
    &self,
    node: T::Id,
    msg: Message<T::Id, T::ResolvedAddress>,
    notify_tx: Option<Sender<()>>,
  ) {
    let _ = self.queue_broadcast(node, msg, notify_tx).await;
  }

  #[inline]
  pub(crate) async fn broadcast(&self, node: T::Id, msg: Message<T::Id, T::ResolvedAddress>) {
    let _ = self.queue_broadcast(node, msg, None).await;
  }

  #[inline]
  async fn queue_broadcast(
    &self,
    node: T::Id,
    msg: Message<T::Id, T::ResolvedAddress>,
    notify_tx: Option<Sender<()>>,
  ) {
    self
      .inner
      .broadcast
      .queue_broadcast(MemberlistBroadcast {
        node,
        msg,
        notify: notify_tx,
      })
      .await
  }

  /// Used to return a slice of broadcasts to send up to
  /// a maximum byte size, while imposing a per-broadcast overhead. This is used
  /// to fill a UDP packet with piggybacked data
  #[inline]
  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    to_send: TinyVec<Message<T::Id, T::ResolvedAddress>>,
    limit: usize,
  ) -> Result<TinyVec<Message<T::Id, T::ResolvedAddress>>, Error<T, D>> {
    // Get memberlist messages first
    let mut to_send = self
      .inner
      .broadcast
      .get_broadcast_with_prepend(to_send, limit)
      .await;

    // Check if the user has anything to broadcast
    if let Some(delegate) = &self.delegate {
      // Determine the bytes used already
      let mut bytes_used = 0;
      for msg in to_send.iter() {
        bytes_used += msg.encoded_len();
      }

      // Check space remaining for user messages
      let avail = limit.saturating_sub(bytes_used);
      to_send.extend(
        delegate
          .broadcast_messages(avail, |b| {
            let msg = Message::<T::Id, T::ResolvedAddress>::UserData(b);
            let len = msg.encoded_len();
            (len, msg.unwrap_user_data())
          })
          .await
          .map(Message::UserData),
      );
    }

    Ok(to_send)
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use futures::FutureExt;
  use smol_str::SmolStr;

  use super::*;
  use crate::proto::{Alive, Node};

  fn broadcast(
    node: &str,
    notify: Option<async_channel::Sender<()>>,
  ) -> MemberlistBroadcast<SmolStr, SocketAddr> {
    MemberlistBroadcast {
      node: node.into(),
      msg: Message::Alive(Alive::new(
        1,
        Node::new(
          node.into(),
          "127.0.0.1:1".parse().unwrap(),
        ),
      )),
      notify,
    }
  }

  #[test]
  fn memberlist_broadcast_accessors_and_debug() {
    let item = broadcast("node-a", None);
    let same = broadcast("node-a", None);
    let other = broadcast("node-b", None);

    assert_eq!(item.id(), Some(&"node-a".into()));
    assert!(item.invalidates(&same));
    assert!(!item.invalidates(&other));
    assert_eq!(item.message(), &item.msg);
    assert_eq!(
      MemberlistBroadcast::<SmolStr, SocketAddr>::encoded_len(item.message()),
      item.message().encoded_len_with_length_delimited()
    );
    assert!(!item.is_unique());
    assert!(format!("{item:?}").contains("MemberlistBroadcast"));
  }

  #[tokio::test]
  async fn memberlist_broadcast_finished_notifies_when_open() {
    let (tx, rx) = async_channel::bounded(1);
    let item = broadcast("node-a", Some(tx));

    item.finished().await;

    assert!(rx.recv().await.is_ok());
  }

  #[tokio::test]
  async fn memberlist_broadcast_finished_ignores_closed_receiver() {
    let (tx, rx) = async_channel::bounded(1);
    drop(rx);
    let item = broadcast("node-a", Some(tx));

    item.finished().await;
  }

  #[tokio::test]
  async fn memberlist_broadcast_finished_without_notify_is_noop() {
    let item = broadcast("node-a", None);
    item.finished().await;

    futures::select! {
      _ = futures::future::pending::<()>().fuse() => panic!("pending future should not complete"),
      default => {},
    }
  }
}
