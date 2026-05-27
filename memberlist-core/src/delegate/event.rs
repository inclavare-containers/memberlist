use std::{
  future::Future,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use futures::Stream;
use nodecraft::{CheapClone, Id};

use crate::proto::NodeState;

#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
enum EventInner<I, A> {
  /// Join event.
  Join(Arc<NodeState<I, A>>),
  /// Leave event.
  Leave(Arc<NodeState<I, A>>),
  /// Update event.
  Update(Arc<NodeState<I, A>>),
}

impl<I, A> Clone for EventInner<I, A> {
  fn clone(&self) -> Self {
    match self {
      EventInner::Join(node) => EventInner::Join(node.clone()),
      EventInner::Leave(node) => EventInner::Leave(node.clone()),
      EventInner::Update(node) => EventInner::Update(node.clone()),
    }
  }
}

/// Represents the kind of event that has occurred.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[repr(u8)]
#[non_exhaustive]
pub enum EventKind {
  /// A node has joined the cluster.
  Join,
  /// A node has left the cluster.
  Leave,
  /// A node has updated, usually involving the meta data.
  Update,
}

/// A single event related to node activity in the memberlist.
#[derive(Debug)]
pub struct Event<I, A>(EventInner<I, A>);

impl<I, A> Clone for Event<I, A> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A> Event<I, A> {
  /// Returns the node state associated with the event.
  #[inline]
  pub fn node_state(&self) -> &NodeState<I, A> {
    match &self.0 {
      EventInner::Join(node) => node,
      EventInner::Leave(node) => node,
      EventInner::Update(node) => node,
    }
  }

  /// Returns the kind of event that has occurred.
  #[inline]
  pub const fn kind(&self) -> EventKind {
    match &self.0 {
      EventInner::Join(_) => EventKind::Join,
      EventInner::Leave(_) => EventKind::Leave,
      EventInner::Update(_) => EventKind::Update,
    }
  }

  pub(crate) fn join(node: Arc<NodeState<I, A>>) -> Self {
    Event(EventInner::Join(node))
  }

  pub(crate) fn leave(node: Arc<NodeState<I, A>>) -> Self {
    Event(EventInner::Leave(node))
  }

  pub(crate) fn update(node: Arc<NodeState<I, A>>) -> Self {
    Event(EventInner::Update(node))
  }
}

/// A simpler delegate that is used only to receive
/// notifications about members joining and leaving. The methods in this
/// delegate may be called by multiple threads, but never concurrently.
/// This allows you to reason about ordering.
#[auto_impl::auto_impl(Box, Arc)]
pub trait EventDelegate: Send + Sync + 'static {
  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;

  /// Invoked when a node is detected to have joined the cluster
  fn notify_join(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send;

  /// Invoked when a node is detected to have left the cluster
  fn notify_leave(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send;

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  fn notify_update(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send;
}

/// Used to enable an application to receive
/// events about joins and leaves over a subscriber instead of a direct
/// function call.
pub struct SubscribleEventDelegate<I, A>(async_channel::Sender<Event<I, A>>);

impl<I, A> SubscribleEventDelegate<I, A> {
  /// Creates a new `EventDelegate` and unbounded subscriber.
  pub fn unbounded() -> (Self, EventSubscriber<I, A>) {
    let (tx, rx) = async_channel::unbounded();
    (Self(tx), EventSubscriber(rx))
  }

  /// Creates a new `EventDelegate` and bounded subscriber.
  ///
  /// Care must be taken that events are processed in a timely manner from
  /// the channel, since this delegate will block until an event can be sent.
  pub fn bounded(capacity: usize) -> (Self, EventSubscriber<I, A>) {
    let (tx, rx) = async_channel::bounded(capacity);
    (Self(tx), EventSubscriber(rx))
  }
}

impl<I, A> EventDelegate for SubscribleEventDelegate<I, A>
where
  I: Id + Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
{
  type Id = I;

  /// The address type of the delegate
  type Address = A;

  /// Invoked when a node is detected to have joined the cluster
  async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    let _ = self.0.send(Event::join(node)).await;
  }

  /// Invoked when a node is detected to have left the cluster
  async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    let _ = self.0.send(Event::leave(node)).await;
  }

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    let _ = self.0.send(Event::update(node)).await;
  }
}

/// A subscriber for receiving events about joins and leaves.
#[pin_project::pin_project]
pub struct EventSubscriber<I, A>(#[pin] async_channel::Receiver<Event<I, A>>);

impl<I, A> EventSubscriber<I, A> {
  /// Receives the next event from the subscriber.
  pub async fn recv(&self) -> Result<Event<I, A>, async_channel::RecvError> {
    self.0.recv().await
  }

  /// Tries to receive the next event from the subscriber without blocking.
  pub fn try_recv(&self) -> Result<Event<I, A>, async_channel::TryRecvError> {
    self.0.try_recv()
  }

  /// Returns the capacity of the subscriber.
  pub fn capacity(&self) -> Option<usize> {
    self.0.capacity()
  }

  /// Returns the number of events in the subscriber.
  pub fn len(&self) -> usize {
    self.0.len()
  }

  /// Returns `true` if the subscriber is empty.
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns `true` if the subscriber is full.
  pub fn is_full(&self) -> bool {
    self.0.is_full()
  }
}

impl<I, A> Stream for EventSubscriber<I, A> {
  type Item = Event<I, A>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<Event<I, A>> as Stream>::poll_next(self.project().0, cx)
  }
}

#[cfg(test)]
mod tests {
  use std::{net::SocketAddr, sync::Arc};

  use smol_str::SmolStr;

  use super::*;
  use crate::proto::State;

  fn node(name: &str) -> Arc<NodeState<SmolStr, SocketAddr>> {
    Arc::new(NodeState::new(
      name.into(),
      "127.0.0.1:1".parse().unwrap(),
      State::Alive,
    ))
  }

  #[test]
  fn event_constructors_clone_and_accessors() {
    let joined = Event::join(node("join"));
    let left = Event::leave(node("leave"));
    let updated = Event::update(node("update"));

    assert_eq!(joined.kind(), EventKind::Join);
    assert_eq!(left.kind(), EventKind::Leave);
    assert_eq!(updated.kind(), EventKind::Update);
    assert_eq!(joined.clone().node_state().id().as_str(), "join");
    assert_eq!(format!("{:?}", updated.kind()), "Update");
  }

  #[tokio::test]
  async fn subscribe_event_delegate_sends_all_event_kinds() {
    let (delegate, subscriber) = SubscribleEventDelegate::bounded(3);

    assert_eq!(subscriber.capacity(), Some(3));
    assert!(subscriber.is_empty());
    assert!(!subscriber.is_full());

    delegate.notify_join(node("join")).await;
    delegate.notify_leave(node("leave")).await;
    delegate.notify_update(node("update")).await;

    assert_eq!(subscriber.len(), 3);
    assert!(subscriber.is_full());
    assert_eq!(subscriber.try_recv().unwrap().kind(), EventKind::Join);
    assert_eq!(subscriber.recv().await.unwrap().kind(), EventKind::Leave);
    assert_eq!(subscriber.recv().await.unwrap().kind(), EventKind::Update);
    assert!(subscriber.is_empty());
  }

  #[tokio::test]
  async fn unbounded_subscriber_reports_no_capacity() {
    let (delegate, subscriber) = SubscribleEventDelegate::unbounded();
    assert_eq!(subscriber.capacity(), None);

    delegate.notify_join(node("join")).await;
    assert_eq!(
      subscriber.recv().await.unwrap().node_state().id().as_str(),
      "join"
    );
  }
}
