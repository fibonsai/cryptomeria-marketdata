use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// A registered subscriber connection.
#[derive(Debug, Clone)]
pub struct Subscriber {
    /// Stable identifier used to unregister the subscriber (e.g. `stdout_subscriber`).
    pub id: String,
    /// `true` when the subscriber receives every topic (subscribe-all).
    pub all: bool,
    /// Specific topics the subscriber is interested in.
    pub topics: HashSet<String>,
}

impl Subscriber {
    /// A subscriber that receives every current and future topic.
    pub fn all(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            all: true,
            topics: HashSet::new(),
        }
    }

    /// A subscriber interested in the given specific topics.
    pub fn topics(id: impl Into<String>, topics: impl IntoIterator<Item = String>) -> Self {
        Self {
            id: id.into(),
            all: false,
            topics: topics.into_iter().collect(),
        }
    }
}

/// Tracks per-topic subscriber counts and the set of known topics.
///
/// NNG PUB/SUB does not expose subscription state to the publisher, so the
/// marketdata service keeps its own registry of known subscribers and counts
/// subscribers per topic from it.
#[derive(Debug, Default)]
pub struct SubscriberRegistry {
    subscribers: Vec<Subscriber>,
    known_topics: HashSet<String>,
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a subscriber.
    pub fn add(&mut self, subscriber: Subscriber) {
        for topic in &subscriber.topics {
            self.known_topics.insert(topic.clone());
        }
        self.subscribers.push(subscriber);
    }

    /// Unregister a subscriber by id; returns `true` when found.
    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.subscribers.len();
        self.subscribers.retain(|s| s.id != id);
        self.subscribers.len() != before
    }

    /// Record that a topic was published; used to report topics even when
    /// all subscribers use subscribe-all semantics.
    pub fn record_topic(&mut self, topic: &str) {
        self.known_topics.insert(topic.to_string());
    }

    /// Number of subscribers receiving `topic` (subscribe-all plus specific).
    pub fn count_for_topic(&self, topic: &str) -> usize {
        self.subscribers
            .iter()
            .filter(|s| s.all || s.topics.contains(topic))
            .count()
    }

    /// Total number of registered subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Snapshot of `(topic, count)` for every known topic, sorted by topic.
    pub fn snapshot_counts(&self) -> Vec<(String, usize)> {
        let mut counts: Vec<(String, usize)> = self
            .known_topics
            .iter()
            .map(|t| (t.clone(), self.count_for_topic(t)))
            .collect();
        counts.sort();
        counts
    }
}

/// A thread-safe handle to a [`SubscriberRegistry`].
#[derive(Debug, Clone, Default)]
pub struct SharedRegistry(Arc<Mutex<SubscriberRegistry>>);

impl SharedRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, subscriber: Subscriber) {
        self.0
            .lock()
            .expect("registry mutex poisoned")
            .add(subscriber);
    }

    pub fn remove(&self, id: &str) -> bool {
        self.0.lock().expect("registry mutex poisoned").remove(id)
    }

    pub fn record_topic(&self, topic: &str) {
        self.0
            .lock()
            .expect("registry mutex poisoned")
            .record_topic(topic);
    }

    pub fn count_for_topic(&self, topic: &str) -> usize {
        self.0
            .lock()
            .expect("registry mutex poisoned")
            .count_for_topic(topic)
    }

    pub fn subscriber_count(&self) -> usize {
        self.0
            .lock()
            .expect("registry mutex poisoned")
            .subscriber_count()
    }

    pub fn snapshot_counts(&self) -> Vec<(String, usize)> {
        self.0
            .lock()
            .expect("registry mutex poisoned")
            .snapshot_counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(id: &str) -> Subscriber {
        Subscriber::all(id)
    }

    fn topics(id: &str, topics: &[&str]) -> Subscriber {
        Subscriber::topics(id, topics.iter().map(|s| s.to_string()))
    }

    #[test]
    fn counts_subscribe_all_subscriber_for_every_known_topic() {
        let mut registry = SubscriberRegistry::new();
        registry.add(all("stdout_subscriber"));
        registry.record_topic("lob__btcusdt");
        registry.record_topic("trade__btcusdt");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 1);
        assert_eq!(registry.count_for_topic("trade__btcusdt"), 1);
    }

    #[test]
    fn counts_specific_topic_subscribers() {
        let mut registry = SubscriberRegistry::new();
        registry.add(topics("a", &["lob__btcusdt"]));
        registry.add(topics("b", &["lob__btcusdt", "trade__btcusdt"]));
        registry.record_topic("lob__btcusdt");
        registry.record_topic("trade__btcusdt");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 2);
        assert_eq!(registry.count_for_topic("trade__btcusdt"), 1);
    }

    #[test]
    fn sums_subscribe_all_and_specific_subscribers() {
        let mut registry = SubscriberRegistry::new();
        registry.add(all("stdout_subscriber"));
        registry.add(topics("a", &["lob__btcusdt"]));
        registry.record_topic("lob__btcusdt");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 2);
    }

    #[test]
    fn removes_subscriber_and_decrements_count() {
        let mut registry = SubscriberRegistry::new();
        registry.add(all("stdout_subscriber"));
        registry.record_topic("lob__btcusdt");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 1);
        assert!(registry.remove("stdout_subscriber"));
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 0);
        assert!(!registry.remove("nonexistent"));
    }

    #[test]
    fn snapshot_returns_sorted_known_topics() {
        let mut registry = SubscriberRegistry::new();
        registry.add(all("stdout_subscriber"));
        registry.record_topic("trade__btcusdt");
        registry.record_topic("lob__btcusdt");
        assert_eq!(
            registry.snapshot_counts(),
            vec![
                ("lob__btcusdt".to_string(), 1),
                ("trade__btcusdt".to_string(), 1),
            ]
        );
    }

    #[test]
    fn unknown_topic_counts_zero() {
        let registry = SubscriberRegistry::new();
        assert_eq!(registry.count_for_topic("nope__x"), 0);
    }

    #[test]
    fn shared_registry_counts_across_add_remove() {
        let registry = SharedRegistry::new();
        registry.add(all("stdout_subscriber"));
        registry.record_topic("lob__btcusdt");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 1);
        assert_eq!(registry.subscriber_count(), 1);
        registry.remove("stdout_subscriber");
        assert_eq!(registry.count_for_topic("lob__btcusdt"), 0);
        assert_eq!(registry.subscriber_count(), 0);
    }
}
