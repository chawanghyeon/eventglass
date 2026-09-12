//! Small, disposable access history. Scores only order already-safe eviction candidates.
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

const CAPACITY: usize = 256;
const WINDOW: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct ReuseHistory {
    entries: HashMap<String, Entry>,
}

struct Entry {
    last: Instant,
    hits: u8,
}

impl ReuseHistory {
    pub(super) fn record(&mut self, id: &str, now: Instant) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.hits = if recent(entry.last, now) {
                entry.hits.saturating_add(1).min(4)
            } else {
                1
            };
            entry.last = now;
            return;
        }
        if self.entries.len() == CAPACITY {
            // Ties are deterministic; no dependence on HashMap's randomized iteration.
            let victim = self
                .entries
                .iter()
                .min_by_key(|(id, entry)| (entry.last, *id))
                .map(|(id, _)| id.clone());
            if let Some(victim) = victim {
                self.entries.remove(&victim);
            }
        }
        self.entries
            .insert(id.to_owned(), Entry { last: now, hits: 1 });
    }

    pub(super) fn score(&self, id: &str, now: Instant) -> u8 {
        self.entries
            .get(id)
            .filter(|entry| recent(entry.last, now))
            .map_or(0, |entry| entry.hits.saturating_sub(1))
    }
}

fn recent(last: Instant, now: Instant) -> bool {
    now.checked_duration_since(last)
        .is_some_and(|age| age < WINDOW)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_reads_are_protected_but_expire_and_never_grow_history() {
        let mut history = ReuseHistory::default();
        let now = Instant::now();
        assert_eq!(history.score("missing", now), 0);
        history.record("hot", now);
        assert_eq!(history.score("hot", now), 0);
        for _ in 0..20 {
            history.record("hot", now);
        }
        assert_eq!(history.score("hot", now), 3);
        assert_eq!(history.score("hot", now + WINDOW), 0);
        assert_eq!(history.score("hot", now - Duration::from_secs(1)), 0);
        history.record("hot", now + WINDOW);
        assert_eq!(history.score("hot", now + WINDOW), 0);
        for index in 0..CAPACITY + 1 {
            history.record(&format!("shard-{index:04}"), now + WINDOW);
        }
        assert_eq!(history.entries.len(), CAPACITY);
        assert!(!history.entries.contains_key("hot"));
        assert!(!history.entries.contains_key("shard-0000"));
        assert!(history.entries.contains_key("shard-0256"));
        let reset = ReuseHistory::default();
        assert_eq!(reset.score("hot", now), 0);
    }

    #[test]
    fn scan_pollution_trace_keeps_repeated_work_without_pinning_it_forever() {
        fn run(protect: bool) -> usize {
            let mut history = ReuseHistory::default();
            let mut cache = Vec::<String>::new();
            let mut misses = 0;
            let now = Instant::now();
            for round in 0..32 {
                let hot = if round < 16 { "hot-a" } else { "hot-b" };
                // Repeated interactive reads followed by an unrelated historical scan.
                let reads = [
                    hot.to_owned(),
                    hot.to_owned(),
                    hot.to_owned(),
                    format!("scan-{round}-a"),
                    format!("scan-{round}-b"),
                    format!("scan-{round}-c"),
                ];
                let at = now + Duration::from_secs(round * 5);
                for id in reads {
                    if let Some(index) = cache.iter().position(|cached| cached == &id) {
                        cache.remove(index);
                    } else {
                        misses += 1;
                        if cache.len() == 3 {
                            let index = if protect {
                                (0..cache.len())
                                    .min_by_key(|index| history.score(&cache[*index], at))
                                    .unwrap()
                            } else {
                                0
                            };
                            cache.remove(index);
                        }
                    }
                    cache.push(id.clone());
                    history.record(&id, at);
                }
            }
            misses
        }
        let baseline = run(false);
        let candidate = run(true);
        assert_eq!(baseline, 128);
        assert!(
            candidate < baseline,
            "candidate {candidate}, baseline {baseline}"
        );
        println!("bounded reuse trace: baseline misses={baseline}, candidate misses={candidate}");
    }
}
