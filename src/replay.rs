//! Bounded, on-demand analysis of SDK events. Raw events remain solely in blob storage.
use crate::sentry::replay::{event_timestamp_ms, frustration};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet, VecDeque};

const MAX_TIMELINE: usize = 5000;
const MAX_PAGES: usize = 500;
const MAX_EXAMPLES: usize = 128;

/// Classify the complete observed recording, never infer a device from its width.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewportClass {
    #[default]
    Unknown,
    Narrow,
    Wide,
    Mixed,
}
#[derive(Debug, Clone, Serialize)]
pub struct ReplayExample {
    pub kind: String,
    pub key: String,
    pub timestamp_ms: i64,
    pub replay_id: String,
}
impl PageActivity {
    fn example(&mut self, kind: &str, key: &str, timestamp_ms: i64) {
        self.insert_example(ReplayExample {
            kind: kind.into(),
            key: key.into(),
            timestamp_ms,
            replay_id: String::new(),
        });
    }
    fn insert_example(&mut self, example: ReplayExample) {
        if self
            .examples
            .iter()
            .any(|e| e.kind == example.kind && e.key == example.key)
        {
            return;
        }
        if self.examples.len() >= MAX_EXAMPLES {
            // Dense movement must not crowd out actionable selectors/frustration.
            let removable = self
                .examples
                .iter()
                .position(|e| e.kind == "movement" && example.kind != "movement")
                .or_else(|| {
                    self.examples
                        .iter()
                        .position(|e| e.kind == "clicks" && example.kind == "frustration")
                });
            let Some(index) = removable else {
                return;
            };
            self.examples.remove(index);
        }
        self.examples.push(example);
    }
}
#[derive(Debug, Default, Serialize)]
pub struct PageActivity {
    pub examples: Vec<ReplayExample>,
    pub clicks: BTreeMap<String, u32>,
    pub movement: BTreeMap<String, u32>,
    pub elements: BTreeMap<String, u32>,
    pub visits: u32,
    pub sampled_replays: u32,
    pub observed_time_ms: i64,
    pub timed_visits: u32,
    pub last_observed_replays: u32,
    pub narrow_replays: u32,
    pub wide_replays: u32,
    pub next_pages: BTreeMap<String, u32>,
    pub previous_pages: BTreeMap<String, u32>,
    /// Clicks observed while the top document was scrolled by N viewport heights.
    /// Sticky controls remain viewport clicks, not fabricated document positions.
    pub depth_clicks: BTreeMap<String, u32>,
    pub depth_elements: BTreeMap<String, BTreeMap<String, u32>>,
    pub replay_ids: Vec<String>,
    pub frustration: crate::sentry::replay::Frustration,
    /// Maximum observed top-document scroll offset measured in viewport heights.
    /// This is not percentage of document height or a count of unique humans.
    pub max_scroll_viewports: f64,
    pub scroll_samples: u32,
    pub scroll_reach_replays: BTreeMap<String, u32>,
}
#[derive(Debug, Serialize)]
pub struct TimelineEvent {
    pub timestamp_ms: i64,
    pub kind: String,
    pub label: String,
    pub url: Option<String>,
    pub duration_ms: Option<f64>,
    pub event_id: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct Visit {
    pub url: String,
    pub started_at_ms: i64,
    pub duration_ms: Option<i64>,
}
#[derive(Debug, Default, Serialize)]
pub struct Analysis {
    pub viewport_class: ViewportClass,
    pub timeline: Vec<TimelineEvent>,
    pub journey: Vec<Visit>,
    pub pages: BTreeMap<String, PageActivity>,
    pub truncated: bool,
    pub gaps: Vec<u32>,
    #[serde(skip)]
    url: Option<String>,
    #[serde(skip)]
    viewport: Option<(f64, f64)>,
    #[serde(skip)]
    scroll_offset: Option<f64>,
    #[serde(skip)]
    viewport_history: VecDeque<(i64, f64, f64)>,
    #[serde(skip)]
    document: Option<i64>,
    #[serde(skip)]
    top_nodes: HashSet<i64>,
    #[serde(skip)]
    signals: Vec<(i64, crate::sentry::replay::Frustration)>,
}
impl Analysis {
    pub fn gap(&mut self, next: u32) {
        self.gaps.push(next);
        self.url = None;
        self.viewport = None;
        self.scroll_offset = None;
        self.viewport_history.clear();
        self.document = None;
        self.top_nodes.clear();
        // Unknown time across a gap must never be converted to page dwell time.
    }
    pub fn event(&mut self, event: &Value) {
        let Some(time) = event_timestamp_ms(event) else {
            return;
        };
        let data = &event["data"];
        match event["type"].as_u64() {
            Some(4) => {
                self.resize(data, time);
                if let Some(href) = data["href"].as_str() {
                    self.navigate(href, time, "page load");
                }
            }
            Some(2) => {
                self.top_nodes.clear();
                self.document = data.pointer("/node/id").and_then(Value::as_i64);
                collect_top_nodes(&data["node"], self.document, &mut self.top_nodes);
                self.scroll_offset = data
                    .pointer("/initialOffset/top")
                    .and_then(Value::as_f64)
                    .filter(|v| v.is_finite() && *v >= 0.0);
            }
            Some(3) => match data["source"].as_u64() {
                Some(0) => {
                    if let Some(adds) = data["adds"].as_array() {
                        for add in adds {
                            if add["parentId"]
                                .as_i64()
                                .is_some_and(|id| self.top_nodes.contains(&id))
                            {
                                collect_top_nodes(&add["node"], self.document, &mut self.top_nodes);
                            }
                        }
                    }
                }
                Some(1) => {
                    if let Some(positions) = data["positions"].as_array() {
                        for position in positions {
                            let offset = position["timeOffset"]
                                .as_f64()
                                .filter(|n| n.is_finite() && (-60_000.0..=60_000.0).contains(n))
                                .unwrap_or(0.0) as i64;
                            self.point(position, false, time.saturating_add(offset));
                        }
                    }
                }
                Some(2) if data["type"] == 2 => {
                    self.point(data, true, time);
                    self.push(time, "click", "click".into(), None, None);
                }
                Some(3) => {
                    if self.document.is_some() && data["id"].as_i64() == self.document {
                        self.scroll_offset =
                            data["y"].as_f64().filter(|v| v.is_finite() && *v >= 0.0);
                    }
                    self.push(time, "scroll", "scroll".into(), None, None);
                    if data["id"].as_i64() == self.document
                        && self.document.is_some()
                        && let Some((_, height)) = self.viewport
                        && let Some(y) = data["y"].as_f64().filter(|y| y.is_finite() && *y >= 0.0)
                        && let Some(page) = self.page()
                    {
                        page.max_scroll_viewports = page.max_scroll_viewports.max(y / height);
                        page.scroll_samples += 1;
                        for threshold in [0, 1, 2, 3, 5] {
                            if y / height >= f64::from(threshold) {
                                page.example("scroll", &threshold.to_string(), time);
                            }
                        }
                    }
                }
                Some(4) => self.resize(data, time),
                _ => {}
            },
            Some(5) => match data["tag"].as_str() {
                Some("breadcrumb") => {
                    let payload = &data["payload"];
                    let category = payload["category"].as_str().unwrap_or("");
                    let label = payload["message"]
                        .as_str()
                        .unwrap_or(category)
                        .chars()
                        .take(512)
                        .collect::<String>();
                    if category == "navigation" {
                        if let Some(to) = payload.pointer("/data/to").and_then(Value::as_str) {
                            self.navigate(to, time, "navigation");
                        }
                    } else if category == "ui.click" {
                        let depth = self.depth();
                        if !label.is_empty()
                            && let Some(page) = self.page()
                            && (page.elements.len() < 1000 || page.elements.contains_key(&label))
                        {
                            *page.elements.entry(label.clone()).or_default() += 1;
                            page.example("element", &label, time);
                            record_depth_element(page, depth.as_deref(), &label, time);
                        }
                        self.push(time, category, label, None, Some(payload));
                    } else if matches!(category, "ui.slowClickDetected" | "ui.multiClick") {
                        let f = frustration(event);
                        if self.signals.len() < MAX_TIMELINE {
                            self.signals.push((time, f));
                        } else {
                            self.truncated = true;
                        }
                        let kind = if f.rage > 0 {
                            "rage click"
                        } else if f.dead > 0 {
                            "dead click"
                        } else if f.slow > 0 {
                            "slow click"
                        } else {
                            "multi click"
                        };
                        self.push(
                            time,
                            kind,
                            label,
                            payload
                                .pointer("/data/timeAfterClickMs")
                                .and_then(Value::as_f64),
                            Some(payload),
                        );
                    } else if matches!(
                        category,
                        "console" | "error" | "sentry.event" | "sentry.feedback" | "fetch" | "xhr"
                    ) {
                        self.push(time, category, label, None, Some(payload));
                    }
                }
                Some("performanceSpan") => {
                    let span = &data["payload"];
                    let op = span["op"].as_str().unwrap_or("");
                    if op.starts_with("resource.") || op == "navigation.navigate" {
                        let method = span
                            .pointer("/data/method")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        let status = span
                            .pointer("/data/statusCode")
                            .and_then(Value::as_i64)
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        let description = span["description"].as_str().unwrap_or(op);
                        let duration = span["endTimestamp"]
                            .as_f64()
                            .zip(span["startTimestamp"].as_f64())
                            .map(|(end, start)| (end - start) * 1000.0)
                            .filter(|d| d.is_finite() && *d >= 0.0);
                        self.push(
                            time,
                            op,
                            format!("{method} {description} {status}")
                                .trim()
                                .chars()
                                .take(512)
                                .collect(),
                            duration,
                            Some(span),
                        );
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    pub fn finish(&mut self, end: i64) {
        for page in self.pages.values_mut() {
            for threshold in [0, 1, 2, 3, 5] {
                page.scroll_reach_replays.insert(
                    threshold.to_string(),
                    u32::from(page.max_scroll_viewports >= f64::from(threshold)),
                );
            }
        }
        if self.url.is_some()
            && let Some(last) = self.journey.last_mut()
        {
            last.duration_ms = Some(end.saturating_sub(last.started_at_ms).max(0));
        }
        for visit in &self.journey {
            if let Some(page) = self.pages.get_mut(&visit.url) {
                page.visits += 1;
                page.sampled_replays = 1;
                if let Some(duration) = visit.duration_ms {
                    page.observed_time_ms += duration;
                    page.timed_visits += 1;
                }
            }
        }
        for pair in self.journey.windows(2) {
            if pair[0].duration_ms.is_some() {
                if let Some(page) = self.pages.get_mut(&pair[0].url) {
                    *page.next_pages.entry(pair[1].url.clone()).or_default() += 1;
                }
                if let Some(page) = self.pages.get_mut(&pair[1].url) {
                    *page.previous_pages.entry(pair[0].url.clone()).or_default() += 1;
                }
            }
        }
        if !self.truncated
            && let Some(url) = &self.url
            && let Some(page) = self.pages.get_mut(url)
        {
            page.last_observed_replays = 1;
        }
        // Slow/rage breadcrumbs can arrive after navigation but refer to the original click time.
        let page_at = |time: i64| {
            self.journey
                .iter()
                .rev()
                .find(|visit| {
                    visit.started_at_ms <= time
                        && visit.duration_ms.is_some_and(|duration| {
                            time <= visit.started_at_ms.saturating_add(duration)
                        })
                })
                .map(|visit| visit.url.clone())
        };
        for event in &mut self.timeline {
            event.url = page_at(event.timestamp_ms);
        }
        for (time, signal) in &self.signals {
            if let Some(url) = page_at(*time)
                && let Some(page) = self.pages.get_mut(&url)
            {
                add_frustration(&mut page.frustration, *signal);
                for (kind, count) in [
                    ("rage", signal.rage),
                    ("dead", signal.dead),
                    ("slow", signal.slow),
                    ("multi", signal.multi),
                ] {
                    if count > 0 {
                        page.example("frustration", kind, *time);
                    }
                }
            }
        }
        self.timeline.sort_by_key(|event| event.timestamp_ms);
    }
    fn navigate(&mut self, href: &str, time: i64, kind: &str) {
        let parsed = url::Url::parse(href).ok().or_else(|| {
            self.url
                .as_ref()
                .and_then(|base| url::Url::parse(base).ok()?.join(href).ok())
        });
        let Some(mut url) = parsed.filter(|u| matches!(u.scheme(), "https" | "http")) else {
            return;
        };
        url.set_query(None);
        url.set_fragment(None);
        let _ = url.set_username("");
        let _ = url.set_password(None);
        let url = url.to_string();
        if self.url.as_ref() == Some(&url) {
            return;
        }
        if self.url.is_some()
            && let Some(previous) = self.journey.last_mut()
        {
            previous.duration_ms = Some(time.saturating_sub(previous.started_at_ms).max(0));
        }
        self.url = Some(url.clone());
        let width = self.viewport.map(|v| v.0);
        if let Some(page) = self.page()
            && let Some(width) = width
        {
            if width < 768.0 {
                page.narrow_replays = 1;
            } else {
                page.wide_replays = 1;
            }
        }
        if self.journey.len() < MAX_PAGES {
            self.journey.push(Visit {
                url: url.clone(),
                started_at_ms: time,
                duration_ms: None,
            });
        } else {
            self.truncated = true;
        }
        self.push(time, kind, url, None, None);
    }
    fn resize(&mut self, data: &Value, time: i64) {
        self.viewport = viewport(data);
        if let Some((width, height)) = self.viewport {
            let group = if width < 768.0 {
                ViewportClass::Narrow
            } else {
                ViewportClass::Wide
            };
            self.viewport_class = match self.viewport_class {
                ViewportClass::Unknown => group,
                previous if previous == group => previous,
                _ => ViewportClass::Mixed,
            };
            if self.viewport_history.len() >= 128 {
                self.viewport_history.pop_front();
            }
            self.viewport_history.push_back((time, width, height));
        }
    }
    fn point(&mut self, point: &Value, click: bool, time: i64) {
        if !point["id"]
            .as_i64()
            .is_some_and(|id| self.top_nodes.contains(&id))
        {
            return;
        }
        let Some((_, width, height)) = self
            .viewport_history
            .iter()
            .rev()
            .find(|(t, _, _)| *t <= time)
            .copied()
        else {
            return;
        };
        let (Some(x), Some(y)) = (point["x"].as_f64(), point["y"].as_f64()) else {
            return;
        };
        if !(x.is_finite() && y.is_finite() && x >= 0.0 && y >= 0.0 && x < width && y < height) {
            return;
        }
        let cell = format!(
            "{},{}",
            (x / width * 20.0) as u32,
            (y / height * 20.0) as u32
        );
        let url = self
            .journey
            .iter()
            .rev()
            .find(|visit| {
                visit.started_at_ms <= time
                    && visit
                        .duration_ms
                        .is_none_or(|duration| time <= visit.started_at_ms.saturating_add(duration))
            })
            .map(|visit| visit.url.clone());
        let Some(url) = url else { return };
        if !self.pages.contains_key(&url) && self.pages.len() >= MAX_PAGES {
            self.truncated = true;
            return;
        }
        let depth = if click && self.url.as_deref() == Some(url.as_str()) {
            self.depth()
        } else {
            None
        };
        if let Some(page) = self.pages.get_mut(&url) {
            if let Some(depth) = depth {
                page.example("depth", &depth, time);
                *page.depth_clicks.entry(depth).or_default() += 1;
            }
            page.example(if click { "clicks" } else { "movement" }, &cell, time);
            let cells = if click {
                &mut page.clicks
            } else {
                &mut page.movement
            };
            *cells.entry(cell).or_default() += 1;
        }
    }
    fn depth(&self) -> Option<String> {
        let (_, height) = self.viewport?;
        let depth = self.scroll_offset? / height;
        (depth.is_finite() && depth < 100.0).then(|| (depth.floor() as u32).to_string())
    }
    fn page(&mut self) -> Option<&mut PageActivity> {
        let url = self.url.as_ref()?;
        if !self.pages.contains_key(url) && self.pages.len() >= MAX_PAGES {
            self.truncated = true;
            return None;
        }
        Some(self.pages.entry(url.clone()).or_default())
    }
    fn push(
        &mut self,
        time: i64,
        kind: &str,
        label: String,
        duration_ms: Option<f64>,
        payload: Option<&Value>,
    ) {
        if self.timeline.len() >= MAX_TIMELINE {
            self.truncated = true;
            return;
        }
        let id = |name: &str| {
            payload
                .and_then(|v| v.get(name).or_else(|| v.get("data")?.get(name)))
                .and_then(Value::as_str)
                .filter(|s| s.len() <= 32 && s.bytes().all(|b| b.is_ascii_hexdigit()))
                .map(str::to_owned)
        };
        self.timeline.push(TimelineEvent {
            timestamp_ms: time,
            kind: kind.into(),
            label,
            url: self.url.clone(),
            duration_ms,
            event_id: id("event_id"),
            trace_id: id("trace_id"),
            span_id: id("span_id"),
        });
    }
}
fn record_depth_element(page: &mut PageActivity, depth: Option<&str>, label: &str, time: i64) {
    let Some(depth) = depth else { return };
    page.example("depth", depth, time);
    let elements = page.depth_elements.entry(depth.to_owned()).or_default();
    if elements.len() < 50 || elements.contains_key(label) {
        *elements.entry(label.to_owned()).or_default() += 1;
    }
}
fn viewport(data: &Value) -> Option<(f64, f64)> {
    let w = data["width"].as_f64()?;
    let h = data["height"].as_f64()?;
    (w.is_finite() && h.is_finite() && w > 0.0 && h > 0.0 && w <= 100_000.0 && h <= 100_000.0)
        .then_some((w, h))
}
fn collect_top_nodes(node: &Value, document: Option<i64>, nodes: &mut HashSet<i64>) {
    if node["type"] == 0 && node["id"].as_i64() != document {
        return;
    }
    if nodes.len() >= 200_000 {
        return;
    }
    if let Some(id) = node["id"].as_i64() {
        nodes.insert(id);
    }
    if let Some(children) = node["childNodes"].as_array() {
        for child in children {
            collect_top_nodes(child, document, nodes);
        }
    }
}

pub struct ReadBudget {
    remaining_bytes: usize,
    deadline: std::time::Instant,
}
impl Default for ReadBudget {
    fn default() -> Self {
        Self {
            remaining_bytes: 64 * 1024 * 1024,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(10),
        }
    }
}
pub fn analyze(
    root: &std::path::Path,
    segments: Vec<crate::db::replays::SegmentReference>,
    end: i64,
    budget: &mut ReadBudget,
) -> anyhow::Result<Analysis> {
    let mut analysis = Analysis::default();
    let mut expected = 0;
    for segment in segments {
        if std::time::Instant::now() >= budget.deadline {
            analysis.truncated = true;
            break;
        }
        if segment.segment_id != expected {
            analysis.gap(segment.segment_id);
        }
        expected = segment.segment_id + 1;
        let bytes = crate::storage::replay::read(root, &segment.blob)?;
        if bytes.len() > budget.remaining_bytes {
            analysis.truncated = true;
            break;
        }
        budget.remaining_bytes -= bytes.len();
        for event in crate::sentry::replay::recording_events(&bytes)? {
            analysis.event(&event);
        }
    }
    if analysis.truncated {
        analysis.url = None;
    }
    analysis.finish(end);
    Ok(analysis)
}

#[derive(Debug, Default, Serialize)]
pub struct PageMaps {
    pub pages: BTreeMap<String, PageActivity>,
    pub replays_analyzed: u32,
    pub truncated: bool,
}
impl PageMaps {
    pub fn include(&mut self, analysis: Analysis) {
        self.replays_analyzed += 1;
        self.truncated |= analysis.truncated || !analysis.gaps.is_empty();
        for (url, page) in analysis.pages {
            if !self.pages.contains_key(&url) && self.pages.len() >= MAX_PAGES {
                self.truncated = true;
                continue;
            }
            let total = self.pages.entry(url).or_default();
            for example in page.examples {
                total.insert_example(example);
            }
            for (a, b) in [
                (&mut total.clicks, page.clicks),
                (&mut total.movement, page.movement),
                (&mut total.elements, page.elements),
                (&mut total.depth_clicks, page.depth_clicks),
                (&mut total.next_pages, page.next_pages),
                (&mut total.previous_pages, page.previous_pages),
            ] {
                for (cell, count) in b {
                    if a.len() < 1000 || a.contains_key(&cell) {
                        *a.entry(cell).or_default() += count;
                    } else {
                        self.truncated = true;
                    }
                }
            }
            total.visits += page.visits;
            total.sampled_replays += page.sampled_replays;
            total.observed_time_ms += page.observed_time_ms;
            total.timed_visits += page.timed_visits;
            total.last_observed_replays += page.last_observed_replays;
            total.narrow_replays += page.narrow_replays;
            total.wide_replays += page.wide_replays;
            add_frustration(&mut total.frustration, page.frustration);
            for id in page.replay_ids {
                if total.replay_ids.len() < 3 && !total.replay_ids.contains(&id) {
                    total.replay_ids.push(id);
                }
            }
            for (depth, elements) in page.depth_elements {
                let merged = total.depth_elements.entry(depth).or_default();
                for (label, count) in elements {
                    if merged.len() < 50 || merged.contains_key(&label) {
                        *merged.entry(label).or_default() += count;
                    }
                }
            }
            total.max_scroll_viewports = total.max_scroll_viewports.max(page.max_scroll_viewports);
            total.scroll_samples += page.scroll_samples;
            for (threshold, count) in page.scroll_reach_replays {
                *total.scroll_reach_replays.entry(threshold).or_default() += count;
            }
        }
    }
}

fn add_frustration(
    total: &mut crate::sentry::replay::Frustration,
    value: crate::sentry::replay::Frustration,
) {
    total.slow += value.slow;
    total.dead += value.dead;
    total.rage += value.rage;
    total.multi += value.multi;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example(kind: &str, key: usize) -> ReplayExample {
        ReplayExample {
            kind: kind.into(),
            key: key.to_string(),
            timestamp_ms: key as i64,
            replay_id: String::new(),
        }
    }

    #[test]
    fn example_capacity_preserves_actionable_signals() {
        let mut page = PageActivity::default();
        for key in 0..MAX_EXAMPLES {
            page.insert_example(example("movement", key));
        }
        page.insert_example(example("element", MAX_EXAMPLES));
        assert_eq!(page.examples.len(), MAX_EXAMPLES);
        assert_eq!(page.examples.last().unwrap().kind, "element");
        page.insert_example(example("element", MAX_EXAMPLES));
        assert_eq!(page.examples.len(), MAX_EXAMPLES);

        let mut clicks = PageActivity::default();
        for key in 0..MAX_EXAMPLES {
            clicks.insert_example(example("clicks", key));
        }
        clicks.insert_example(example("frustration", MAX_EXAMPLES));
        assert_eq!(clicks.examples.last().unwrap().kind, "frustration");
        clicks.insert_example(example("movement", MAX_EXAMPLES + 1));
        assert_eq!(clicks.examples.len(), MAX_EXAMPLES);
    }

    #[test]
    fn navigation_viewports_and_points_cover_rejection_boundaries() {
        let mut analysis = Analysis::default();
        analysis.event(&serde_json::json!({"type": 1, "data": {}}));
        analysis.navigate("javascript:alert(1)", 0, "invalid");
        assert!(analysis.journey.is_empty());

        analysis.resize(&serde_json::json!({"width": 375, "height": 800}), 1);
        analysis.navigate(
            "https://user:pass@example.test/a?q=secret#fragment", // pragma: allowlist secret -- parser fixture
            2,
            "load",
        );
        analysis.navigate("https://example.test/a", 3, "duplicate");
        analysis.resize(&serde_json::json!({"width": 1024, "height": 800}), 4);
        assert_eq!(analysis.viewport_class, ViewportClass::Mixed);
        for time in 5..140 {
            analysis.resize(&serde_json::json!({"width": 1024, "height": 800}), time);
        }
        assert_eq!(analysis.viewport_history.len(), 128);

        analysis.top_nodes.insert(1);
        analysis.point(&serde_json::json!({"id": 2, "x": 1, "y": 1}), false, 10);
        let saved = std::mem::take(&mut analysis.viewport_history);
        analysis.point(&serde_json::json!({"id": 1, "x": 1, "y": 1}), false, 10);
        analysis.viewport_history = saved;
        analysis.point(&serde_json::json!({"id": 1}), false, 10);
        analysis.point(&serde_json::json!({"id": 1, "x": -1, "y": 1}), false, 10);
        analysis.journey.clear();
        analysis.point(&serde_json::json!({"id": 1, "x": 1, "y": 1}), false, 139);
        analysis.journey.push(Visit {
            url: "https://missing.test/".into(),
            started_at_ms: 0,
            duration_ms: None,
        });
        analysis.point(&serde_json::json!({"id": 1, "x": 1, "y": 1}), false, 139);
    }

    #[test]
    fn page_timeline_and_node_caps_fail_closed() {
        let mut analysis = Analysis {
            url: Some("https://overflow.test/".into()),
            ..Analysis::default()
        };
        for key in 0..MAX_PAGES {
            analysis
                .pages
                .insert(key.to_string(), PageActivity::default());
        }
        assert!(analysis.page().is_none());
        assert!(analysis.truncated);

        analysis.truncated = false;
        analysis.timeline = (0..MAX_TIMELINE)
            .map(|time| TimelineEvent {
                timestamp_ms: time as i64,
                kind: String::new(),
                label: String::new(),
                url: None,
                duration_ms: None,
                event_id: None,
                trace_id: None,
                span_id: None,
            })
            .collect();
        analysis.push(0, "overflow", String::new(), None, None);
        assert!(analysis.truncated);

        let mut nodes = (0..200_000).collect::<HashSet<_>>();
        collect_top_nodes(
            &serde_json::json!({"id": 200001, "type": 1}),
            None,
            &mut nodes,
        );
        assert_eq!(nodes.len(), 200_000);
        collect_top_nodes(
            &serde_json::json!({"id": 1, "type": 0}),
            Some(2),
            &mut HashSet::new(),
        );
    }

    #[test]
    fn finish_assigns_journeys_signals_and_aggregate_caps() {
        let first = "https://one.test/".to_owned();
        let second = "https://two.test/".to_owned();
        let mut analysis = Analysis {
            url: Some(second.clone()),
            ..Analysis::default()
        };
        analysis
            .pages
            .insert(first.clone(), PageActivity::default());
        analysis
            .pages
            .insert(second.clone(), PageActivity::default());
        analysis.journey.push(Visit {
            url: first.clone(),
            started_at_ms: 1,
            duration_ms: Some(4),
        });
        analysis.journey.push(Visit {
            url: second.clone(),
            started_at_ms: 5,
            duration_ms: None,
        });
        analysis.signals.push((
            2,
            crate::sentry::replay::Frustration {
                slow: 1,
                dead: 1,
                rage: 1,
                multi: 1,
            },
        ));
        analysis.finish(10);
        assert_eq!(analysis.pages[&first].next_pages[&second], 1);
        assert_eq!(analysis.pages[&second].previous_pages[&first], 1);
        assert_eq!(analysis.pages[&first].frustration.rage, 1);
        assert_eq!(analysis.pages[&second].last_observed_replays, 1);

        let mut maps = PageMaps::default();
        for key in 0..MAX_PAGES {
            maps.pages.insert(key.to_string(), PageActivity::default());
        }
        let mut extra = Analysis::default();
        extra
            .pages
            .insert("overflow".into(), PageActivity::default());
        maps.include(extra);
        assert!(maps.truncated);

        let mut maps = PageMaps::default();
        let total = PageActivity {
            clicks: (0..1000).map(|key| (key.to_string(), 1)).collect(),
            ..PageActivity::default()
        };
        maps.pages.insert("page".into(), total);
        let mut extra = Analysis::default();
        let mut page = PageActivity::default();
        page.clicks.insert("overflow".into(), 1);
        extra.pages.insert("page".into(), page);
        maps.include(extra);
        assert!(maps.truncated);
    }

    #[test]
    fn analyze_honors_deadline_byte_budget_and_segment_gaps() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let blob = crate::storage::replay::write(directory.path(), b"[]")?;
        let segment = crate::db::replays::SegmentReference {
            segment_id: 2,
            blob: blob.clone(),
        };
        let mut budget = ReadBudget::default();
        let analysis = analyze(directory.path(), vec![segment.clone()], 10, &mut budget)?;
        assert_eq!(analysis.gaps, vec![2]);

        let mut budget = ReadBudget {
            remaining_bytes: 0,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
        };
        assert!(analyze(directory.path(), vec![segment.clone()], 10, &mut budget)?.truncated);
        let mut budget = ReadBudget {
            remaining_bytes: usize::MAX,
            deadline: std::time::Instant::now() - std::time::Duration::from_secs(1),
        };
        assert!(analyze(directory.path(), vec![segment], 10, &mut budget)?.truncated);
        Ok(())
    }

    #[test]
    fn page_map_merges_examples_ids_depth_and_scroll_counts() {
        let mut maps = PageMaps::default();
        let mut first = Analysis::default();
        let mut page = PageActivity::default();
        page.examples.push(example("element", 1));
        page.replay_ids = vec!["one".into(), "two".into(), "three".into(), "four".into()];
        page.depth_elements
            .entry("1".into())
            .or_default()
            .insert("button".into(), 2);
        page.scroll_reach_replays.insert("1".into(), 1);
        first.pages.insert("page".into(), page);
        maps.include(first);
        assert_eq!(maps.pages["page"].examples.len(), 1);
        assert_eq!(maps.pages["page"].replay_ids.len(), 3);
        assert_eq!(maps.pages["page"].depth_elements["1"]["button"], 2);
        assert_eq!(maps.pages["page"].scroll_reach_replays["1"], 1);
    }

    #[test]
    fn rrweb_mutations_movements_and_click_metadata_are_bounded() {
        let mut analysis = Analysis::default();
        analysis.event(&serde_json::json!({
            "type": 4, "timestamp": 1,
            "data": {"href": "https://example.test/", "width": 320, "height": 200}
        }));
        analysis.event(&serde_json::json!({
            "type": 2, "timestamp": 2,
            "data": {"node": {"type": 0, "id": 1, "childNodes": [
                {"type": 2, "id": 2, "childNodes": []}
            ]}, "initialOffset": {"top": 250}}
        }));
        analysis.event(&serde_json::json!({
            "type": 3, "timestamp": 3,
            "data": {"source": 0, "adds": [
                {"parentId": 2, "node": {"type": 2, "id": 3}},
                {"parentId": 999, "node": {"type": 2, "id": 4}}
            ]}
        }));
        analysis.event(&serde_json::json!({
            "type": 3, "timestamp": 4, "data": {"source": 0, "adds": null}
        }));
        analysis.event(&serde_json::json!({
            "type": 3, "timestamp": 5,
            "data": {"source": 1, "positions": [
                {"id": 3, "x": 16, "y": 20, "timeOffset": 1},
                {"id": 3, "x": 32, "y": 40, "timeOffset": 100000}
            ]}
        }));
        analysis.event(&serde_json::json!({
            "type": 3, "timestamp": 6, "data": {"source": 1, "positions": null}
        }));
        assert!(analysis.top_nodes.contains(&3));
        assert!(!analysis.top_nodes.contains(&4));
        assert_eq!(
            analysis.pages["https://example.test/"]
                .movement
                .values()
                .sum::<u32>(),
            2
        );

        analysis.event(&serde_json::json!({
            "type": 5, "timestamp": 7,
            "data": {"tag": "breadcrumb", "payload": {
                "category": "ui.click", "message": "button.save"
            }}
        }));
        let page = analysis.pages.get_mut("https://example.test/").unwrap();
        assert_eq!(page.depth_elements["1"]["button.save"], 1);
        page.elements = (0..1000).map(|n| (n.to_string(), 1)).collect();
        analysis.event(&serde_json::json!({
            "type": 5, "timestamp": 8,
            "data": {"tag": "breadcrumb", "payload": {
                "category": "ui.click", "message": "overflow"
            }}
        }));
        assert!(
            !analysis.pages["https://example.test/"]
                .elements
                .contains_key("overflow")
        );
    }

    #[test]
    fn frustration_categories_and_limits_preserve_their_meaning() {
        let mut analysis = Analysis::default();
        analysis.navigate("https://example.test/", 0, "load");
        for (time, category, end_reason, tag, count, expected) in [
            (
                1,
                "ui.slowClickDetected",
                "timeout",
                "button",
                5,
                "rage click",
            ),
            (
                2,
                "ui.slowClickDetected",
                "timeout",
                "button",
                1,
                "dead click",
            ),
            (
                3,
                "ui.slowClickDetected",
                "mutation",
                "div",
                1,
                "slow click",
            ),
            (4, "ui.multiClick", "", "div", 1, "multi click"),
        ] {
            analysis.event(&serde_json::json!({
                "type": 5, "timestamp": time,
                "data": {"tag": "breadcrumb", "payload": {
                    "category": category, "message": expected,
                    "data": {"endReason": end_reason, "node": {"tagName": tag},
                             "clickCount": count, "timeAfterClickMs": 7}
                }}
            }));
        }
        assert_eq!(
            analysis
                .timeline
                .iter()
                .skip(1)
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["rage click", "dead click", "slow click", "multi click"]
        );

        analysis
            .signals
            .resize(MAX_TIMELINE, (0, Default::default()));
        analysis.event(&serde_json::json!({
            "type": 5, "timestamp": 5,
            "data": {"tag": "breadcrumb", "payload": {
                "category": "ui.multiClick", "data": {"clickCount": 1}
            }}
        }));
        assert!(analysis.truncated);
        analysis.finish(10);
        let page = &analysis.pages["https://example.test/"];
        assert_eq!(
            (
                page.frustration.slow,
                page.frustration.dead,
                page.frustration.rage,
                page.frustration.multi
            ),
            (3, 2, 1, 1)
        );
    }

    #[test]
    fn journey_and_point_caps_mark_incomplete_analysis() {
        let mut untimed = Analysis::default();
        untimed.pages.insert("one".into(), PageActivity::default());
        untimed.pages.insert("two".into(), PageActivity::default());
        untimed.journey.push(Visit {
            url: "one".into(),
            started_at_ms: 0,
            duration_ms: None,
        });
        untimed.journey.push(Visit {
            url: "two".into(),
            started_at_ms: 1,
            duration_ms: None,
        });
        untimed.finish(2);
        assert_eq!(untimed.pages["one"].visits, 1);
        assert!(untimed.pages["one"].next_pages.is_empty());

        let mut capped = Analysis::default();
        capped.resize(&serde_json::json!({"width": 1000, "height": 100}), 0);
        capped.journey = (0..MAX_PAGES)
            .map(|n| Visit {
                url: format!("https://{n}.test/"),
                started_at_ms: n as i64,
                duration_ms: Some(1),
            })
            .collect();
        capped.navigate("https://overflow.test/", 1000, "navigation");
        assert!(capped.truncated);
        assert_eq!(capped.journey.len(), MAX_PAGES);
        assert_eq!(capped.pages["https://overflow.test/"].wide_replays, 1);

        let mut point = Analysis::default();
        point.top_nodes.insert(1);
        point.viewport_history.push_back((0, 100.0, 100.0));
        point.journey.push(Visit {
            url: "overflow".into(),
            started_at_ms: 0,
            duration_ms: None,
        });
        point.pages = (0..MAX_PAGES)
            .map(|n| (format!("page-{n}"), PageActivity::default()))
            .collect();
        point.point(&serde_json::json!({"id": 1, "x": 1, "y": 1}), false, 1);
        assert!(point.truncated);
    }

    #[test]
    fn replay_analysis_handles_absent_and_saturated_optional_state() {
        let mut analysis = Analysis::default();
        analysis.resize(&serde_json::json!({"width": 0, "height": 100}), 0);
        assert!(analysis.viewport.is_none());
        analysis.resize(&serde_json::json!({"width": 100, "height": 100}), 1);
        analysis.navigate("https://example.test/", 1, "load");
        analysis.top_nodes.insert(1);
        for point in [
            serde_json::json!({"id": 1, "y": 1}),
            serde_json::json!({"id": 1, "x": 1}),
            serde_json::json!({"id": 1, "x": 100, "y": 1}),
            serde_json::json!({"id": 1, "x": 1, "y": 100}),
        ] {
            analysis.point(&point, false, 2);
        }
        analysis.point(&serde_json::json!({"id": 1, "x": 20, "y": 20}), false, 2);
        analysis.point(&serde_json::json!({"id": 1, "x": 20, "y": 20}), true, 2);
        assert_eq!(analysis.pages["https://example.test/"].movement["4,4"], 1);
        assert_eq!(analysis.pages["https://example.test/"].clicks["4,4"], 1);

        let page = analysis.pages.get_mut("https://example.test/").unwrap();
        page.depth_elements
            .insert("0".into(), (0..50).map(|n| (n.to_string(), 1)).collect());
        analysis.scroll_offset = Some(0.0);
        analysis.event(&serde_json::json!({
            "type": 5, "timestamp": 3, "data": {"tag": "breadcrumb", "payload": {
                "category": "ui.click", "message": "new-at-cap"
            }}
        }));
        assert!(
            !analysis.pages["https://example.test/"].depth_elements["0"].contains_key("new-at-cap")
        );

        analysis.url = None;
        analysis.pages.remove("https://example.test/");
        analysis.journey.push(Visit {
            url: "missing-page".into(),
            started_at_ms: 4,
            duration_ms: Some(1),
        });
        analysis.signals.push((100, Default::default()));
        analysis.finish(5);
    }
}
