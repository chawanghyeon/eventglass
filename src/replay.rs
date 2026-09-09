//! Bounded, on-demand analysis of SDK events. Raw events remain solely in blob storage.
use crate::sentry::replay::{event_timestamp_ms, frustration};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet, VecDeque};

const MAX_TIMELINE: usize = 5000;
const MAX_PAGES: usize = 500;
#[derive(Debug, Default, Serialize)]
pub struct PageActivity {
    pub clicks: BTreeMap<String, u32>,
    pub movement: BTreeMap<String, u32>,
    pub elements: BTreeMap<String, u32>,
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
    viewport_history: VecDeque<(i64, f64, f64)>,
    #[serde(skip)]
    document: Option<i64>,
    #[serde(skip)]
    top_nodes: HashSet<i64>,
}
impl Analysis {
    pub fn gap(&mut self, next: u32) {
        self.gaps.push(next);
        self.url = None;
        self.viewport = None;
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
                    self.push(time, "scroll", "scroll".into(), None, None);
                    if data["id"].as_i64() == self.document
                        && self.document.is_some()
                        && let Some((_, height)) = self.viewport
                        && let Some(y) = data["y"].as_f64().filter(|y| y.is_finite() && *y >= 0.0)
                        && let Some(page) = self.page()
                    {
                        page.max_scroll_viewports = page.max_scroll_viewports.max(y / height);
                        page.scroll_samples += 1;
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
                        if !label.is_empty()
                            && let Some(page) = self.page()
                            && page.elements.len() < 1000
                        {
                            *page.elements.entry(label.clone()).or_default() += 1;
                        }
                        self.push(time, category, label, None, Some(payload));
                    } else if matches!(category, "ui.slowClickDetected" | "ui.multiClick") {
                        let f = frustration(event);
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
        let _ = self.page();
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
        let Some(url) = self
            .journey
            .iter()
            .rev()
            .find(|visit| {
                visit.started_at_ms <= time
                    && visit
                        .duration_ms
                        .is_none_or(|duration| time <= visit.started_at_ms.saturating_add(duration))
            })
            .map(|visit| visit.url.clone())
        else {
            return;
        };
        if !self.pages.contains_key(&url) && self.pages.len() >= MAX_PAGES {
            self.truncated = true;
            return;
        }
        if let Some(page) = self.pages.get_mut(&url) {
            let cells = if click {
                &mut page.clicks
            } else {
                &mut page.movement
            };
            *cells.entry(cell).or_default() += 1;
        }
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
    if !analysis.truncated {
        analysis.finish(end);
    } else {
        analysis.timeline.sort_by_key(|e| e.timestamp_ms);
    }
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
        self.truncated |= analysis.truncated;
        for (url, page) in analysis.pages {
            if !self.pages.contains_key(&url) && self.pages.len() >= MAX_PAGES {
                self.truncated = true;
                continue;
            }
            let total = self.pages.entry(url).or_default();
            for (a, b) in [
                (&mut total.clicks, page.clicks),
                (&mut total.movement, page.movement),
                (&mut total.elements, page.elements),
            ] {
                for (cell, count) in b {
                    if a.len() < 1000 || a.contains_key(&cell) {
                        *a.entry(cell).or_default() += count;
                    } else {
                        self.truncated = true;
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
