use std::{fmt::Debug, marker::PhantomData, time::Duration};

use async_trait::async_trait;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    config::ChannelEventMode,
    router::{
        RouterChannelEvent, RouterOutputSink, render_compact_channel_events,
        render_live_compact_channel_events,
    },
    text::{append_reply_message_break, truncate_chars},
};

#[async_trait]
pub(crate) trait ChannelReplyPort: Clone + Send + Sync + 'static {
    type Target: Clone + Send + Sync + 'static;

    async fn post_text(&self, target: &Self::Target, text: &str) -> anyhow::Result<PostedMessage>;
    async fn post_markdown(
        &self,
        target: &Self::Target,
        text: &str,
    ) -> anyhow::Result<PostedMessage>;
    async fn update_text(&self, target: &Self::Target, id: &str, text: &str) -> anyhow::Result<()>;
    async fn update_markdown(
        &self,
        target: &Self::Target,
        id: &str,
        text: &str,
    ) -> anyhow::Result<()>;
    async fn delete(&self, target: &Self::Target, id: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostedMessage {
    pub id: Option<String>,
}

impl PostedMessage {
    pub(crate) fn without_id() -> Self {
        Self { id: None }
    }

    pub(crate) fn with_id(id: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChannelReplyStyle {
    StreamingDraft,
    CheckpointMessages,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactActivityStyle {
    LiveUpdate,
    FinalSummary,
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelOutputPolicy {
    pub activity_mode: ChannelEventMode,
    pub compact_activity_style: CompactActivityStyle,
    pub reply_style: ChannelReplyStyle,
    pub draft_update_interval: Duration,
    pub draft_update_growth: usize,
    pub draft_initial_min_len: usize,
    pub draft_preview_max_bytes: usize,
    pub draft_truncated_prefix: &'static str,
    pub draft_marker: &'static str,
    pub checkpoint_min_interval: Duration,
    pub checkpoint_min_growth: usize,
    pub checkpoint_preview_chars: usize,
    pub checkpoint_prefix: &'static str,
}

impl ChannelOutputPolicy {
    pub(crate) fn streaming_draft(activity_mode: ChannelEventMode) -> Self {
        Self {
            activity_mode,
            compact_activity_style: CompactActivityStyle::LiveUpdate,
            reply_style: ChannelReplyStyle::StreamingDraft,
            draft_update_interval: Duration::from_secs(2),
            draft_update_growth: 500,
            draft_initial_min_len: 40,
            draft_preview_max_bytes: 8_000,
            draft_truncated_prefix: "...\n",
            draft_marker: "[router-draft]",
            checkpoint_min_interval: Duration::from_secs(20),
            checkpoint_min_growth: 500,
            checkpoint_preview_chars: 240,
            checkpoint_prefix: "Still generating reply:\n",
        }
    }

    pub(crate) fn checkpoint_messages(activity_mode: ChannelEventMode) -> Self {
        Self {
            activity_mode,
            compact_activity_style: CompactActivityStyle::FinalSummary,
            reply_style: ChannelReplyStyle::CheckpointMessages,
            draft_update_interval: Duration::from_secs(2),
            draft_update_growth: 500,
            draft_initial_min_len: 40,
            draft_preview_max_bytes: 8_000,
            draft_truncated_prefix: "...\n",
            draft_marker: "[router-draft]",
            checkpoint_min_interval: Duration::from_secs(20),
            checkpoint_min_growth: 500,
            checkpoint_preview_chars: 240,
            checkpoint_prefix: "Still generating reply:\n",
        }
    }
}

pub(crate) struct ChannelOutputSink<P>
where
    P: ChannelReplyPort,
{
    port: P,
    target: P::Target,
    policy: ChannelOutputPolicy,
    activity: ChannelActivity<P>,
    reply: ChannelReplyStream<P>,
}

impl<P> ChannelOutputSink<P>
where
    P: ChannelReplyPort,
{
    pub(crate) fn new(port: P, target: P::Target, policy: ChannelOutputPolicy) -> Self {
        Self {
            port,
            target,
            policy,
            activity: ChannelActivity::default(),
            reply: ChannelReplyStream::default(),
        }
    }

    async fn flush_activity(&mut self) -> Option<String> {
        match self.policy.activity_mode {
            ChannelEventMode::Off => None,
            ChannelEventMode::Compact => match self.policy.compact_activity_style {
                CompactActivityStyle::LiveUpdate => self.activity.compact.flush_live().await,
                CompactActivityStyle::FinalSummary => {
                    self.activity
                        .compact
                        .flush_final(self.port.clone(), self.target.clone())
                        .await
                }
            },
            ChannelEventMode::Verbose => {
                self.activity.verbose.flush().await;
                None
            }
        }
    }
}

#[async_trait]
impl<P> RouterOutputSink for ChannelOutputSink<P>
where
    P: ChannelReplyPort,
{
    fn send_channel_event(&mut self, event: RouterChannelEvent) {
        match self.policy.activity_mode {
            ChannelEventMode::Off => {}
            ChannelEventMode::Compact => match self.policy.compact_activity_style {
                CompactActivityStyle::LiveUpdate => {
                    self.activity
                        .compact
                        .send_live(self.port.clone(), self.target.clone(), event);
                }
                CompactActivityStyle::FinalSummary => {
                    self.activity.compact.push(event);
                }
            },
            ChannelEventMode::Verbose => {
                self.activity
                    .verbose
                    .send(self.port.clone(), self.target.clone(), event);
            }
        }
    }

    fn send_reply_chunk(&mut self, chunk: String) {
        match self.policy.reply_style {
            ChannelReplyStyle::StreamingDraft => {
                self.reply.draft.send_chunk(
                    self.port.clone(),
                    self.target.clone(),
                    &self.policy,
                    chunk,
                );
            }
            ChannelReplyStyle::CheckpointMessages => {
                let Some(checkpoint) =
                    self.reply
                        .checkpoints
                        .push_chunk(chunk, CheckpointClock::Now, &self.policy)
                else {
                    return;
                };
                self.reply.checkpoint_poster.send(
                    self.port.clone(),
                    self.target.clone(),
                    checkpoint,
                );
            }
        }
    }

    fn send_reply_break(&mut self) {
        match self.policy.reply_style {
            ChannelReplyStyle::StreamingDraft => self.reply.draft.break_message(),
            ChannelReplyStyle::CheckpointMessages => self.reply.checkpoints.break_message(),
        }
    }

    async fn discard_reply_stream(&mut self) {
        if self.policy.activity_mode == ChannelEventMode::Compact
            && self.policy.compact_activity_style == CompactActivityStyle::LiveUpdate
            && let Some(id) = self.activity.compact.discard_live().await
            && let Err(err) = self.port.delete(&self.target, &id).await
        {
            tracing::warn!(error = %err, "failed to delete discarded channel activity message");
        }
        match self.policy.reply_style {
            ChannelReplyStyle::StreamingDraft => self.reply.draft.discard().await,
            ChannelReplyStyle::CheckpointMessages => {
                self.reply.checkpoint_poster.abort().await;
                self.reply.checkpoints.finish();
            }
        }
    }

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        match self.policy.reply_style {
            ChannelReplyStyle::StreamingDraft => {
                let compact_activity_id = self.flush_activity().await;
                let result = self
                    .reply
                    .draft
                    .finalize(self.port.clone(), self.target.clone(), text)
                    .await;
                if result.is_ok()
                    && let Some(id) = compact_activity_id
                    && let Err(err) = self.port.delete(&self.target, &id).await
                {
                    tracing::warn!(error = %err, "failed to delete final channel activity message");
                }
                result
            }
            ChannelReplyStyle::CheckpointMessages => {
                self.reply.checkpoint_poster.flush().await;
                self.flush_activity().await;
                self.reply.checkpoints.finish();
                self.port
                    .post_markdown(&self.target, &text)
                    .await
                    .map(|_| ())
            }
        }
    }
}

struct ChannelActivity<P>
where
    P: ChannelReplyPort,
{
    verbose: VerboseActivity<P>,
    compact: CompactActivity<P>,
}

impl<P> Default for ChannelActivity<P>
where
    P: ChannelReplyPort,
{
    fn default() -> Self {
        Self {
            verbose: VerboseActivity::default(),
            compact: CompactActivity::default(),
        }
    }
}

struct VerboseActivity<P>
where
    P: ChannelReplyPort,
{
    poster: Option<VerboseActivityPoster>,
    _port: PhantomData<P>,
}

impl<P> Default for VerboseActivity<P>
where
    P: ChannelReplyPort,
{
    fn default() -> Self {
        Self {
            poster: None,
            _port: PhantomData,
        }
    }
}

struct VerboseActivityPoster {
    events: mpsc::UnboundedSender<RouterChannelEvent>,
    handle: JoinHandle<()>,
}

impl<P> VerboseActivity<P>
where
    P: ChannelReplyPort,
{
    fn send(&mut self, port: P, target: P::Target, event: RouterChannelEvent) {
        if self.poster.is_none() {
            self.poster = Some(spawn_verbose_activity_poster(port, target));
        }
        if let Some(poster) = &self.poster
            && poster.events.send(event).is_err()
        {
            tracing::warn!("verbose channel activity poster stopped before receiving event");
        }
    }

    async fn flush(&mut self) {
        if let Some(poster) = self.poster.take() {
            drop(poster.events);
            await_unit_worker(poster.handle, "verbose_activity").await;
        }
    }
}

struct CompactActivity<P>
where
    P: ChannelReplyPort,
{
    events: Vec<RouterChannelEvent>,
    updater: Option<CompactActivityUpdater>,
    _port: PhantomData<P>,
}

impl<P> Default for CompactActivity<P>
where
    P: ChannelReplyPort,
{
    fn default() -> Self {
        Self {
            events: Vec::new(),
            updater: None,
            _port: PhantomData,
        }
    }
}

struct CompactActivityUpdater {
    updates: mpsc::UnboundedSender<String>,
    handle: JoinHandle<Option<String>>,
}

impl<P> CompactActivity<P>
where
    P: ChannelReplyPort,
{
    fn push(&mut self, event: RouterChannelEvent) {
        self.events.push(event);
    }

    fn send_live(&mut self, port: P, target: P::Target, event: RouterChannelEvent) {
        self.events.push(event);
        let Some(summary) = render_live_compact_channel_events(&self.events) else {
            return;
        };
        if self.updater.is_none() {
            self.updater = Some(spawn_compact_activity_updater(port, target));
        }
        if let Some(updater) = &self.updater
            && updater.updates.send(summary).is_err()
        {
            tracing::warn!("compact channel activity updater stopped before receiving update");
        }
    }

    async fn flush_live(&mut self) -> Option<String> {
        if let Some(updater) = self.updater.take() {
            drop(updater.updates);
            return await_option_worker(updater.handle, "compact_activity").await;
        }
        None
    }

    async fn discard_live(&mut self) -> Option<String> {
        let id = self.flush_live().await;
        self.events.clear();
        id
    }

    async fn flush_final(&mut self, port: P, target: P::Target) -> Option<String> {
        let summary = render_compact_channel_events(&self.events)?;
        match port.post_text(&target, &summary).await {
            Ok(posted) => posted.id,
            Err(err) => {
                tracing::warn!(error = %err, "failed to post compact channel activity summary");
                None
            }
        }
    }
}

struct ChannelReplyStream<P>
where
    P: ChannelReplyPort,
{
    draft: StreamingDraft,
    checkpoints: CheckpointMessages,
    checkpoint_poster: CheckpointPoster<P>,
}

impl<P> Default for ChannelReplyStream<P>
where
    P: ChannelReplyPort,
{
    fn default() -> Self {
        Self {
            draft: StreamingDraft::default(),
            checkpoints: CheckpointMessages::default(),
            checkpoint_poster: CheckpointPoster::default(),
        }
    }
}

#[derive(Default)]
struct StreamingDraft {
    text: String,
    updater: Option<StreamingDraftUpdater>,
    last_update_at: Option<Instant>,
    last_update_len: usize,
}

struct StreamingDraftUpdater {
    updates: mpsc::UnboundedSender<StreamingDraftUpdate>,
    handle: JoinHandle<()>,
}

enum StreamingDraftUpdate {
    Preview(String),
    Final {
        text: String,
        done: oneshot::Sender<anyhow::Result<()>>,
    },
    Discard {
        done: oneshot::Sender<()>,
    },
}

impl StreamingDraft {
    fn break_message(&mut self) {
        append_reply_message_break(&mut self.text);
    }

    fn send_chunk<P>(
        &mut self,
        port: P,
        target: P::Target,
        policy: &ChannelOutputPolicy,
        chunk: String,
    ) where
        P: ChannelReplyPort,
    {
        self.text.push_str(&chunk);
        if self.text.trim().is_empty() {
            return;
        }
        if self.updater.is_none() {
            self.updater = Some(spawn_streaming_draft_updater(port, target, policy.clone()));
        }
        let now = Instant::now();
        if !self.should_send_preview(now, policy) {
            return;
        }
        if let Some(updater) = &self.updater {
            if updater
                .updates
                .send(StreamingDraftUpdate::Preview(self.text.clone()))
                .is_ok()
            {
                self.last_update_at = Some(now);
                self.last_update_len = self.text.len();
            } else {
                tracing::warn!("channel reply draft updater stopped before receiving preview");
            }
        }
    }

    fn should_send_preview(&self, now: Instant, policy: &ChannelOutputPolicy) -> bool {
        if let Some(last) = self.last_update_at {
            return now.duration_since(last) >= policy.draft_update_interval
                || self.text.len().saturating_sub(self.last_update_len)
                    >= policy.draft_update_growth;
        }
        self.text.len() >= policy.draft_initial_min_len
    }

    async fn finalize<P>(&mut self, port: P, target: P::Target, text: String) -> anyhow::Result<()>
    where
        P: ChannelReplyPort,
    {
        self.text = text.clone();
        let Some(updater) = self.updater.take() else {
            return port.post_markdown(&target, &text).await.map(|_| ());
        };
        let (done_tx, done_rx) = oneshot::channel();
        if updater
            .updates
            .send(StreamingDraftUpdate::Final {
                text,
                done: done_tx,
            })
            .is_err()
        {
            await_unit_worker(updater.handle, "reply_draft").await;
            anyhow::bail!("channel reply draft updater stopped before final reply");
        }
        drop(updater.updates);
        let result = done_rx
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("channel reply draft dropped final ack")));
        await_unit_worker(updater.handle, "reply_draft").await;
        if result.is_ok() {
            self.last_update_len = self.text.len();
            self.last_update_at = Some(Instant::now());
        }
        result
    }

    async fn discard(&mut self) {
        self.text.clear();
        let Some(updater) = self.updater.take() else {
            return;
        };
        let (done_tx, done_rx) = oneshot::channel();
        if updater
            .updates
            .send(StreamingDraftUpdate::Discard { done: done_tx })
            .is_err()
        {
            await_unit_worker(updater.handle, "reply_draft").await;
            return;
        }
        drop(updater.updates);
        let _ = done_rx.await;
        await_unit_worker(updater.handle, "reply_draft").await;
    }
}

#[derive(Clone, Copy)]
enum CheckpointClock {
    Now,
}

#[derive(Default)]
struct CheckpointMessages {
    text: String,
    started_at: Option<Instant>,
    last_checkpoint_at: Option<Instant>,
    last_checkpoint_len: usize,
}

impl CheckpointMessages {
    fn push_chunk(
        &mut self,
        chunk: String,
        _clock: CheckpointClock,
        policy: &ChannelOutputPolicy,
    ) -> Option<String> {
        let now = Instant::now();
        self.started_at.get_or_insert(now);
        self.text.push_str(&chunk);
        if self.text.trim().is_empty() {
            return None;
        }
        let growth = self.text.len().saturating_sub(self.last_checkpoint_len);
        let should_send = if let Some(last) = self.last_checkpoint_at {
            now.duration_since(last) >= policy.checkpoint_min_interval && growth > 0
        } else {
            self.started_at.is_some_and(|started| {
                now.duration_since(started) >= policy.checkpoint_min_interval
            }) || self.text.len() >= policy.checkpoint_min_growth
        };
        if !should_send {
            return None;
        }
        self.last_checkpoint_at = Some(now);
        self.last_checkpoint_len = self.text.len();
        Some(render_reply_checkpoint(&self.text, policy))
    }

    fn break_message(&mut self) {
        append_reply_message_break(&mut self.text);
    }

    fn finish(&mut self) {
        self.text.clear();
    }
}

struct CheckpointPoster<P>
where
    P: ChannelReplyPort,
{
    poster: Option<CheckpointPosterWorker>,
    _port: PhantomData<P>,
}

impl<P> Default for CheckpointPoster<P>
where
    P: ChannelReplyPort,
{
    fn default() -> Self {
        Self {
            poster: None,
            _port: PhantomData,
        }
    }
}

struct CheckpointPosterWorker {
    checkpoints: mpsc::UnboundedSender<String>,
    handle: JoinHandle<()>,
}

impl<P> CheckpointPoster<P>
where
    P: ChannelReplyPort,
{
    fn send(&mut self, port: P, target: P::Target, checkpoint: String) {
        if self.poster.is_none() {
            self.poster = Some(spawn_checkpoint_poster(port, target));
        }
        if let Some(poster) = &self.poster
            && poster.checkpoints.send(checkpoint).is_err()
        {
            tracing::warn!("channel reply checkpoint poster stopped before receiving checkpoint");
        }
    }

    async fn flush(&mut self) {
        if let Some(poster) = self.poster.take() {
            drop(poster.checkpoints);
            await_unit_worker(poster.handle, "reply_checkpoint").await;
        }
    }

    async fn abort(&mut self) {
        if let Some(poster) = self.poster.take() {
            drop(poster.checkpoints);
            poster.handle.abort();
            let _ = poster.handle.await;
        }
    }
}

fn spawn_verbose_activity_poster<P>(port: P, target: P::Target) -> VerboseActivityPoster
where
    P: ChannelReplyPort,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<RouterChannelEvent>();
    let handle = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(err) = port.post_text(&target, &event.render_text()).await {
                tracing::warn!(error = %err, "failed to post channel event");
            }
        }
    });
    VerboseActivityPoster { events: tx, handle }
}

fn spawn_compact_activity_updater<P>(port: P, target: P::Target) -> CompactActivityUpdater
where
    P: ChannelReplyPort,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let handle = tokio::spawn(async move {
        let mut message_id = None;
        while let Some(mut summary) = rx.recv().await {
            while let Ok(next) = rx.try_recv() {
                summary = next;
            }
            if let Some(id) = message_id.as_deref() {
                if let Err(err) = port.update_text(&target, id, &summary).await {
                    tracing::warn!(error = %err, "failed to update compact channel activity message");
                }
            } else {
                match port.post_text(&target, &summary).await {
                    Ok(PostedMessage { id: Some(id) }) => message_id = Some(id),
                    Ok(PostedMessage { id: None }) => {
                        tracing::warn!("compact channel activity message response omitted id");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to post compact channel activity message");
                    }
                }
            }
        }
        message_id
    });
    CompactActivityUpdater {
        updates: tx,
        handle,
    }
}

fn spawn_streaming_draft_updater<P>(
    port: P,
    target: P::Target,
    policy: ChannelOutputPolicy,
) -> StreamingDraftUpdater
where
    P: ChannelReplyPort,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<StreamingDraftUpdate>();
    let handle = tokio::spawn(async move {
        let mut message_id = None;
        while let Some(mut update) = rx.recv().await {
            while let Ok(next) = rx.try_recv() {
                update = next;
            }
            match update {
                StreamingDraftUpdate::Preview(text) => {
                    let text = streaming_draft_text(&text, &policy);
                    if let Err(err) =
                        upsert_streaming_draft(&port, &target, &mut message_id, &text).await
                    {
                        tracing::warn!(error = %err, "failed to upsert channel reply draft");
                    }
                }
                StreamingDraftUpdate::Final { text, done } => {
                    let result = finalize_streaming_draft(&port, &target, &mut message_id, &text)
                        .await
                        .map(|_| ());
                    let _ = done.send(result);
                    break;
                }
                StreamingDraftUpdate::Discard { done } => {
                    if let Some(id) = message_id.as_deref()
                        && let Err(err) = port.delete(&target, id).await
                    {
                        tracing::warn!(error = %err, "failed to delete channel reply draft");
                    }
                    let _ = done.send(());
                    break;
                }
            }
        }
    });
    StreamingDraftUpdater {
        updates: tx,
        handle,
    }
}

async fn upsert_streaming_draft<P>(
    port: &P,
    target: &P::Target,
    message_id: &mut Option<String>,
    text: &str,
) -> anyhow::Result<()>
where
    P: ChannelReplyPort,
{
    if let Some(id) = message_id.as_deref() {
        port.update_text(target, id, text).await?;
        return Ok(());
    }
    let posted = port.post_text(target, text).await?;
    let Some(id) = posted.id else {
        anyhow::bail!("channel reply draft message response omitted id");
    };
    *message_id = Some(id);
    Ok(())
}

async fn finalize_streaming_draft<P>(
    port: &P,
    target: &P::Target,
    message_id: &mut Option<String>,
    text: &str,
) -> anyhow::Result<()>
where
    P: ChannelReplyPort,
{
    if let Some(id) = message_id.as_deref() {
        if let Err(err) = port.update_markdown(target, id, text).await {
            tracing::warn!(
                error = %err,
                "failed to update channel reply draft as final reply; posting final reply instead"
            );
            let posted = port.post_markdown(target, text).await?;
            if let Some(new_id) = posted.id {
                *message_id = Some(new_id);
            }
            return Ok(());
        }
        return Ok(());
    }
    let posted = port.post_markdown(target, text).await?;
    if let Some(id) = posted.id {
        *message_id = Some(id);
    }
    Ok(())
}

fn spawn_checkpoint_poster<P>(port: P, target: P::Target) -> CheckpointPosterWorker
where
    P: ChannelReplyPort,
{
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let handle = tokio::spawn(async move {
        while let Some(mut checkpoint) = rx.recv().await {
            while let Ok(next) = rx.try_recv() {
                checkpoint = next;
            }
            if let Err(err) = port.post_text(&target, &checkpoint).await {
                tracing::warn!(error = %err, "failed to post channel reply checkpoint");
            }
        }
    });
    CheckpointPosterWorker {
        checkpoints: tx,
        handle,
    }
}

fn streaming_draft_text(text: &str, policy: &ChannelOutputPolicy) -> String {
    let preview = streaming_draft_preview(text, policy);
    format!("{preview}\n\n{}", policy.draft_marker)
}

fn streaming_draft_preview(text: &str, policy: &ChannelOutputPolicy) -> String {
    if text.len() <= policy.draft_preview_max_bytes {
        return text.to_string();
    }

    let suffix_budget = policy
        .draft_preview_max_bytes
        .saturating_sub(policy.draft_truncated_prefix.len());
    let mut start = text.len().saturating_sub(suffix_budget);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    format!("{}{}", policy.draft_truncated_prefix, &text[start..])
}

fn render_reply_checkpoint(text: &str, policy: &ChannelOutputPolicy) -> String {
    format!(
        "{}{}",
        policy.checkpoint_prefix,
        truncate_chars(text.trim(), policy.checkpoint_preview_chars)
    )
}

async fn await_unit_worker(handle: JoinHandle<()>, mode: &'static str) {
    if let Err(err) = handle.await {
        tracing::warn!(error = %err, mode, "channel output worker failed");
    }
}

async fn await_option_worker<T>(handle: JoinHandle<Option<T>>, mode: &'static str) -> Option<T>
where
    T: Send + Debug + 'static,
{
    match handle.await {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, mode, "channel output worker failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::router::{RouterChannelEvent, RouterChannelEventKind};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Delivery {
        PostText {
            target: String,
            text: String,
            id: u64,
        },
        PostMarkdown {
            target: String,
            text: String,
            id: u64,
        },
        UpdateText {
            target: String,
            id: String,
            text: String,
        },
        UpdateMarkdown {
            target: String,
            id: String,
            text: String,
        },
        Delete {
            target: String,
            id: String,
        },
    }

    #[derive(Debug, Clone, Default)]
    struct RecordingPort {
        deliveries: Arc<Mutex<Vec<Delivery>>>,
        next_id: Arc<AtomicU64>,
        post_text_delay: Duration,
        markdown_without_id: bool,
    }

    impl RecordingPort {
        async fn deliveries(&self) -> Vec<Delivery> {
            self.deliveries.lock().await.clone()
        }
    }

    #[async_trait]
    impl ChannelReplyPort for RecordingPort {
        type Target = String;

        async fn post_text(
            &self,
            target: &Self::Target,
            text: &str,
        ) -> anyhow::Result<PostedMessage> {
            if !self.post_text_delay.is_zero() {
                tokio::time::sleep(self.post_text_delay).await;
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            self.deliveries.lock().await.push(Delivery::PostText {
                target: target.clone(),
                text: text.to_string(),
                id,
            });
            Ok(PostedMessage::with_id(id.to_string()))
        }

        async fn post_markdown(
            &self,
            target: &Self::Target,
            text: &str,
        ) -> anyhow::Result<PostedMessage> {
            if self.markdown_without_id {
                self.deliveries.lock().await.push(Delivery::PostMarkdown {
                    target: target.clone(),
                    text: text.to_string(),
                    id: 0,
                });
                return Ok(PostedMessage::without_id());
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            self.deliveries.lock().await.push(Delivery::PostMarkdown {
                target: target.clone(),
                text: text.to_string(),
                id,
            });
            Ok(PostedMessage::with_id(id.to_string()))
        }

        async fn update_text(
            &self,
            target: &Self::Target,
            id: &str,
            text: &str,
        ) -> anyhow::Result<()> {
            self.deliveries.lock().await.push(Delivery::UpdateText {
                target: target.clone(),
                id: id.to_string(),
                text: text.to_string(),
            });
            Ok(())
        }

        async fn update_markdown(
            &self,
            target: &Self::Target,
            id: &str,
            text: &str,
        ) -> anyhow::Result<()> {
            self.deliveries.lock().await.push(Delivery::UpdateMarkdown {
                target: target.clone(),
                id: id.to_string(),
                text: text.to_string(),
            });
            Ok(())
        }

        async fn delete(&self, target: &Self::Target, id: &str) -> anyhow::Result<()> {
            self.deliveries.lock().await.push(Delivery::Delete {
                target: target.clone(),
                id: id.to_string(),
            });
            Ok(())
        }
    }

    fn event(title: &str) -> RouterChannelEvent {
        RouterChannelEvent {
            kind: RouterChannelEventKind::ToolCall,
            executor: "kimi".to_string(),
            title: title.to_string(),
            text: format!("{title} text"),
        }
    }

    fn progress_event(text: &str) -> RouterChannelEvent {
        RouterChannelEvent {
            kind: RouterChannelEventKind::AgentProgress,
            executor: "pi".to_string(),
            title: "Progress".to_string(),
            text: text.to_string(),
        }
    }

    #[tokio::test]
    async fn compact_activity_accumulates_and_flushes_before_final_reply() {
        let port = RecordingPort::default();
        let mut sink = ChannelOutputSink::new(
            port.clone(),
            "target".to_string(),
            ChannelOutputPolicy::streaming_draft(ChannelEventMode::Compact),
        );

        sink.send_channel_event(event("one"));
        sink.send_channel_event(event("two"));
        sink.send_final_reply("done".to_string()).await.unwrap();

        let deliveries = port.deliveries().await;
        assert!(matches!(deliveries[0], Delivery::PostText { .. }));
        assert!(deliveries.iter().any(
            |delivery| matches!(delivery, Delivery::PostMarkdown { text, .. } if text == "done")
                || matches!(delivery, Delivery::UpdateMarkdown { text, .. } if text == "done")
        ));
        assert!(
            deliveries
                .iter()
                .any(|delivery| matches!(delivery, Delivery::Delete { .. }))
        );
    }

    #[tokio::test]
    async fn discarded_streaming_draft_deletes_live_compact_activity() {
        let port = RecordingPort::default();
        let mut sink = ChannelOutputSink::new(
            port.clone(),
            "target".to_string(),
            ChannelOutputPolicy::streaming_draft(ChannelEventMode::Compact),
        );

        sink.send_channel_event(progress_event("thinking"));
        sink.discard_reply_stream().await;

        let deliveries = port.deliveries().await;
        assert!(deliveries.iter().any(
            |delivery| matches!(delivery, Delivery::PostText { text, .. } if text == "[pi] Activity\nProgress:\n- thinking")
        ));
        assert!(
            deliveries
                .iter()
                .any(|delivery| matches!(delivery, Delivery::Delete { .. }))
        );
    }

    #[tokio::test]
    async fn verbose_activity_posts_each_event() {
        let port = RecordingPort::default();
        let mut sink = ChannelOutputSink::new(
            port.clone(),
            "target".to_string(),
            ChannelOutputPolicy::checkpoint_messages(ChannelEventMode::Verbose),
        );

        sink.send_channel_event(event("one"));
        sink.send_channel_event(event("two"));
        sink.send_final_reply("done".to_string()).await.unwrap();

        let deliveries = port.deliveries().await;
        let text_posts = deliveries
            .iter()
            .filter(|delivery| matches!(delivery, Delivery::PostText { .. }))
            .count();
        assert_eq!(text_posts, 2);
        assert!(deliveries.iter().any(
            |delivery| matches!(delivery, Delivery::PostMarkdown { text, .. } if text == "done")
        ));
    }

    #[tokio::test]
    async fn streaming_draft_updates_final_reply_and_deletes_compact_activity() {
        let port = RecordingPort::default();
        let mut policy = ChannelOutputPolicy::streaming_draft(ChannelEventMode::Compact);
        policy.draft_initial_min_len = 4;
        let mut sink = ChannelOutputSink::new(port.clone(), "target".to_string(), policy);

        sink.send_channel_event(event("activity"));
        sink.send_reply_chunk("prev".to_string());
        tokio::task::yield_now().await;
        sink.send_final_reply("final".to_string()).await.unwrap();

        let deliveries = port.deliveries().await;
        assert!(deliveries.iter().any(
            |delivery| matches!(delivery, Delivery::UpdateMarkdown { text, .. } if text == "final")
        ));
        assert!(
            deliveries
                .iter()
                .any(|delivery| matches!(delivery, Delivery::Delete { .. }))
        );
    }

    #[tokio::test]
    async fn streaming_draft_final_post_without_id_is_controlled_by_delivery_port() {
        let port = RecordingPort {
            markdown_without_id: true,
            ..Default::default()
        };
        let mut sink = ChannelOutputSink::new(
            port.clone(),
            "target".to_string(),
            ChannelOutputPolicy::streaming_draft(ChannelEventMode::Off),
        );

        sink.send_final_reply("final".to_string()).await.unwrap();

        assert!(matches!(
            port.deliveries().await.as_slice(),
            [Delivery::PostMarkdown { text, .. }] if text == "final"
        ));
    }

    #[tokio::test]
    async fn checkpoint_messages_suppress_short_reply_and_send_final_reply() {
        let port = RecordingPort::default();
        let mut policy = ChannelOutputPolicy::checkpoint_messages(ChannelEventMode::Off);
        policy.checkpoint_min_growth = 100;
        let mut sink = ChannelOutputSink::new(port.clone(), "target".to_string(), policy);

        sink.send_reply_chunk("short".to_string());
        sink.send_final_reply("final".to_string()).await.unwrap();

        let deliveries = port.deliveries().await;
        assert_eq!(deliveries.len(), 1);
        assert!(matches!(
            &deliveries[0],
            Delivery::PostMarkdown { text, .. } if text == "final"
        ));
    }

    #[tokio::test]
    async fn checkpoint_messages_throttle_and_preserve_reply_breaks() {
        let port = RecordingPort::default();
        let mut policy = ChannelOutputPolicy::checkpoint_messages(ChannelEventMode::Off);
        policy.checkpoint_min_growth = 12;
        policy.checkpoint_preview_chars = 100;
        let mut sink = ChannelOutputSink::new(port.clone(), "target".to_string(), policy);

        sink.send_reply_chunk("first".to_string());
        sink.send_reply_break();
        sink.send_reply_chunk("second".to_string());
        sink.send_reply_chunk("third".to_string());
        sink.send_final_reply("final".to_string()).await.unwrap();

        let deliveries = port.deliveries().await;
        assert!(
            deliveries
                .iter()
                .any(|delivery| matches!(delivery, Delivery::PostText { text, .. } if text.contains("first\n\nsecond")))
        );
        let checkpoint_posts = deliveries
            .iter()
            .filter(|delivery| matches!(delivery, Delivery::PostText { .. }))
            .count();
        assert_eq!(checkpoint_posts, 1);
    }

    #[tokio::test]
    async fn discarded_streaming_draft_deletes_pending_draft() {
        let port = RecordingPort::default();
        let mut policy = ChannelOutputPolicy::streaming_draft(ChannelEventMode::Off);
        policy.draft_initial_min_len = 4;
        let mut sink = ChannelOutputSink::new(port.clone(), "target".to_string(), policy);

        sink.send_reply_chunk("draft".to_string());
        tokio::time::sleep(Duration::from_millis(10)).await;
        sink.discard_reply_stream().await;

        let deliveries = port.deliveries().await;
        assert!(
            deliveries
                .iter()
                .any(|delivery| matches!(delivery, Delivery::Delete { .. }))
        );
    }

    #[tokio::test]
    async fn discarded_checkpoints_stop_pending_worker() {
        let port = RecordingPort {
            post_text_delay: Duration::from_secs(60),
            ..Default::default()
        };
        let mut policy = ChannelOutputPolicy::checkpoint_messages(ChannelEventMode::Off);
        policy.checkpoint_min_growth = 4;
        let mut sink = ChannelOutputSink::new(port.clone(), "target".to_string(), policy);

        sink.send_reply_chunk("checkpoint".to_string());
        tokio::task::yield_now().await;
        sink.discard_reply_stream().await;
        sink.send_final_reply("final".to_string()).await.unwrap();

        let deliveries = port.deliveries().await;
        assert!(
            deliveries
                .iter()
                .all(|delivery| !matches!(delivery, Delivery::PostText { text, .. } if text.contains("checkpoint")))
        );
        assert!(deliveries.iter().any(
            |delivery| matches!(delivery, Delivery::PostMarkdown { text, .. } if text == "final")
        ));
    }
}
