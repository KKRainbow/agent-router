use super::*;
use crate::channel::context::{
    ChannelContextResolveRequest, ChannelContextResolveResult, ChannelContextResolver,
};

pub(super) struct SlackContextResolver<'a> {
    channel: &'a SlackSocketModeChannel,
    event: &'a SlackMessageEvent,
    bot_user_id: &'a str,
    cache_token: Option<&'a SlackContextCacheToken>,
}

impl<'a> SlackContextResolver<'a> {
    pub(super) fn new(
        channel: &'a SlackSocketModeChannel,
        event: &'a SlackMessageEvent,
        bot_user_id: &'a str,
        cache_token: Option<&'a SlackContextCacheToken>,
    ) -> Self {
        Self {
            channel,
            event,
            bot_user_id,
            cache_token,
        }
    }
}

#[async_trait::async_trait]
impl ChannelContextResolver for SlackContextResolver<'_> {
    async fn resolve(
        &self,
        request: ChannelContextResolveRequest,
    ) -> anyhow::Result<ChannelContextResolveResult> {
        let build = self
            .channel
            .current_thread_context_request(
                self.event,
                &request.session_key,
                self.bot_user_id,
                &request.existing_artifacts,
                self.cache_token,
            )
            .await;
        Ok(ChannelContextResolveResult {
            sync_request: build.request,
            succeeded_cache_keys: build.succeeded_file_sync_keys,
            failed_cache_keys: build.failed_file_sync_keys,
        })
    }
}

impl SlackSocketModeChannel {
    pub(super) async fn current_thread_context_request(
        &self,
        event: &SlackMessageEvent,
        session_key: &str,
        bot_user_id: &str,
        existing_context: &[ContextArtifactRecord],
        context_cache_token: Option<&SlackContextCacheToken>,
    ) -> SlackContextSyncBuild {
        let thread_ts = event.thread_root_ts();
        let mut unresolved = Vec::new();
        let mut remove_artifacts = Vec::new();
        let mut current_thread_fresh = false;
        let mut prune_derived_artifacts = false;
        let messages = if self.cfg.context_sync.current_thread {
            tracing::info!(
                channel = %event.channel,
                thread_ts = %thread_ts,
                "fetching Slack current thread context"
            );
            match self
                .fetch_thread_messages_cached(&event.channel, &thread_ts, context_cache_token)
                .await
            {
                Ok(fetch) => {
                    prune_derived_artifacts = true;
                    let fresh = fetch.stale_reason.is_none();
                    if let Some(reason) = fetch.stale_reason {
                        tracing::info!(
                            channel = %event.channel,
                            thread_ts = %thread_ts,
                            reason = %reason,
                            "using cached Slack current thread context"
                        );
                        unresolved.push(ContextSyncIssueInput {
                            kind: "current_thread_cache".to_string(),
                            reference: format!("{}:{}", event.channel, thread_ts),
                            reason,
                        });
                    } else {
                        current_thread_fresh = true;
                    }
                    let messages = filter_context_messages(fetch.messages, bot_user_id);
                    tracing::info!(
                        channel = %event.channel,
                        thread_ts = %thread_ts,
                        message_count = messages.len(),
                        fresh,
                        "fetched Slack current thread context"
                    );
                    messages
                }
                Err(err) => {
                    if slack_error_is_access_denied(&err) {
                        prune_derived_artifacts = true;
                        remove_artifacts
                            .push(slack_current_thread_removal(&event.channel, &thread_ts));
                        remove_artifacts.push(slack_all_linked_threads_removal());
                    }
                    tracing::info!(
                        channel = %event.channel,
                        thread_ts = %thread_ts,
                        reason = %context_error_reason(&err),
                        "failed to fetch Slack current thread context"
                    );
                    unresolved.push(ContextSyncIssueInput {
                        kind: "current_thread".to_string(),
                        reference: format!("{}:{}", event.channel, thread_ts),
                        reason: err.to_string(),
                    });
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            message_count = messages.len(),
            "synced Slack current thread context"
        );

        let mut linked_threads = Vec::new();
        let mut retained_linked_thread_artifact_ids = BTreeSet::new();
        if self.cfg.context_sync.linked_threads && self.cfg.context_sync.linked_thread_depth > 0 {
            let link_messages = context_link_messages(event, &messages);
            let mut linked_thread_fetches = 0usize;
            for link in collect_slack_thread_links(&link_messages, &event.channel, &thread_ts) {
                let linked_artifact_id =
                    slack_linked_thread_artifact_id(&link.channel, &link.thread_ts);
                if !self.should_accept_linked_thread_channel(&event.channel, &link.channel) {
                    tracing::info!(
                        linked_channel = %link.channel,
                        linked_thread_ts = %link.thread_ts,
                        source_message_ts = %link.source_message_ts,
                        "skipping Slack linked thread outside allowed channels"
                    );
                    unresolved.push(ContextSyncIssueInput {
                        kind: "linked_thread".to_string(),
                        reference: slack_thread_reference(&link.channel, &link.thread_ts),
                        reason: "linked thread channel is not allowed".to_string(),
                    });
                    continue;
                }
                if linked_thread_fetches >= self.cfg.context_sync.max_linked_threads_per_turn {
                    tracing::info!(
                        linked_channel = %link.channel,
                        linked_thread_ts = %link.thread_ts,
                        source_message_ts = %link.source_message_ts,
                        "skipping Slack linked thread due fetch limit"
                    );
                    unresolved.push(ContextSyncIssueInput {
                        kind: "linked_thread".to_string(),
                        reference: slack_thread_reference(&link.channel, &link.thread_ts),
                        reason: "linked thread fetch limit reached".to_string(),
                    });
                    retained_linked_thread_artifact_ids.insert(linked_artifact_id);
                    continue;
                }
                linked_thread_fetches += 1;
                tracing::info!(
                    linked_channel = %link.channel,
                    linked_thread_ts = %link.thread_ts,
                    source_message_ts = %link.source_message_ts,
                    "fetching Slack linked thread context"
                );
                match self
                    .fetch_thread_messages_cached(
                        &link.channel,
                        &link.thread_ts,
                        context_cache_token,
                    )
                    .await
                {
                    Ok(fetch) => {
                        let fresh = fetch.stale_reason.is_none();
                        if let Some(reason) = fetch.stale_reason {
                            tracing::info!(
                                linked_channel = %link.channel,
                                linked_thread_ts = %link.thread_ts,
                                reason = %reason,
                                "using cached Slack linked thread context"
                            );
                            unresolved.push(ContextSyncIssueInput {
                                kind: "linked_thread_cache".to_string(),
                                reference: slack_thread_reference(&link.channel, &link.thread_ts),
                                reason,
                            });
                        }
                        linked_threads.push(LinkedSlackThread {
                            link,
                            fresh,
                            messages: filter_context_messages(fetch.messages, bot_user_id),
                        });
                        if let Some(linked_thread) = linked_threads.last() {
                            tracing::info!(
                                linked_channel = %linked_thread.link.channel,
                                linked_thread_ts = %linked_thread.link.thread_ts,
                                message_count = linked_thread.messages.len(),
                                fresh = linked_thread.fresh,
                                "fetched Slack linked thread context"
                            );
                        }
                    }
                    Err(err) => {
                        if slack_error_is_access_denied(&err) {
                            remove_artifacts
                                .push(slack_linked_thread_removal(&link.channel, &link.thread_ts));
                        } else {
                            retained_linked_thread_artifact_ids.insert(linked_artifact_id);
                        }
                        tracing::info!(
                            linked_channel = %link.channel,
                            linked_thread_ts = %link.thread_ts,
                            reason = %context_error_reason(&err),
                            "failed to fetch Slack linked thread context"
                        );
                        unresolved.push(ContextSyncIssueInput {
                            kind: "linked_thread".to_string(),
                            reference: slack_thread_reference(&link.channel, &link.thread_ts),
                            reason: err.to_string(),
                        });
                    }
                }
            }
        }
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            linked_thread_count = linked_threads.len(),
            "synced Slack linked thread context"
        );

        let mut downloaded_files = Vec::new();
        let mut succeeded_file_sync_keys = Vec::new();
        let mut failed_file_sync_keys = Vec::new();
        let mut skipped_cached_files = 0usize;
        let mut failed_files = 0usize;
        let file_refs = if self.cfg.context_sync.files {
            collect_context_file_refs(event, &messages, &linked_threads)
        } else {
            Vec::new()
        };
        let file_selection = select_file_sync_attempts(
            session_key,
            file_refs,
            &self.file_sync_state(session_key, existing_context).await,
            self.cfg.context_sync.max_files_per_turn,
        );
        let mut retained_file_artifact_ids = BTreeSet::new();
        skipped_cached_files += file_selection.skipped_synced_files.len();
        for file in &file_selection.skipped_synced_files {
            retained_file_artifact_ids.insert(slack_file_artifact_id(&file.id));
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                "skipping already synced Slack file context"
            );
        }
        let omitted_file_count = file_selection.omitted_files.len();
        for file in &file_selection.omitted_files {
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                max_files_per_turn = self.cfg.context_sync.max_files_per_turn,
                "skipping Slack file context due sync limit"
            );
            unresolved.push(ContextSyncIssueInput {
                kind: "file".to_string(),
                reference: file.id.clone(),
                reason: "file sync limit reached".to_string(),
            });
        }
        for (file_cache_key, file) in file_selection.attempts {
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                size_bytes = file.size_bytes,
                "downloading Slack file context"
            );
            match self.download_slack_file(file.clone()).await {
                Ok(downloaded) => {
                    tracing::info!(
                        file_id = %downloaded.file.id,
                        file_name = %downloaded.file.name,
                        downloaded_bytes = downloaded.bytes.len(),
                        extracted_text = downloaded.extracted_text.is_some(),
                        "downloaded Slack file context"
                    );
                    retained_file_artifact_ids.insert(slack_file_artifact_id(&downloaded.file.id));
                    succeeded_file_sync_keys.push(file_cache_key);
                    downloaded_files.push(downloaded);
                }
                Err(err) => {
                    failed_files += 1;
                    let reason = context_error_reason(&err);
                    tracing::info!(
                        file_id = %file.id,
                        file_name = %file.name,
                        reason = %reason,
                        "failed to download Slack file context"
                    );
                    failed_file_sync_keys.push(file_cache_key);
                    unresolved.push(ContextSyncIssueInput {
                        kind: "file".to_string(),
                        reference: file.id,
                        reason,
                    });
                }
            }
        }
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            downloaded_file_count = downloaded_files.len(),
            skipped_cached_file_count = skipped_cached_files,
            omitted_file_count,
            failed_file_count = failed_files,
            "synced Slack file context"
        );

        SlackContextSyncBuild {
            request: build_slack_context_request(
                session_key,
                CurrentSlackThreadContext {
                    channel: &event.channel,
                    thread_ts: &thread_ts,
                    fresh: current_thread_fresh,
                    messages: &messages,
                },
                &linked_threads,
                &downloaded_files,
                SlackDerivedArtifactRetention::new(
                    retained_linked_thread_artifact_ids,
                    retained_file_artifact_ids,
                    prune_derived_artifacts,
                ),
                remove_artifacts,
                unresolved,
            ),
            succeeded_file_sync_keys,
            failed_file_sync_keys,
        }
    }

    async fn fetch_thread_messages_cached(
        &self,
        channel: &str,
        thread_ts: &str,
        context_cache_token: Option<&SlackContextCacheToken>,
    ) -> anyhow::Result<SlackThreadFetch> {
        self.fetch_thread_messages_cached_with(
            channel,
            thread_ts,
            self.fetch_thread_messages(channel, thread_ts),
            context_cache_token,
        )
        .await
    }

    pub(super) async fn fetch_thread_messages_cached_with(
        &self,
        channel: &str,
        thread_ts: &str,
        fetch: impl std::future::Future<Output = anyhow::Result<Vec<SlackThreadMessage>>>,
        context_cache_token: Option<&SlackContextCacheToken>,
    ) -> anyhow::Result<SlackThreadFetch> {
        let key = slack_thread_cache_key(channel, thread_ts);
        match fetch.await {
            Ok(messages) => {
                let mut cache = self.context_cache.lock().await;
                upsert_thread_cache_if_current(
                    &mut cache,
                    key,
                    messages.clone(),
                    context_cache_token,
                );
                Ok(SlackThreadFetch {
                    messages,
                    stale_reason: None,
                })
            }
            Err(err) if slack_error_allows_cached_context(&err) => {
                let fetch = {
                    let cache = self.context_cache.lock().await;
                    cached_thread_fetch_after_error(&err, || {
                        cache
                            .threads
                            .get(&key)
                            .and_then(|entry| entry.messages.clone())
                            .unwrap_or_default()
                    })
                };
                if let Some(fetch) = fetch {
                    Ok(fetch)
                } else {
                    Err(err)
                }
            }
            Err(err) if slack_error_clears_thread_cache(&err) => {
                let mut cache = self.context_cache.lock().await;
                remove_thread_cache_if_current(&mut cache, &key, context_cache_token);
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    async fn fetch_thread_messages(
        &self,
        channel: &str,
        thread_ts: &str,
    ) -> anyhow::Result<Vec<SlackThreadMessage>> {
        let mut cursor = None;
        let mut messages = Vec::new();
        loop {
            let mut query = vec![
                ("channel", channel.to_string()),
                ("ts", thread_ts.to_string()),
                ("limit", "15".to_string()),
            ];
            if let Some(cursor) = cursor.clone() {
                query.push(("cursor", cursor));
            }
            let response = self
                .http
                .get("https://slack.com/api/conversations.replies")
                .bearer_auth(&self.cfg.bot_token)
                .query(&query)
                .send()
                .await?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                return Err(SlackApiError::new("conversations.replies", "rate_limited").into());
            }
            let response = response.json::<SlackThreadResponse>().await?;
            if !response.ok {
                return Err(SlackApiError::new(
                    "conversations.replies",
                    response
                        .error
                        .unwrap_or_else(|| "unknown_error".to_string()),
                )
                .into());
            }
            for raw in response.messages.unwrap_or_default() {
                if let Some(message) = parse_slack_thread_message(raw) {
                    messages.push(message);
                }
            }
            cursor = response
                .response_metadata
                .and_then(|metadata| metadata.next_cursor)
                .filter(|cursor| !cursor.trim().is_empty());
            if cursor.is_none() {
                break;
            }
        }
        self.resolve_thread_message_authors(&mut messages).await;
        Ok(messages)
    }

    pub(super) async fn resolve_thread_message_authors(&self, messages: &mut [SlackThreadMessage]) {
        let inline_names = slack_inline_user_author_names(messages);
        let user_ids = messages
            .iter()
            .filter(|message| message.author_name.is_none())
            .filter_map(|message| message.user.as_deref())
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        if user_ids.is_empty() {
            if !inline_names.is_empty() {
                let mut cache = self.context_cache.lock().await;
                cache.user_names.extend(inline_names);
            }
            return;
        }

        let mut resolved = inline_names.clone();
        let mut missing = Vec::new();
        {
            let mut cache = self.context_cache.lock().await;
            cache.user_names.extend(inline_names);
            for user_id in user_ids {
                if let Some(name) = cache.user_names.get(&user_id) {
                    resolved.insert(user_id, name.clone());
                } else {
                    missing.push(user_id);
                }
            }
        }

        let mut fetched = BTreeMap::new();
        for user_id in missing {
            match self.fetch_user_display_name(&user_id).await {
                Ok(Some(name)) => {
                    fetched.insert(user_id.clone(), name.clone());
                    resolved.insert(user_id, name);
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::info!(
                        user_id = %user_id,
                        reason = %context_error_reason(&err),
                        "failed to resolve Slack user display name"
                    );
                }
            }
        }
        if !fetched.is_empty() {
            let mut cache = self.context_cache.lock().await;
            cache.user_names.extend(fetched);
        }

        apply_slack_user_author_names(messages, &resolved);
    }

    async fn fetch_user_display_name(&self, user_id: &str) -> anyhow::Result<Option<String>> {
        let response = self
            .http
            .get("https://slack.com/api/users.info")
            .bearer_auth(&self.cfg.bot_token)
            .query(&[("user", user_id)])
            .send()
            .await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(SlackApiError::new("users.info", "rate_limited").into());
        }
        let response = response.json::<SlackUserInfoResponse>().await?;
        if !response.ok {
            return Err(SlackApiError::new(
                "users.info",
                response
                    .error
                    .unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
        }
        Ok(response.user.as_ref().and_then(slack_user_display_name))
    }

    async fn download_slack_file(&self, file: SlackFileRef) -> anyhow::Result<DownloadedSlackFile> {
        let file = if slack_file_ref_needs_info(&file) {
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                "fetching Slack file metadata"
            );
            let file = self.fetch_slack_file_info(file).await?;
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                size_bytes = file.size_bytes,
                has_private_download_url = file.url_private_download.is_some(),
                "fetched Slack file metadata"
            );
            file
        } else {
            file
        };
        if file.size_bytes > self.cfg.context_sync.max_file_bytes {
            anyhow::bail!(
                "file exceeds max_file_bytes ({} > {})",
                file.size_bytes,
                self.cfg.context_sync.max_file_bytes
            );
        }
        let url = file
            .url_private_download
            .as_deref()
            .or(file.url_private.as_deref())
            .ok_or_else(|| anyhow::anyhow!("file has no private download URL"))
            .and_then(parse_slack_file_url)?;
        let mut response = self
            .http
            .get(url)
            .bearer_auth(&self.cfg.bot_token)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Slack file download request failed"))?;
        if !response.status().is_success() {
            anyhow::bail!("Slack file download failed: {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.cfg.context_sync.max_file_bytes as u64)
        {
            anyhow::bail!(
                "file exceeds max_file_bytes (content-length > {})",
                self.cfg.context_sync.max_file_bytes
            );
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("Slack file download read failed"))?
        {
            if bytes.len() + chunk.len() > self.cfg.context_sync.max_file_bytes {
                anyhow::bail!(
                    "file exceeds max_file_bytes (downloaded > {})",
                    self.cfg.context_sync.max_file_bytes
                );
            }
            bytes.extend_from_slice(&chunk);
        }
        let extracted_text = if is_text_slack_file(&file) {
            std::str::from_utf8(&bytes).ok().map(ToOwned::to_owned)
        } else {
            None
        };
        Ok(DownloadedSlackFile {
            file,
            bytes,
            extracted_text,
        })
    }

    async fn fetch_slack_file_info(&self, file: SlackFileRef) -> anyhow::Result<SlackFileRef> {
        let response = self
            .http
            .get("https://slack.com/api/files.info")
            .bearer_auth(&self.cfg.bot_token)
            .query(&[("file", file.id.as_str())])
            .send()
            .await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(SlackApiError::new("files.info", "rate_limited").into());
        }
        let response = response.json::<SlackFileInfoResponse>().await?;
        if !response.ok {
            return Err(SlackApiError::new(
                "files.info",
                response
                    .error
                    .unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
        }
        let Some(file_info) = response.file else {
            anyhow::bail!("Slack files.info response omitted file");
        };
        let Some(info) = parse_slack_file_ref(&file_info) else {
            anyhow::bail!("Slack files.info response file is invalid");
        };
        Ok(merge_slack_file_info(file, info))
    }

    async fn file_sync_state(
        &self,
        session_key: &str,
        existing_context: &[ContextArtifactRecord],
    ) -> SlackFileSyncState {
        let cache = self.context_cache.lock().await;
        file_sync_state_from_context_artifacts(session_key, existing_context, &cache)
    }

    pub(super) async fn remember_context_cache_sequence(
        &self,
        session_key: &str,
        cache_sequence: u64,
    ) {
        let mut cache = self.context_cache.lock().await;
        let latest = cache
            .latest_context_cache_sequences
            .entry(session_key.to_string())
            .or_default();
        *latest = (*latest).max(cache_sequence);
    }

    pub(super) async fn mark_file_sync_succeeded(
        &self,
        cache_key: String,
        turn: Option<&SlackContextCacheToken>,
    ) {
        let mut cache = self.context_cache.lock().await;
        if !context_cache_token_is_current(&cache, turn) {
            return;
        }
        cache.failed_files.remove(&cache_key);
        cache.synced_files.insert(cache_key);
    }

    pub(super) async fn mark_file_sync_failed(
        &self,
        cache_key: String,
        turn: Option<&SlackContextCacheToken>,
    ) {
        let mut cache = self.context_cache.lock().await;
        if !context_cache_token_is_current(&cache, turn) {
            return;
        }
        if !cache.synced_files.contains(&cache_key) {
            cache.failed_files.insert(cache_key);
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct SlackThreadResponse {
    pub(super) ok: bool,
    pub(super) messages: Option<Vec<Value>>,
    pub(super) error: Option<String>,
    pub(super) response_metadata: Option<SlackResponseMetadata>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SlackFileInfoResponse {
    pub(super) ok: bool,
    pub(super) file: Option<Value>,
    pub(super) error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SlackUserInfoResponse {
    pub(super) ok: bool,
    pub(super) user: Option<Value>,
    pub(super) error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SlackResponseMetadata {
    pub(super) next_cursor: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct SlackContextCache {
    pub(super) latest_context_cache_sequences: BTreeMap<String, u64>,
    pub(super) threads: BTreeMap<String, CachedSlackThreadMessages>,
    pub(super) user_names: BTreeMap<String, String>,
    pub(super) synced_files: BTreeSet<String>,
    pub(super) failed_files: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub(super) struct CachedSlackThreadMessages {
    pub(super) messages: Option<Vec<SlackThreadMessage>>,
    pub(super) cache_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct SlackContextCacheToken {
    pub(super) session_key: String,
    pub(super) cache_sequence: u64,
}

pub(super) fn upsert_thread_cache_if_current(
    cache: &mut SlackContextCache,
    key: String,
    messages: Vec<SlackThreadMessage>,
    turn: Option<&SlackContextCacheToken>,
) {
    let cache_sequence = turn.map(|turn| turn.cache_sequence);
    if !context_cache_token_is_current(cache, turn)
        || thread_cache_entry_is_newer(cache.threads.get(&key), cache_sequence)
    {
        return;
    }
    cache.threads.insert(
        key,
        CachedSlackThreadMessages {
            messages: Some(messages),
            cache_sequence,
        },
    );
}

pub(super) fn remove_thread_cache_if_current(
    cache: &mut SlackContextCache,
    key: &str,
    turn: Option<&SlackContextCacheToken>,
) {
    let cache_sequence = turn.map(|turn| turn.cache_sequence);
    if !context_cache_token_is_current(cache, turn)
        || thread_cache_entry_is_newer(cache.threads.get(key), cache_sequence)
    {
        return;
    }
    cache.threads.insert(
        key.to_string(),
        CachedSlackThreadMessages {
            messages: None,
            cache_sequence,
        },
    );
}

pub(super) fn context_cache_token_is_current(
    cache: &SlackContextCache,
    turn: Option<&SlackContextCacheToken>,
) -> bool {
    turn.is_none_or(|turn| {
        cache
            .latest_context_cache_sequences
            .get(&turn.session_key)
            .is_none_or(|cache_sequence| *cache_sequence <= turn.cache_sequence)
    })
}

pub(super) fn thread_cache_entry_is_newer(
    entry: Option<&CachedSlackThreadMessages>,
    cache_sequence: Option<u64>,
) -> bool {
    match (entry.and_then(|entry| entry.cache_sequence), cache_sequence) {
        (Some(existing), Some(incoming)) => existing > incoming,
        _ => false,
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct SlackFileSyncState {
    pub(super) synced_files: BTreeSet<String>,
    pub(super) failed_files: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub(super) struct SlackContextSyncBuild {
    pub(super) request: ContextSyncRequest,
    pub(super) succeeded_file_sync_keys: Vec<String>,
    pub(super) failed_file_sync_keys: Vec<String>,
}

pub(super) fn file_sync_state_from_context_artifacts(
    session_key: &str,
    records: &[ContextArtifactRecord],
    cache: &SlackContextCache,
) -> SlackFileSyncState {
    let mut state = SlackFileSyncState {
        synced_files: BTreeSet::new(),
        failed_files: cache.failed_files.clone(),
    };
    for record in records {
        if record.source != "slack" {
            continue;
        }
        if record.kind == "slack_file" {
            if let Some(file_id) = context_record_file_id(record) {
                let key = format!("{session_key}:{file_id}");
                state.failed_files.remove(&key);
                state.synced_files.insert(key);
            }
        } else if record.kind == "manifest" {
            for file_id in context_record_unresolved_file_ids(record) {
                let key = format!("{session_key}:{file_id}");
                if !state.synced_files.contains(&key) {
                    state.failed_files.insert(key);
                }
            }
        }
    }
    state
}

pub(super) fn context_record_file_id(record: &ContextArtifactRecord) -> Option<String> {
    record
        .metadata
        .get("file_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| record.id.strip_prefix("slack:file:").map(ToOwned::to_owned))
}

pub(super) fn context_record_unresolved_file_ids(record: &ContextArtifactRecord) -> Vec<String> {
    record
        .metadata
        .get("unresolved")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|issue| {
            (issue.get("kind").and_then(Value::as_str) == Some("file"))
                .then(|| issue.get("reference").and_then(Value::as_str))
                .flatten()
                .map(ToOwned::to_owned)
        })
        .collect()
}

#[derive(Debug, Clone)]
pub(super) struct SlackThreadMessage {
    pub(super) ts: String,
    pub(super) user: Option<String>,
    pub(super) author_name: Option<String>,
    pub(super) bot_id: Option<String>,
    pub(super) text: String,
    pub(super) files: Vec<SlackFileRef>,
}

#[derive(Debug, Clone)]
pub(super) struct SlackThreadLink {
    pub(super) channel: String,
    pub(super) thread_ts: String,
    pub(super) url: String,
    pub(super) source_message_ts: String,
}

#[derive(Debug, Clone)]
pub(super) struct LinkedSlackThread {
    pub(super) link: SlackThreadLink,
    pub(super) fresh: bool,
    pub(super) messages: Vec<SlackThreadMessage>,
}

#[derive(Debug, Clone)]
pub(super) struct SlackThreadFetch {
    pub(super) messages: Vec<SlackThreadMessage>,
    pub(super) stale_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct SlackFileRef {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) mimetype: Option<String>,
    pub(super) size_bytes: usize,
    pub(super) url_private: Option<String>,
    pub(super) url_private_download: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct DownloadedSlackFile {
    pub(super) file: SlackFileRef,
    pub(super) bytes: Vec<u8>,
    pub(super) extracted_text: Option<String>,
}

pub(super) fn parse_slack_thread_message(raw: Value) -> Option<SlackThreadMessage> {
    let ts = raw.get("ts").and_then(Value::as_str)?.to_string();
    let text = raw
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(SlackThreadMessage {
        ts,
        user: raw
            .get("user")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        author_name: slack_message_author_name(&raw),
        bot_id: raw
            .get("bot_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        text,
        files: parse_slack_file_refs(&raw),
    })
}

pub(super) fn slack_message_author_name(message: &Value) -> Option<String> {
    message
        .get("user_profile")
        .and_then(slack_profile_display_name)
        .or_else(|| {
            message
                .get("bot_profile")
                .and_then(slack_profile_display_name)
        })
        .or_else(|| {
            message
                .get("username")
                .and_then(Value::as_str)
                .and_then(clean_slack_author_name)
        })
}

pub(super) fn slack_user_display_name(user: &Value) -> Option<String> {
    user.get("profile")
        .and_then(slack_profile_display_name)
        .or_else(|| {
            user.get("real_name")
                .and_then(Value::as_str)
                .and_then(clean_slack_author_name)
        })
        .or_else(|| {
            user.get("name")
                .and_then(Value::as_str)
                .and_then(clean_slack_author_name)
        })
}

pub(super) fn slack_profile_display_name(profile: &Value) -> Option<String> {
    ["display_name", "real_name", "name"]
        .into_iter()
        .find_map(|field| {
            profile
                .get(field)
                .and_then(Value::as_str)
                .and_then(clean_slack_author_name)
        })
}

pub(super) fn clean_slack_author_name(raw: &str) -> Option<String> {
    let name = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    (!name.is_empty()).then_some(name)
}

pub(super) fn slack_inline_user_author_names(
    messages: &[SlackThreadMessage],
) -> BTreeMap<String, String> {
    messages
        .iter()
        .filter_map(|message| {
            Some((
                message.user.as_ref()?.clone(),
                message.author_name.as_ref()?.clone(),
            ))
        })
        .collect()
}

pub(super) fn apply_slack_user_author_names(
    messages: &mut [SlackThreadMessage],
    names: &BTreeMap<String, String>,
) {
    for message in messages {
        if message.author_name.is_none()
            && let Some(user_id) = &message.user
        {
            message.author_name = names.get(user_id).cloned();
        }
    }
}

pub(super) fn parse_slack_file_refs(message: &Value) -> Vec<SlackFileRef> {
    message
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_slack_file_ref)
        .collect()
}

pub(super) fn parse_slack_file_ref(file: &Value) -> Option<SlackFileRef> {
    let id = file.get("id").and_then(Value::as_str)?.to_string();
    let name = file
        .get("name")
        .or_else(|| file.get("title"))
        .and_then(Value::as_str)
        .unwrap_or(&id)
        .to_string();
    let size_bytes = file
        .get("size")
        .and_then(Value::as_u64)
        .and_then(|size| usize::try_from(size).ok())
        .unwrap_or(0);
    Some(SlackFileRef {
        id,
        name,
        mimetype: file
            .get("mimetype")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        size_bytes,
        url_private: file
            .get("url_private")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        url_private_download: file
            .get("url_private_download")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(super) fn merge_slack_file_info(original: SlackFileRef, info: SlackFileRef) -> SlackFileRef {
    let SlackFileRef {
        id,
        name,
        mimetype,
        size_bytes,
        url_private,
        url_private_download,
    } = original;
    let info_name_is_fallback = info.name == info.id;
    SlackFileRef {
        id: id.clone(),
        name: if info_name_is_fallback && name != id {
            name
        } else {
            info.name
        },
        mimetype: info.mimetype.or(mimetype),
        size_bytes: if info.size_bytes == 0 {
            size_bytes
        } else {
            info.size_bytes
        },
        url_private: info.url_private.or(url_private),
        url_private_download: info.url_private_download.or(url_private_download),
    }
}

pub(super) fn slack_file_ref_needs_info(file: &SlackFileRef) -> bool {
    file.url_private_download.is_none() && file.url_private.is_none()
}

pub(super) fn collect_context_file_refs(
    event: &SlackMessageEvent,
    current_messages: &[SlackThreadMessage],
    linked_threads: &[LinkedSlackThread],
) -> Vec<SlackFileRef> {
    let mut seen = BTreeSet::new();
    let mut files = Vec::new();
    for file in event
        .files
        .iter()
        .chain(
            current_messages
                .iter()
                .flat_map(|message| message.files.iter()),
        )
        .chain(
            linked_threads
                .iter()
                .flat_map(|thread| thread.messages.iter())
                .flat_map(|message| message.files.iter()),
        )
    {
        if seen.insert(file.id.clone()) {
            files.push(file.clone());
        }
    }
    for text in std::iter::once(event.text.as_str())
        .chain(current_messages.iter().map(|message| message.text.as_str()))
        .chain(
            linked_threads
                .iter()
                .flat_map(|thread| thread.messages.iter())
                .map(|message| message.text.as_str()),
        )
    {
        for file in extract_slack_file_link_refs(text) {
            if seen.insert(file.id.clone()) {
                files.push(file);
            }
        }
    }
    files
}

pub(super) fn context_link_messages(
    event: &SlackMessageEvent,
    current_messages: &[SlackThreadMessage],
) -> Vec<SlackThreadMessage> {
    let mut messages = current_messages.to_vec();
    if !messages.iter().any(|message| message.ts == event.ts) {
        messages.push(SlackThreadMessage {
            ts: event.ts.clone(),
            user: Some(event.user.clone()),
            author_name: None,
            bot_id: None,
            text: event.text.clone(),
            files: event.files.clone(),
        });
    }
    messages
}

pub(super) fn select_file_sync_attempts(
    session_key: &str,
    file_refs: Vec<SlackFileRef>,
    state: &SlackFileSyncState,
    max_files_per_turn: usize,
) -> SlackFileSyncSelection {
    let mut skipped_synced_files = Vec::new();
    let mut fresh_attempts = Vec::new();
    let mut retry_attempts = Vec::new();
    for file in file_refs {
        let file_cache_key = format!("{session_key}:{}", file.id);
        if state.synced_files.contains(&file_cache_key) {
            skipped_synced_files.push(file);
            continue;
        }
        if state.failed_files.contains(&file_cache_key) {
            retry_attempts.push((file_cache_key, file));
        } else {
            fresh_attempts.push((file_cache_key, file));
        }
    }
    let mut attempts = Vec::new();
    let mut omitted_files = Vec::new();
    for (file_cache_key, file) in fresh_attempts.into_iter().chain(retry_attempts) {
        if attempts.len() < max_files_per_turn {
            attempts.push((file_cache_key, file));
        } else {
            omitted_files.push(file);
        }
    }
    SlackFileSyncSelection {
        attempts,
        skipped_synced_files,
        omitted_files,
    }
}

pub(super) struct SlackFileSyncSelection {
    pub(super) attempts: Vec<(String, SlackFileRef)>,
    pub(super) skipped_synced_files: Vec<SlackFileRef>,
    pub(super) omitted_files: Vec<SlackFileRef>,
}

pub(super) fn collect_slack_thread_links(
    messages: &[SlackThreadMessage],
    current_channel: &str,
    current_thread_ts: &str,
) -> Vec<SlackThreadLink> {
    let mut seen = BTreeSet::new();
    let mut links = Vec::new();
    for message in messages {
        for url in extract_slack_urls(&message.text) {
            if let Some((channel, thread_ts)) = parse_slack_thread_permalink(&url) {
                if channel == current_channel && thread_ts == current_thread_ts {
                    continue;
                }
                let key = format!("{channel}:{thread_ts}");
                if !seen.insert(key) {
                    continue;
                }
                links.push(SlackThreadLink {
                    channel,
                    thread_ts,
                    url,
                    source_message_ts: message.ts.clone(),
                });
            }
        }
    }
    links
}

pub(super) fn extract_slack_urls(text: &str) -> Vec<String> {
    text.split(|ch: char| ch.is_whitespace() || matches!(ch, '<' | '>' | '|'))
        .filter(|part| part.contains("/archives/"))
        .map(|part| {
            part.trim_matches(|ch| matches!(ch, ',' | ')' | ']'))
                .to_string()
        })
        .filter(|part| part.starts_with("http://") || part.starts_with("https://"))
        .collect()
}

pub(super) fn parse_slack_thread_permalink(url: &str) -> Option<(String, String)> {
    let url = Url::parse(url).ok()?;
    if url.scheme() != "https" || !is_allowed_slack_thread_host(url.host_str()?) {
        return None;
    }
    let segments = url.path_segments()?.collect::<Vec<_>>();
    let archive_index = segments.iter().position(|segment| *segment == "archives")?;
    let channel = segments.get(archive_index + 1)?.to_string();
    let path_ts = segments.get(archive_index + 2)?;
    if channel.is_empty() {
        return None;
    }
    let thread_ts = url
        .query_pairs()
        .find_map(|(key, value)| {
            (key == "thread_ts")
                .then(|| parse_slack_timestamp(&value))
                .flatten()
        })
        .or_else(|| parse_slack_timestamp(path_ts))?;
    Some((channel, thread_ts))
}

pub(super) fn is_allowed_slack_thread_host(host: &str) -> bool {
    host == "slack.com" || host.ends_with(".slack.com")
}

pub(super) fn parse_slack_timestamp(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if let Some((seconds, micros)) = raw.split_once('.') {
        if !seconds.is_empty()
            && seconds.chars().all(|ch| ch.is_ascii_digit())
            && micros.len() == 6
            && micros.chars().all(|ch| ch.is_ascii_digit())
        {
            return Some(raw.to_string());
        }
        return None;
    }
    let timestamp = raw
        .strip_prefix('p')
        .unwrap_or(raw)
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if timestamp.len() <= 6 {
        return None;
    }
    let (seconds, micros) = timestamp.split_at(timestamp.len() - 6);
    Some(format!("{seconds}.{micros}"))
}

pub(super) fn extract_slack_file_link_refs(text: &str) -> Vec<SlackFileRef> {
    text.split(|ch: char| ch.is_whitespace() || matches!(ch, '<' | '>' | '|'))
        .filter(|part| part.contains("/files/"))
        .map(|part| part.trim_matches(|ch| matches!(ch, ',' | ')' | ']' | '.')))
        .filter(|part| part.starts_with("http://") || part.starts_with("https://"))
        .filter_map(parse_slack_file_permalink)
        .collect()
}

pub(super) fn parse_slack_file_permalink(raw: &str) -> Option<SlackFileRef> {
    let url = Url::parse(raw).ok()?;
    if url.scheme() != "https" || !is_allowed_slack_file_host(url.host_str()?) {
        return None;
    }
    let segments = url.path_segments()?.collect::<Vec<_>>();
    let file_index = segments.iter().position(|segment| *segment == "files")?;
    let file_id = segments.get(file_index + 2)?.trim();
    if file_id.is_empty() {
        return None;
    }
    let name = segments
        .get(file_index + 3)
        .filter(|name| !name.is_empty())
        .unwrap_or(&file_id)
        .to_string();
    Some(SlackFileRef {
        id: file_id.to_string(),
        name,
        mimetype: None,
        size_bytes: 0,
        url_private: None,
        url_private_download: None,
    })
}

pub(super) fn parse_slack_file_url(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("Slack file URL is invalid"))?;
    anyhow::ensure!(url.scheme() == "https", "Slack file URL must use https");
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Slack file URL has no host"))?;
    anyhow::ensure!(
        is_allowed_slack_file_host(host),
        "Slack file URL host is not allowed"
    );
    Ok(url)
}

pub(super) fn is_allowed_slack_file_host(host: &str) -> bool {
    host == "slack.com"
        || host.ends_with(".slack.com")
        || host == "slack-files.com"
        || host.ends_with(".slack-files.com")
}

pub(super) fn filter_context_messages(
    messages: Vec<SlackThreadMessage>,
    bot_user_id: &str,
) -> Vec<SlackThreadMessage> {
    messages
        .into_iter()
        .filter(|message| {
            !(message.user.as_deref() == Some(bot_user_id)
                && is_router_noise_message(&message.text))
        })
        .collect()
}

pub(super) fn is_router_noise_message(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.starts_with("Approval required:")
        || trimmed.starts_with("Auto-approved in YOLO mode:")
        || trimmed.starts_with("Approved ")
        || trimmed.starts_with("Denied ")
        || trimmed.ends_with(SLACK_REPLY_DRAFT_MARKER)
    {
        return true;
    }
    let Some(rest) = trimmed.strip_prefix('[') else {
        return false;
    };
    let Some((_, rest)) = rest.split_once("] ") else {
        return false;
    };
    let title = rest.lines().next().unwrap_or("").trim();
    title == "Activity"
        || title == "Progress"
        || title == "Reasoning summary"
        || title == "Tool call"
        || title.starts_with("Tool call: ")
}

pub(super) fn slack_thread_cache_key(channel: &str, thread_ts: &str) -> String {
    format!("{channel}:{thread_ts}")
}

pub(super) fn slack_thread_reference(channel: &str, thread_ts: &str) -> String {
    format!("{channel}:{thread_ts}")
}

pub(super) fn slack_current_thread_removal(
    channel: &str,
    thread_ts: &str,
) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::Exact {
        id: format!("slack:thread:{channel}:{thread_ts}"),
        kind: "slack_current_thread".to_string(),
    }
}

pub(super) fn slack_linked_thread_artifact_id(channel: &str, thread_ts: &str) -> String {
    format!("slack:linked-thread:{channel}:{thread_ts}")
}

pub(super) fn slack_linked_thread_removal(
    channel: &str,
    thread_ts: &str,
) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::Exact {
        id: slack_linked_thread_artifact_id(channel, thread_ts),
        kind: "slack_linked_thread".to_string(),
    }
}

pub(super) fn slack_all_linked_threads_removal() -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::Kind {
        kind: "slack_linked_thread".to_string(),
    }
}

pub(super) fn slack_retained_linked_threads_removal(
    retain_ids: BTreeSet<String>,
) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::ExceptKind {
        kind: "slack_linked_thread".to_string(),
        retain_ids,
    }
}

pub(super) fn slack_file_artifact_id(file_id: &str) -> String {
    format!("slack:file:{file_id}")
}

pub(super) fn slack_retained_files_removal(
    retain_ids: BTreeSet<String>,
) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::ExceptKind {
        kind: "slack_file".to_string(),
        retain_ids,
    }
}

pub(super) fn context_error_reason(err: &anyhow::Error) -> String {
    let raw = err.to_string();
    if raw.contains("url_private") || raw.contains("slack-files.com") || raw.contains("http") {
        "Slack context sync request failed".to_string()
    } else {
        raw
    }
}

pub(super) struct CurrentSlackThreadContext<'a> {
    pub(super) channel: &'a str,
    pub(super) thread_ts: &'a str,
    pub(super) fresh: bool,
    pub(super) messages: &'a [SlackThreadMessage],
}

pub(super) struct SlackDerivedArtifactRetention {
    pub(super) linked_thread_ids: BTreeSet<String>,
    pub(super) file_ids: BTreeSet<String>,
    pub(super) prune: bool,
}

impl SlackDerivedArtifactRetention {
    pub(super) fn new(
        linked_thread_ids: BTreeSet<String>,
        file_ids: BTreeSet<String>,
        prune: bool,
    ) -> Self {
        Self {
            linked_thread_ids,
            file_ids,
            prune,
        }
    }
}

pub(super) fn build_slack_context_request(
    session_key: &str,
    current_thread: CurrentSlackThreadContext<'_>,
    linked_threads: &[LinkedSlackThread],
    downloaded_files: &[DownloadedSlackFile],
    mut retention: SlackDerivedArtifactRetention,
    remove_artifacts: Vec<ContextArtifactRemovalInput>,
    unresolved: Vec<ContextSyncIssueInput>,
) -> ContextSyncRequest {
    let mut artifacts = Vec::new();
    let mut remove_artifacts = remove_artifacts;
    if !current_thread.messages.is_empty() {
        let mut metadata = BTreeMap::new();
        let channel = current_thread.channel;
        let thread_ts = current_thread.thread_ts;
        let thread_ref = format!("{channel}:{thread_ts}");
        metadata.insert("channel".to_string(), json!(channel));
        metadata.insert("thread_ts".to_string(), json!(thread_ts));
        metadata.insert(
            "message_count".to_string(),
            json!(current_thread.messages.len()),
        );
        let mut resolves_unresolved =
            vec![json!({"kind": "current_thread", "reference": &thread_ref})];
        if current_thread.fresh {
            resolves_unresolved
                .push(json!({"kind": "current_thread_cache", "reference": &thread_ref}));
        }
        metadata.insert(
            "resolves_unresolved".to_string(),
            json!(resolves_unresolved),
        );
        artifacts.push(ContextArtifactInput {
            id: format!("slack:thread:{channel}:{thread_ts}"),
            kind: "slack_current_thread".to_string(),
            title: "Current Slack thread".to_string(),
            source_locator: Some(format!("slack://{channel}/{thread_ts}")),
            files: vec![
                ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.md"),
                    content: ContextFileContent::Text(render_slack_thread_markdown(
                        channel,
                        thread_ts,
                        current_thread.messages,
                    )),
                },
                ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.jsonl"),
                    content: ContextFileContent::Text(render_slack_thread_jsonl(
                        current_thread.messages,
                    )),
                },
            ],
            metadata,
        });
    }

    for linked_thread in linked_threads {
        if linked_thread.messages.is_empty() {
            continue;
        }
        retention
            .linked_thread_ids
            .insert(slack_linked_thread_artifact_id(
                &linked_thread.link.channel,
                &linked_thread.link.thread_ts,
            ));
        artifacts.push(linked_thread_artifact(linked_thread));
    }
    if retention.prune {
        remove_artifacts.push(slack_retained_linked_threads_removal(
            retention.linked_thread_ids,
        ));
    }

    for downloaded in downloaded_files {
        retention
            .file_ids
            .insert(slack_file_artifact_id(&downloaded.file.id));
        artifacts.push(slack_file_artifact(downloaded));
    }
    if retention.prune {
        remove_artifacts.push(slack_retained_files_removal(retention.file_ids));
    }

    ContextSyncRequest {
        session_key: session_key.to_string(),
        source: "slack".to_string(),
        base_path: PathBuf::from("slack"),
        artifacts,
        remove_artifacts,
        unresolved,
    }
}

pub(super) fn linked_thread_artifact(linked_thread: &LinkedSlackThread) -> ContextArtifactInput {
    let channel = &linked_thread.link.channel;
    let thread_ts = &linked_thread.link.thread_ts;
    let safe_channel = sanitize_path_segment(channel);
    let safe_ts = sanitize_path_segment(thread_ts);
    let base_name = format!("{safe_channel}-{safe_ts}");
    let base_dir = PathBuf::from("slack").join("linked-threads");
    let mut metadata = BTreeMap::new();
    metadata.insert("channel".to_string(), json!(channel));
    metadata.insert("thread_ts".to_string(), json!(thread_ts));
    let thread_ref = slack_thread_reference(channel, thread_ts);
    let mut resolves_unresolved = vec![json!({"kind": "linked_thread", "reference": &thread_ref})];
    if linked_thread.fresh {
        resolves_unresolved.push(json!({"kind": "linked_thread_cache", "reference": &thread_ref}));
    }
    metadata.insert(
        "resolves_unresolved".to_string(),
        json!(resolves_unresolved),
    );
    metadata.insert(
        "source_message_ts".to_string(),
        json!(linked_thread.link.source_message_ts),
    );
    metadata.insert("url".to_string(), json!(linked_thread.link.url));
    metadata.insert(
        "message_count".to_string(),
        json!(linked_thread.messages.len()),
    );
    ContextArtifactInput {
        id: slack_linked_thread_artifact_id(channel, thread_ts),
        kind: "slack_linked_thread".to_string(),
        title: format!("Linked Slack thread {channel} {thread_ts}"),
        source_locator: Some(format!("slack://{channel}/{thread_ts}")),
        files: vec![
            ContextFileInput {
                relative_path: base_dir.join(format!("{base_name}.md")),
                content: ContextFileContent::Text(render_slack_thread_markdown(
                    channel,
                    thread_ts,
                    &linked_thread.messages,
                )),
            },
            ContextFileInput {
                relative_path: base_dir.join(format!("{base_name}.jsonl")),
                content: ContextFileContent::Text(render_slack_thread_jsonl(
                    &linked_thread.messages,
                )),
            },
            ContextFileInput {
                relative_path: base_dir.join(format!("{base_name}.metadata.json")),
                content: ContextFileContent::Text(
                    serde_json::to_string_pretty(&metadata).unwrap_or_else(|_| "{}".to_string()),
                ),
            },
        ],
        metadata,
    }
}

pub(super) fn slack_file_artifact(downloaded: &DownloadedSlackFile) -> ContextArtifactInput {
    let safe_id = sanitize_path_segment(&downloaded.file.id);
    let safe_name = sanitize_path_segment(&downloaded.file.name);
    let base = PathBuf::from("slack").join("files").join(&safe_id);
    let mut metadata = BTreeMap::new();
    metadata.insert("file_id".to_string(), json!(downloaded.file.id));
    metadata.insert("name".to_string(), json!(downloaded.file.name));
    metadata.insert("size_bytes".to_string(), json!(downloaded.file.size_bytes));
    metadata.insert(
        "resolves_unresolved".to_string(),
        json!([{"kind": "file", "reference": &downloaded.file.id}]),
    );
    if let Some(mimetype) = &downloaded.file.mimetype {
        metadata.insert("mimetype".to_string(), json!(mimetype));
    }

    let mut files = vec![
        ContextFileInput {
            relative_path: base.join("metadata.json"),
            content: ContextFileContent::Text(
                serde_json::to_string_pretty(&metadata).unwrap_or_else(|_| "{}".to_string()),
            ),
        },
        ContextFileInput {
            relative_path: base.join("original").join(&safe_name),
            content: ContextFileContent::Bytes(downloaded.bytes.clone()),
        },
    ];
    if let Some(text) = &downloaded.extracted_text {
        files.push(ContextFileInput {
            relative_path: base.join("extracted.md"),
            content: ContextFileContent::Text(text.clone()),
        });
    }

    ContextArtifactInput {
        id: slack_file_artifact_id(&downloaded.file.id),
        kind: "slack_file".to_string(),
        title: format!("Slack file {}", downloaded.file.name),
        source_locator: None,
        files,
        metadata,
    }
}

pub(super) fn render_slack_thread_markdown(
    channel: &str,
    thread_ts: &str,
    messages: &[SlackThreadMessage],
) -> String {
    let mut lines = vec![format!("# Slack thread {channel} {thread_ts}")];
    for message in messages {
        let author = slack_thread_message_author_label(message);
        lines.push(String::new());
        lines.push(format!("## {message_ts} {author}", message_ts = message.ts));
        if message.text.trim().is_empty() {
            lines.push("[no text]".to_string());
        } else {
            lines.push(message.text.clone());
        }
        if !message.files.is_empty() {
            lines.push("Files:".to_string());
            for file in &message.files {
                lines.push(format!(
                    "- {} ({}, {} bytes)",
                    file.name,
                    file.mimetype.as_deref().unwrap_or("unknown"),
                    file.size_bytes
                ));
            }
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

pub(super) fn render_slack_thread_jsonl(messages: &[SlackThreadMessage]) -> String {
    let mut lines = Vec::new();
    for message in messages {
        let files = message
            .files
            .iter()
            .map(|file| {
                json!({
                    "id": file.id,
                    "name": file.name,
                    "mimetype": file.mimetype,
                    "size_bytes": file.size_bytes,
                })
            })
            .collect::<Vec<_>>();
        lines.push(
            json!({
                "ts": message.ts,
                "user": message.user,
                "author_name": message.author_name,
                "bot_id": message.bot_id,
                "text": message.text,
                "files": files,
            })
            .to_string(),
        );
    }
    if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    }
}

pub(super) fn slack_thread_message_author_label(message: &SlackThreadMessage) -> String {
    let author_id = message.user.as_deref().or(message.bot_id.as_deref());
    match (message.author_name.as_deref(), author_id) {
        (Some(name), Some(id)) => format!("{name} ({id})"),
        (Some(name), None) => name.to_string(),
        (None, Some(id)) => id.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

pub(super) fn is_text_slack_file(file: &SlackFileRef) -> bool {
    let mimetype = file.mimetype.as_deref().unwrap_or("");
    if mimetype.starts_with("text/")
        || matches!(
            mimetype,
            "application/json" | "application/xml" | "application/yaml" | "application/x-yaml"
        )
    {
        return true;
    }
    let lower_name = file.name.to_ascii_lowercase();
    [
        ".md", ".txt", ".json", ".yaml", ".yml", ".toml", ".csv", ".rs", ".py", ".js", ".ts",
        ".tsx", ".jsx", ".go", ".java", ".c", ".cc", ".cpp", ".h", ".hpp", ".sh", ".sql",
    ]
    .iter()
    .any(|suffix| lower_name.ends_with(suffix))
}
