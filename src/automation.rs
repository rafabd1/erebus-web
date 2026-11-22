use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify, oneshot};
use uuid::Uuid;

use crate::error::AppError;

const CLIENT_STALE_AFTER_MS: u64 = 45_000;
const MAX_POLL_MS: u64 = 20_000;
const MAX_QUEUED_COMMANDS_PER_CLIENT: usize = 64;

#[derive(Clone, Default)]
pub struct ExtensionAutomation {
    state: Arc<Mutex<AutomationState>>,
    changed: Arc<Notify>,
}

#[derive(Default)]
struct AutomationState {
    clients: HashMap<String, AutomationClient>,
    pending: HashMap<String, PendingResult>,
}

struct AutomationClient {
    session_id: String,
    producer_id: String,
    browser: String,
    version: String,
    last_seen_ms: u64,
    tabs: Vec<ExtensionBrowserTab>,
    commands: VecDeque<ExtensionAutomationCommand>,
}

struct PendingResult {
    session_id: String,
    producer_id: String,
    sender: oneshot::Sender<ExtensionAutomationResult>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionHeartbeatInput {
    pub session_id: String,
    pub producer_id: String,
    pub browser: String,
    pub version: String,
    #[serde(default)]
    pub tabs: Vec<ExtensionBrowserTab>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionBrowserTab {
    pub tab_id: i64,
    pub window_id: i64,
    pub active: bool,
    pub attached: bool,
    pub title: Option<String>,
    pub url: Option<String>,
    pub status: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutomationClientView {
    pub session_id: String,
    pub producer_id: String,
    pub browser: String,
    pub version: String,
    pub last_seen_ms: u64,
    pub connected: bool,
    pub tabs: Vec<ExtensionBrowserTab>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionPollInput {
    pub session_id: String,
    pub producer_id: String,
    #[serde(default)]
    pub wait_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutomationInvokeInput {
    pub session_id: String,
    pub producer_id: String,
    pub tab_id: Option<i64>,
    pub operation: String,
    #[serde(default)]
    pub input: Value,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutomationCommand {
    pub id: String,
    pub tab_id: Option<i64>,
    pub operation: String,
    pub input: Value,
    pub timeout_ms: u64,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutomationError {
    pub tag: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutomationResult {
    pub command_id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ExtensionAutomationError>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionAutomationResultInput {
    pub session_id: String,
    pub producer_id: String,
    #[serde(flatten)]
    pub result: ExtensionAutomationResult,
}

impl ExtensionAutomation {
    pub async fn heartbeat(&self, input: ExtensionHeartbeatInput) -> ExtensionAutomationClientView {
        let now = now_ms();
        let mut state = self.state.lock().await;
        let commands = state
            .clients
            .remove(&input.producer_id)
            .filter(|client| client.session_id == input.session_id)
            .map(|client| client.commands)
            .unwrap_or_default();
        let client = AutomationClient {
            session_id: input.session_id,
            producer_id: input.producer_id.clone(),
            browser: input.browser,
            version: input.version,
            last_seen_ms: now,
            tabs: input.tabs,
            commands,
        };
        let view = client.view(now);
        state.clients.insert(input.producer_id, client);
        drop(state);
        self.changed.notify_waiters();
        view
    }

    pub async fn clients(&self) -> Vec<ExtensionAutomationClientView> {
        let now = now_ms();
        let state = self.state.lock().await;
        state
            .clients
            .values()
            .map(|client| client.view(now))
            .collect()
    }

    pub async fn poll(
        &self,
        input: &ExtensionPollInput,
    ) -> Result<Option<ExtensionAutomationCommand>, AppError> {
        let wait_ms = input.wait_ms.unwrap_or(MAX_POLL_MS).min(MAX_POLL_MS);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let notified = self.changed.notified();
            {
                let mut state = self.state.lock().await;
                let client = state.clients.get_mut(&input.producer_id).ok_or_else(|| {
                    AppError::InvalidInput("extension must send a heartbeat before polling".into())
                })?;
                if client.session_id != input.session_id {
                    return Err(AppError::Forbidden);
                }
                client.last_seen_ms = now_ms();
                if let Some(command) = client.commands.pop_front() {
                    return Ok(Some(command));
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(None);
            }
        }
    }

    pub async fn invoke(
        &self,
        input: ExtensionAutomationInvokeInput,
    ) -> Result<ExtensionAutomationResult, AppError> {
        if input.timeout_ms == 0 || input.timeout_ms > 60_000 {
            return Err(AppError::InvalidInput(
                "extension automation timeout must be between 1 and 60000 milliseconds".into(),
            ));
        }
        let command_id = Uuid::now_v7().to_string();
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            let client = state.clients.get_mut(&input.producer_id).ok_or_else(|| {
                AppError::InvalidInput("browser extension is not connected".into())
            })?;
            if client.session_id != input.session_id {
                return Err(AppError::Forbidden);
            }
            if now_ms().saturating_sub(client.last_seen_ms) > CLIENT_STALE_AFTER_MS {
                return Err(AppError::InvalidInput(
                    "browser extension connection is stale".into(),
                ));
            }
            if client.commands.len() >= MAX_QUEUED_COMMANDS_PER_CLIENT {
                return Err(AppError::Busy);
            }
            client.commands.push_back(ExtensionAutomationCommand {
                id: command_id.clone(),
                tab_id: input.tab_id,
                operation: input.operation,
                input: input.input,
                timeout_ms: input.timeout_ms,
                created_at_ms: now_ms(),
            });
            state.pending.insert(
                command_id.clone(),
                PendingResult {
                    session_id: input.session_id,
                    producer_id: input.producer_id,
                    sender,
                },
            );
        }
        self.changed.notify_waiters();
        let wait = Duration::from_millis(input.timeout_ms.saturating_add(1_000));
        match tokio::time::timeout(wait, receiver).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(AppError::InvalidInput(
                "browser extension disconnected before returning a result".into(),
            )),
            Err(_) => {
                self.cancel(&command_id).await;
                Err(AppError::InvalidInput(
                    "browser extension command timed out".into(),
                ))
            }
        }
    }

    pub async fn complete(&self, input: ExtensionAutomationResultInput) -> Result<(), AppError> {
        let mut state = self.state.lock().await;
        let pending = state
            .pending
            .remove(&input.result.command_id)
            .ok_or(AppError::NotFound)?;
        if pending.session_id != input.session_id || pending.producer_id != input.producer_id {
            state
                .pending
                .insert(input.result.command_id.clone(), pending);
            return Err(AppError::Forbidden);
        }
        let _ = pending.sender.send(input.result);
        Ok(())
    }

    pub async fn disconnect_session(&self, session_id: &str) {
        let mut state = self.state.lock().await;
        let producer_ids = state
            .clients
            .iter()
            .filter_map(|(producer_id, client)| {
                (client.session_id == session_id).then_some(producer_id.clone())
            })
            .collect::<Vec<_>>();
        for producer_id in producer_ids {
            state.clients.remove(&producer_id);
        }
        state
            .pending
            .retain(|_, pending| pending.session_id != session_id);
        drop(state);
        self.changed.notify_waiters();
    }

    async fn cancel(&self, command_id: &str) {
        let mut state = self.state.lock().await;
        state.pending.remove(command_id);
        for client in state.clients.values_mut() {
            client.commands.retain(|command| command.id != command_id);
        }
    }
}

impl AutomationClient {
    fn view(&self, now: u64) -> ExtensionAutomationClientView {
        ExtensionAutomationClientView {
            session_id: self.session_id.clone(),
            producer_id: self.producer_id.clone(),
            browser: self.browser.clone(),
            version: self.version.clone(),
            last_seen_ms: self.last_seen_ms,
            connected: now.saturating_sub(self.last_seen_ms) <= CLIENT_STALE_AFTER_MS,
            tabs: self.tabs.clone(),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat() -> ExtensionHeartbeatInput {
        ExtensionHeartbeatInput {
            session_id: "session".into(),
            producer_id: "producer".into(),
            browser: "chromium".into(),
            version: "1".into(),
            tabs: vec![],
        }
    }

    #[tokio::test]
    async fn routes_one_command_and_result_to_the_paired_client() {
        let automation = ExtensionAutomation::default();
        automation.heartbeat(heartbeat()).await;
        let invoke = tokio::spawn({
            let automation = automation.clone();
            async move {
                automation
                    .invoke(ExtensionAutomationInvokeInput {
                        session_id: "session".into(),
                        producer_id: "producer".into(),
                        tab_id: Some(7),
                        operation: "status".into(),
                        input: Value::Null,
                        timeout_ms: 1_000,
                    })
                    .await
                    .unwrap()
            }
        });
        let command = automation
            .poll(&ExtensionPollInput {
                session_id: "session".into(),
                producer_id: "producer".into(),
                wait_ms: Some(100),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(command.operation, "status");
        automation
            .complete(ExtensionAutomationResultInput {
                session_id: "session".into(),
                producer_id: "producer".into(),
                result: ExtensionAutomationResult {
                    command_id: command.id,
                    ok: true,
                    result: Some(serde_json::json!({ "available": true })),
                    error: None,
                },
            })
            .await
            .unwrap();
        assert!(invoke.await.unwrap().ok);
    }

    #[tokio::test]
    async fn rejects_cross_session_results() {
        let automation = ExtensionAutomation::default();
        automation.heartbeat(heartbeat()).await;
        let invoke = tokio::spawn({
            let automation = automation.clone();
            async move {
                automation
                    .invoke(ExtensionAutomationInvokeInput {
                        session_id: "session".into(),
                        producer_id: "producer".into(),
                        tab_id: None,
                        operation: "status".into(),
                        input: Value::Null,
                        timeout_ms: 1_000,
                    })
                    .await
            }
        });
        let command = automation
            .poll(&ExtensionPollInput {
                session_id: "session".into(),
                producer_id: "producer".into(),
                wait_ms: Some(100),
            })
            .await
            .unwrap()
            .unwrap();
        let error = automation
            .complete(ExtensionAutomationResultInput {
                session_id: "other".into(),
                producer_id: "producer".into(),
                result: ExtensionAutomationResult {
                    command_id: command.id,
                    ok: true,
                    result: None,
                    error: None,
                },
            })
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::Forbidden));
        invoke.abort();
    }
}
