//! Provider-neutral dialog state machine. The host supplies model and tool I/O.
use openplotva_dialog::SessionMessage;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

pub const MAX_TOOL_CALLS: u32 = 32;
pub const TURN_SECONDS: u64 = 120;
pub const FINAL_RESERVE_SECONDS: u64 = 20;
pub const MAX_STEPS: i32 = 36;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("tool dispatch failed: {0}")]
    ToolDispatch(String),
}

/// State retained across every model step, including newly observed chat messages.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AgentLoop {
    pub transcript: Vec<SessionMessage>,
    pub iteration: i32,
    pub observed_message_id: i32,
    pub tool_attempts: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NextStep {
    Tools,
    Final,
    Exhausted,
}

impl AgentLoop {
    pub fn next_step(&mut self, remaining: Duration, max_steps: i32) -> NextStep {
        self.iteration += 1;
        if remaining.is_zero() || self.iteration > max_steps.max(1) {
            NextStep::Exhausted
        } else if self.iteration == max_steps.max(1)
            || self.tool_attempts >= MAX_TOOL_CALLS
            || remaining <= Duration::from_secs(FINAL_RESERVE_SECONDS)
        {
            NextStep::Final
        } else {
            NextStep::Tools
        }
    }

    /// Count attempts, including invalid, cached, and failed calls.
    pub fn admit_tool(&mut self, remaining: Duration) -> bool {
        if self.tool_attempts >= MAX_TOOL_CALLS
            || remaining <= Duration::from_secs(FINAL_RESERVE_SECONDS)
        {
            return false;
        }
        self.tool_attempts += 1;
        true
    }
}

/// Drive one host through model decisions and effects until it returns a terminal result.
pub async fn run<S, T, F, Fut>(mut state: S, mut step: F) -> T
where
    F: FnMut(S) -> Fut,
    Fut: std::future::Future<Output = std::ops::ControlFlow<T, S>>,
{
    loop {
        match step(state).await {
            std::ops::ControlFlow::Break(result) => return result,
            std::ops::ControlFlow::Continue(next) => state = next,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tool_limit_survives_checkpoint_and_reserves_final_answer() {
        let mut state = AgentLoop::default();
        for _ in 0..32 {
            assert!(state.admit_tool(Duration::from_secs(60)));
        }
        let mut state: AgentLoop =
            serde_json::from_str(&serde_json::to_string(&state).expect("serialize state"))
                .expect("restore state");
        assert!(!state.admit_tool(Duration::from_secs(60)));
        assert_eq!(
            state.next_step(Duration::from_secs(60), MAX_STEPS),
            NextStep::Final
        );
    }
    #[test]
    fn deadline_stops_tools_before_final_answer() {
        let mut state = AgentLoop::default();
        assert!(!state.admit_tool(Duration::from_secs(20)));
        assert_eq!(
            state.next_step(Duration::from_secs(20), MAX_STEPS),
            NextStep::Final
        );
        assert_eq!(
            state.next_step(Duration::ZERO, MAX_STEPS),
            NextStep::Exhausted
        );
    }
}
