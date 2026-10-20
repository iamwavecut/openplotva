//! Python sandbox for dialog tools: wire types, the HTTP client the bot uses,
//! and the supervisor that runs scripts under nsjail in disposable sessions.

pub mod api;
pub mod client;
pub mod server;
