// Isolated acceptance fixture; this module is not used by the bot runtime.
fn retries_left(limit: u32, attempts: u32) -> u32 {
    limit.saturating_sub(attempts)
}

#[test]
fn retry_budget_saturates_after_exhaustion() {
    assert_eq!(retries_left(3, 0), 3);
    assert_eq!(retries_left(3, 3), 0);
    assert_eq!(retries_left(3, 4), 0);
}
