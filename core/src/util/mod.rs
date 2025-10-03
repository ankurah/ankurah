pub mod expand_states;
pub mod iterable;
pub mod ivec;
pub mod ready_chunks;
pub mod safemap;
pub mod safeset;
pub use iterable::Iterable;
pub use ivec::IVec;

/// Formats an action log with consistent styling.
/// First argument is always a "thing" that performed the action (in bold blue)
/// Second argument is the action name (in green)
/// Remaining arguments are formatted as additional context (dimmed)
#[macro_export]
macro_rules! action_info {
    // Thing + action
    ($thing:expr, $action:expr) => {
        tracing::info!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m", $thing, $action)
    };
    // Thing + action + args
    ($thing:expr, $action:expr, $($arg:expr),+) => {
        tracing::info!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m \x1b[2m{}\x1b[0m", $thing, $action, format!("{}", format_args!($($arg),+)))
    };
}

#[macro_export]
macro_rules! action_debug {
    // Thing + action
    ($thing:expr, $action:expr) => {
        tracing::debug!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m", $thing, $action)
    };
    // Thing + action + args
    ($thing:expr, $action:expr, $($arg:expr),+) => {
        tracing::debug!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m \x1b[2m{}\x1b[0m", $thing, $action, format!("{}", format_args!($($arg),+)))
    };
}

#[macro_export]
macro_rules! action_warn {
    // Thing + action
    ($thing:expr, $action:expr) => {
        tracing::warn!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m", $thing, $action)
    };
    // Thing + action + args
    ($thing:expr, $action:expr, $($arg:expr),+) => {
        tracing::warn!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m \x1b[2m{}\x1b[0m", $thing, $action, format!("{}", format_args!($($arg),+)))
    };
}

#[macro_export]
macro_rules! action_error {
    // Thing + action
    ($thing:expr, $action:expr) => {
        tracing::error!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m", $thing, $action)
    };
    // Thing + action + args
    ($thing:expr, $action:expr, $($arg:expr),+) => {
        tracing::error!("\x1b[1;34m{}\x1b[0m → \x1b[32m{}\x1b[0m \x1b[2m{}\x1b[0m", $thing, $action, format!("{}", format_args!($($arg),+)))
    };
}

/// Formats a notice log with consistent styling.
/// The message is displayed in bold yellow to make it stand out.
#[macro_export]
macro_rules! notice_info {
    ($($arg:tt)*) => {
        tracing::info!("\x1b[1;33m{}\x1b[0m", format!($($arg)*))
    };
}
