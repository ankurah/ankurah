//! Macros for ergonomic entity creation.

/// Macro for ergonomic entity creation with automatic `.into()` on fields.
///
/// Transforms struct initialization to call `.into()` on each field value:
/// ```rust,ignore
/// use ankurah::into;
///
/// // This:
/// into!(ConnectionEvent { &user, session: &session_view, timestamp: ts })
///
/// // Expands to:
/// ConnectionEvent { user: (&user).into(), session: (&session_view).into(), timestamp: ts.into() }
/// ```
///
/// This works because:
/// - Views implement `From<&View> for Ref<Model>` (blanket impl in entity_ref.rs)
/// - All types implement `Into<Self>` (reflexive impl)
#[macro_export]
macro_rules! into {
    ($ty:ident { $($tt:tt)* }) => {
        $crate::into!(@expand $ty {} $($tt)*)
    };
    (@expand $ty:ident { $($out:tt)* }) => { $ty { $($out)* } };
    (@expand $ty:ident { $($out:tt)* } ,) => { $ty { $($out)* } };
    // Shorthand ref: &field, ...
    (@expand $ty:ident { $($out:tt)* } & $field:ident, $($rest:tt)*) => {
        $crate::into!(@expand $ty { $($out)* $field: (&$field).into(), } $($rest)*)
    };
    (@expand $ty:ident { $($out:tt)* } & $field:ident) => {
        $crate::into!(@expand $ty { $($out)* $field: (&$field).into(), })
    };
    // Shorthand: field, ...
    (@expand $ty:ident { $($out:tt)* } $field:ident, $($rest:tt)*) => {
        $crate::into!(@expand $ty { $($out)* $field: ($field).into(), } $($rest)*)
    };
    (@expand $ty:ident { $($out:tt)* } $field:ident) => {
        $crate::into!(@expand $ty { $($out)* $field: ($field).into(), })
    };
    // Explicit: field: expr, ...
    (@expand $ty:ident { $($out:tt)* } $field:ident : $value:expr, $($rest:tt)*) => {
        $crate::into!(@expand $ty { $($out)* $field: ($value).into(), } $($rest)*)
    };
    (@expand $ty:ident { $($out:tt)* } $field:ident : $value:expr) => {
        $crate::into!(@expand $ty { $($out)* $field: ($value).into(), })
    };
}

/// Macro for creating entities with automatic `.into()` on fields.
///
/// Combines a transaction's `create` call with the `into!` macro:
/// ```rust,ignore
/// use ankurah::create;
///
/// // This:
/// create!(trx, ConnectionEvent { &user, session: &session_view, timestamp: ts })
///
/// // Expands to:
/// trx.create(&into!(ConnectionEvent { &user, session: &session_view, timestamp: ts }))
/// ```
#[macro_export]
macro_rules! create {
    ($trx:expr, $ty:ident { $($tt:tt)* }) => {
        $trx.create(&$crate::into!($ty { $($tt)* }))
    };
}
