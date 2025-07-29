# IMPORTANT! THIS CRATE (ankurah-react-signals) IS DEPRECATED - Do not use or edit this file. Use signals/tasks.md for more details

[x] implement a single useSignals hook with a "thread local" render context for signal value usage registration, and call forceUpdate (useReducer) on notification
[x] update react-signals value getter to register a subscription in this render context
[ ] reenable the derive macro stuff and remove ConnectionStateSignal from wasm-client
[ ] implement a build script that shims this into wasm-bindings - or load it as a separate ts package
[ ] implement equivalent of https://github.com/preactjs/signals/tree/main/packages/react-transform for ankura-react-signals
[X] inquire with leptos folks if reactive_graph might implement Render trait for Into<String> or AsRef so we don't have to map everything to <String> (discontinue use of reactive_graph and this crate)

Maybe future:

- fully brain, and fork https://github.com/preactjs/signals/blob/main/packages/react/runtime/
