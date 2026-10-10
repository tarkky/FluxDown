# Rust crates

`engine` owns download protocols and persistence; `protocol` owns transport-independent wire contracts. Desktop and headless clients use `agent` (UI gateway, cloud and platform services) with `daemon` (download core). `api` exposes the shared HTTP compatibility surface, and `cli` supports HTTP or an embedded engine.

Native Android (`mobile/Android`) and iOS (`mobile/FluxDown`) use the UniFFI entry point `mobile`: local sessions embed the same daemon and agent, while remote sessions connect over `/rpc`. Generate Kotlin/Swift bindings through the native projects' build scripts; do not hand-edit generated bindings.

The Flutter/Rinf `hub` has been retired. `server` is frozen and is not built or released; new implementations must use agent + daemon. Shared assets and legacy data/theme upgrade compatibility remain in use.
