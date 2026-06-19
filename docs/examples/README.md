# Engine Examples

This directory contains JSON fixtures for the engine stream contract.

All JSON fixtures in this directory are validated by `cargo test -p protocol` against the current `EngineInput`, `EngineReply`, and `EngineEvent` variants in `crates/protocol/src/engine.rs`.

The fixtures include mark price, funding, account delta, liquidation lifecycle, ADL, orderbook snapshot, and checkpoint messages. Metadata fields such as `engine_event_id`, `source_input_id`, and `source_input_offset` are first-class Rust protocol fields where they appear in the structs; optional fields should stay in fixtures when producers are expected to set them.
