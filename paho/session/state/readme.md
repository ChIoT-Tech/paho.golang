paho.golang/paho/session/state
==============================

This is an implementation of the `paho.golang/paho/session.SessionManager` interface and is the implementation used
by default (should be suitable for mose use cases but can be replaced if it does not meet your needs).

On `Issue-216a`, outgoing publications carry local `store.Metadata` through `AddToSession`.
State caches this value after the initial store write succeeds, preserves it when replacing PUBLISH with PUBREL,
and restores it when loading a persistent session. Publish callbacks receive a copy independently of any requested
packet source. See the [store design notes](../../store/readme.md) for the contracts and recovery behaviour.
