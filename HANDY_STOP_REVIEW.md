# Handy HDSP Stop review candidate

This branch is a proposal for upstream discussion, based on
`179b0d9306f519912c10bdddd111686622b7f63a` (`buttplug_server` 12.0.2).
It has not been installed into Intiface Central or tested on physical hardware.

The position-only Handy definition generates no generic zero-output Stop
command. On the base revision, `DeviceHandle::stop` can therefore return `Ok`
without sending any message to the device task, leaving queued position writes
untouched. A protocol-specific HDSP Stop is available without inventing a
target position.

## Proposed scope

- Add an optional protocol hook for stopping position outputs. Other protocols
  retain their existing behavior by default.
- Implement the hook for `thehandy-v3` with `RequestHdspStop`, using the same
  feature UUID as position writes. Existing urgent-batch overlap removal then
  supersedes position commands already queued ahead of the Stop.
- Clear the feature's output-deduplication cache when stopping so that a later
  intentional identical position request can be sent again.
- Return errors for failed writes, a closed task/ack channel, or a timeout
  instead of acknowledging an unsuccessful Stop as `Ok`. The timeout includes
  waiting to enqueue the command.

The existing normal motion payload, `stop_on_target: false`, position scaling,
command duration, advertised timing floor and batch cadence are unchanged.

## Reproduction and validation

The regression fixture uses the real `DeviceHandle`, `TheHandyV3` handler,
device configuration and device task with a recording `HardwareInternal`.
It does not open a Bluetooth manager or connect to any hardware.

Three tests were first run against the unmodified production implementation:
missing Stop output, premature acknowledgment, and swallowed write failure.
All three failed as expected. After the candidate changes, the expanded
eight-test suite passes, including an already in-flight write, a full queue,
timeout, repeated idle Stop and feature/input scoping.

```text
cargo test -p buttplug_server --lib thehandy_v3::stop_tests
cargo test -p buttplug_server --lib
cargo test -p buttplug_tests --test test_task_lifecycle \
  --test test_output_observations --test test_disabled_device_features --test test_server
```

Run formatting with nightly Rust as required by this repository.

## Deliberate limits and questions for maintainers

- This is not a complete end-to-end ordering barrier. It covers position
  writes already queued ahead of the Stop. An older command future that is
  only polled/enqueued afterward is not invalidated; the remote server also
  spawns per-message tasks before parsing. Ordered admission and per-feature
  cancellation need a separate design decision.
- The manager's all-devices Stop path currently discards the individual Stop
  results. This patch only makes the per-device write result truthful.
- A successful GATT write receipt is not device-level RPC acknowledgment or
  proof of physical motor halt. An already admitted write cannot be recalled.
- The default hook intentionally does not guess a safe stop for other position
  protocols. The desired unsupported-stop behavior should be agreed upstream.
- The physical hesitation/catch-up issue observed through Intiface is separate
  and not claimed fixed here. Lowering command gaps or changing sampling needs
  actual device-write timing and another controlled hardware comparison.

Please review the protocol hook and error-propagation approach before a PR is
opened, in accordance with the repository's issue-first contribution policy.
