# Disintegration of zingo-common

This repo's two crates are being dissolved into their consumers. The decision record for the
wallet-side half lives in zingolib as `docs/adr/0002-regtest-compiled-out-of-production.md`.
This document is the migration sequence.

## End state

- `zingo-netutils` lives in the zingolib workspace and is published from there.
- `ActivationHeights`, `NetworkType`, and `ActivationHeightsBuilder` live in a new leaf
  crate in the zingolabs/infrastructure workspace, named `zingo-consensus`, with zero
  dependencies (`hex` served only the dropped `TxId`). The old `for_test::all_height_one_nus`
  schedule is the type's documented `Default` impl, not a separate helper.
- `BlockHeight`, `TxId`, and `H0` migrate nowhere. An org-wide audit (2026-07-02) found zero
  consumers. Consumers needing equivalents already use `zcash_protocol` / `zcash_primitives`.
- Dependency rule: a crate that depends on `zcash_local_net` takes the vocabulary through
  its `protocol` re-exports and must NOT also directly depend on `zingo-consensus`. The
  only direct dependents are `zcash_local_net` itself and zingolib's optional `regtest`
  feature. Everything else reaches the types through one of those two re-exporters.
- Regtest support is compiled out of production builds of zingolib, zingo-cli, zingo-mobile,
  and zingo-pc behind default-off `regtest` cargo features.
- This repo is **archived, never deleted**: zingo-pc pins git tag `v0_2_with_nu6_2_upgrade`
  and client_rpc_test_fixtures pins a git rev for `zingo_netutils`. Archiving keeps those
  refs resolvable. Published crates.io versions remain available regardless.

## Sequence

Phase 1 must publish before phases 2 through 4 can land. Phases 2, 3, and 4 are independent
of each other. Phase 5 waits for all of them.

### Phase 0: zingo-netutils moves to zingolib (DONE)

Commit `7468d7da0` on zingolib branch `add_zingo_netutils`. The crate is a workspace member
inheriting all workspace dependency versions. The workspace dep is path-only during
development; restore `version` alongside `path` at release time, per the pepper-sync
convention. Keep publishing to crates.io (zaino stable and client_rpc_test_fixtures consume
published/pinned versions; zaino dev does not use it at all).

### Phase 1: infrastructure grows the leaf types crate (DONE, pending release)

Implemented on infrastructure branch `add_zingo_consensus` (tip `0a003b4`), which also
contains all of `add_client_support` (zingolabs/infrastructure#269) so zaino could bump
its pin. Publishing and tagging remain.

1. New workspace member `zingo-consensus` containing `ActivationHeights`,
   `ActivationHeightsBuilder`, and `NetworkType`, with the all-heights-one schedule as the
   type's documented `Default` impl. Do not port `BlockHeight`, `TxId`, or `H0`.
2. `zcash_local_net` takes it by path and re-exports the builder alongside the two types
   it already re-exports (zaino needs to construct the type, and the fields are private).
   `regtest-launcher` consumes the `zcash_local_net::protocol` re-exports per the
   dependency rule, with no direct dep of its own.
3. Update `cargo_check_external_types` allowlists to name the new crate.
4. Publish the crate and tag infrastructure. The types now release on the same tag train as
   their main consumer, removing one hop from every future network-upgrade cascade.

### Phase 2: zingolib workspace adopts the gate (DONE, pending release)

Implemented on zingolib branch `add_regtest_gate` (`e0e7f259e`). Git-pins the
infrastructure rev until zingo-consensus is published.

1. Add `regtest = ["dep:zingo-consensus"]` to zingolib, default off. Gate the
   `ChainType::Regtest` variant and its fourteen production match arms, the
   `ActivationHeights` re-export in `lib.rs`, and the disk-format arm (tag `2` reads as a
   descriptive error without the feature; tags are not renumbered).
2. `testutils` feature requires `regtest`.
3. zingo-cli: own default-off `regtest = ["zingolib/regtest"]` feature gating its two
   `commands.rs` arms. A default build rejects `regtest` as a chain selection at runtime.
4. Swap the dependency: `zingo_common_components` out everywhere. zingolib gains
   `zingo-consensus` as its optional direct dep. The test crates that depend on
   `zcash_local_net` (zingolib_testutils, libtonode-tests, darkside-tests) import via
   `zcash_local_net::protocol` or zingolib's gated re-export instead, per the dependency
   rule; they take no direct dep.
5. Add the release tripwire: a shell script in release CI fails if `zingo-consensus`
   appears in `cargo tree --edges normal` for the release target.
6. Release zingolib.

### Phase 3: zaino drops the dependency (DONE)

Implemented on zaino branch `drop_zingo_common_components` (`ba014515`).

1. Delete the two `From` impls in `packages/zaino-common/src/config/network.rs` and the
   `zingo_common_components` dependency from `zaino-common`.
2. Recreate them as plain conversion functions in `zaino-testutils` (the orphan rule blocks
   trait impls between two foreign types there), targeting the types re-exported by
   `zcash_local_net`, using the newly re-exported builder.
3. Bump the `zcash_local_net` git pin to the phase-1 tag. Production zaino now has no
   dependency on the activation-heights vocabulary at all.

### Phase 4: zingo-mobile and zingo-pc adopt the gate

For each app:

1. Add a default-off `regtest = ["zingolib/regtest"]` feature.
2. Gate the three call sites: the `"regtest"` chain-hint arm, the regtest entry in the
   address-decoding candidate list (production tries Mainnet and Testnet only), and the
   schedule-helper import.
3. Replace `all_height_one_nus` (0.2 line, `for_test` feature) with
   `ActivationHeights::default()` via zingolib's gated re-export (no direct
   `zingo-consensus` dep), and bump zingolib to the phase-2 release.
4. Add the same shell-script tripwire to the release pipelines (Android, iOS, desktop).

### Phase 5: this repo winds down

1. Remove both crates from the workspace (netutils already moved; `zingo_common_components`
   is superseded by `zingo-consensus`).
2. Do not yank published versions. zaino stable, zingo-mobile 0.2.0 consumers, and anything
   unreleased keeps building.
3. Update the README to point at the new homes, then archive the repository on GitHub.

## Consumers with no action required

- client_rpc_test_fixtures: its `zingo_common_components` is lock-transitive and it tracks
  infrastructure `dev`, so it follows phase 1 automatically. Its git-rev pin on this repo's
  `zingo_netutils` survives archiving.
- zaino stable branch: pins published crates.io versions only.
