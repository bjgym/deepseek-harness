# Agent Note: Workspace follow stream reopen

Status: implemented

English | [中文](2026-11-12-workspace-follow-stream-reopen.zh.md)

## Problem

The Client Workspace follow stream started once and never reopened. When a carrier loss exhausted the Gateway retry budget, `ClientWorkspaceModel` kept its last snapshot in `state: "error"` forever: the sidebar kept grouping from stale `workspace.sessionIds`, so sessions created afterwards rendered under "Ungrouped" until a reload. The failed stream cannot be revived: `RemoteStream` is single-consumer, a terminal failure aborts its lifetime, `restart()` returns immediately once aborted, and `RemoteSnapshotStream.start()` is inert after its first call. A connection-generation callback alone cannot heal it either, because the terminal classification is thrown only while a generation is defined, so the last generation change always precedes the failure.

## Decision

`WorkspaceFollowControl` owns the follow stream. A terminal failure whose code is `gateway/stream-unavailable` is followed by a replacement logical stream after 500 ms, 1 s, then 2 s; other codes stay terminal and surface the model's error state. The control disposes the failed stream before opening the replacement, keeps the last published Workspace state until the replacement baseline arrives, and resets the reopen budget on every accepted baseline. `dispose()` cancels a pending reopen and waits for an in-flight replacement, so unloading the plugin leaves no stream or timer behind. The stream factory moved to `stream.ts`; the public `createWorkspaceStateStream` export is unchanged.

## Alternatives considered

**Restart the failed stream.** `RemoteStream.restart()` returns immediately once the terminal failure aborted the stream lifetime, `RemoteSnapshotStream.start()` is inert after its first call, and the iterator is single-consumer; restarting the failed object is a no-op.

**Reopen only on a connection generation or `connection/reset`.** The terminal classification is thrown only while a generation is defined, so the generation publication precedes the failure and a dead stream answers later changes with the same no-op restart. A failure-driven reopen is required.

**Reopen after every terminal failure.** A protocol violation or a business refusal would then be retried against a Host that keeps refusing; the code distinction keeps those terminal.

**Retry in the UI only.** A retry affordance is product work on top of this recovery and is tracked separately; it does not recover the tab by itself.

## Consequences

A transient carrier outage that exhausts the Gateway budget now self-heals within about 4 s instead of freezing the tab until a reload, and the last Workspace snapshot stays visible while the replacement is pending. After the reopen budget is spent the error state persists, which the UI retry follow-up covers.
