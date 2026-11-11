# Agent Note: Gateway logical-stream carrier retry budget

Status: implemented

English | [中文](2026-11-12-gateway-stream-carrier-retry-budget.zh.md)

## Problem

`RemoteStream` treated a carrier loss as retryable only once while the connection reported a generation, then ended the logical stream as `gateway/internal`. Two consecutive carrier losses inside one reconnect window — a Host restart, a network blip, or a proxy idle timeout — therefore became a permanent failure for every stream consumer even though the connection layer recovered. The Web Workspace sidebar froze at its last snapshot, and sessions created afterwards rendered under "Ungrouped" until a page reload.

## Decision

A carrier loss while `connection.generation` is defined retries after 250 ms, 500 ms, then 1 s. The delay lets Connection publish a generation invalidation before the next open attempt, so a transient loss recovers without ending the stream. Three retries are the budget: a fourth consecutive carrier failure ends the logical stream as `gateway/stream-unavailable`, a client-owned code declared with empty details in `remote-error-codes.ts` that the Host never throws. Business and protocol failures keep `gateway/internal` and stay terminal. `accept()` still resets the attempt count, so every accepted opening baseline restores the full budget.

## Alternatives considered

**Keep one immediate retry.** This is the shipped behavior the change replaces: a second loss before a replacement baseline was accepted ended the stream permanently even though the connection recovered moments later.

**Remove the cap and retry indefinitely.** The healthy-generation branch of `waitForRemoteStreamRetry` resolves immediately, so unbounded retries would reconnect as fast as the socket opens and closes. The delay plus a bounded budget keeps a broken stream from spinning.

**Make the budget a configuration field.** Logical-stream retry pacing has no deployment-varying requirement, and every consumer would have to pass the same value; the module constant keeps one policy for every `$stream()` consumer.

**Report exhaustion as `gateway/internal` with a details flag.** Consumers discriminate failures by code, so a flag inside the details object would need every consumer to read it; a dedicated code keeps the discrimination where the failure vocabulary already puts it.

## Consequences

Short carrier outages recover without ending the logical stream; a genuinely unservable stream ends after about 1.75 s with a distinguishable code. Consumers that can reopen a new logical stream discriminate that case through `gateway/stream-unavailable`, and consumers that cannot still see a terminal `RemoteError` as before. The Workspace client uses the distinction to reopen its follow stream.
