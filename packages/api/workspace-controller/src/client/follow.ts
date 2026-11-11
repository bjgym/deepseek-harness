/** Reopen lifecycle for the single-consumer Workspace follow stream. */

import { isRemoteFailure } from '@deepseek-ai/dsh-api-gateway/client'
import type { WorkspaceStateStreamOptions } from './stream.ts'

/** Stream lifecycle the reopen owner drives; the real stream is a `RemoteSnapshotStream`. */
export interface WorkspaceFollowStream {
  /** Start the single consumer; repeated calls are inert. */
  start(): void
  /** Permanently stop the stream and wait for its consumer to become quiescent. */
  dispose(): Promise<void>
}

/** Open one logical Workspace follow stream over the given sinks. */
export type WorkspaceFollowStreamFactory = (
  options: WorkspaceStateStreamOptions,
) => WorkspaceFollowStream

/**
 * Delays before each replacement stream after the Gateway carrier retry budget
 * is exhausted. The list length is the reopen budget: a stream that cannot be
 * served even after a fresh logical stream stays in the error state.
 */
const REOPEN_DELAYS_MS: readonly number[] = [500, 1_000, 2_000]

/**
 * Owns the Workspace follow stream across terminal carrier exhaustion.
 *
 * The Gateway retry budget bounds one logical stream, and a failed
 * `RemoteStream` cannot be restarted: once its consumer iterator ends, its
 * lifetime is aborted and `start()` is inert. A stream that still ends as
 * `gateway/stream-unavailable` is therefore replaced with a fresh logical
 * stream after a bounded delay, keeping the last published Workspace state
 * until the replacement baseline arrives. Any accepted baseline resets the
 * reopen budget. Business and protocol failures stay terminal and surface
 * through the model's error state.
 */
export class WorkspaceFollowControl {
  private stream: WorkspaceFollowStream | undefined
  private reopenTimer: ReturnType<typeof setTimeout> | undefined
  private reopening: Promise<void> | undefined
  private reopens = 0
  private disposing: Promise<void> | undefined
  private disposed = false

  /**
   * @param open - opens one logical follow stream over the given sinks.
   * @param options - model destinations for decoded Workspace state.
   */
  constructor(
    private readonly open: WorkspaceFollowStreamFactory,
    private readonly options: WorkspaceStateStreamOptions,
  ) {}

  /** Start the first logical stream. */
  start(): void {
    this.attach()
  }

  /**
   * Stop the current stream, cancel a pending reopen, and wait for quiescence.
   * @returns when no stream, timer, or reopen can still run.
   */
  async dispose(): Promise<void> {
    if (this.disposing !== undefined) return this.disposing
    this.disposed = true
    this.cancelReopen()
    const disposing = (async () => {
      await this.reopening
      await this.stream?.dispose()
      this.stream = undefined
    })()
    this.disposing = disposing
    return disposing
  }

  private attach(): void {
    const stream = this.open({
      accept: this.acceptSink(),
      ...(this.options.carrierFailed === undefined
        ? {}
        : { carrierFailed: this.options.carrierFailed }),
      failed: (error) => {
        this.options.failed(error)
        this.scheduleReopen(error)
      },
    })
    this.stream = stream
    stream.start()
  }

  private acceptSink(): WorkspaceStateStreamOptions['accept'] {
    const accept = this.options.accept
    return {
      replaceBaseline: (value) => {
        this.reopens = 0
        accept.replaceBaseline(value)
      },
      upsertView: (value) => { accept.upsertView(value) },
      removeView: (workspaceId) => { accept.removeView(workspaceId) },
      replaceOrder: (workspaceIds) => { accept.replaceOrder(workspaceIds) },
      replaceArchived: (sessionIds) => { accept.replaceArchived(sessionIds) },
      replacePinned: (sessionIds) => { accept.replacePinned(sessionIds) },
    }
  }

  private scheduleReopen(error: unknown): void {
    if (!isRemoteFailure(error) || error.code !== 'gateway/stream-unavailable') return
    const delayMs = REOPEN_DELAYS_MS[this.reopens]
    if (delayMs === undefined) return
    this.reopens++
    this.reopenTimer = setTimeout(() => {
      this.reopenTimer = undefined
      this.beginReopen()
    }, delayMs)
  }

  private beginReopen(): void {
    const reopening = this.replaceStream()
    this.reopening = reopening
    void reopening.then(() => { this.reopening = undefined })
  }

  private async replaceStream(): Promise<void> {
    const previous = this.stream
    this.stream = undefined
    await previous?.dispose()
    if (this.disposed) return
    this.attach()
  }

  private cancelReopen(): void {
    if (this.reopenTimer === undefined) return
    clearTimeout(this.reopenTimer)
    this.reopenTimer = undefined
  }
}
