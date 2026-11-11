/** Reconnecting lifecycle for one single-consumer Remote stream. */

import { RemoteError, remoteErrorOf } from '@deepseek-ai/dsh-typert-protocol'
import type { ConnectionHandle } from '@deepseek-ai/dsh-client-connection/client'
import { RemoteStreamCarrierError } from './stream-client.ts'

/**
 * Delays before each carrier retry while the connection still reports a
 * generation. The list length is the retry budget: a stream that keeps losing
 * its carrier while the connection looks healthy stops after the last delay
 * instead of reconnecting forever, while a transient loss within the budget
 * recovers without ending the logical stream. The first delay also lets the
 * Connection publish a generation invalidation before the next open attempt.
 */
const CARRIER_RETRY_DELAYS_MS: readonly number[] = [250, 500, 1_000]

/** Terminal codes this client-owned supervisor produces; both carry no details. */
type TerminalStreamCode = 'gateway/internal' | 'gateway/stream-unavailable'

/** One item annotated with the physical Remote-stream generation that delivered it. */
export interface RemoteStreamItem<Item> {
  /** Monotone physical generation number within this logical stream. */
  readonly generation: number
  /** Decoded item yielded by the generated Remote method. */
  readonly value: Item
  /** Cancellation lifetime of the generation that delivered this item. */
  readonly signal: AbortSignal
  /** Mark this generation's opening baseline or cursor as accepted. */
  accept(): void
}

/** Domain-owned operations used by {@link RemoteStream}. */
export interface RemoteStreamOptions<Item> {
  /** Diagnostic owner name used for cancellation failures. */
  readonly name: string
  /** Open one physical generation of the logical stream. */
  readonly open: (signal: AbortSignal) => AsyncIterable<Item>
  /** Classify a normal generation end after or before its opening item was accepted. */
  readonly ended: (accepted: boolean) => Error
  /** Observe a retryable carrier loss before the supervisor waits or reopens. */
  readonly carrierFailed?: (error: RemoteStreamCarrierError) => void
}

/**
 * Reopens one logical Remote stream across carrier generations.
 *
 * Connection owns physical retry timing; Gateway paces its own bounded carrier
 * retries and performs each requested replacement. The domain consumer owns its
 * opening item and every later item, and calls {@link RemoteStreamItem.accept}
 * only after validating the opening baseline or cursor.
 */
export class RemoteStream<Item> implements AsyncIterable<RemoteStreamItem<Item>> {
  private readonly lifetime = new AbortController()
  private generationAbort: AbortController | undefined
  private iterator: AsyncGenerator<RemoteStreamItem<Item>> | undefined
  private closing: Promise<void> | undefined
  private revision = 0
  private taken = false

  /**
   * @param connection - observable Host generation source used to pace retries.
   * @param options - domain stream opener, end classification, and diagnostics.
   */
  constructor(
    private readonly connection: Pick<ConnectionHandle, 'generation'>,
    private readonly options: RemoteStreamOptions<Item>,
  ) {}

  /** Cancellation lifetime shared by the stream and sibling page requests. */
  get signal(): AbortSignal {
    return this.lifetime.signal
  }

  /** Interrupt the current generation and immediately request a replacement. */
  restart(): void {
    if (this.lifetime.signal.aborted) return
    this.revision++
    this.generationAbort?.abort(new Error(`${this.options.name} generation restarted`))
  }

  /**
   * Permanently stop this stream and wait for its iterator to close.
   * @returns when the active generation and consumer iterator are quiescent.
   */
  dispose(): Promise<void> {
    if (this.closing !== undefined) return this.closing
    if (!this.lifetime.signal.aborted) {
      const reason = new Error(`${this.options.name} disposed`)
      this.lifetime.abort(reason)
      this.generationAbort?.abort(reason)
    }
    const iterator = this.iterator
    if (iterator === undefined) return Promise.resolve()
    const closing = closeRemoteStreamIterator(iterator)
    this.closing = closing
    return closing
  }

  /** @inheritdoc */
  [Symbol.asyncIterator](): AsyncIterator<RemoteStreamItem<Item>> {
    if (this.taken) throw new Error(`${this.options.name} already has a consumer`)
    this.taken = true
    const iterator = this.read()
    this.iterator = iterator
    return iterator
  }

  private async * read(): AsyncGenerator<RemoteStreamItem<Item>> {
    let attempt = 0
    let generation = 0
    let observedRevision = this.revision
    try {
      while (!isAborted(this.lifetime.signal)) {
        if (observedRevision !== this.revision) {
          observedRevision = this.revision
          attempt = 0
        }
        const revision = this.revision
        const generationAbort = new AbortController()
        this.generationAbort = generationAbort
        const signal = AbortSignal.any([this.lifetime.signal, generationAbort.signal])
        const generationId = ++generation
        let accepted = false
        try {
          for await (const value of this.options.open(signal)) {
            if (isAborted(this.lifetime.signal)) return
            if (revision !== this.revision) break
            yield {
              generation: generationId,
              value,
              signal,
              accept: () => {
                if (this.generationAbort !== generationAbort || revision !== this.revision) return
                accepted = true
                attempt = 0
              },
            }
          }
          if (isAborted(this.lifetime.signal)) return
          if (revision !== this.revision) continue
          throw this.options.ended(accepted)
        } catch (error) {
          if (isAborted(this.lifetime.signal)) return
          if (revision !== this.revision) continue
          if (!(error instanceof RemoteStreamCarrierError)) throw terminalStreamFailure(error)
          this.options.carrierFailed?.(error)
          if (revision !== this.revision) continue
          attempt++
          try {
            await waitForRemoteStreamRetry(this.connection, error, attempt, signal)
          } catch (retryError) {
            if (isAborted(this.lifetime.signal)) return
            if (revision !== this.revision) continue
            throw terminalStreamFailure(retryError, 'gateway/stream-unavailable')
          }
        } finally {
          this.generationAbort = undefined
          if (!generationAbort.signal.aborted) {
            generationAbort.abort(new Error(`${this.options.name} generation ended`))
          }
        }
      }
    } finally {
      if (!this.lifetime.signal.aborted) {
        this.lifetime.abort(new Error(`${this.options.name} consumer closed`))
      }
      this.generationAbort?.abort(this.lifetime.signal.reason)
      this.generationAbort = undefined
    }
  }
}

async function waitForRemoteStreamRetry(
  connection: Pick<ConnectionHandle, 'generation'>,
  error: RemoteStreamCarrierError,
  attempt: number,
  signal: AbortSignal,
): Promise<void> {
  signal.throwIfAborted()
  if (connection.generation.getSnapshot() !== undefined) {
    const delayMs = CARRIER_RETRY_DELAYS_MS[attempt - 1]
    if (delayMs === undefined) throw error
    await sleep(delayMs, signal)
    return
  }
  await new Promise<void>((resolve, reject) => {
    const subscription: {
      dispose?: () => void
      finished: boolean
    } = { finished: false }
    const finish = (failure?: Error): void => {
      if (subscription.finished) return
      subscription.finished = true
      subscription.dispose?.()
      signal.removeEventListener('abort', aborted)
      if (failure === undefined) resolve()
      else reject(failure)
    }
    const inspect = (): void => {
      if (connection.generation.getSnapshot() !== undefined) finish()
    }
    const aborted = (): void => {
      finish(new Error('Remote stream retry aborted', { cause: signal.reason }))
    }
    const dispose = connection.generation.subscribe(inspect)
    subscription.dispose = dispose
    if (subscription.finished) dispose()
    signal.addEventListener('abort', aborted, { once: true })
    if (signal.aborted) aborted()
    else inspect()
  })
}

/**
 * Mark a terminal escape before it crosses the stream boundary: consumers
 * discriminate failures by code, so an unmarked throw reads as a local bug.
 * Marked failures pass through verbatim. The carrier class never escapes as a
 * terminal outcome — it stays the retry-internal signal fed to `carrierFailed`
 * and the `ended(true)` retry trigger; an exhausted retry budget leaves this
 * function as `gateway/stream-unavailable`.
 * @param error - the escaped failure.
 * @param code - terminal code for an unmarked failure.
 * @returns the failure to throw across the stream boundary.
 */
function terminalStreamFailure(
  error: unknown,
  code: TerminalStreamCode = 'gateway/internal',
): Error {
  return remoteErrorOf(error) ?? new RemoteError(
    code,
    error instanceof Error ? error.message : String(error),
    {},
    { cause: error },
  )
}

/** Resolve after `delayMs`, or early when the generation is replaced or disposed. */
function sleep(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(done, delayMs)
    signal.addEventListener('abort', done, { once: true })
    function done(): void {
      clearTimeout(timer)
      signal.removeEventListener('abort', done)
      resolve()
    }
  })
}

function isAborted(signal: AbortSignal): boolean {
  return signal.aborted
}

async function closeRemoteStreamIterator<Item>(
  iterator: AsyncIterator<RemoteStreamItem<Item>>,
): Promise<void> {
  try {
    await iterator.return?.()
  } catch {
    // The disposed logical stream has no remaining consumer for cancellation failures.
  }
}
