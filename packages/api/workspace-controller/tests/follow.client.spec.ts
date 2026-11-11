/** The Workspace follow reopen owner, driven with fake logical streams. */

import { describe, expect, it, onTestFinished, vi } from 'vitest'
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import {
  WorkspaceFollowControl,
  type WorkspaceFollowStream,
  type WorkspaceFollowStreamFactory,
} from '../src/client/follow.ts'
import type { WorkspaceFollowSink } from '../src/client/model.ts'
import type { WorkspaceStateStreamOptions } from '../src/client/stream.ts'
import type { WorkspaceId } from '../src/types.ts'
import { baseline, workspace } from './remote/workspace.client.ts'

const wid = (id: string): WorkspaceId => id as WorkspaceId
const sid = (id: string): SessionId => id as SessionId

interface FakeStream extends WorkspaceFollowStream {
  readonly options: WorkspaceStateStreamOptions
  starts: number
  disposals: number
}

/** Typed mocks for every sink the control forwards to the model. */
interface SinkMocks {
  readonly replaceBaseline: ReturnType<typeof vi.fn<WorkspaceFollowSink['replaceBaseline']>>
  readonly upsertView: ReturnType<typeof vi.fn<WorkspaceFollowSink['upsertView']>>
  readonly removeView: ReturnType<typeof vi.fn<WorkspaceFollowSink['removeView']>>
  readonly replaceOrder: ReturnType<typeof vi.fn<WorkspaceFollowSink['replaceOrder']>>
  readonly replaceArchived: ReturnType<typeof vi.fn<WorkspaceFollowSink['replaceArchived']>>
  readonly replacePinned: ReturnType<typeof vi.fn<WorkspaceFollowSink['replacePinned']>>
}

interface Harness {
  readonly control: WorkspaceFollowControl
  readonly streams: FakeStream[]
  readonly releases: (() => void)[]
  readonly accept: WorkspaceFollowSink
  readonly sinks: SinkMocks
  readonly carrierFailed: ReturnType<typeof vi.fn>
  readonly failed: ReturnType<typeof vi.fn>
}

/**
 * Build a control over fake streams.
 * @param options - whether to supply carrier diagnostics and to hold each disposal.
 * @returns the control, the opened streams, and the recorded sinks.
 */
function harness(options: { carrierFailed?: boolean; deferDispose?: boolean } = {}): Harness {
  const streams: FakeStream[] = []
  const releases: (() => void)[] = []
  const open: WorkspaceFollowStreamFactory = (streamOptions) => {
    let release = (): void => {}
    const disposed = options.deferDispose === true
      ? new Promise<void>((resolve) => { release = resolve })
      : Promise.resolve()
    const stream: FakeStream = {
      options: streamOptions,
      starts: 0,
      disposals: 0,
      start() { this.starts++ },
      dispose() { this.disposals++; return disposed },
    }
    releases.push(release)
    streams.push(stream)
    return stream
  }
  const sinks: SinkMocks = {
    replaceBaseline: vi.fn<WorkspaceFollowSink['replaceBaseline']>(),
    upsertView: vi.fn<WorkspaceFollowSink['upsertView']>(),
    removeView: vi.fn<WorkspaceFollowSink['removeView']>(),
    replaceOrder: vi.fn<WorkspaceFollowSink['replaceOrder']>(),
    replaceArchived: vi.fn<WorkspaceFollowSink['replaceArchived']>(),
    replacePinned: vi.fn<WorkspaceFollowSink['replacePinned']>(),
  }
  const accept: WorkspaceFollowSink = sinks
  const carrierFailed = vi.fn()
  const failed = vi.fn()
  const control = new WorkspaceFollowControl(open, {
    accept,
    failed,
    ...(options.carrierFailed === false ? {} : { carrierFailed }),
  })
  return { control, streams, releases, accept, sinks, carrierFailed, failed }
}

function fakeTimers(): void {
  vi.useFakeTimers()
  onTestFinished(() => { vi.useRealTimers() })
}

const exhausted = (message = 'stream lost'): RemoteError<'gateway/stream-unavailable'> =>
  new RemoteError('gateway/stream-unavailable', message, {})

describe('WorkspaceFollowControl', () => {
  it('starts one stream and replaces it after the carrier retry budget is exhausted', async () => {
    fakeTimers()
    const h = harness()
    h.control.start()
    expect(h.streams).toHaveLength(1)
    expect(h.streams[0]?.starts).toBe(1)
    expect(h.streams[0]?.options.carrierFailed).toBe(h.carrierFailed)

    const error = exhausted()
    h.streams[0]?.options.failed(error)
    expect(h.failed).toHaveBeenCalledWith(error)

    await vi.advanceTimersByTimeAsync(500)
    expect(h.streams).toHaveLength(2)
    expect(h.streams[0]?.disposals).toBe(1)
    expect(h.streams[1]?.starts).toBe(1)
    await h.control.dispose()
  })

  it('forwards every decoded operation and resets the reopen budget on a baseline', async () => {
    fakeTimers()
    const h = harness()
    h.control.start()
    const accept = h.streams[0]!.options.accept

    accept.replaceBaseline(baseline('first').value)
    accept.upsertView(workspace('second'))
    accept.removeView(wid('second'))
    accept.replaceOrder([wid('first')])
    accept.replaceArchived([sid('archived')])
    accept.replacePinned([sid('pinned')])

    expect(h.sinks.replaceBaseline).toHaveBeenCalledWith(baseline('first').value)
    expect(h.sinks.upsertView).toHaveBeenCalledWith(workspace('second'))
    expect(h.sinks.removeView).toHaveBeenCalledWith(wid('second'))
    expect(h.sinks.replaceOrder).toHaveBeenCalledWith([wid('first')])
    expect(h.sinks.replaceArchived).toHaveBeenCalledWith([sid('archived')])
    expect(h.sinks.replacePinned).toHaveBeenCalledWith([sid('pinned')])

    // One replacement spends the first delay; an accepted baseline restores it.
    h.streams[0]!.options.failed(exhausted())
    await vi.advanceTimersByTimeAsync(500)
    expect(h.streams).toHaveLength(2)
    h.streams[1]!.options.accept.replaceBaseline(baseline('recovered').value)
    h.streams[1]!.options.failed(exhausted())
    await vi.advanceTimersByTimeAsync(500)
    expect(h.streams).toHaveLength(3)
    await h.control.dispose()
  })

  it('keeps business and protocol failures terminal', async () => {
    fakeTimers()
    const h = harness()
    h.control.start()
    h.streams[0]!.options.failed(new Error('local fault'))
    h.streams[0]!.options.failed(new RemoteError('gateway/internal', 'protocol violation', {}))
    await vi.advanceTimersByTimeAsync(10_000)
    expect(h.failed).toHaveBeenCalledTimes(2)
    expect(h.streams).toHaveLength(1)
    await h.control.dispose()
  })

  it('stops reopening after the reopen budget', async () => {
    fakeTimers()
    const h = harness()
    h.control.start()
    for (let attempt = 0; attempt < 4; attempt++) {
      h.streams.at(-1)!.options.failed(exhausted())
      await vi.advanceTimersByTimeAsync(2_000)
    }
    expect(h.streams).toHaveLength(4)
    expect(h.failed).toHaveBeenCalledTimes(4)
    await h.control.dispose()
  })

  it('cancels a pending reopen and disposes the live stream once', async () => {
    fakeTimers()
    const h = harness({ carrierFailed: false })
    h.control.start()
    expect(h.streams[0]?.options.carrierFailed).toBeUndefined()
    h.streams[0]!.options.failed(exhausted())

    const disposing = h.control.dispose()
    const again = h.control.dispose()
    await Promise.all([disposing, again])
    await vi.advanceTimersByTimeAsync(10_000)
    expect(h.streams).toHaveLength(1)
    expect(h.streams[0]?.disposals).toBe(1)
  })

  it('stops a replacement whose previous stream is still closing', async () => {
    fakeTimers()
    const h = harness({ deferDispose: true })
    h.control.start()
    h.streams[0]!.options.failed(exhausted())
    await vi.advanceTimersByTimeAsync(500)

    const disposing = h.control.dispose()
    h.releases[0]!()
    await disposing
    await vi.advanceTimersByTimeAsync(10_000)
    expect(h.streams).toHaveLength(1)
  })
})
