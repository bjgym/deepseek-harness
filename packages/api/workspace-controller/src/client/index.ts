/** Workspace-specific adapter for the Gateway-owned snapshot stream lifecycle. */

import type { Context } from '@deepseek-ai/cordis'
import { ClientWorkspaceModel } from './model.ts'
import { WorkspaceController } from './service.ts'
import { WorkspaceFollowControl } from './follow.ts'
import { createWorkspaceStateStream } from './stream.ts'

export { ClientWorkspaceModel } from './model.ts'
export type {
  WorkspaceFollowSink, WorkspaceListPhase, WorkspaceRemote, WorkspaceSnapshot,
} from './model.ts'
export { WorkspaceArchiveError, WorkspaceController, WorkspaceCreateError } from './service.ts'
export type { IWorkspaces, WorkspaceSource } from './service.ts'
export { createWorkspaceStateStream } from './stream.ts'
export type { WorkspaceStateStream, WorkspaceStateStreamOptions } from './stream.ts'
export type {
  SessionActivity, SessionActivityItem, SessionActivityKind, SessionActivityKindMap, WorkspaceId,
  WorkspaceView,
} from '../types.ts'

declare module '@deepseek-ai/cordis' {
  interface Context {
    /** React-free Client Workspace state and commands. */
    workspaces: import('./service.ts').IWorkspaces
  }
}

/** Required Client Remote services. */
export const inject = ['remote', 'remote.workspace']

/**
 * Install Client Workspace state, commands, and reconnecting follow control.
 * @param ctx - Client root Context.
 */
export function apply(ctx: Context): void {
  const model = new ClientWorkspaceModel(ctx.remote.workspace)
  new WorkspaceController(ctx, model)
  const control = new WorkspaceFollowControl(
    options => createWorkspaceStateStream(ctx.remote, options),
    {
      accept: model,
      carrierFailed: () => { model.handleCarrierFailure() },
      failed: (error) => { model.handleStreamFailure(error) },
    },
  )
  control.start()
  ctx.effect(
    () => async () => { await control.dispose() },
    'workspace-controller.client.control',
  )
}
