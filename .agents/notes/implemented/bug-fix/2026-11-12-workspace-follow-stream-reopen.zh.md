# Agent Note: Workspace follow 流重开

Status: implemented

[English](2026-11-12-workspace-follow-stream-reopen.md) | 中文

## Problem

Client 的 Workspace follow 流只启动一次，从不重开。当载体丢失耗尽 Gateway 重试预算后，`ClientWorkspaceModel` 会一直把最后一次快照保持在 `state: "error"`：侧边栏继续按过期的 `workspace.sessionIds` 分组，于是之后创建的 Session 在刷新前一直显示在 "Ungrouped" 下。失败的流无法复活：`RemoteStream` 是单消费者，终止失败会 abort 它的生命周期，`restart()` 在 abort 后立即返回，`RemoteSnapshotStream.start()` 在首次调用后不再生效。仅靠 connection-generation 回调也修不好，因为终止分类只在 generation 已定义时抛出，最后一次 generation 变化总是发生在失败之前。

## Decision

`WorkspaceFollowControl` 拥有 follow 流。code 为 `gateway/stream-unavailable` 的终止失败会在 500 ms、1 s、2 s 后由新的逻辑流替换；其他 code 仍终止并暴露模型的 error 状态。控制器先 dispose 失败的流再打开替换流，在替换 baseline 到达前保留最后发布的 Workspace 状态，并在每次接受 baseline 时重置重开预算。`dispose()` 取消待定的重开并等待进行中的替换，因此卸载插件不会留下流或定时器。流工厂移到 `stream.ts`；公开的 `createWorkspaceStateStream` 导出保持不变。

## Alternatives considered

**重启失败的流。** 终止失败 abort 流生命周期后 `RemoteStream.restart()` 立即返回，`RemoteSnapshotStream.start()` 在首次调用后不再生效，迭代器又是单消费者；重启失败对象是空操作。

**只在 connection generation 或 `connection/reset` 时重开。** 终止分类只在 generation 已定义时抛出，因此 generation 发布发生在失败之前，已死的流对之后的变化同样只是空操作重启。必须由失败驱动重开。

**任何终止失败都重开。** 那样协议违规或业务拒绝会被拿去反复重试一个持续拒绝的 Host；用 code 区分可以保持这些失败终止。

**只在 UI 里重试。** 重试入口是本次恢复之上的产品工作，另行跟进；它本身不能恢复该标签页。

## Consequences

耗尽 Gateway 预算的短暂载体中断现在会在约 4 s 内自愈，而不是冻结标签页直到刷新；替换待定期间最后一份 Workspace 快照仍然可见。重开预算用尽后 error 状态会保留，由 UI 重试的后续工作覆盖。
