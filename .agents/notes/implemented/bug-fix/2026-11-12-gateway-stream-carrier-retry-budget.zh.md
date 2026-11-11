# Agent Note: Gateway 逻辑流载体重试预算

Status: implemented

[English](2026-11-12-gateway-stream-carrier-retry-budget.md) | 中文

## Problem

`RemoteStream` 在连接层仍报告 generation 时只允许重试一次载体丢失，随后以 `gateway/internal` 结束逻辑流。因此同一个重连窗口内的两次连续载体丢失——Host 重启、网络抖动或代理空闲超时——会变成每个流消费者的永久失败，尽管连接层已经恢复。Web 的 Workspace 侧边栏冻结在最后一次快照上，之后创建的 Session 在页面刷新前一直显示在 "Ungrouped" 下。

## Decision

当 `connection.generation` 已定义时，载体丢失按 250 ms、500 ms、1 s 依次重试。延迟让 Connection 在下一次打开尝试前发布 generation 失效，因此瞬时丢失可以恢复而不结束流。三次重试即预算：第四次连续载体丢失以 `gateway/stream-unavailable` 结束逻辑流，该代码由客户端拥有、在 `remote-error-codes.ts` 中声明为空 details，Host 从不抛出。业务与协议失败仍保持 `gateway/internal` 并终止。`accept()` 仍会重置尝试计数，因此每次被接受的起始 baseline 都会恢复完整预算。

## Alternatives considered

**保留一次立即重试。** 这正是本次替换的既有行为：替换 baseline 被接受前的第二次丢失会让流永久结束，即使连接层稍后恢复。

**取消上限、无限重试。** `waitForRemoteStreamRetry` 的 healthy-generation 分支会立即返回，因此无上限重试会以 socket 打开与关闭的速度反复重连。延迟加有界预算可以避免坏流空转。

**把预算做成配置字段。** 逻辑流的重试节奏没有随部署变化的需求，且每个消费者都得传同一个值；模块常量让所有 `$stream()` 消费者共用一份策略。

**用 `gateway/internal` 加 details 标记表示预算耗尽。** 消费者按 code 区分失败，details 里的标记需要每个消费者自己读取；专用 code 把区分放在失败词汇表已有的位置。

## Consequences

短暂的载体中断可以恢复而不结束逻辑流；确实无法服务的流在约 1.75 s 后以可区分的 code 结束。能重开新逻辑流的消费者可以通过 `gateway/stream-unavailable` 区分该情况，不能重开的消费者仍看到与之前一致的终止 `RemoteError`。Workspace 客户端正是用这一区分重开它的 follow 流。
