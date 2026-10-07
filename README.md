# go-shard-reassignment：可恢复的分阶段切换

在分片迁移基础上实现“可恢复的分阶段切换”（checkpoint cutover）：复制未追平前
绝不改变正式归属，进程重启或目标节点短暂不可用后，从持久化检查点继续推进。

## 阶段模型

```
COPYING --> CAUGHT_UP --> CUTTING_OVER --> COMPLETED
    |            ^   ^         |
    |            |   |         | 目标失联 / 主动放弃 / 崩溃恢复
    |            |   +---------+
    +------------+  失败（任意非终态）--> FAILED

COPYING / CAUGHT_UP 且原目标明确不可继续时，可申请重新规划：

    原迁移 --RequestReplan--> 冻结重新规划（PENDING）
      |--ConfirmReplan--> 原迁移进入 SUPERSEDED，生成新版本迁移（源不变）
      +--AbortReplan---> 新目标准备失败，保留原迁移与已确认进度
```

- `COPYING` 复制中：发起迁移时冻结源、目标、迁移版本与检查点范围 `[From, To]`。
- `CAUGHT_UP` 追平待切换：复制水位达到冻结范围上限，正式归属仍属源节点。
- `CUTTING_OVER` 切换中：签发切换租约，等待确认。
- `COMPLETED` 已完成：切换确认后正式归属才改写为目标节点。
- `FAILED` 已失败：回滚到原归属，已确认进度与失败原因保留可查。
- `SUPERSEDED` 已被取代：重新规划确认后旧迁移进入该终态，旧目标停止领取，
  迟到的复制/切换回执只记入历史，不能完成新版本或改变正式归属。

## 迁移途中重新规划

- **申请前提**：迁移尚未切换正式归属（`COPYING`/`CAUGHT_UP`）且原目标明确
  不可继续（已失联）。`RequestReplan` 冻结原迁移版本、已确认检查点、原目标、
  候选新目标与失败原因。
- **可验证的进度复用**：`ReuseCheckpoint` 不得超过原迁移的已确认水位——系统
  绝不把原目标上报的最高数字当成新目标已拥有的数据；`ConfirmReplan` 要求
  新目标出示与冻结摘要一致的证明（`ErrDigestMismatch` 则拒绝），无法证明
  一致的部分从安全位置重新复制。
- **确认生效**：生成新的迁移版本（`Version+1`），源归属与正式归属保持不变，
  旧迁移进入 `SUPERSEDED`，旧目标停止领取；新版本从已验证的检查点继续。
- **单条主线**：重新规划、原目标恢复、节点下线与切换确认并发时，只有一条
  迁移主线生效（切换先确认则重新规划失效，反之旧迁移不可再切换）。新目标
  准备失败时 `AbortReplan` 保留原迁移及已确认进度，不会留下两个可切换目标。
- **幂等与冲突**：相同重新规划号且内容一致返回原结果；检查点、候选目标或
  迁移版本变化返回 `ErrConflict`。
- **可查询性**：`Status`/`Migration` 展示版本关系（`ReplanOf`/`ReplannedBy`）、
  进度复用依据（`ReuseCheckpoint`/`ReuseDigest`）、待确认规划与最终目标。

## 关键保证

- **冻结参数**：`StartMigration` 冻结源/目标/版本/检查点范围；同一 `RequestID`
  重复提交返回原任务，参数变化返回 `ErrConflict`；同一分片同时只允许一条有效迁移。
- **回执校验**：回执必须携带迁移版本与单调检查点。重复回执幂等；旧版本
  （`ErrStaleVersion`）、倒退水位（`ErrWatermarkRegression`）、超出冻结范围
  （`ErrOutOfRange`）均被拒绝并记入审计；目标失联时返回 `ErrNodeUnavailable`，
  可重试或暂停，绝不把部分复制误报为完成。
- **切换唯一性**：只有水位达到冻结范围上限才能 `BeginCutover`；`ConfirmCutover`
  仅在租约匹配且处于切换中时改写正式归属，归属改写与任务完成在同一持久化事务
  内提交。切换、节点下线、失败并发时只有一个有效结果；相同租约重复确认幂等。
- **崩溃恢复**：所有状态整体原子落盘（临时文件 + rename）。重启后停留在
  “切换中”的任务安全回退到“追平待切换”并作废租约，不重复切换、不丢失检查点。
- **可查询性**：`Status`/`Migration` 展示当前归属、复制水位、目标水位、迁移版本、
  租约/尝试次数与审计历史；`Owner` 返回当前正式归属。

## API 概览

| 方法 | 说明 |
| --- | --- |
| `NewManager(store)` | 加载持久化状态并执行崩溃恢复 |
| `RegisterShard` / `Owner` | 登记 / 查询分片正式归属 |
| `SetNodeUp` / `SetNodeDown` | 节点上下线；切换中的目标失联会安全回退 |
| `StartMigration` | 发起迁移（幂等，冲突检测） |
| `SubmitReceipt` | 处理复制回执（版本/水位/范围校验） |
| `BeginCutover` / `ConfirmCutover` / `AbortCutover` | 切换租约的签发、确认与放弃 |
| `FailMigration` | 失败回滚，保留进度与原因 |
| `RequestReplan` / `ConfirmReplan` / `AbortReplan` | 迁移途中重新规划目标节点 |
| `Replan` | 按重新规划号查询规划记录 |
| `Status` / `Migration` | 查询归属、水位、版本、租约/尝试与审计历史 |

## 存储

- `Store` 接口：`Load` / `Save`。
- `MemoryStore`：内存实现，用于测试。
- `FileStore`：JSON 文件持久化，临时文件 + rename + 目录 fsync 保证原子性。

## 测试

```sh
go test ./...
```

覆盖：幂等提交与参数冲突、回执单调性与追平、节点故障阻断与恢复、切换竞态
唯一结果、切换放弃与重试、重启恢复（不重复切换、不丢检查点）、迟到回执、
失败回滚保留进度、状态查询内容、重新规划冻结与幂等/冲突、复用摘要校验、
新旧版本单主线竞态、放弃后保留原迁移、迟到回执只进历史、跨重启的版本关系
与复用依据查询。
