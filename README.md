# go-shard-reassignment

本项目实现**可恢复的分阶段切换**（resumable phased cutover）分片迁移：
复制未追平时绝不改变正式归属，进程重启或目标节点短暂不可用后，
从持久化检查点继续，不重复切换、不丢失水位。

## 设计

### 状态机

```
COPYING --(复制水位达到冻结范围)--> AWAITING_CUTOVER
AWAITING_CUTOVER --(BeginCutover 签发租约)--> CUTTING_OVER
CUTTING_OVER --(ConfirmCutover 租约匹配)--> COMPLETED   # 此刻才改写正式归属
CUTTING_OVER --(AbortCutover / 目标下线 / 重启恢复)--> AWAITING_CUTOVER
任意非终态 --(FailMigration)--> FAILED                  # 回滚到原归属
```

- 发起迁移时冻结源、目标、迁移版本与检查点范围 `[From, To]`（`StartMigration`）。
- 正式归属只在 `ConfirmCutover` 成功时变化，且归属改写与任务完成在
  同一个持久化事务内落盘（`store.Save` 原子写）。

### 复制回执

- 回执必须携带迁移版本与检查点（`Receipt`）。
- 重复回执幂等；旧版本（`ErrStaleVersion`）、倒退水位
  （`ErrWatermarkRegression`）、超出冻结范围（`ErrOutOfRange`）被拒绝。
- 目标节点失联时回执被拒绝（`ErrNodeUnavailable`），调用方可重试或暂停，
  部分复制绝不会被误报为完成。

### 并发与恢复

- 所有状态变更在单把互斥锁下完成并整体原子落盘，切换、节点下线、
  迁移失败与恢复并发时只有一个有效结果。
- 每次切换尝试签发独立租约（`lease = mig-xxx/attempt-N`），过期租约的
  确认被拒绝（`ErrLeaseMismatch`）；使用同一租约重复确认是幂等的。
- 失败回滚到原归属，已确认的复制水位与失败原因仍可查询。
- 重启恢复（`NewManager`）：停留在 `CUTTING_OVER` 的任务回退到
  `AWAITING_CUTOVER` 并作废租约，已确认水位保留。

### 幂等与冲突

- 相同 `RequestID` 重复提交返回原任务；源/目标/版本/范围任一变化
  返回 `ErrConflict`。
- 同一分片存在另一条非终态迁移时拒绝发起新迁移（`ErrConflict`）。

### 查询

`Status(shardID)` 返回当前归属、复制水位、目标水位、迁移版本、
租约/尝试次数、失败原因与审计历史；`Migration(id)` 按任务 ID 查询；
`Owner(shardID)` 返回当前正式归属。

## 存储

- `FileStore`：JSON 文件持久化，临时文件 + rename 原子写，用于崩溃恢复。
- `MemoryStore`：内存实现，用于测试。

## 测试

```sh
go test ./...
```

覆盖：幂等发起与冲突、单调回执与追平、迟到回执、倒退水位、节点故障、
切换竞态（过期租约/唯一有效结果）、重启恢复（文件存储）、失败回滚与
进度保留、查询视图与审计历史。
