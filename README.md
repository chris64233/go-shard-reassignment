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
```

- `COPYING` 复制中：发起迁移时冻结源、目标、迁移版本与检查点范围 `[From, To]`。
- `CAUGHT_UP` 追平待切换：复制水位达到冻结范围上限，正式归属仍属源节点。
- `CUTTING_OVER` 切换中：签发切换租约，等待确认。
- `COMPLETED` 已完成：切换确认后正式归属才改写为目标节点。
- `FAILED` 已失败：回滚到原归属，已确认进度与失败原因保留可查。

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
失败回滚保留进度、状态查询内容。
