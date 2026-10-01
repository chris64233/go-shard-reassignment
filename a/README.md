# go-shard-reassignment

分片安全迁移服务：登记分片、节点与迁移任务，并保证一个分片在任意时刻只有一个有效归属。

## 核心概念

- **节点（Node）**：状态为 `active` / `draining` / `offline`，只有 `active` 节点可以接收分片。
- **分片（Shard）**：记录容量与正式归属节点（`OwnerID`），迁移完成前归属不变。
- **迁移任务（Migration）**：记录源节点、目标节点、迁移原因与状态，状态机为
  `pending -> in_progress -> completed`，`pending`/`in_progress` 可转为 `failed` 并记录失败原因。
- **历史（HistoryEntry）**：按时间顺序记录分片创建、迁移提交/开始/完成/失败与节点状态变更，
  可追踪分片在节点之间的完整迁移过程。

## 安全性保证

- 重复节点 ID、重复分片 ID、未知归属节点、非正容量都会被拒绝。
- 同一分片最多存在一条有效迁移（`pending`/`in_progress`）；重复提交相同
  （分片、目标节点、原因）的请求会返回原任务，实现幂等。
- 目标节点必须处于可接收状态（`active`），且不能是当前归属节点。
- 迁移完成前正式归属保持为源节点；查询分片时临时目标节点永远不会被显示为归属。
- 分片查询返回明确状态：`active` / `migrating` / `offline`（归属节点已下线）。
- 迁移失败后归属仍留在源节点，失败原因可通过 `GetMigration` 查询，之后可重新发起迁移完成恢复。

## 使用示例

```go
svc := goshardreassignment.NewService()

svc.CreateNode("node-a", goshardreassignment.NodeStatusActive)
svc.CreateNode("node-b", goshardreassignment.NodeStatusActive)
svc.CreateShard("shard-1", 1024, "node-a")

mig, _ := svc.SubmitMigration("shard-1", "node-b", "rebalance")
svc.StartMigration(mig.ID)
svc.CompleteMigration(mig.ID) // 直到这里归属才切换到 node-b

view, _ := svc.GetShard("shard-1")        // OwnerID = node-b, Status = active
history := svc.ShardHistory("shard-1")    // 完整的归属变更轨迹
```

## 测试

```sh
go test ./...
```

测试覆盖：基础状态流转、重复/冲突迁移、失败原因记录与失败恢复、目标节点可接收校验、
节点下线与迁移中状态查询，以及分片/节点/任务/历史列表查询。
