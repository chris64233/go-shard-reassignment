# go-shard-reassignment

分片安全迁移服务：登记分片、节点与迁移任务，保证任一时刻一个分片只有一个有效归属。

## 功能

- **节点管理**：`RegisterNode` 注册节点（默认 `active`），`UpdateNodeStatus` 在
  `active` / `draining` / `offline` 间切换；重复节点 ID 与非法状态被拒绝。
- **分片登记**：`CreateShard` 记录分片容量与初始归属节点；容量必须为正，
  归属节点必须已注册，重复分片 ID 被拒绝。
- **迁移任务**：`SubmitMigration` 保存源节点、目标节点、迁移原因，任务初始为
  `pending`；目标节点必须处于可接收状态（`active`），源节点必须是当前归属。
- **状态流转**：`StartMigration`（pending → in_progress）、
  `CompleteMigration`（in_progress → completed，此刻才切换正式归属）、
  `FailMigration`（pending/in_progress → failed，记录失败原因，归属不变）。
- **并发安全与幂等**：同一分片同时只允许一条有效迁移（pending/in_progress）；
  携带相同 `RequestKey` 的重复提交直接返回原任务，不产生新任务。
- **明确的状态查询**：`GetShard` 返回 `ShardView`，有效状态为
  `stable` / `migrating` / `owner_offline`；迁移期间归属仍是源节点，
  临时目标节点只出现在 `PendingTargetNodeID`，绝不会被显示成已完成的归属。
- **历史追踪**：`ShardHistory` 按时间顺序记录分片创建、迁移开始、完成、失败事件，
  可完整追踪分片在节点间的移动过程。

## 快速开始

```go
svc := goshardreassignment.NewService()
svc.RegisterNode("node-a")
svc.RegisterNode("node-b")
svc.CreateShard("shard-1", 1024, "node-a")

mig, _ := svc.SubmitMigration(goshardreassignment.MigrationRequest{
    RequestKey:   "req-1",
    ShardID:      "shard-1",
    SourceNodeID: "node-a",
    TargetNodeID: "node-b",
    Reason:       "rebalance",
})
svc.StartMigration(mig.ID)

// 完成前归属仍是 node-a
view, _ := svc.GetShard("shard-1") // view.OwnerNodeID == "node-a", EffectiveStatus == "migrating"

svc.CompleteMigration(mig.ID)
// 完成后归属切换为 node-b
```

## 查询接口

| 方法 | 说明 |
| --- | --- |
| `GetNode` / `ListNodes` | 查询节点及其状态 |
| `GetShard` / `ListShards` | 查询分片视图（含有效状态与临时目标） |
| `GetMigration` / `ListMigrations` | 查询迁移任务（含失败原因） |
| `ShardHistory` | 查询分片的历史变更事件 |

## 测试

```sh
go test ./...
```

测试覆盖：基础状态流转（含非法流转拒绝）、重复/幂等迁移提交、
失败后归属不变与失败恢复重试、节点下线状态查询、历史变更追踪。

## 代码结构

- `types.go` — 节点、分片、迁移任务、历史事件等领域类型
- `errors.go` — 可被 `errors.Is` 判断的哨兵错误
- `service.go` — 内存实现的 `Service`，读写锁保证并发安全
- `service_test.go` — 状态流转、重复迁移、失败恢复等测试
