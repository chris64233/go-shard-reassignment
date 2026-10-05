// Package goshardreassignment 实现可恢复的分阶段分片切换：
// 迁移在复制追平并通过切换确认后才改写正式归属，进程重启或目标
// 节点短暂不可用后从持久化检查点继续推进。
package goshardreassignment
