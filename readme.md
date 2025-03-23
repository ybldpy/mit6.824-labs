# MIT-6.824 Labs

# MapReduce

## 核心

本项目的核心主要在于 **Coordinator** 对任务的分配与对崩溃（crash）Worker 的处理策略。

------

## Coordinator 端设计

### 1. 任务初始化

- 将所有任务初始化为“空闲”状态，等待分配。

### 2. 接收并处理 Worker 请求

- **Worker 注册请求**：
  - 分配 Worker ID 并进行注册。
  - 开启监控线程，如果 Worker 超过 10s 没有请求发送，则自动注销该 Worker。
- **Worker 获取任务请求**：
  - 刷新 Worker 的心跳时间戳。
  - 如果存在空闲任务，将任务分配给该 Worker；否则返回空任务，提示 Worker 稍后再尝试。
- **Worker 完成任务请求**：
  - Worker 提交已完成的任务结果。
  - 如果发现提交者已不在注册表中，视为崩溃 Worker，拒绝任务提交。
  - 如果 Worker 正常，刷新其心跳并记录任务完成状态。
- **Worker 心跳请求**：
  - 更新 Worker 最近活动时间，维持活跃状态。

### 3. Coordinator 关闭条件

- 当所有 Map 与 Reduce 任务均已完成后，自动关闭。

------

## Worker 端流程

### 1. 注册

- 向 Coordinator 发送注册请求，获得唯一 Worker ID。

### 2. 请求任务

- 向 Coordinator 询问任务。
- 若返回空任务，等待一段时间后重试。

### 3. 执行任务

- 获取任务后，启动独立 goroutine 定时向 Coordinator 发送心跳，防止因任务执行时间过长被误判为崩溃。
- 任务完成后，停止心跳 goroutine，向 Coordinator 提交任务结果。

### 4. 循环处理

- 持续循环请求任务并执行，直到所有任务处理完成或无法连接到 Coordinator（Coordinator 关闭）。



## 测试结果

![image-20250323130515038](img/mapreduce.png)





## Raft

## Part 2A: Leader Election

### 测试结果

18个server，迭代800次，每次迭代随机4台机器掉线

![image-20250323130515038](img/2a.png)





