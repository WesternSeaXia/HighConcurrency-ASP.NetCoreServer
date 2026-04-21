# 高并发服务器

一个基于 [ASP.NET](https://asp.net/) Core 的高性能传感器数据接收与分发服务，专为大规模物联网数据接入设计，单机可稳定支撑 **40,000+ QPS** 的写入吞吐，并实时推送订阅数据至监控上位机。

------

## 📌 项目定位

本项目是整体解决方案的数据中枢，承接来自「WPF 高并发数据模拟器」的海量传感器数据，完成：

- 高速接收与缓冲
- 批量持久化至 SQL Server
- 基于 SignalR 的实时数据推流

与另外两个项目协同工作：

| 项目                                               | 职责                     |
| :------------------------------------------------- | :----------------------- |
| [WPF高并发数据模拟器](https://./link-to-simulator) | 产生压测流量             |
| **高并发服务器（本项目）**                         | **数据接收、存储与转发** |
| [WPF上位机监控平台](https://./link-to-monitor)     | 可视化展示与订阅         |

------

## 🚀 性能指标

- **写入 QPS**：实测 > 40,000 条/秒（取决于硬件与数据库配置）
- **数据积压**：Channel 容量 100,000 条，高负载下自动丢弃最旧数据
- **实时推送**：订阅设备数据以 **100ms 间隔** 批量推送到上位机

------

## 🧱 架构设计

text

```
┌─────────────────┐     HTTP POST      ┌─────────────────────────────────┐
│  数据模拟器集群  │ ──────────────────▶ │          IngestionController     │
└─────────────────┘                    │               ↓                  │
                                       │      SensorDataBuffer (Channel)  │
                                       │               ↓                  │
                                       │   GatewayProcessorService (后台)  │
                                       │      ├─ 批量入库 (EF Core Bulk)   │
                                       │      ├─ 订阅判断 → DeviceStreamManager │
                                       │      └─ 推送告警 (SignalR)        │
                                       │                                  │
                                       │   DetailPushService (100ms 定时)  │
                                       │      └─ 批量推流订阅数据           │
                                       └─────────────────────────────────┘
                                                 │
                                                 ▼ SignalR
                                       ┌─────────────────┐
                                       │  WPF 上位机集群  │
                                       └─────────────────┘
```



------

## 🔧 核心组件详解

### 1. `SensorDataBuffer` – 内存缓冲区

csharp

```
Channel<SensorMessage> DataChannel = Channel.CreateBounded<SensorMessage>(
    new BoundedChannelOptions(100_000)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleReader = true,
        SingleWriter = false
    });
```



- 使用 .NET `System.Threading.Channels` 实现 **无锁生产者-消费者队列**。
- 容量设置为 100,000，当缓冲区满时自动丢弃最旧数据（`DropOldest`），防止背压阻塞上游写入。
- `SingleReader = true` 优化读取性能，因为仅有一个后台服务消费数据。

### 2. `IngestionController` – 极简数据入口

csharp

```
[HttpPost("upload")]
public IActionResult Upload([FromBody] SensorMessage message)
{
    buffer.DataChannel.Writer.TryWrite(message);
    return Ok();
}
```



- 不做任何业务逻辑处理，直接将数据写入 Channel，**耗时 ≤ 0.1ms**。
- 保证了极高的接收吞吐量。

### 3. `GatewayProcessorService` – 数据消费与持久化

该后台服务负责从 Channel 中读取数据，并执行以下操作：

#### a. 批量读取与聚合

- 每次读取最多等待 **1 秒** 或积累 **5000 条** 数据后触发批量入库。
- 使用 `CancellationTokenSource.CancelAfter` 实现可中断的异步等待。

#### b. 订阅数据分流

csharp

```
if (streamManager.IsSubscribed(message.Id))
{
    streamManager.EnqueueData(message);
}
```



- `DeviceStreamManager` 通过 `ConcurrentDictionary` 快速判断设备是否被订阅。
- 若被订阅，将数据写入该设备的专属队列，供后续推流使用。

#### c. 批量写入数据库

- 采用 **EF Core BulkExtensions** 的 `BulkInsertAsync`，绕过 ChangeTracker，将 5000 条数据一次性写入 SQL Server。
- 配置 `BulkCopyTimeout = 120` 秒，避免大批量写入超时。
- 若写入失败，丢弃当前批次并在日志中记录（可根据需要改为持久化到本地文件）。

#### d. 实时告警推送

- 当数据值 `>90` 或 `<0` 时，收集告警数据并通过 SignalR 全量广播 `ReceiveAlertBatch`。

#### e. 性能计数器

- 使用 `Interlocked` 维护 TPS 和总处理量，每秒通过 `PeriodicTimer` 精准推送至所有连接的监控端。

### 4. `DeviceStreamManager` – 订阅管理与数据缓存

csharp

```
public class DeviceStreamManager
{
    private readonly ConcurrentDictionary<string, int> _subscribers;      // 设备 → 订阅人数
    private readonly ConcurrentDictionary<string, ConcurrentQueue<SensorMessage>> _buffers; // 设备 → 数据队列

    public bool IsSubscribed(string deviceId) => _subscribers.ContainsKey(deviceId);

    public void EnqueueData(SensorMessage message) { ... }

    public Dictionary<string, List<SensorMessage>> GetAndClearAllSnapshots() { ... }
}
```



- **零锁设计**：所有字典操作均使用 `ConcurrentDictionary`，无显式锁。
- **按需创建队列**：仅当有客户端订阅时才为该设备分配队列，节省内存。
- **原子快照提取**：`GetAndClearAllSnapshots` 一次性清空并返回所有队列数据，避免逐条推送带来的网络开销。

### 5. `DetailPushService` – 高频批量推流

- 以 **100ms 为周期** 运行，调用 `GetAndClearAllSnapshots` 获取这段时间内积累的订阅数据。

- 通过 SignalR 的 **Group** 机制，将数据精准推送给订阅了该设备的上位机：

  csharp

  ```
  await hubContext.Clients.Group(deviceId).SendAsync("ReceiveDetailBatch", messages);
  ```

  

- 并行推送所有设备组，充分利用网络 I/O。

### 6. SignalR Hub – `MonitorHub`

- 管理客户端连接与设备订阅关系。
- 提供 `SubscribeDevice` / `UnsubscribeDevice` 方法，上位机调用后加入/退出对应的 SignalR Group。
- **注意**：实际生产环境需自行维护 `ConnectionId → 订阅设备列表` 映射，以便在连接断开时自动清理订阅。

------

## ⚙️ 配置说明

### 数据库连接字符串

在 `appsettings.json` 中配置 SQL Server 连接字符串：

json

```
{
  "ConnectionStrings": {
    "HighConcurrencyDb": "Server=localhost;Database=HighConcurrencyDb;User Id=sa;Password=YourPassword;TrustServerCertificate=True;"
  }
}
```



### 数据库表结构

启动时程序会自动调用 `context.Database.EnsureCreated()` 创建数据库和表（基于 `SensorEntity` 模型）。
表名默认为 `Sensors`，包含字段：

| 字段      | 类型     | 说明            |
| :-------- | :------- | :-------------- |
| PkId      | bigint   | 自增主键        |
| SensorId  | nvarchar | 传感器 ID       |
| Value     | float    | 数值            |
| Timestamp | bigint   | Unix 毫秒时间戳 |

### 关键参数调整

| 参数         | 位置                                          | 默认值   | 说明                     |
| :----------- | :-------------------------------------------- | :------- | :----------------------- |
| Channel 容量 | `SensorDataBuffer`                            | 100,000  | 根据内存和峰值流量调整   |
| 批量入库大小 | `GatewayProcessorService.BatchSize`           | 5,000    | 增大可减少数据库交互次数 |
| 最大等待时间 | `GatewayProcessorService.MaxWaitMilliseconds` | 1,000 ms | 控制低流量时的写入延迟   |
| 推流频率     | `DetailPushService`                           | 100 ms   | 可根据上位机刷新率调整   |

------

## 🔬 性能优化要点

### 1. 避免锁竞争

- 使用 `Channel` 替代 `BlockingCollection`，其内部基于无锁算法实现。
- 所有计数器使用 `Interlocked` 操作。
- `ConcurrentDictionary` + `ConcurrentQueue` 保证订阅数据流转无锁。

### 2. 数据库写入优化

- **批量操作**：`BulkInsert` 相比逐条 `Add` + `SaveChanges` 性能提升 **50~100 倍**。
- **关闭 Change Tracking**：`UseQueryTrackingBehavior(NoTracking)`。
- **使用 DbContextFactory**：在 `BackgroundService` 中避免 DbContext 生命周期问题。

### 3. 网络 IO 优化

- HTTP 数据接收与数据库写入完全异步，不阻塞请求线程。
- SignalR 推送采用 **Group 广播**，减少遍历客户端列表的开销。
- 订阅数据以 **批量数组** 形式推送，而非逐条发送，降低网络包数量。

### 4. 背压处理

- 当 Channel 写满时，自动丢弃最旧数据，保证系统不会因处理能力不足而崩溃。
- 生产环境中可结合监控告警，当丢弃率上升时动态扩容或限流。

------

## 📌 关联项目

- **[WPF 高并发数据模拟器]**
  用于生成压测流量，验证服务器吞吐能力。
- **[WPF 上位机监控平台]**
  连接至本服务器的 SignalR Hub，订阅并实时展示传感器数据。
- <p align="center"> <img src="./Assets/main2.png" alt="关联项目" width="900"> </p>

------

## 📄 许可证

[MIT License]