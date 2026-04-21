using EFCore.BulkExtensions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using WebApplication2.Core;
using WebApplication2.Data;
using WebApplication2.Hubs;
using WebApplication2.Models;

namespace WebApplication2.Services
{
    public class GatewayProcessorService(SensorDataBuffer buffer, IDbContextFactory<AppDbContext> dbContextFactory, IHubContext<MonitorHub> hubContext, ILogger<GatewayProcessorService> logger, DeviceStreamManager streamManager) : BackgroundService
    {
        private long tpsCounter = 0; // 每秒处理的数据量
        private long totalCounter = 0; // 总处理数据量
        private const int BatchSize = 5000; // 批量入库的数据量
        private const int MaxWaitMilliseconds = 1000; // 最大等待入库的时间

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _ = Task.Run(() => BroadcastTpsAsync(stoppingToken), stoppingToken); // 启动一个支线任务,每秒钟在屏幕上更新一次处理进度,不阻塞主数据处理任务
            await ProcessDataAsync(stoppingToken); // 主任务,负责从缓冲区拿数据并存库
        }

        // 主任务,处理数据并存库
        private async Task ProcessDataAsync(CancellationToken stoppingToken)
        {
            var batch = new List<SensorEntity>(BatchSize);
            var alerts = new List<SensorEntity>();
            var stopwatch = Stopwatch.StartNew(); // 用于控制最大等待时间的计时器

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // 设置读取超时，防止 Channel 为空时一直阻塞导致无法触发 1 秒一保存的机制
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    cts.CancelAfter(MaxWaitMilliseconds);

                    try
                    {
                        var message = await buffer.DataChannel.Reader.ReadAsync(cts.Token);

                        // 线程安全计数器
                        Interlocked.Increment(ref tpsCounter);
                        Interlocked.Increment(ref totalCounter);

                        if (streamManager.IsSubscribed(message.Id))
                        {
                            streamManager.EnqueueData(message);
                        }

                        var sensorEntity = new SensorEntity
                        {
                            SensorId = message.Id,
                            Value = message.Val,
                            Timestamp = message.Timestamp
                        };

                        batch.Add(sensorEntity);

                        if (message.Val > 90 || message.Val < 0)
                        {
                            alerts.Add(sensorEntity);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // 超时，说明当前无数据流入，跳出 catch 直接进入后续的落盘判断
                    }

                    // 触发落盘的条件：达到 5000 条，或者距离上次落盘超过 1 秒且集合内有数据
                    if (batch.Count >= BatchSize || (stopwatch.ElapsedMilliseconds >= MaxWaitMilliseconds && batch.Count > 0))
                    {
                        try
                        {
                            if (batch.Count > 0) await SaveBatchAsync(batch, stoppingToken);
                            if (alerts.Count > 0) await hubContext.Clients.All.SendAsync("ReceiveAlertBatch", alerts, stoppingToken);
                        }
                        finally
                        {
                            // 只有在这里 Clear 才是正确的
                            batch.Clear();
                            alerts.Clear();
                            stopwatch.Restart();
                        }                      
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "消费数据时发生异常。");
                }
            }
        }

        private async Task SaveBatchAsync(List<SensorEntity> batch, CancellationToken stoppingToken)
        {
            try
            {
                await using var context = await dbContextFactory.CreateDbContextAsync(stoppingToken);

                // 增加 Bulk 操作的超时时间
                var bulkConfig = new BulkConfig
                {
                    BulkCopyTimeout = 120,
                    BatchSize = BatchSize
                };

                // 使用 EFCore.BulkExtensions,在高并发场景性能更高效
                await context.BulkInsertAsync(batch, bulkConfig: bulkConfig, cancellationToken: stoppingToken);

                logger.LogInformation("成功批量写入 {Count} 条数据。", batch.Count);
            }
            catch (Exception ex)
            {
                // 可以把丢失数据暂存Log
                logger.LogError(ex, "批量插入数据库失败，丢弃当前批次 {Count} 条数据。", batch.Count);
            }
        }

        private async Task BroadcastTpsAsync(CancellationToken stoppingToken)
        {
            // 使用 PeriodicTimer 替代 Task.Delay，时间轴更精准，不会因为 GC 等导致时间偏移累加
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                // 原子提取并重置 TPS
                var currentTps = Interlocked.Exchange(ref tpsCounter, 0);
                var total = Interlocked.Read(ref totalCounter);

                var currentBacklog = buffer.DataChannel.Reader.Count;
                try
                {
                    logger.LogInformation(
                        "TPS: {Tps}    队列积压: {Backlog}    总计处理: {Total}",
                        currentTps,
                        currentBacklog,
                        total);

                    await hubContext.Clients.All.SendAsync("ReceiveStatus", new
                    {
                        Tps = currentTps,
                        TotalProcessed = total,
                        ServerTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    }, stoppingToken);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "SignalR 推送 TPS 信息失败。");
                }
            }
        }
    }
}
