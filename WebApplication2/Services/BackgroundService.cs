using Microsoft.AspNetCore.SignalR;
using WebApplication2.Hubs;

namespace WebApplication2.Services
{
    public class DetailPushService(IHubContext<MonitorHub> hubContext, DeviceStreamManager streamManager) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 核心频率控制：100毫秒 (10Hz)
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                // 拿到所有“被订阅设备”在这 0.1 秒内的数组
                var snapshots = streamManager.GetAndClearAllSnapshots();

                if (snapshots.Count == 0) continue;

                // 并行推流：只推给订阅了该频道的组
                var tasks = snapshots.Select(kvp =>
                    hubContext.Clients.Group(kvp.Key).SendAsync("ReceiveDetailBatch", kvp.Value, stoppingToken)
                );

                await Task.WhenAll(tasks);
            }
        }
    }
}