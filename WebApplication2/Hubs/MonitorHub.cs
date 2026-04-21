using Microsoft.AspNetCore.SignalR;
using WebApplication2.Services;

namespace WebApplication2.Hubs
{
    public class MonitorHub(ILogger<MonitorHub> logger, DeviceStreamManager streamManager) : Hub
    {
        public override async Task OnConnectedAsync()
        {
            logger.LogInformation("【监控中心】新的上位机已接入。连接 ID: {Id}", Context.ConnectionId);
            await base.OnConnectedAsync();
        }

        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            logger.LogWarning("【监控中心】上位机已断开。原因: {Msg}", exception?.Message ?? "正常退出");
            // 注意：真实项目中，断开时需要清理该 ConnectionId 订阅过的所有设备，
            // 避免内存泄漏。可以通过自定义字典记录 ConnectionId -> List<DeviceId> 来实现。
            await base.OnDisconnectedAsync(exception);
        }

        // WPF 选中方块时调用
        public async Task SubscribeDevice(string deviceId)
        {
            streamManager.Subscribe(deviceId);
            await Groups.AddToGroupAsync(Context.ConnectionId, deviceId); // 加入专属频道
            logger.LogDebug("上位机 {Id} 订阅了设备 {Device}", Context.ConnectionId, deviceId);
        }

        // WPF 取消选中时调用
        public async Task UnsubscribeDevice(string deviceId)
        {
            streamManager.Unsubscribe(deviceId);
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, deviceId); // 退出专属频道
        }
    }
}