using System.Collections.Concurrent;
using WebApplication2.Models;

namespace WebApplication2.Services
{
    public class DeviceStreamManager
    {
        // 记录设备的订阅人数 (可能有多个 WPF 客户端同时看同一个设备)
        private readonly ConcurrentDictionary<string, int> _subscribers = new();

        // 为正在被观看的设备开辟的数据队列
        private readonly ConcurrentDictionary<string, ConcurrentQueue<SensorMessage>> _buffers = new();

        public void Subscribe(string deviceId)
        {
            _subscribers.AddOrUpdate(deviceId, 1, (_, count) => count + 1);
            _buffers.GetOrAdd(deviceId, _ => new ConcurrentQueue<SensorMessage>());
        }

        public void Unsubscribe(string deviceId)
        {
            if (_subscribers.TryGetValue(deviceId, out var count))
            {
                if (count <= 1)
                {
                    _subscribers.TryRemove(deviceId, out _);
                    _buffers.TryRemove(deviceId, out _); // 没人看了，销毁队列释放内存
                }
                else
                {
                    _subscribers.TryUpdate(deviceId, count - 1, count);
                }
            }
        }

        // 极速判断：这个设备现在有人看吗？(用于在主处理循环中拦截)
        public bool IsSubscribed(string deviceId) => _subscribers.ContainsKey(deviceId);

        // 往队列里塞数据 (无锁、极速)
        public void EnqueueData(SensorMessage message)
        {
            if (_buffers.TryGetValue(message.Id, out var queue))
            {
                queue.Enqueue(message);
            }
        }

        // 提取并清空 0.1 秒内的数据供推送
        public Dictionary<string, List<SensorMessage>> GetAndClearAllSnapshots()
        {
            var snapshots = new Dictionary<string, List<SensorMessage>>();
            foreach (var kvp in _buffers)
            {
                if (!kvp.Value.IsEmpty)
                {
                    var list = new List<SensorMessage>();
                    while (kvp.Value.TryDequeue(out var msg))
                    {
                        list.Add(msg);
                    }
                    if (list.Count > 0) snapshots[kvp.Key] = list;
                }
            }
            return snapshots;
        }
    }
}