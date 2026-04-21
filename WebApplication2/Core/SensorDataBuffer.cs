using System.Threading.Channels;
using WebApplication2.Models;

namespace WebApplication2.Core
{
    public class SensorDataBuffer // 传感器数据缓冲区,使用Channel实现生产者消费者模式,提高性能和吞吐量
    {
        public Channel<SensorMessage> DataChannel { get; }

        public SensorDataBuffer()
        {
            var options = new BoundedChannelOptions(100000)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true, // 单一读取,优化性能
                SingleWriter = false
            };
            DataChannel = Channel.CreateBounded<SensorMessage>(options);
        }
    }
}
