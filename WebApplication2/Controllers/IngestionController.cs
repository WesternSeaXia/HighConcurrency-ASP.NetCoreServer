using Microsoft.AspNetCore.Mvc;
using WebApplication2.Core;
using WebApplication2.Models;

namespace WebApplication2.Controllers
{
    [ApiController]
    [Route("api/[Controller]")]
    public class IngestionController(SensorDataBuffer buffer) : ControllerBase
    {
        [HttpPost]
        [Route("upload")]
        public IActionResult Upload([FromBody] SensorMessage message)
        {
            // 收到数据直接向缓冲区写入数据
            buffer.DataChannel.Writer.TryWrite(message);

            return Ok();
        }
    }
}
