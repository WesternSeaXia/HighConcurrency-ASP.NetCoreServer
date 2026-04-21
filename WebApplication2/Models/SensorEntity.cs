using System.ComponentModel.DataAnnotations;

namespace WebApplication2.Models
{
    public record SensorMessage(string Id, double Val, long Timestamp);

    public class SensorEntity
    {
        [Key]
        public long PkId { get; set; }
        public string SensorId { get; set; } = null!;
        public double Value { get; set; }
        public long Timestamp { get; set; }
    }
}
