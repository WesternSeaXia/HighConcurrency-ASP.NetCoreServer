using Microsoft.EntityFrameworkCore;
using WebApplication2.Models;

namespace WebApplication2.Data
{
    public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
    {
        // " => Set<T>() "可以防止空引用异常
        public DbSet<SensorEntity> Sensors => Set<SensorEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // 创建索引,提高查询性能
            modelBuilder.Entity<SensorEntity>()
            .HasIndex(e => e.Timestamp);

            modelBuilder.Entity<SensorEntity>()
                .HasIndex(e => e.SensorId);
        }
    }
}
