using Microsoft.EntityFrameworkCore;
using WebApplication2.Core;
using WebApplication2.Data;
using WebApplication2.Hubs;
using WebApplication2.Services;

var builder = WebApplication.CreateBuilder(args);

// 注册基础服务
builder.Services.AddControllers();
builder.Services.AddSignalR();

// 注册 EF Core (使用 DbContextFactory 以供 BackgroundService 多线程安全调用)
builder.Services.AddDbContextFactory<AppDbContext>(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("HighConcurrencyDb"));
    options.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking); // 纯写入读取,关闭跟踪以提高性能
});

builder.Services.AddSingleton<SensorDataBuffer>(); // 注册传感器数据缓冲区
builder.Services.AddHostedService<GatewayProcessorService>(); // 注册后台数据处理服务
builder.Services.AddSingleton<DeviceStreamManager>();
builder.Services.AddHostedService<DetailPushService>();

var app = builder.Build();

// 数据库初始化,确保数据库和表存在,如果不存在则创建
using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    context.Database.EnsureCreated();
}

app.MapControllers();
app.MapHub<MonitorHub>("/monitorHub");

using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();

    logger.LogInformation("正在检查数据库状态...");

    // 确保数据库和表已经建立
    context.Database.EnsureCreated();

    try
    {
        // 打开底层连接
        var connection = context.Database.GetDbConnection();
        connection.Open();

        logger.LogWarning("！！！程序实际连接的数据库是: {ConnString} ！！！", connection.ConnectionString);

        using var command = connection.CreateCommand();
        // 142 IQ 的专属查询法：直接查 SQL Server 的分区统计表，耗时 0 毫秒，且绝对不锁表
        command.CommandText = "SELECT ISNULL(SUM(row_count), 0) FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID('Sensors') AND index_id IN (0,1)";

        var result = command.ExecuteScalar();
        long rowCount = Convert.ToInt64(result);

        logger.LogInformation("==========================================");
        logger.LogInformation("🚀 【网关启动完毕】当前 [Sensors] 表中已囤积数据：{RowCount} 条！", rowCount);
        logger.LogInformation("==========================================");
    }
    catch (Exception ex)
    {
        logger.LogWarning(ex, "无法读取表数据量，表可能为空或初始化未完成。");
    }
}

app.Run();
