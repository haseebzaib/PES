#include "pes/core/application.hpp"
#include "pes/modules/ping/ping.hpp"
#include "pes/core/networking_thread.hpp"
#include "pes/core/sensor_thread.hpp"
#include "spdlog/spdlog.h"
#include "vector"
#include <thread>

namespace core
{

  module::ping::PingModule ping_module;
  pes::storage::RedisStorage redis_storage({
    .host = "127.0.0.1",
    .port = 6379,
    .database = 0,
  });

  void run_application()
  {
    SPDLOG_INFO("Application started");

    if (!redis_storage.Initialize())
    {
      SPDLOG_ERROR("Redis storage initialization failed");
    }
    else
    {
      SPDLOG_INFO("Redis storage initialized");
    }

    std::jthread NetworkThread(network_thread);
    std::jthread SensorThread(sensor_thread);
   
    ping_module.ping_start();




  }
}
