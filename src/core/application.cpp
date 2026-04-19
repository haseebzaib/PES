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

  void run_application()
  {
    SPDLOG_INFO("Application started");

    std::jthread NetworkThread(network_thread);
    std::jthread SensorThread(sensor_thread);
   
    ping_module.ping_start();




  }
}