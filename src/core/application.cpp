#include "pes/core/application.hpp"
#include "pes/modules/ping/ping.hpp"
#include "spdlog/spdlog.h"
#include "vector"

namespace core
{
  module::ping::PingModule ping_module;
  std::vector<std::string> ping_hosts {
    "8.8.8.8",
    "1.1.1.1",
    "google.com"
  };

  void run_application()
  {
    SPDLOG_INFO("Application started");
    ping_module.SetPingHosts(ping_hosts);
    ping_module.ping_start();


    while(1)
    {
       SPDLOG_INFO("Application started");
    }

      // module::ping::PingResult PingResult = ping_module.ping_once("8.8.8.8");

      // if (PingResult.success)
      // {
      //   SPDLOG_INFO("Ping module ran for: {}", PingResult.target);
      // }
      // else
      // {
      //   SPDLOG_ERROR("Ping failed: {}", PingResult.error_message);
      // }
  
  }
}