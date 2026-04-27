#pragma once
#include "pes/storage/redis_storage.hpp"
#include "pes/storage/sensor_live_publisher.hpp"
#include "pes/storage/sensor_storage.hpp"
#include "pes/modules/ping/ping.hpp"
#include <thread>

namespace core 
{

    extern module::ping::PingModule ping_module;
    extern storage::RedisStorage redis_storage;
    extern storage::SensorStorage sensor_storage;
    extern storage::SensorLivePublisher sensor_live_publisher;

    extern std::jthread NetworkThread;
    
    void run_application();
}
