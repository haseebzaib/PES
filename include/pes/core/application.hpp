#pragma once
#include "pes/storage/redis_storage.hpp"
#include "pes/modules/ping/ping.hpp"
#include <thread>

namespace core 
{

    extern module::ping::PingModule ping_module;
    extern pes::storage::RedisStorage redis_storage;

    extern std::jthread NetworkThread;
    
    void run_application();
}
