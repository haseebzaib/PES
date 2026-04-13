#pragma once
#include "pes/modules/ping/ping.hpp"
#include <thread>

namespace core 
{

    extern module::ping::PingModule ping_module;

    extern std::jthread NetworkThread;
    
    void run_application();
}