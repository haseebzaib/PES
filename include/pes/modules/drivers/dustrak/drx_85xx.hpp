#pragma once

#include "cstdint"
#include "string"
#include "vector"
#include <istream>
#include "pes/modules/protocols/serial/serial.hpp"

namespace module::drivers::dustrak
{

    class drx85xx
    {
    public:
        /**
         * json packet
         * {
         *   Service_code: ""
         *   Data: "xxx.xxx mg/m3"
         * }
         */
        drx85xx();
        ~drx85xx();
        drx85xx(const drx85xx &) = delete;
        drx85xx &operator=(const drx85xx &) = delete;
        void init(module::protocols::serial::serialPort& sePort_);
        void loop(std::string json_packet);

    private:
        void ASRVCK();
        void ASPOLL();
        void AQDATA();
        void ASDATAxx();
        std::string json_packet_;
        module::protocols::serial::serialPort* serialPort_ = nullptr;
    };

}