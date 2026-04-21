#include "pes/modules/drivers/dustrak/drx_85xx.hpp"
#include "spdlog/spdlog.h"
#include <istream>

namespace module::drivers::dustrak
{

    drx85xx::drx85xx() = default;

    drx85xx::~drx85xx()
    {
        AQDATA();
    }

    void drx85xx::init(module::protocols::serial::serialPort &sePort_)
    {
        serialPort_ = &sePort_;
    }

    void drx85xx::loop(std::string json_packet)
    {
        if (!serialPort_)
        {
            SPDLOG_ERROR("No serial port assigned");
            return;
        }

        ASRVCK();
        ASPOLL();


    }

    void drx85xx::ASRVCK()
    {
        if (!serialPort_)
        {
            SPDLOG_ERROR("No serial port assigned");
            return;
        }

        if (!serialPort_->write("ASRVCK\r\n"))
        {
            SPDLOG_ERROR("ASRVCK: write failed");
            return;
        }

        std::string response;
        serialPort_->read_line(response);


        SPDLOG_INFO("ASRVCK service codes: {}", response);
    }

    void drx85xx::ASPOLL()
    {
        if (!serialPort_)
        {
            SPDLOG_ERROR("No serial port assigned");
            return;
        }

        if (!serialPort_->write("ASPOLL\r\n"))
        {
            SPDLOG_ERROR("ASPOLL: write failed");
            return;
        }

        std::string response;
        serialPort_->read_line(response);
        SPDLOG_INFO("ASPOLL reading: {} mg/m3", response);
    }

    void drx85xx::AQDATA()
    {
        if (!serialPort_)
        {
            SPDLOG_ERROR("No serial port assigned");
            return;
        }
    }

    void drx85xx::ASDATAxx()
    {
        if (!serialPort_)
        {
            SPDLOG_ERROR("No serial port assigned");
            return;
        }
    }

}
