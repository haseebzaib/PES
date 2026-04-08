#pragma once

#include "cstdint"
#include "optional"
#include "string"
#include "vector"
#include <span>
#include "boost/asio.hpp"
#include "pes/modules/ping/icmp_header.hpp"
#include "pes/modules/ping/ipv4_header.hpp"
#include <istream>
#include <iostream>
#include <ostream>

namespace module::ping
{
    using boost::asio::steady_timer;
    using boost::asio::ip::icmp;
    namespace chrono = boost::asio::chrono;

    struct PingResult
    {
        bool success{false};
        std::string target{};
        std::optional<double> latency_ms{};
        std::string error_message{};
    };

    class PingModule
    {
    public:
        PingModule();
        void SetPingHostsCritical(std::vector<std::string> hosts);
        void SetPingHostsNormal(std::vector<std::string> hosts);
        void ping_start();
        void ping_stop();
         
    private:
        static unsigned short get_identifier();
        void handle_receive(std::size_t length);
        void handle_timeout();
        void start_receive();
        void start_send();

        std::vector<std::string> PingHostsCritical;
        std::vector<std::string> PingHostsNormal;
        boost::asio::io_context io_context;
        steady_timer timer_;
        icmp::resolver resolver_;
        icmp::socket socket_;
        unsigned short sequence_number_;
        chrono::steady_clock::time_point time_sent_;
        boost::asio::streambuf reply_buffer_;
        std::size_t num_replies_;
        icmp::endpoint destination_; 
        bool ReadDone = false;
        bool TimedOut = false;
    };

} // namespace modules::ping
