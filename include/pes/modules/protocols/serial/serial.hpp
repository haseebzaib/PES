#pragma once 

#include "boost/asio.hpp"
#include "cstdint"
#include "string"
#include "vector"
#include <istream>


namespace module::protocols::serial {

    enum class parity {
        None,
        Even,
        Odd,
    };

    enum class stopBits {
        One,
        Two,
    };

    struct serialConfig {
        std::string devicePath_ {};
        unsigned int baudRate_ {115200};
        unsigned int charSize_ {8};
        parity parity_ {parity::None};
        stopBits stopBit_ {stopBits::One};
    };
    
    
    class serialPort {

        public:
        serialPort();
        ~serialPort();

        serialPort(const serialPort&) = delete;
        serialPort& operator=(const serialPort&) = delete;

        bool open(const serialConfig& serialConfig);
        void close();
        bool is_open() const;

        bool write(const std::string& data);
        bool write(const std::vector<std::uint8_t>& data);

        bool read_line(std::string& outLine);
        bool read_some(std::vector<std::uint8_t>& outData, std::size_t maxBytes);

        private:
        bool apply_config(const serialConfig& serialConfig);

        boost::asio::io_context boostIoContext_ {};
        boost::asio::serial_port boostSerialPort_ {boostIoContext_};
        boost::asio::streambuf readStreamBuffer_;




    };




}