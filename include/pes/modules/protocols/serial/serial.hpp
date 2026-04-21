#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace LibSerial
{
    class SerialPort;
}

namespace module::protocols::serial
{

    enum class parity
    {
        None,
        Even,
        Odd,
    };

    enum class stopBits
    {
        One,
        Two,
    };

    struct serialConfig
    {
        std::string devicePath_ {};
        unsigned int baudRate_ {115200};
        unsigned int charSize_ {8};
        parity parity_ {parity::None};
        stopBits stopBit_ {stopBits::One};
        unsigned int startupSettleDelayMs_ {0};
        unsigned int txPostWriteDelayMs_ {0};
        unsigned int rxTimeoutMs_ {300};
    };

    class serialPort
    {
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
        void flush_rx();

        private:
        bool apply_config(const serialConfig& serialConfig);
        void flush_input();

        std::unique_ptr<LibSerial::SerialPort> port_ {};
        std::string readBuffer_ {};
        unsigned int txPostWriteDelayMs_ {0};
        unsigned int rxTimeoutMs_ {300};
    };

}
