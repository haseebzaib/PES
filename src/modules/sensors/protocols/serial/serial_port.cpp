#include "pes/modules/protocols/serial/serial.hpp"

#include "libserial/SerialPort.h"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <chrono>
#include <ios>
#include <stdexcept>
#include <thread>

namespace module::protocols::serial
{
    namespace
    {
        LibSerial::BaudRate to_libserial_baud(const unsigned int baudRate)
        {
            switch (baudRate)
            {
            case 50:
                return LibSerial::BaudRate::BAUD_50;
            case 75:
                return LibSerial::BaudRate::BAUD_75;
            case 110:
                return LibSerial::BaudRate::BAUD_110;
            case 134:
                return LibSerial::BaudRate::BAUD_134;
            case 150:
                return LibSerial::BaudRate::BAUD_150;
            case 200:
                return LibSerial::BaudRate::BAUD_200;
            case 300:
                return LibSerial::BaudRate::BAUD_300;
            case 600:
                return LibSerial::BaudRate::BAUD_600;
            case 1200:
                return LibSerial::BaudRate::BAUD_1200;
            case 1800:
                return LibSerial::BaudRate::BAUD_1800;
            case 2400:
                return LibSerial::BaudRate::BAUD_2400;
            case 4800:
                return LibSerial::BaudRate::BAUD_4800;
            case 9600:
                return LibSerial::BaudRate::BAUD_9600;
            case 19200:
                return LibSerial::BaudRate::BAUD_19200;
            case 38400:
                return LibSerial::BaudRate::BAUD_38400;
            case 57600:
                return LibSerial::BaudRate::BAUD_57600;
            case 115200:
                return LibSerial::BaudRate::BAUD_115200;
            case 230400:
                return LibSerial::BaudRate::BAUD_230400;
            default:
                throw std::invalid_argument("Unsupported baud rate");
            }
        }

        LibSerial::CharacterSize to_libserial_char_size(const unsigned int charSize)
        {
            switch (charSize)
            {
            case 5:
                return LibSerial::CharacterSize::CHAR_SIZE_5;
            case 6:
                return LibSerial::CharacterSize::CHAR_SIZE_6;
            case 7:
                return LibSerial::CharacterSize::CHAR_SIZE_7;
            case 8:
                return LibSerial::CharacterSize::CHAR_SIZE_8;
            default:
                throw std::invalid_argument("Unsupported character size");
            }
        }

        LibSerial::Parity to_libserial_parity(const parity value)
        {
            switch (value)
            {
            case parity::None:
                return LibSerial::Parity::PARITY_NONE;
            case parity::Even:
                return LibSerial::Parity::PARITY_EVEN;
            case parity::Odd:
                return LibSerial::Parity::PARITY_ODD;
            }

            throw std::invalid_argument("Unsupported parity");
        }

        LibSerial::StopBits to_libserial_stop_bits(const stopBits value)
        {
            switch (value)
            {
            case stopBits::One:
                return LibSerial::StopBits::STOP_BITS_1;
            case stopBits::Two:
                return LibSerial::StopBits::STOP_BITS_2;
            }

            throw std::invalid_argument("Unsupported stop bits");
        }

    }

    serialPort::serialPort()
        : port_(std::make_unique<LibSerial::SerialPort>())
    {
    }

    serialPort::~serialPort()
    {
        close();
    }

    bool serialPort::open(const serialConfig& serialConfig)
    {
        if (is_open())
        {
            close();
        }

        try
        {
            port_->Open(serialConfig.devicePath_, std::ios_base::in | std::ios_base::out, true);
            if (!apply_config(serialConfig))
            {
                close();
                return false;
            }

            txPostWriteDelayMs_ = serialConfig.txPostWriteDelayMs_;
            rxTimeoutMs_ = serialConfig.rxTimeoutMs_;
            readBuffer_.clear();

            if (serialConfig.startupSettleDelayMs_ > 0)
            {
                SPDLOG_INFO("Settling serial port {} for {} ms",
                    serialConfig.devicePath_,
                    serialConfig.startupSettleDelayMs_);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(serialConfig.startupSettleDelayMs_));
                flush_input();
            }

            SPDLOG_INFO("Opened serial port {}", serialConfig.devicePath_);
            return true;
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Failed to open serial port {}: {}",
                serialConfig.devicePath_,
                exception.what());
            close();
            return false;
        }
    }

    void serialPort::close()
    {
        if (!port_ || !port_->IsOpen())
        {
            return;
        }

        try
        {
            port_->Close();
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Failed to close serial port: {}", exception.what());
        }

        readBuffer_.clear();
    }

    bool serialPort::is_open() const
    {
        return port_ && port_->IsOpen();
    }

    bool serialPort::write(const std::string& data)
    {
        if (!is_open())
        {
            SPDLOG_ERROR("Write failed: serial port not open");
            return false;
        }

        try
        {
            port_->Write(data);
            port_->DrainWriteBuffer();

            if (txPostWriteDelayMs_ > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(txPostWriteDelayMs_));
            }

            return true;
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Serial write failed: {}", exception.what());
            return false;
        }
    }

    bool serialPort::write(const std::vector<std::uint8_t>& data)
    {
        if (!is_open())
        {
            SPDLOG_ERROR("Write failed: serial port not open");
            return false;
        }

        try
        {
            port_->Write(LibSerial::DataBuffer(data.begin(), data.end()));
            port_->DrainWriteBuffer();

            if (txPostWriteDelayMs_ > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(txPostWriteDelayMs_));
            }

            return true;
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Serial write failed: {}", exception.what());
            return false;
        }
    }

    bool serialPort::read_line(std::string& outLine)
    {
        outLine.clear();

        if (!is_open())
        {
            SPDLOG_ERROR("ReadLine failed: serial port not open");
            return false;
        }

        if (!readBuffer_.empty())
        {
            const std::size_t lineEndPos = readBuffer_.find_first_of("\r\n");
            if (lineEndPos != std::string::npos)
            {
                outLine = readBuffer_.substr(0, lineEndPos);

                std::size_t consumed = lineEndPos + 1;
                while (consumed < readBuffer_.size()
                    && (readBuffer_[consumed] == '\r' || readBuffer_[consumed] == '\n'))
                {
                    ++consumed;
                }
                readBuffer_.erase(0, consumed);
                return true;
            }
        }

        try
        {
            port_->ReadLine(outLine, '\r', rxTimeoutMs_);
            while (!outLine.empty() && (outLine.back() == '\r' || outLine.back() == '\n'))
            {
                outLine.pop_back();
            }
            return true;
        }
        catch (const LibSerial::ReadTimeout&)
        {
            // Do not flush RX here. A timeout can simply mean the first byte has
            // not arrived yet, and flushing would discard a late reply.
            return false;
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Serial read failed: {}", exception.what());
            // Leave RX buffer intact here as well. Protocol-level desync is
            // handled by higher layers once they detect malformed responses.
            return false;
        }
    }

    bool serialPort::read_some(std::vector<std::uint8_t>& outData, std::size_t maxBytes)
    {
        outData.clear();

        if (!is_open())
        {
            SPDLOG_ERROR("ReadSome failed: serial port not open");
            return false;
        }

        if (maxBytes == 0)
        {
            return true;
        }

        if (!readBuffer_.empty())
        {
            const std::size_t bufferedBytes = std::min(maxBytes, readBuffer_.size());
            outData.assign(readBuffer_.begin(), readBuffer_.begin() + bufferedBytes);
            readBuffer_.erase(0, bufferedBytes);
            return true;
        }

        try
        {
            LibSerial::DataBuffer buffer;
            port_->Read(buffer, maxBytes, rxTimeoutMs_);
            outData.assign(buffer.begin(), buffer.end());
            return !outData.empty();
        }
        catch (const LibSerial::ReadTimeout&)
        {
            return false;
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Serial read_some failed: {}", exception.what());
            return false;
        }
    }

    void serialPort::flush_rx()
    {
        flush_input();
    }

    bool serialPort::apply_config(const serialConfig& serialConfig)
    {
        try
        {
            port_->SetBaudRate(to_libserial_baud(serialConfig.baudRate_));
            port_->SetCharacterSize(to_libserial_char_size(serialConfig.charSize_));
            port_->SetFlowControl(LibSerial::FlowControl::FLOW_CONTROL_NONE);
            port_->SetParity(to_libserial_parity(serialConfig.parity_));
            port_->SetStopBits(to_libserial_stop_bits(serialConfig.stopBit_));
            port_->SetVMin(0);
            port_->SetVTime(0);
            flush_input();
            return true;
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Failed to apply serial attributes: {}", exception.what());
            return false;
        }
    }

    void serialPort::flush_input()
    {
        readBuffer_.clear();

        if (!is_open())
        {
            return;
        }

        try
        {
            port_->FlushInputBuffer();
        }
        catch (const std::exception& exception)
        {
            SPDLOG_ERROR("Failed to flush serial input: {}", exception.what());
        }
    }
}
