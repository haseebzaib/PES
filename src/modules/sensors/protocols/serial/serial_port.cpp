#include "pes/modules/protocols/serial/serial.hpp"
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/steady_timer.hpp>
#include "spdlog/spdlog.h"
#include <chrono>
#include <fcntl.h>
#include <istream>
#include <termios.h>
#include <unistd.h>

namespace module::protocols::serial
{

    // Kernel serial driver buffers incoming bytes as they arrive over the wire.
    // By the time read_line() is called the response is already queued, so a
    // short deadline is sufficient.  If nothing arrives within this window the
    // sensor is considered unresponsive and the caller retries on the next cycle.
    static constexpr std::chrono::milliseconds READ_LINE_TIMEOUT{300};

    serialPort::serialPort() = default;

    serialPort::~serialPort()
    {
        close();
    }

    bool serialPort::open(const serialConfig &serialConfig)
    {
        boost::system::error_code ec;

        if (is_open())
        {
            boostSerialPort_.close(ec);
        }

        boostSerialPort_.open(serialConfig.devicePath_, ec);
        if (ec)
        {
            SPDLOG_ERROR("Failed to open serial port {}: {}", serialConfig.devicePath_, ec.message());
            return false;
        }

        if (!apply_config(serialConfig))
        {
            close();
            return false;
        }

        SPDLOG_INFO("Opened serial port {}", serialConfig.devicePath_);
        return true;
    }

    void serialPort::close()
    {
        if (!is_open())
        {
            SPDLOG_WARN("Serial port was never opened");
            return;
        }

        boost::system::error_code ec;
        boostSerialPort_.close(ec);

        if (ec)
        {
            SPDLOG_ERROR("Failed to close serial port: {}", ec.message());
        }
    }

    bool serialPort::is_open() const
    {
        return boostSerialPort_.is_open();
    }

    // boost::asio sets the fd to O_NONBLOCK when the first async operation is
    // started, and it stays non-blocking after run()/restart().  Synchronous
    // write() on a non-blocking fd can fail with EAGAIN or send partial frames,
    // which causes the emulator to receive fragmented commands (e.g. "POLL"
    // instead of "ASPOLL").  Force blocking mode for the duration of each write.
    static void set_blocking(int fd, bool blocking)
    {
        int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags == -1) return;
        flags = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
        ::fcntl(fd, F_SETFL, flags);
    }

    bool serialPort::write(const std::string &data)
    {
        if (!is_open())
        {
            SPDLOG_ERROR("Write failed: serial port not open");
            return false;
        }

        int fd = boostSerialPort_.native_handle();
        set_blocking(fd, true);

        boost::system::error_code ec;
        boost::asio::write(boostSerialPort_, boost::asio::buffer(data), ec);

        set_blocking(fd, false);

        if (ec)
        {
            SPDLOG_ERROR("Serial write failed: {}", ec.message());
            return false;
        }

        return true;
    }

    bool serialPort::write(const std::vector<std::uint8_t> &data)
    {
        if (!is_open())
        {
            SPDLOG_ERROR("Write failed: serial port not open");
            return false;
        }

        int fd = boostSerialPort_.native_handle();
        set_blocking(fd, true);

        boost::system::error_code ec;
        boost::asio::write(boostSerialPort_, boost::asio::buffer(data), ec);

        set_blocking(fd, false);

        if (ec)
        {
            SPDLOG_ERROR("Serial write failed: {}", ec.message());
            return false;
        }

        return true;
    }

    bool serialPort::read_line(std::string &outLine)
    {
        outLine.clear();

        if (!is_open())
        {
            SPDLOG_ERROR("ReadLine failed: serial port not open");
            return false;
        }

        bool timed_out = false;
        boost::system::error_code read_ec;

        boost::asio::steady_timer timer(boostIoContext_);
        timer.expires_after(READ_LINE_TIMEOUT);

        timer.async_wait([&](const boost::system::error_code &ec) {
            if (!ec)
            {
                timed_out = true;
                boostSerialPort_.cancel();
            }
        });

        boost::asio::async_read_until(boostSerialPort_, readStreamBuffer_, '\n',
            [&](const boost::system::error_code &ec, std::size_t) {
                read_ec = ec;
                timer.cancel();
            });

        boostIoContext_.run();
        boostIoContext_.restart();

        if (timed_out)
        {
            //SPDLOG_WARN("read_line timed out (no data within {}ms)", READ_LINE_TIMEOUT.count());
            return false;
        }

        if (read_ec)
        {
            SPDLOG_ERROR("Serial read_until failed: {}", read_ec.message());
            return false;
        }

        std::istream input(&readStreamBuffer_);
        std::getline(input, outLine);

        if (!outLine.empty() && outLine.back() == '\r')
        {
            outLine.pop_back();
        }

        return true;
    }

    bool serialPort::read_some(std::vector<std::uint8_t> &outData, std::size_t maxBytes)
    {
        outData.clear();

        if (!is_open())
        {
            SPDLOG_ERROR("ReadSome failed: serial port not open");
            return false;
        }

        outData.resize(maxBytes);

        boost::system::error_code ec;
        const std::size_t bytes_read =
            boostSerialPort_.read_some(boost::asio::buffer(outData.data(), outData.size()), ec);

        if (ec)
        {
            SPDLOG_ERROR("Serial read_some failed: {}", ec.message());
            outData.clear();
            return false;
        }

        outData.resize(bytes_read);
        return true;
    }

    bool serialPort::apply_config(const serialConfig &serialConfig)
    {
        boost::system::error_code ec;

        boostSerialPort_.set_option(
            boost::asio::serial_port_base::baud_rate(serialConfig.baudRate_), ec);
        if (ec)
        {
            SPDLOG_ERROR("Failed to set baud rate: {}", ec.message());
            return false;
        }

        boostSerialPort_.set_option(
            boost::asio::serial_port_base::character_size(serialConfig.charSize_), ec);
        if (ec)
        {
            SPDLOG_ERROR("Failed to set character size: {}", ec.message());
            return false;
        }

        boost::asio::serial_port_base::parity::type parity_value =
            boost::asio::serial_port_base::parity::none;

        switch (serialConfig.parity_)
        {
        case parity::None:
            parity_value = boost::asio::serial_port_base::parity::none;
            break;
        case parity::Even:
            parity_value = boost::asio::serial_port_base::parity::even;
            break;
        case parity::Odd:
            parity_value = boost::asio::serial_port_base::parity::odd;
            break;
        }

        boostSerialPort_.set_option(
            boost::asio::serial_port_base::parity(parity_value), ec);
        if (ec)
        {
            SPDLOG_ERROR("Failed to set parity: {}", ec.message());
            return false;
        }

        boost::asio::serial_port_base::stop_bits::type stop_bits_value =
            boost::asio::serial_port_base::stop_bits::one;

        switch (serialConfig.stopBit_)
        {
        case stopBits::One:
            stop_bits_value = boost::asio::serial_port_base::stop_bits::one;
            break;
        case stopBits::Two:
            stop_bits_value = boost::asio::serial_port_base::stop_bits::two;
            break;
        }

        boostSerialPort_.set_option(
            boost::asio::serial_port_base::stop_bits(stop_bits_value), ec);
        if (ec)
        {
            SPDLOG_ERROR("Failed to set stop bits: {}", ec.message());
            return false;
        }

        return true;
    }

}
