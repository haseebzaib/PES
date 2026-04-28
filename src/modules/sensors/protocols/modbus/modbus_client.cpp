#include "pes/modules/protocols/modbus/modbus_client.hpp"

#if __has_include(<modbus/modbus.h>)
#include <modbus/modbus.h>
#elif __has_include(<modbus.h>)
#include <modbus.h>
#else
#error "libmodbus headers were not found"
#endif

#include "spdlog/spdlog.h"

#include <cerrno>
#include <cstring>

namespace module::protocols::modbus
{
    namespace
    {
        int register_count(const dataType type)
        {
            switch (type)
            {
            case dataType::Int16:
            case dataType::UInt16:
                return 1;
            case dataType::Int32:
            case dataType::UInt32:
            case dataType::Float32:
                return 2;
            }

            return 1;
        }

        int normalize_address(const registerConfig& config)
        {
            if (config.registerType_ == registerType::Holding && config.address >= 40001)
            {
                return config.address - 40001;
            }

            if (config.registerType_ == registerType::Input && config.address >= 30001)
            {
                return config.address - 30001;
            }

            return config.address;
        }

        std::uint32_t combine_words(const std::uint16_t* words, const wordOrder order)
        {
            const std::uint16_t highWord = order == wordOrder::Big ? words[0] : words[1];
            const std::uint16_t lowWord = order == wordOrder::Big ? words[1] : words[0];
            return (static_cast<std::uint32_t>(highWord) << 16U) | lowWord;
        }

        double decode_value(const std::uint16_t* words, const registerConfig& config)
        {
            switch (config.dataType_)
            {
            case dataType::Int16:
                return static_cast<double>(static_cast<std::int16_t>(words[0]));
            case dataType::UInt16:
                return static_cast<double>(words[0]);
            case dataType::Int32:
                return static_cast<double>(static_cast<std::int32_t>(combine_words(words, config.wordOrder_)));
            case dataType::UInt32:
                return static_cast<double>(combine_words(words, config.wordOrder_));
            case dataType::Float32:
            {
                const std::uint32_t rawValue = combine_words(words, config.wordOrder_);
                float value = 0.0F;
                static_assert(sizeof(value) == sizeof(rawValue));
                std::memcpy(&value, &rawValue, sizeof(value));
                return static_cast<double>(value);
            }
            }

            return 0.0;
        }

        void set_response_timeout(modbus_t* context, const std::chrono::milliseconds timeout)
        {
            const auto seconds = static_cast<std::uint32_t>(timeout.count() / 1000);
            const auto microseconds = static_cast<std::uint32_t>((timeout.count() % 1000) * 1000);
            modbus_set_response_timeout(context, seconds, microseconds);
        }

        bool is_connection_lost_error(const int error)
        {
            return error == EPIPE || error == ECONNRESET || error == ECONNABORTED || error == ENOTCONN || error == EBADF;
        }
    }

    client::~client()
    {
        disconnect();
    }

    bool client::connect_rtu(const rtuConfig& config)
    {
        disconnect();

        context_ = modbus_new_rtu(
            config.devicePath.c_str(),
            config.baudRate,
            config.parity,
            config.dataBits,
            config.stopBits);

        if (!context_)
        {
            SPDLOG_ERROR("Failed to create Modbus RTU context endpoint={}", config.devicePath);
            return false;
        }

        registers_ = config.registers;
        endpoint_ = config.devicePath;
        return connect_common(config.slaveAddress, config.responseTimeout);
    }

    bool client::connect_tcp(const tcpConfig& config)
    {
        disconnect();

        context_ = modbus_new_tcp(config.ip.c_str(), config.port);
        if (!context_)
        {
            SPDLOG_ERROR("Failed to create Modbus TCP context endpoint={}:{}", config.ip, config.port);
            return false;
        }

        registers_ = config.registers;
        endpoint_ = config.ip + ":" + std::to_string(config.port);
        if (!config.interface.empty())
        {
            SPDLOG_INFO(
                "Modbus TCP connection {} configured for interface {}; OS routing selects the outbound device",
                endpoint_,
                config.interface);
        }

        return connect_common(config.unitId, config.responseTimeout);
    }

    void client::disconnect()
    {
        if (!context_)
        {
            return;
        }

        modbus_close(context_);
        modbus_free(context_);
        context_ = nullptr;
        registers_.clear();
        endpoint_.clear();
        connectionLost_ = false;
    }

    bool client::is_connected() const
    {
        return context_ != nullptr;
    }

    bool client::connection_lost() const noexcept
    {
        return connectionLost_;
    }

    bool client::connect_common(const int slaveAddress, const std::chrono::milliseconds responseTimeout)
    {
        if (!context_)
        {
            return false;
        }

        if (modbus_set_slave(context_, slaveAddress) == -1)
        {
            SPDLOG_ERROR("Failed to set Modbus unit/slave id endpoint={} id={} error={}", endpoint_, slaveAddress, modbus_strerror(errno));
            disconnect();
            return false;
        }

        set_response_timeout(context_, responseTimeout);

        if (modbus_connect(context_) == -1)
        {
            SPDLOG_ERROR("Failed to connect Modbus endpoint={} error={}", endpoint_, modbus_strerror(errno));
            disconnect();
            return false;
        }

        SPDLOG_INFO("Connected Modbus endpoint={} registers={}", endpoint_, registers_.size());
        connectionLost_ = false;
        return true;
    }

    bool client::poll(std::vector<sample>& outSamples)
    {
        outSamples.clear();
        lastErrors_.clear();
        connectionLost_ = false;

        if (!context_)
        {
            SPDLOG_ERROR("Modbus poll failed: no active context");
            lastErrors_.push_back({
                .name = "connection",
                .message = "Modbus poll failed: no active context",
            });
            connectionLost_ = true;
            return false;
        }

        bool allReadsOk = true;
        for (const registerConfig& config : registers_)
        {
            sample value;
            if (read_register(config, value))
            {
                outSamples.push_back(value);
            }
            else
            {
                allReadsOk = false;
                if (connectionLost_)
                {
                    break;
                }
            }
        }

        return allReadsOk;
    }

    const std::vector<readError>& client::last_errors() const noexcept
    {
        return lastErrors_;
    }

    bool client::read_register(const registerConfig& config, sample& outSample)
    {
        std::uint16_t rawWords[2] {};
        const int address = normalize_address(config);
        const int count = register_count(config.dataType_);

        int readCount = -1;
        switch (config.registerType_)
        {
        case registerType::Holding:
            readCount = modbus_read_registers(context_, address, count, rawWords);
            break;
        case registerType::Input:
            readCount = modbus_read_input_registers(context_, address, count, rawWords);
            break;
        }

        if (readCount != count)
        {
            const int error = errno;
            const std::string errorMessage = modbus_strerror(error);
            SPDLOG_WARN(
                "Modbus register read failed endpoint={} name={} address={} normalized={} count={} error={}",
                endpoint_,
                config.name,
                config.address,
                address,
                count,
                errorMessage);
            lastErrors_.push_back({
                .name = config.name,
                .message = errorMessage,
                .source = config,
            });
            if (is_connection_lost_error(error))
            {
                connectionLost_ = true;
            }
            return false;
        }

        outSample.name = config.name;
        outSample.value = decode_value(rawWords, config) * config.scale;
        outSample.unit = config.unit;
        outSample.source = config;
        return true;
    }

}
