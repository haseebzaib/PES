#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

typedef struct _modbus modbus_t;

namespace module::protocols::modbus
{

    enum class registerType
    {
        Holding,
        Input,
    };

    enum class dataType
    {
        Int16,
        UInt16,
        Int32,
        UInt32,
        Float32,
    };

    enum class wordOrder
    {
        Big,
        Little,
    };

    struct registerConfig
    {
        std::string name {};
        registerType registerType_ {registerType::Holding};
        int address {0};
        dataType dataType_ {dataType::Int16};
        wordOrder wordOrder_ {wordOrder::Big};
        double scale {1.0};
        std::string unit {};
    };

    struct rtuConfig
    {
        std::string name {};
        std::string devicePath {};
        int baudRate {9600};
        char parity {'N'};
        int dataBits {8};
        int stopBits {1};
        int slaveAddress {1};
        std::chrono::milliseconds pollInterval {1000};
        std::chrono::milliseconds responseTimeout {1000};
        std::vector<registerConfig> registers {};
    };

    struct tcpConfig
    {
        std::string id {};
        std::string name {};
        std::string interface {};
        std::string ip {};
        int port {502};
        int unitId {1};
        std::chrono::milliseconds pollInterval {1000};
        std::chrono::milliseconds responseTimeout {1000};
        std::vector<registerConfig> registers {};
    };

    struct sample
    {
        std::string name {};
        double value {0.0};
        std::string unit {};
        registerConfig source {};
    };

    struct readError
    {
        std::string name {};
        std::string message {};
        registerConfig source {};
    };

    class client
    {
        public:
        client() = default;
        ~client();

        client(const client&) = delete;
        client& operator=(const client&) = delete;

        bool connect_rtu(const rtuConfig& config);
        bool connect_tcp(const tcpConfig& config);
        void disconnect();
        bool is_connected() const;
        [[nodiscard]] bool connection_lost() const noexcept;

        bool poll(std::vector<sample>& outSamples);
        [[nodiscard]] const std::vector<readError>& last_errors() const noexcept;

        private:
        bool connect_common(int slaveAddress, std::chrono::milliseconds responseTimeout);
        bool read_register(const registerConfig& config, sample& outSample);

        modbus_t* context_ {nullptr};
        std::vector<registerConfig> registers_ {};
        std::vector<readError> lastErrors_ {};
        std::string endpoint_ {};
        bool connectionLost_ {false};
    };

}
