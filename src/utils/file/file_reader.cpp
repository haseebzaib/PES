#include "pes/utils/file/file_reader.hpp"
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <sstream>
namespace utils::file
{

    bool fileReader::exists(std::string_view path)
    {
        return std::filesystem::exists(std::filesystem::path(path));
    }

    std::optional<std::string> fileReader::read_text(std::string_view path)
    {

        if (!exists(path))
        {
            return std::nullopt;
        }

        std::ifstream file{std::filesystem::path(path)};
        if (!file.is_open())
        {
            return std::nullopt;
        }

        std::ostringstream stream;
        stream << file.rdbuf();
        return stream.str();
    }

    bool fileReader::write_text(std::string_view path, std::string_view content)
    {

        std::ofstream file{std::filesystem::path(path)};
        if (!file.is_open())
        {
            return false;
        }

        file << content;
        return file.good();
    }

}