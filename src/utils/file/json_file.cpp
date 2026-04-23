#include "pes/utils/file/json_file.hpp"

namespace utils::file
{
    std::optional<jsonFile::json> jsonFile::load(std::string_view path)
    {
        const auto content = fileReader::read_text(path);
        if (!content.has_value())
        {
            return std::nullopt;
        }

        json json_ = json::parse(*content, nullptr, false);
        if (json_.is_discarded())
        {
            return std::nullopt;
        }

        return json_;
    }

    bool jsonFile::save(std::string_view path, const json &json)
    {
        return fileReader::write_text(path, json.dump(4));
    }

}
