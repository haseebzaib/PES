#pragma once

#include "pes/utils/file/file_reader.hpp"
#include "pes/utils/file/thirdparty/json.hpp"
#include "optional"
#include "string_view"



namespace utils::file {


    class jsonFile {
        public:
        using json = nlohmann::json;
        static std::optional<json> load(std::string_view path);
        static bool save(std::string_view path,const json& json);

    };



}
