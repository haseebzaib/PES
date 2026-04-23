#pragma once


#include "optional"
#include "string"
#include "string_view"



namespace utils::file {


class fileReader {

    public:
    static bool exists(std::string_view path);
    static std::optional<std::string> read_text(std::string_view path);
    static bool write_text(std::string_view path, std::string_view context);


};


}