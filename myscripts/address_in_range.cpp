#include <iostream>
#include <cstdio>
#include <fstream>
#include <map>
#include <string>
#include <vector>

std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> result;
    std::string_view sv(str);
    size_t start = 0;
    size_t end = sv.find(delimiter);
    
    while (end != std::string_view::npos) {
        result.emplace_back(sv.substr(start, end - start));
        start = end + 1;
        end = sv.find(delimiter, start);
    }
    result.emplace_back(sv.substr(start));
    return result;
}

std::pair<size_t,size_t> decode_address_range(const std::string& range_str)
{
    std::pair<size_t,size_t> range;
    if (range_str.find('-') != std::string::npos)
    {
        auto parts = split(range_str, '-');
        range.first = std::stoull(parts[0], nullptr, 16);
        range.second = std::stoull(parts[1], nullptr, 16);
    }
    else if (range_str.find(',') != std::string::npos)
    {
        auto parts = split(range_str, ',');
        range.first = std::stoull(parts[0], nullptr, 16);
        range.second = range.first + std::stoull(parts[1], nullptr, 16);
    }
    else
    {
        std::cerr << "Invalid range format: " << range_str << std::endl;
        exit(1);
    }
    return range;
}

bool is_address_in_range(size_t addr_start, size_t addr_end, std::map<size_t,size_t>& address_ranges)
{
    for (const auto& [range_start, range_end] : address_ranges)
    {
        if (addr_start >= range_start && addr_end <= range_end)
        {
            return true;
        }
    }
    return false;
}

int main(int argc,char **argv) {
    
    // First argument is the .dat file with all addresses
    std::string input_file = argv[1];

    std::map<size_t,size_t> address_ranges;

    for (int i = 2; i < argc; i++) {
        auto range = decode_address_range(argv[i]);
        address_ranges[range.first] = range.second;
    }

    std::ifstream f(input_file);
    std::string line;
    while (std::getline(f, line)) {
        auto parts = split(line, ',');
        if (parts.size() != 3) {
            std::cerr << "Invalid line: " << line << std::endl;
            continue;
        }
        size_t start = std::stoull(parts[1], nullptr, 16);
        size_t end = std::stoull(parts[2], nullptr, 16);
        if (!is_address_in_range(start, end, address_ranges)) {
            std::cout << line << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    std::cout<< "All addresses are within the specified ranges." << std::endl;
    
    return 0;
}