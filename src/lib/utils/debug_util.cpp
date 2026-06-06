#include "debug_util.hpp"

#include <boost/stacktrace.hpp>

std::string print_backtrace()
{
    std::ostringstream oss;
    oss << "Backtrace:\n" << boost::stacktrace::stacktrace();
    return oss.str();
}