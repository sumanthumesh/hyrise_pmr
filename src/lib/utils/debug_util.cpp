#include "debug_util.hpp"

std::string print_backtrace()
{
    constexpr int max_frames = 64;
    void *frames[max_frames];

    // Capture the backtrace
    int num_frames = backtrace(frames, max_frames);

    // Convert addresses to symbols
    char **symbols = backtrace_symbols(frames, num_frames);

    if (symbols == nullptr)
    {
        std::cerr << "Failed to get backtrace symbols\n";
        return "Failed to get backtrace symbols\n";
    }

    std::ostringstream oss;

    oss << "Backtrace (" << num_frames << " frames):\n";
    for (int i = 0; i < num_frames; ++i)
    {
        oss << "  [" << i << "] " << symbols[i] << "\n";
    }

    free(symbols);

    return oss.str();
}