#pragma once

#include <chrono>
#include <string>

namespace hyrise
{

/**
 * Starts a std::chrono::steady_clock base timer on construction and returns and resets measurement when
 * lap() is called.
 */
class Timer final
{
  public:
    Timer();

    /**
     * @return Time elapsed since construction or the last call to lap(), whichever was later
     */
    std::chrono::nanoseconds lap();

    /**
     * @return The instant this timer started measuring, i.e. construction or the last lap(),
     *         whichever was later. begin() + lap() is exactly the instant lap() was called.
     */
    std::chrono::steady_clock::time_point begin() const;

    /**
     * Calls lap() and formats the result into a human-readable form
     */
    std::string lap_formatted();

  private:
    std::chrono::steady_clock::time_point _begin;
};

} // namespace hyrise
