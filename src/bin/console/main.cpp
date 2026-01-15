#include "console.hpp"

#include <setjmp.h>  // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "hyrise.hpp"

#define ANSI_COLOR_RED "\x1B[31m"    // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_GREEN "\x1B[32m"  // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_RESET "\x1B[0m"   // NOLINT(cppcoreguidelines-macro-usage)

#define ANSI_COLOR_RED_RL "\001\x1B[31m\002"    // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_GREEN_RL "\001\x1B[32m\002"  // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_RESET_RL "\001\x1B[0m\002"   // NOLINT(cppcoreguidelines-macro-usage)

sigjmp_buf jmp_env;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

int main(int argc, char** argv) {
  // Make sure the TransactionManager is initialized before the console so that we don't run into destruction order
  // problems (#1635).
  hyrise::Hyrise::get();

  using Return = hyrise::Console::ReturnCode;
  auto& console = hyrise::Console::get();

  // Bind CTRL-C to behaviour specified in Console::handle_signal.
  static_cast<void>(std::signal(SIGINT, &hyrise::Console::handle_signal));

  console.set_prompt("> ");
  console.set_logfile("console.log");
  console.set_console_path(argv[0]);

  // Load command history
  console.load_history(".repl_history");

  // Timestamp dump only to logfile
  console.out("--- Session start --- " + current_timestamp() + "\n", false);

  // TODO(anyone): Use std::to_underlying(ReturnCode::Ok) once we use C++23.
  auto return_code = magic_enum::enum_underlying(Return::Ok);

  // Display usage if too many arguments are provided.
  if (argc > 2) {
    return_code = Return::Quit;
    console.out("Usage:\n");
    console.out("  ./hyriseConsole [SCRIPTFILE] - Start the interactive SQL interface.\n");
    console.out("                                 Execute script if specified by SCRIPTFILE.\n");
  }

  // Execute .sql script if specified.
  if (argc == 2) {
    return_code = console.execute_script(std::string(argv[1]));
    // Terminate Console if an error occured during script execution
    if (return_code == Return::Error) {
      return_code = Return::Quit;
    }
  }

  // Display welcome message if console started normally.
  if (argc == 1) {
    console.out("HYRISE SQL Interface\n");
    console.out("Type 'help' for more information.\n\n");

    console.out("Hyrise is running a ");
    if constexpr (HYRISE_DEBUG) {
      console.out(ANSI_COLOR_RED "(debug)" ANSI_COLOR_RESET);
    } else {
      console.out(ANSI_COLOR_GREEN "(release)" ANSI_COLOR_RESET);
    }
    console.out(" build.\n\n");
  }

  // Set jmp_env to current program state in preparation for siglongjmp(2). See comment on jmp_env for details.
  while (sigsetjmp(jmp_env, 1) != 0) {}

  // Main REPL loop.
  while (return_code != Return::Quit) {
    return_code = console.read();
    if (return_code == Return::Ok) {
      console.set_prompt("> ");
    } else if (return_code == Return::Multiline) {
      console.set_prompt("... ");
    } else {
      console.set_prompt("!> ");
    }
  }
}