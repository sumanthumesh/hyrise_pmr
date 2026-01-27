#include "console.hpp"

#include <setjmp.h> // NOLINT(hicpp-deprecated-headers,modernize-deprecated-headers)

#include <algorithm>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <inttypes.h>
#include <iomanip>
#include <iostream>
#include <malloc.h>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>
#include <readline/history.h>  // NOLINT(build/include_order): cpplint considers readline headers as C system headers.
#include <readline/readline.h> // NOLINT(build/include_order)

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "magic_enum/magic_enum.hpp"

#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/export.hpp"
#include "operators/get_table.hpp"
#include "operators/import.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/optimizer.hpp"
#include "pagination.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "ssb/ssb_table_generator.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/debug_util.hpp"
#include "utils/invalid_input_exception.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/print_utils.hpp"
#include "utils/string_utils.hpp"
#include "visualization/join_graph_visualizer.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

#define ANSI_COLOR_RED "\x1B[31m"   // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_GREEN "\x1B[32m" // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_RESET "\x1B[0m"  // NOLINT(cppcoreguidelines-macro-usage)

#define ANSI_COLOR_RED_RL "\001\x1B[31m\002"   // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_GREEN_RL "\001\x1B[32m\002" // NOLINT(cppcoreguidelines-macro-usage)
#define ANSI_COLOR_RESET_RL "\001\x1B[0m\002"  // NOLINT(cppcoreguidelines-macro-usage)

// Returns a string containing a timestamp of the current date and time.
std::string current_timestamp()
{
    auto time = std::time(nullptr);
    const auto local_time = *std::localtime(&time); // NOLINT(concurrency-mt-unsafe): not called concurrently.

    auto oss = std::ostringstream{};
    oss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

namespace
{

/**
 * Buffer for program state
 *
 * We use this to make Ctrl+C work on all platforms by jumping back into main() from the Ctrl+C signal handler. This
 * was the only way to get this to work on all platforms inclusing macOS.
 * See here (https://github.com/hyrise/hyrise/pull/198#discussion_r135539719) for a discussion about this.
 *
 * The known caveats of goto/longjmp aside, this will probably also cause problems (queries continuing to run in the
 * background) when the scheduler/multithreading is enabled.
 */
sigjmp_buf jmp_env; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)


// Removes the coloring commands (e.g. '\x1B[31m') from input, to have a clean logfile.
// If remove_rl_codes_only is true, then it only removes the Readline specific escape sequences '\001' and '\002'
std::string remove_coloring(const std::string &input, bool remove_rl_codes_only = false)
{
    // Matches any characters that need to be escaped in RegEx except for '|'.
    const auto special_chars = std::regex{R"([-[\]{}()*+?.,\^$#\s])"};
    auto sequences = std::string{"\x1B[31m|\x1B[32m|\x1B[0m|\001|\002"};
    if (remove_rl_codes_only)
    {
        sequences = "\001|\002";
    }
    const auto sanitized_sequences = std::regex_replace(sequences, special_chars, R"(\$&)");

    // Remove coloring commands and escape sequences before writing to logfile.
    const auto expression = std::regex{"(" + sanitized_sequences + ")"};
    return std::regex_replace(input, expression, "");
}

std::vector<std::string> tokenize(std::string input)
{
    boost::algorithm::trim<std::string>(input);

    // Remove whitespace duplicates to not get empty tokens after boost::algorithm::split.
    const auto both_are_spaces = [](char left, char right)
    {
        return (left == right) && (left == ' ');
    };
    const auto unique_range = std::ranges::unique(input, both_are_spaces);
    input.erase(unique_range.begin(), unique_range.end());

    auto tokens = std::vector<std::string>{};
    boost::algorithm::split(tokens, input, boost::is_space());

    return tokens;
}

} // namespace

namespace hyrise
{

Console::Console()
    : _prompt("> "),
      _out(std::cout.rdbuf()),
      _log("console.log", std::ios_base::app | std::ios_base::out),
      _verbose(false),
      _pagination_active(false)
{
    // Init readline basics, tells readline to use our custom command completion function.
    rl_attempted_completion_function = &Console::_command_completion;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    rl_completer_word_break_characters = const_cast<char *>(" \t\n\"\\'`@$><=;|&{(");

    // Set Hyrise caches.
    Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
    Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

    // Use scheduler.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

    // Register default commands.
    register_command("exit", std::bind(&Console::_exit, this, std::placeholders::_1));
    register_command("quit", std::bind(&Console::_exit, this, std::placeholders::_1));

    register_command("help", std::bind(&Console::_help, this, std::placeholders::_1));
    register_command("generate_tpcc", std::bind(&Console::_generate_tpcc, this, std::placeholders::_1));
    register_command("generate_tpch", std::bind(&Console::_generate_tpch, this, std::placeholders::_1));
    register_command("generate_tpcds", std::bind(&Console::_generate_tpcds, this, std::placeholders::_1));
    register_command("generate_ssb", std::bind(&Console::_generate_ssb, this, std::placeholders::_1));
    register_command("load", std::bind(&Console::_load_table, this, std::placeholders::_1));
    register_command("export", std::bind(&Console::_export_table, this, std::placeholders::_1));
    register_command("script", std::bind(&Console::_exec_script, this, std::placeholders::_1));
    register_command("print", std::bind(&Console::_print_table, this, std::placeholders::_1));
    register_command("visualize", std::bind(&Console::_visualize, this, std::placeholders::_1));
    register_command("txinfo", std::bind(&Console::_print_transaction_info, this));
    register_command("pwd", std::bind(&Console::_print_current_working_directory, this));
    register_command("pid", std::bind(&Console::_print_current_process_id, this));
    register_command("setting", std::bind(&Console::_change_runtime_setting, this, std::placeholders::_1));
    register_command("load_plugin", std::bind(&Console::_load_plugin, this, std::placeholders::_1));
    register_command("unload_plugin", std::bind(&Console::_unload_plugin, this, std::placeholders::_1));
    register_command("reset", std::bind(&Console::_reset, this));
    register_command("move2cxl", std::bind(&Console::_move2cxl, this, std::placeholders::_1));
    register_command("create_mem", std::bind(&Console::_create_mem, this, std::placeholders::_1));
    register_command("hsh", std::bind(&Console::_hshell, this, std::placeholders::_1));
    register_command("dump_addr", std::bind(&Console::_dump_addr, this));
}

Console::~Console()
{
    _rollback();
    Hyrise::get().scheduler()->finish();

    out("Bye.\n");

    // Timestamp dump only to logfile.
    out("--- Session end --- " + current_timestamp() + "\n", false);
}

int Console::read()
{
    char *buffer = nullptr;

    // Prompt user for input.
    buffer = readline(_prompt.c_str());
    if (!buffer)
    {
        return ReturnCode::Quit;
    }

    auto input = std::string{buffer};
    boost::algorithm::trim<std::string>(input);

    // Only save non-empty commands to history.
    if (!input.empty())
    {
        add_history(buffer);
        // Save command to history file.
        if (!_history_file.empty())
        {
            if (append_history(1, _history_file.c_str()) != 0)
            {
                out("Error appending to history file: " + _history_file + "\n");
            }
        }
    }

    // Free buffer, since readline() allocates new string every time.
    std::free(buffer); // NOLINT(cppcoreguidelines-no-malloc,hicpp-no-malloc,cppcoreguidelines-owning-memory)

    return _eval(input);
}

int Console::execute_script(const std::string &filepath)
{
    return _exec_script(filepath);
}

int Console::_eval(const std::string &input)
{
    // Do nothing if no input was given.
    if (input.empty() && _multiline_input.empty())
    {
        return ReturnCode::Ok;
    }

    // Dump command to logfile, and to the Console if input comes from a script file. Also remove Readline specific
    // escape sequences ('\001' and '\002') to make it look normal.
    out(remove_coloring(_prompt + input + "\n", true), _verbose);

    // Check if we already are in multiline input.
    if (_multiline_input.empty())
    {
        // Check if a registered command was entered.
        const auto it = _commands.find(input.substr(0, input.find_first_of(" \n;")));
        if (it != _commands.end())
        {
            return _eval_command(it->second, input);
        }

        // Regard query as complete if input is valid and not already in multiline.
        auto parse_result = hsql::SQLParserResult{};
        hsql::SQLParser::parse(input, &parse_result);
        if (parse_result.isValid())
        {
            return _eval_sql(input);
        }
    }

    // Regard query as complete if last character is semicolon, regardless of multiline or not.
    if (input.back() == ';')
    {
        const auto return_code = _eval_sql(_multiline_input + input);
        _multiline_input = "";
        return return_code;
    }

    // If query is not complete(/valid), and the last character is not a semicolon, enter/continue multiline.
    _multiline_input += input;
    _multiline_input += '\n';
    return ReturnCode::Multiline;
}

int Console::_eval_command(const CommandFunction &func, const std::string &command)
{
    auto cmd = command;
    if (command.back() == ';')
    {
        cmd = command.substr(0, command.size() - 1);
    }
    boost::algorithm::trim<std::string>(cmd);

    const auto first = cmd.find(' ');
    const auto last = cmd.find('\n');

    // If no whitespace is found, zero arguments are provided.
    if (std::string::npos == first)
    {
        return static_cast<int>(func(""));
    }

    auto args = cmd.substr(first + 1, last - (first + 1));

    // Remove whitespace duplicates in args.
    const auto both_are_spaces = [](char left, char right)
    {
        return (left == right) && (left == ' ');
    };
    const auto unique_range = std::ranges::unique(args, both_are_spaces);
    args.erase(unique_range.begin(), unique_range.end());

    return static_cast<int>(func(args));
}

bool Console::_initialize_pipeline(const std::string &sql)
{
    try
    {
        auto builder = SQLPipelineBuilder{sql};
        if (_explicitly_created_transaction_context)
        {
            builder.with_transaction_context(_explicitly_created_transaction_context);
        }
        _sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
    }
    catch (const InvalidInputException &exception)
    {
        out(std::string(exception.what()) + '\n');
        return false;
    }

    return true;
}

int Console::_eval_sql(const std::string &sql)
{
    if (!_initialize_pipeline(sql))
    {
        return ReturnCode::Error;
    }

    try
    {
        _sql_pipeline->get_result_tables();
    }
    catch (const InvalidInputException &exception)
    {
        out(std::string(exception.what()) + "\n");
        out("Following statements have not been executed.\n");
        if (!_explicitly_created_transaction_context && _sql_pipeline->statement_count() > 1)
        {
            out("All previous statements have been committed.\n");
        }

        // Store the transaction context as potentially modified by the pipeline. It might be a new context if a transaction
        // was started or nullptr if we are in auto-commit mode or the last transaction was finished.
        _explicitly_created_transaction_context = _sql_pipeline->transaction_context();

        _sql_pipeline.reset();

        return ReturnCode::Error;
    }

    _explicitly_created_transaction_context = _sql_pipeline->transaction_context();

    const auto [pipeline_status, table] = _sql_pipeline->get_result_table();
    // Failed (i.e., conflicted) pipelines should be impossible in the single-user console.
    Assert(pipeline_status == SQLPipelineStatus::Success, "Unexpected pipeline status.");

    const auto row_count = table ? table->row_count() : 0;

    // Print result to console and logfile.
    if (table && Hyrise::get().print_out())
    {
        out(table);
    }

    out("===\n");
    out(std::to_string(row_count) + " rows total\n");

    auto stream = std::ostringstream{};
    stream << _sql_pipeline->metrics();

    out(stream.str());

    if (_sql_pipeline->pqp_cache)
    {
        _sql_pipeline->pqp_cache->clear();
    }
    if (_sql_pipeline->lqp_cache)
    {
        _sql_pipeline->lqp_cache->clear();
    }

    _sql_pipeline.reset();

    // Debug: check if the result table is still alive
    if (table) {
    std::cout << "After reset, table use_count: " << table.use_count() << "\n";
    
    // If use_count > 0, something else still owns it
    if (table.use_count() > 1) {
        std::cout << "WARNING: Table still has " << (table.use_count() - 1) 
                  << " other owner(s) after pipeline reset!\n";
    }
    }

    return ReturnCode::Ok;
}

void Console::register_command(const std::string &name, const CommandFunction &func)
{
    _commands[name] = func;
}

Console::RegisteredCommands Console::commands()
{
    return _commands;
}

void Console::set_prompt(const std::string &prompt)
{
    if constexpr (HYRISE_DEBUG)
    {
        _prompt = ANSI_COLOR_RED_RL "(debug)" ANSI_COLOR_RESET_RL + prompt;
    }
    else
    {
        _prompt = ANSI_COLOR_GREEN_RL "(release)" ANSI_COLOR_RESET_RL + prompt;
    }
}

void Console::set_logfile(const std::string &logfile)
{
    _log = std::ofstream(logfile, std::ios_base::app | std::ios_base::out);
}

void Console::set_console_path(const std::string &path)
{
    _path = path;
}

void Console::load_history(const std::string &history_file)
{
    _history_file = history_file;

    // Check if history file exist, create empty history file if not.
    const auto file = std::ifstream{_history_file};
    if (!file.good())
    {
        out("Creating history file: " + _history_file + "\n");
        if (write_history(_history_file.c_str()) != 0)
        {
            out("Error creating history file: " + _history_file + "\n");
            return;
        }
    }

    if (read_history(_history_file.c_str()) != 0)
    {
        out("Error reading history file: " + _history_file + "\n");
    }
}

void Console::out(const std::string &output, bool console_print)
{
    if (console_print)
    {
        _out << output;
    }
    // Remove coloring commands like '\x1B[32m' when writing to logfile.
    _log << remove_coloring(output);
    _log.flush();
}

void Console::out(const std::shared_ptr<const Table> &table, const PrintFlags flags)
{
    auto size_y = int32_t{0};
    auto size_x = int32_t{0};
    rl_get_screen_size(&size_y, &size_x);

    auto stream = std::stringstream{};
    Print::print(table, flags, stream);

    auto fits_on_one_page = true;
    const auto stream_backup = stream.str();
    auto line = std::string{};
    auto line_count = size_t{0};
    while (std::getline(stream, line, '\n'))
    {
        ++line_count;
        if (line.length() > static_cast<uint64_t>(size_x) || line_count > static_cast<uint64_t>(size_y) - 2)
        {
            fits_on_one_page = false;
            break;
        }
    }
    stream.str(stream_backup);

    static bool pagination_disabled = false;
    if (!fits_on_one_page && !std::getenv("TERM") && !pagination_disabled)
    { // NOLINT(concurrency-mt-unsafe)
        out("Your TERM environment variable is not set - most likely because you are running the console from an IDE. "
            "Pagination is disabled.\n\n");
        pagination_disabled = true;
    }

    // Paginate only if table has more rows or printed columns that fit in the terminal.
    if (fits_on_one_page || pagination_disabled)
    {
        _out << stream.rdbuf();
    }
    else
    {
        _pagination_active = true;
        Pagination(stream).display();
        _pagination_active = false;
    }
}

// Command functions

// NOLINTNEXTLINE: while this particular method could be made static, others cannot.
int Console::_exit(const std::string & /*args*/)
{
    return ReturnCode::Quit;
}

int Console::_help(const std::string & /*args*/)
{
    auto encoding_options = std::string{"                                                 Encoding options: "};
    encoding_options += all_encoding_options();
    // Split the encoding options in lines of 120 and add padding. For each input line, it takes up to 120 characters
    // and replaces the following space(s) with a new line. `(?: +|$)` is a non-capturing group that matches either
    // a non-zero number of spaces or the end of the line.
    const auto line_wrap = std::regex{"(.{1,120})(?: +|$)"};
    encoding_options =
        regex_replace(encoding_options, line_wrap, "$1\n                                                    ");
    // Remove the 49 spaces and the new line added at the end.
    encoding_options.resize(encoding_options.size() - 50);

    // clang-format off
  out("HYRISE SQL Interface\n\n");
  out("Available commands:\n");
  out("  generate_tpcc NUM_WAREHOUSES [CHUNK_SIZE] - Generate all TPC-C tables\n");
  out("  generate_tpch SCALE_FACTOR [CHUNK_SIZE]   - Generate all TPC-H tables\n");
  out("  generate_tpcds SCALE_FACTOR [CHUNK_SIZE]  - Generate all TPC-DS tables\n");
  out("  generate_ssb SCALE_FACTOR [CHUNK_SIZE]    - Generate all SSB tables\n");
  out("  load FILEPATH [TABLENAME [ENCODING]]      - Load table from disk specified by filepath FILEPATH, store it with name TABLENAME\n");  // NOLINT(whitespace/line_length)
  out("                                                   The import type is chosen by the type of FILEPATH.\n");
  out("                                                     Supported types: '.bin', '.csv', '.tbl'\n");
  out("                                                   If no table name is specified, the filename without extension is used\n");  // NOLINT(whitespace/line_length)
  out(encoding_options + "\n");
  out("  export TABLENAME FILEPATH                 - Export table named TABLENAME from storage manager to filepath FILEPATH\n");  // NOLINT(whitespace/line_length)
  out("                                                 The export type is chosen by the type of FILEPATH.\n");
  out("                                                   Supported types: '.bin', '.csv'\n");
  out("  script SCRIPTFILE                         - Execute script specified by SCRIPTFILE\n");
  out("  print TABLENAME                           - Fully print the given table (including MVCC data)\n");
  out("  visualize [options] [SQL]                 - Visualize a SQL query\n");
  out("                                                 Options\n");
  out("                                                  - {exec, noexec} Execute the query before visualization.\n");
  out("                                                                   Default: exec\n");
  out("                                                  - {lqp, unoptlqp, pqp, joins} Type of plan to visualize. unoptlqp gives the\n");  // NOLINT(whitespace/line_length)
  out("                                                                         unoptimized lqp; joins visualized the join graph.\n");  // NOLINT(whitespace/line_length)
  out("                                                                         Default: pqp\n");
  out("                                                SQL\n");
  out("                                                  - Optional, a query to visualize. If not specified, the last\n");  // NOLINT(whitespace/line_length)
  out("                                                    previously executed query is visualized.\n");
  out("  txinfo                                    - Print information on the current transaction\n");
  out("  pwd                                       - Print current working directory\n");
  out("  load_plugin FILE                          - Load and start plugin stored at FILE\n");
  out("  unload_plugin NAME                        - Stop and unload the plugin libNAME.so/dylib (also clears the query cache)\n");  // NOLINT(whitespace/line_length)
  out("  quit                                      - Exit the HYRISE Console\n");
  out("  help                                      - Show this message\n");
  out("  setting [property] [value]                - Change a runtime setting\n");
  out("           scheduler (on|off)               - Turn the scheduler on (default) or off\n");
  out("           binary_caching (on|off)          - Use cached binary tables for benchmarks (default) or not\n");
  out("  reset                                     - Clear all stored tables and cached query plans and restore the default settings\n\n");  // NOLINT(whitespace/line_length)
    // clang-format on

    return ReturnCode::Ok;
}

int Console::_generate_tpcc(const std::string &args)
{
    const auto arguments = tokenize(args);

    if (arguments.empty() || arguments.size() > 2)
    {
        // clang-format off
    out("Usage: ");
    out("  generate_tpcc NUM_WAREHOUSES [CHUNK_SIZE]   Generate TPC-C tables with the specified number of warehouses. \n");  // NOLINT(whitespace/line_length)
    out("                                              Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) + " by default. \n");  // NOLINT(whitespace/line_length)
        // clang-format on
        return ReturnCode::Error;
    }

    const auto num_warehouses = boost::lexical_cast<size_t>(arguments[0]);

    auto chunk_size = Chunk::DEFAULT_SIZE;
    if (arguments.size() > 1)
    {
        chunk_size = ChunkOffset{boost::lexical_cast<ChunkOffset::base_type>(arguments[1])};
    }

    out("Generating all TPCC tables (this might take a while) ...\n");
    const auto config = std::make_shared<BenchmarkConfig>(chunk_size, _binary_caching);
    TPCCTableGenerator{num_warehouses, config}.generate_and_store();

    return ReturnCode::Ok;
}

int Console::_generate_tpch(const std::string &args)
{
    const auto arguments = tokenize(args);

    if (arguments.empty() || arguments.size() > 2)
    {
        // clang-format off
    out("Usage: ");
    out("  generate_tpch SCALE_FACTOR [CHUNK_SIZE]   Generate TPC-H tables with the specified scale factor. \n");
    out("                                            Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) + " by default. \n");  // NOLINT(whitespace/line_length)
        // clang-format on
        return ReturnCode::Error;
    }

    const auto scale_factor = boost::lexical_cast<float>(arguments[0]);

    auto chunk_size = Chunk::DEFAULT_SIZE;
    if (arguments.size() > 1)
    {
        chunk_size = ChunkOffset{boost::lexical_cast<ChunkOffset::base_type>(arguments[1])};
    }

    out("Generating all TPCH tables (this might take a while) ...\n");
    const auto config = std::make_shared<BenchmarkConfig>(chunk_size, _binary_caching);
    TPCHTableGenerator{scale_factor, ClusteringConfiguration::None, config}.generate_and_store();

    return ReturnCode::Ok;
}

int Console::_generate_tpcds(const std::string &args)
{
    const auto arguments = tokenize(args);

    if (arguments.empty() || arguments.size() > 2)
    {
        out("Usage: ");
        out("  generate_tpcds SCALE_FACTOR [CHUNK_SIZE]   Generate TPC-DS tables with the specified scale factor. \n");
        out("                                             Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) +
            " by default. \n");
        return ReturnCode::Error;
    }

    const auto scale_factor = boost::lexical_cast<uint32_t>(arguments[0]);

    auto chunk_size = Chunk::DEFAULT_SIZE;
    if (arguments.size() > 1)
    {
        chunk_size = ChunkOffset{boost::lexical_cast<ChunkOffset::base_type>(arguments[1])};
    }

    out("Generating all TPC-DS tables (this might take a while) ...\n");
    const auto config = std::make_shared<BenchmarkConfig>(chunk_size, _binary_caching);
    TPCDSTableGenerator{scale_factor, config}.generate_and_store();

    return ReturnCode::Ok;
}

int Console::_generate_ssb(const std::string &args)
{
    const auto arguments = tokenize(args);

    if (arguments.empty() || arguments.size() > 2)
    {
        out("Usage: ");
        out("  generate_ssb SCALE_FACTOR [CHUNK_SIZE]   Generate SSB tables with the specified scale factor. \n");
        out("                                           Chunk size is " + std::to_string(Chunk::DEFAULT_SIZE) +
            " by default. \n");
        return ReturnCode::Error;
    }

    const auto scale_factor = boost::lexical_cast<float>(arguments[0]);

    auto chunk_size = Chunk::DEFAULT_SIZE;
    if (arguments.size() > 1)
    {
        chunk_size = ChunkOffset{boost::lexical_cast<ChunkOffset::base_type>(arguments[1])};
    }

    // Try to find dbgen binary.
    const auto executable_path = std::filesystem::canonical(_path).remove_filename();
    const auto ssb_dbgen_path = executable_path / "third_party/ssb-dbgen";
    const auto csv_meta_path = executable_path / "../resources/benchmark/ssb/schema";

    if (!std::filesystem::exists(ssb_dbgen_path / "dbgen"))
    {
        out(std::string{"SSB dbgen not found at "} + ssb_dbgen_path.string() + "\n");
        return ReturnCode::Error;
    }

    // Create the ssb_data directory (if needed) and generate the ssb_data/sf-... path.
    auto ssb_data_path = std::stringstream{};
    ssb_data_path << "ssb_data/sf-" << std::noshowpoint << scale_factor;
    std::filesystem::create_directories(ssb_data_path.str());

    out("Generating all SSB tables (this might take a while) ...\n");
    const auto config = std::make_shared<BenchmarkConfig>(chunk_size, _binary_caching);
    SSBTableGenerator{ssb_dbgen_path, csv_meta_path, ssb_data_path.str(), scale_factor, config}.generate_and_store();

    return ReturnCode::Ok;
}

int Console::_load_table(const std::string &args)
{
    const auto arguments = trim_and_split(args);

    if (arguments.empty() || arguments.size() > 3)
    {
        out("Usage:\n");
        out("  load FILEPATH [TABLENAME [ENCODING]]\n");
        return ReturnCode::Error;
    }

    const auto filepath = std::filesystem::path{arguments.at(0)};
    const auto tablename = arguments.size() >= 2 ? arguments.at(1) : std::string{filepath.stem()};

    out("Loading " + filepath.string() + " into table \"" + tablename + "\"\n");

    if (Hyrise::get().storage_manager.has_table(tablename))
    {
        out("Table \"" + tablename + "\" already existed. Replacing it.\n");
    }

    try
    {
        const auto importer = std::make_shared<Import>(filepath, tablename, Chunk::DEFAULT_SIZE);
        importer->execute();
    }
    catch (const std::exception &exception)
    {
        out("Error: Exception thrown while importing table:\n  " + std::string(exception.what()) + "\n");
        return ReturnCode::Error;
    }

    const auto encoding = arguments.size() == 3 ? arguments.at(2) : "Unencoded";

    const auto encoding_type = magic_enum::enum_cast<EncodingType>(encoding);
    if (!encoding_type)
    {
        out("Error: Invalid encoding type: '" + encoding + "', try one of these: " + all_encoding_options() + "\n");
        return ReturnCode::Error;
    }

    // Check if the specified encoding can be used.
    const auto &table = Hyrise::get().storage_manager.get_table(tablename);
    auto supported = true;
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id)
    {
        if (!encoding_supports_data_type(*encoding_type, table->column_data_type(column_id)))
        {
            out("Encoding \"" + encoding + "\" not supported for column \"" + table->column_name(column_id) +
                "\", table left unencoded\n");
            supported = false;
        }
    }

    if (supported)
    {
        out("Encoding \"" + tablename + "\" using " + encoding + "\n");
        auto immutable_chunks = std::vector<ChunkID>{};
        for (ChunkID chunk_id(0); chunk_id < table->chunk_count(); ++chunk_id)
        {
            if (!table->get_chunk(chunk_id)->is_mutable())
            {
                immutable_chunks.emplace_back(chunk_id);
            }
        }
        ChunkEncoder::encode_chunks(table, immutable_chunks, SegmentEncodingSpec{*encoding_type});
    }

    return ReturnCode::Ok;
}

int Console::_export_table(const std::string &args)
{
    const auto arguments = trim_and_split(args);

    if (arguments.size() != 2)
    {
        out("Usage:\n");
        out("  export TABLENAME FILEPATH\n");
        return ReturnCode::Error;
    }

    const auto &tablename = arguments.at(0);
    const auto &filepath = arguments.at(1);

    const auto &storage_manager = Hyrise::get().storage_manager;
    const auto &meta_table_manager = Hyrise::get().meta_table_manager;

    auto table_operator = std::shared_ptr<AbstractOperator>{};
    if (MetaTableManager::is_meta_table_name(tablename))
    {
        if (!meta_table_manager.has_table(tablename))
        {
            out("Error: MetaTable does not exist in MetaTableManager\n");
            return ReturnCode::Error;
        }
        table_operator = std::make_shared<TableWrapper>(meta_table_manager.generate_table(tablename));
    }
    else
    {
        if (!storage_manager.has_table(tablename))
        {
            out("Error: Table does not exist in StorageManager\n");
            return ReturnCode::Error;
        }
        table_operator = std::make_shared<GetTable>(tablename);
    }

    table_operator->execute();
    out("Exporting \"" + tablename + "\" into \"" + filepath + "\" ...\n");

    try
    {
        auto exporter = std::make_shared<Export>(table_operator, filepath);
        exporter->execute();
    }
    catch (const std::exception &exception)
    {
        out("Error: Exception thrown while exporting:\n  " + std::string(exception.what()) + "\n");
        return ReturnCode::Error;
    }

    return ReturnCode::Ok;
}

int Console::_print_table(const std::string &args)
{
    const auto arguments = trim_and_split(args);

    if (arguments.size() != 1)
    {
        out("Usage:\n");
        out("  print TABLENAME\n");
        return ReturnCode::Error;
    }

    const auto &tablename = arguments.at(0);

    const auto &storage_manager = Hyrise::get().storage_manager;
    if (!storage_manager.has_table(tablename))
    {
        out("Error: Table does not exist in StorageManager\n");
        return ReturnCode::Error;
    }

    const auto get_table = std::make_shared<GetTable>(tablename);
    get_table->execute();

    out(get_table->get_output(), PrintFlags::Mvcc);

    return ReturnCode::Ok;
}

int Console::_visualize(const std::string &input)
{
    /**
     * "visualize" supports three dimensions of options:
     *    - "noexec"; or implicit "exec", the execution of the specified query
     *    - "lqp", "unoptlqp", "joins"; or implicit "pqp"
     *    - A sql query can either be specified or not. If it is not, the previously executed query is visualized.
     */

    auto input_words = std::vector<std::string>{};
    boost::algorithm::split(input_words, input, boost::is_any_of(" \n"));

    constexpr auto EXEC = "exec";
    constexpr auto NOEXEC = "noexec";
    constexpr auto PQP = "pqp";
    constexpr auto LQP = "lqp";
    constexpr auto UNOPTLQP = "unoptlqp";
    constexpr auto JOINS = "joins";

    // Determine whether the specified query is to be executed before visualization.
    auto no_execute = false; // Default
    if (input_words.front() == NOEXEC || input_words.front() == EXEC)
    {
        no_execute = input_words.front() == NOEXEC;
        input_words.erase(input_words.begin());
    }

    // Determine the plan type to visualize.
    enum class PlanType : uint8_t
    {
        LQP,
        UnoptLQP,
        PQP,
        Joins
    };
    auto plan_type = PlanType::PQP;
    auto plan_type_str = std::string{"pqp"};
    if (input_words.front() == LQP || input_words.front() == UNOPTLQP || input_words.front() == PQP ||
        input_words.front() == JOINS)
    {
        if (input_words.front() == LQP)
        {
            plan_type = PlanType::LQP;
        }
        else if (input_words.front() == UNOPTLQP)
        {
            plan_type = PlanType::UnoptLQP;
        }
        else if (input_words.front() == JOINS)
        {
            plan_type = PlanType::Joins;
        }

        plan_type_str = input_words.front();
        input_words.erase(input_words.begin());
    }

    // Removes plan type and noexec (+ leading whitespace) so that only the sql string is left.
    const auto sql = boost::algorithm::join(input_words, " ");

    // If no SQL is provided, use the last execution. Else, create a new pipeline.
    if (!sql.empty() && !_initialize_pipeline(sql))
    {
        return ReturnCode::Error;
    }

    // If there is no pipeline (i.e., neither was SQL passed in with the visualize command, nor was there a previous
    // execution), return an error.
    if (!_sql_pipeline)
    {
        out("Error: Nothing to visualize.\n");
        return ReturnCode::Error;
    }

    if (no_execute && !sql.empty() && _sql_pipeline->requires_execution())
    {
        out("Error: We do not support the visualization of multiple dependent statements in 'noexec' mode.\n");
        return ReturnCode::Error;
    }

    const auto img_filename = plan_type_str + ".png";

    try
    {
        switch (plan_type)
        {
        case PlanType::LQP:
        case PlanType::UnoptLQP:
        {
            auto lqp_roots = std::vector<std::shared_ptr<AbstractLQPNode>>{};

            const auto &lqps = (plan_type == PlanType::LQP) ? _sql_pipeline->get_optimized_logical_plans()
                                                            : _sql_pipeline->get_unoptimized_logical_plans();

            lqp_roots.reserve(lqps.size());

            for (const auto &lqp : lqps)
            {
                lqp_roots.emplace_back(lqp);
            }

            auto visualizer = LQPVisualizer{};
            visualizer.visualize(lqp_roots, img_filename);
        }
        break;

        case PlanType::PQP:
        {
            if (!no_execute)
            {
                _sql_pipeline->get_result_table();

                // Store the transaction context as potentially modified by the pipeline. It might be a new context if a
                // transaction was started or nullptr if we are in auto-commit mode or the last transaction was finished.
                _explicitly_created_transaction_context = _sql_pipeline->transaction_context();
            }

            auto visualizer = PQPVisualizer{};
            visualizer.visualize(_sql_pipeline->get_physical_plans(), img_filename);
        }
        break;

        case PlanType::Joins:
        {
            out("NOTE: Join graphs will show only Cross and Inner joins, not Semi, Left, Right, Full outer, "
                "AntiNullAsTrue and AntiNullAsFalse joins.\n");

            auto join_graphs = std::vector<JoinGraph>{};

            const auto &lqps = _sql_pipeline->get_optimized_logical_plans();
            for (const auto &lqp : lqps)
            {
                const auto sub_lqps = lqp_find_subplan_roots(lqp);

                for (const auto &sub_lqp : sub_lqps)
                {
                    const auto sub_lqp_join_graphs = JoinGraph::build_all_in_lqp(sub_lqp);
                    for (const auto &sub_lqp_join_graph : sub_lqp_join_graphs)
                    {
                        join_graphs.emplace_back(sub_lqp_join_graph);
                    }
                }
            }

            auto visualizer = JoinGraphVisualizer{};
            visualizer.visualize(join_graphs, img_filename);
        }
        break;
        }
    }
    catch (const InvalidInputException &exception)
    {
        out(std::string(exception.what()) + '\n');
        return 0;
    }

    // NOLINTBEGIN(concurrency-mt-unsafe): system() is not thread-safe, but it is not used concurrently here.
    auto scripts_dir = std::string{"./scripts/"};
    auto ret = system((scripts_dir + "planviz/is_iterm2.sh 2>/dev/null").c_str());
    if (ret != 0)
    {
        // Try in parent directory.
        scripts_dir = std::string{"."} + scripts_dir;
        ret = system((scripts_dir + "planviz/is_iterm2.sh").c_str());
    }
    if (ret != 0)
    {
        std::string msg{"Currently, only iTerm2 can print the visualization inline. You can find the plan at "};
        msg += img_filename + "\n";
        out(msg);

        return ReturnCode::Ok;
    }

    const auto cmd = scripts_dir + "/planviz/imgcat.sh " + img_filename;
    ret = system(cmd.c_str());
    Assert(ret == 0, "Printing the image using ./scripts/imgcat.sh failed.");
    // NOLINTEND(concurrency-mt-unsafe)

    return ReturnCode::Ok;
}

int Console::_change_runtime_setting(const std::string &input)
{
    const auto property = input.substr(0, input.find_first_of(" \n"));
    const auto value = input.substr(input.find_first_of(" \n") + 1, input.size());

    if (property == "scheduler")
    {
        if (value == "on")
        {
            Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
            out("Scheduler turned on\n");
        }
        else if (value == "off")
        {
            Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
            out("Scheduler turned off\n");
        }
        else
        {
            out("Usage: scheduler (on|off)\n");
            return 1;
        }
        return 0;
    }

    if (property == "workers")
    {
        auto num_workers = std::stoull(value);
        Assertf(num_workers > 0, "Number of workers must be greater than zero\n");
        NodeQueueScheduler::_preferred_worker_count = num_workers;
        Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
        out("Scheduler set to use " + std::to_string(num_workers) + " workers\n");
        return 0;
    }

    if (property == "print")
    {
        if (value == "on")
        {
            Hyrise::get().print_out() = true;
            out("Print output turned on\n");
        }
        else if (value == "off")
        {
            Hyrise::get().print_out() = false;
            out("Print output turned off\n");
        }
        else
        {
            out("Usage: print (on|off)\n");
            return 1;
        }
        return 0;
    }

    if (property == "binary_caching")
    {
        if (value == "on")
        {
            _binary_caching = true;
            out("Binary caching turned on\n");
        }
        else if (value == "off")
        {
            _binary_caching = false;
            out("Binary caching turned off\n");
        }
        else
        {
            out("Usage: binary_caching (on|off)\n");
            return 1;
        }
        return 0;
    }

    out("Error: Unknown property\n");
    return 1;
}

int Console::_exec_script(const std::string &script_file)
{
    auto filepath = script_file;
    boost::algorithm::trim(filepath);
    auto script = std::ifstream{filepath};

    if (!script.good())
    {
        out("Error: Script file '" + filepath + "' does not exist.\n");
        return ReturnCode::Error;
    }

    if (!std::filesystem::is_regular_file(filepath))
    {
        out("Error: '" + filepath + "' is not a regular file.\n");
        return ReturnCode::Error;
    }

    out("Executing script file: " + filepath + "\n");
    Hyrise::get().recently_parsed_script_file = std::filesystem::absolute(filepath).string();
    _verbose = true;
    auto command = std::string{};
    // TODO(anyone): Use std::to_underlying(ReturnCode::Ok) once we use C++23.
    auto return_code = magic_enum::enum_underlying(ReturnCode::Ok);
    while (std::getline(script, command))
    {
        return_code = _eval(command);
        if (return_code == ReturnCode::Error || return_code == ReturnCode::Quit)
        {
            break;
        }
    }
    out("Executing script file done\n");
    _verbose = false;
    return return_code;
}

void print_memory()
{
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line))
    {
        if (line.rfind("VmRSS:", 0) == 0 || line.rfind("VmSize:", 0) == 0)
        {
            std::cout << line << "\n";
        }
    }
}

// int Console::_move2cxl(const std::string &args)
// {
//     const auto arguments = tokenize(args);

//     if (arguments.size() != 3)
//     {
//         // clang-format off
//     out("Usage: ");
//     out("  move2cxl TABLE_NAME COLUMN_NAME POOL_NAME  Move the column to cxl memory \n");  // NOLINT(whitespace/line_length)

//         // clang-format on
//         return ReturnCode::Error;
//     }

//     // Fetch table manager
//     auto &storage_manager = Hyrise::get().storage_manager;
//     // Check if table exists
//     auto &table_name = arguments[0];
//     if (!storage_manager.has_table(table_name))
//     {
//         std::cerr << "Cannot find table " << table_name << "\n";
//         return ReturnCode::Error;
//     }
//     auto table = storage_manager.get_table(table_name);
//     auto chunk_count = table->chunk_count();

//     auto &column_name = arguments[1];
//     // This will throw error if column does not exist
//     auto column_id = table->column_id_by_name(column_name);

//     // Fetch memory pool
//     auto &pool_name = arguments[2];
//     auto &pool_manager = Hyrise::get().mem_pool_manager;
//     auto mem_pool = pool_manager.get_pool(pool_name);

//     print_memory();

//     // Fetch migration engine
//     auto &migration_engine = Hyrise::get().migration_engine;

//     auto start_migration = std::chrono::system_clock::now();

//     size_t moved_bytes = 0;

//     // Loop over chunks
//     for (ChunkID cid = ChunkID{0}; cid < chunk_count; cid++)
//     {
//         auto chunk_ptr = table->get_chunk(cid);
//         // Fetch segments corresponding to column id
//         auto segment_ptr = chunk_ptr->get_segment(column_id);
//         Assertf(segment_ptr != nullptr, "Failed to fetch segment\n");

//         moved_bytes += segment_ptr->memory_usage(MemoryUsageCalculationMode::Full);

//         // Need to copy segment and replace the original segment ptr with this new one
//         // Cast to correct dictionary segment first
//         auto data_type = segment_ptr->data_type();
//         // auto abs_encoded_segment_ptr = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment_ptr);
//         // Assertf(abs_encoded_segment_ptr != nullptr, "AbstractSegment to AbstractEncodedSegment conversion failed\n");

//         if (std::dynamic_pointer_cast<AbstractEncodedSegment>(segment_ptr))
//         {
//             switch (data_type)
//             {
//             case hyrise::DataType::Int:
//             {
//                 migration_engine.migrate_numerical_dictionary_segment<int32_t>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case hyrise::DataType::Long:
//             {
//                 migration_engine.migrate_numerical_dictionary_segment<int64_t>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case hyrise::DataType::Float:
//             {
//                 migration_engine.migrate_numerical_dictionary_segment<float>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case hyrise::DataType::Double:
//             {
//                 migration_engine.migrate_numerical_dictionary_segment<double>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case hyrise::DataType::String:
//             {
//                 migration_engine.migrate_string_dictionary_segment(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             default:
//                 std::cout << "Should not have reached here\n";
//                 return ReturnCode::Error;
//             }
//         }
//         else if (std::dynamic_pointer_cast<BaseValueSegment>(segment_ptr))
//         {
//             switch (data_type)
//             {
//             case DataType::Int:
//             {
//                 migration_engine.migrate_numerical_value_segment<int32_t>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case DataType::Long:
//             {
//                 migration_engine.migrate_numerical_value_segment<int64_t>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case DataType::Float:
//             {
//                 migration_engine.migrate_numerical_value_segment<float>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             case DataType::Double:
//             {
//                 migration_engine.migrate_numerical_value_segment<double>(chunk_ptr, segment_ptr, column_id, mem_pool);
//                 break;
//             }
//             default:
//                 std::cout << "Should not have reached here\n";
//                 return ReturnCode::Error;
//             }
//         }
//         else
//         {
//             std::cout << "Segment type not Dictionary or ValueSegment, but is instead " << Print::_segment_type(segment_ptr) << "\n";
//             return ReturnCode::Error;
//         }
//     }

//     auto end_migration = std::chrono::system_clock::now();

//     auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_migration - start_migration);

//     print_memory();
//     malloc_trim(0);
//     print_memory();

//     // std::cout<<"Migrated "<<moved_bytes<<" in "<<duration.count()<<" ns\n";
//     std::cout << moved_bytes << "," << duration.count() << "," << ((double)moved_bytes * std::pow(2, -30)) / ((double)duration.count() * 1e-9) << "GB/s\n";
//     // std::cout << "Migration Duration: " << duration.count() << "ns\n";
//     std::ofstream migration_log("migration_log.txt", std::ios_base::app);
//     migration_log << table_name << "," << column_name << "," << pool_name << "," << moved_bytes << "," << duration.count() << "\n";
//     migration_log.close();
//     return ReturnCode::Ok;
// }

int Console::_move2cxl(const std::string &args)
{
    const auto arguments = tokenize(args);

    if (arguments.size() != 3)
    {
        // clang-format off
    out("Usage: ");
    out("  move2cxl TABLE_NAME COLUMN_NAME NUMA_NODE  Move the column to cxl memory \n");  // NOLINT(whitespace/line_length)

        // clang-format on
        return ReturnCode::Error;
    }

    // Fetch table manager
    auto &storage_manager = Hyrise::get().storage_manager;
    // Check if table exists
    auto &table_name = arguments[0];
    if (!storage_manager.has_table(table_name))
    {
        std::cerr << "Cannot find table " << table_name << "\n";
        return ReturnCode::Error;
    }
    auto table_ptr = storage_manager.get_table(table_name);

    auto &column_name = arguments[1];

    int numa_node = boost::lexical_cast<int>(arguments[2]);

    // Fetch migration engine
    auto &migration_engine = Hyrise::get().migration_engine;
    
    size_t moved_bytes = 0;

    auto start_migration = std::chrono::system_clock::now();

    migration_engine->migrate_column(table_ptr, column_name , numa_node);
    auto end_migration = std::chrono::system_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_migration - start_migration);

    print_memory();
    malloc_trim(0);
    print_memory();

    // std::cout<<"Migrated "<<moved_bytes<<" in "<<duration.count()<<" ns\n";
    std::cout << moved_bytes << "," << duration.count() << "," << ((double)moved_bytes * std::pow(2, -30)) / ((double)duration.count() * 1e-9) << "GB/s\n";
    // std::cout << "Migration Duration: " << duration.count() << "ns\n";
    std::ofstream migration_log("migration_log.txt", std::ios_base::app);
    migration_log << table_name << "," << column_name << "," << "," << moved_bytes << "," << duration.count() << "\n";
    migration_log.close();
    return ReturnCode::Ok;
}

int Console::_create_mem(const std::string &args)
{
    const auto arguments = tokenize(args);

    if (arguments.size() != 2)
    {
        // clang-format off
    out("Usage: ");
    out("  create_mem SIZE_IN_BYTES NUMA_NODE  Create memory resource for std::pmr\n");  // NOLINT(whitespace/line_length)

        // clang-format on
        return ReturnCode::Error;
    }

    // Fetch pool manager
    auto &mem_pool_manager = Hyrise::get().mem_pool_manager;

    auto pool_size = boost::lexical_cast<uint64_t>(arguments[1]);
    auto numa_node = boost::lexical_cast<int>(arguments[2]);

    size_t pool_id = mem_pool_manager.create_pool(pool_size, numa_node);

    // Print out created pool details
    std::printf("%lu,0x%016" PRIxPTR ",0x%016" PRIxPTR ",%lu\n",
                pool_id,
                reinterpret_cast<uintptr_t>(mem_pool_manager.get_pool(pool_id)->start_address()),
                reinterpret_cast<uintptr_t>(mem_pool_manager.get_pool(pool_id)->end_address()),
                mem_pool_manager.get_pool(pool_id)->size());

    return ReturnCode::Ok;
}

int Console::_hshell(const std::string &args)
{
    const auto arguments = tokenize(args);

    auto &cmd = arguments[0];
    if (cmd == "size")
    {
        if (arguments.size() != 3)
        {
            out("Usage: ");
            out("  hshell size TABLE_NAME COLUMN_NAME  Size of the column in bytes\n");
            return ReturnCode::Error;
        }

        // Fetch table manager
        auto &storage_manager = Hyrise::get().storage_manager;
        // Check if table exists
        auto &table_name = arguments[1];
        if (!storage_manager.has_table(table_name))
        {
            std::cerr << "Cannot find table " << table_name << "\n";
            return ReturnCode::Error;
        }

        size_t column_size = 0;

        auto table = storage_manager.get_table(table_name);
        auto &column_name = arguments[2];
        auto column_id = table->column_id_by_name(column_name);
        auto chunk_count = table->chunk_count();

        // Loop over chunks
        for (ChunkID cid = ChunkID{0}; cid < chunk_count; cid++)
        {
            auto chunk_ptr = table->get_chunk(cid);
            // Fetch segments corresponding to column id
            auto segment_ptr = chunk_ptr->get_segment(column_id);
            Assertf(segment_ptr != nullptr, "Failed to fetch segment\n");

            column_size += segment_ptr->memory_usage(MemoryUsageCalculationMode::Full);
        }

        std::cout << column_size << "B\n";
    }
    else if (cmd == "find_numa")
    {
        if (arguments.size() != 2)
        {
            out("Usage: ");
            out("  hshell find_numa MEM_RESOURCE_NAME  Find the NUMA node for the given memory resource\n");
            return ReturnCode::Error;
        }
        auto pool_id = boost::lexical_cast<size_t>(arguments[1]);
        auto &pool_manager = Hyrise::get().mem_pool_manager;
        auto mem_pool = pool_manager.get_pool(pool_id);

        std::cout << mem_pool->verify_numa_node() << "\n";
    }
    else if (cmd == "ops")
    {
        if (arguments.size() != 1)
        {
            out("Usage: ");
            out("  hsh ops  Print all the operators used\n");
            return ReturnCode::Error;
        }
        OperatorsUsed::get().print_operators_used();
    }
    else if (cmd == "segments")
    {
        if (arguments.size() != 1)
        {
            out("Usage: ");
            out("  hsh segments  Print all the segments used\n");
            return ReturnCode::Error;
        }
        SegmentsUsed::get().print_segments_used();
    }
    else if (cmd == "tables")
    {
        if (arguments.size() != 1)
        {
            out("Usage: ");
            out("  hsh tables  Print all the segments used\n");
            return ReturnCode::Error;
        }
        for (auto &id: Table::_existing_table_ids) {
            std::cout << id << "\n";
        }
    }
    else
    {
        out("Error: Unknown hshell command\n");
        return ReturnCode::Error;
    }

    return ReturnCode::Ok;
}

int Console::_dump_addr()
{
    // Function lists all the TPCH tables
    // Access storate manager
    auto &storage_manager = Hyrise::get().storage_manager;
    // From the storage manager try to access the table pointers and print out
    // the data if you can access them
    //  std::vector<std::string> field_names =
    //  storage_manager.get_table("region")->column_names(); for(std::string &s:
    //  field_names) {
    //    std::cout<<s<<",";
    //  }
    // std::cout << "\n";
    const std::vector<std::string> &table_names = storage_manager.table_names();
    // table_names.emplace_back("customer");
    // table_names.emplace_back("orders");
    // table_names.emplace_back("lineitem");
    // table_names.emplace_back("part");
    // table_names.emplace_back("partsupp");
    // table_names.emplace_back("supplier");
    // table_names.emplace_back("nation");
    // table_names.emplace_back("region");

    // Data structure to keep track of uniq id associated with each column
    // #ifdef GEM5_RUN
    std::unordered_map<size_t, std::pair<std::string, std::string>> uniq_id_table;
    // #endif
    // std::vector<std::pair<uintptr_t, uintptr_t>> addr_ranges;
    std::vector<std::pair<size_t, std::pair<uintptr_t, uintptr_t>>> addr_ranges;

    // Print out number of segments and chunks
    std::ostringstream counters;
    std::ofstream f("chunks.txt");
    for (const std::string &table_name : table_names)
    {
        f << "Name: " << table_name << "\n";
        auto num_chunks = storage_manager.get_table(table_name)->chunk_count();
        f << "Chunks: " << num_chunks << "\n";
        auto chunk_ptr = storage_manager.get_table(table_name)->get_chunk(ChunkID{0});
        auto col_count = chunk_ptr->column_count();
        f << "Segments: " << col_count << "\n";
    }
    // #ifdef GEM5_RUN
    size_t uniq_id;
    // #endif

#ifndef GEM5_RUN
    std::ofstream fout("mem_regions.dat");
#endif
    // For every table print out some stuff
    for (size_t table_id = 0; table_id < table_names.size(); table_id++)
    // for (std::string &table_name : table_names)
    {

        std::string table_name = table_names[table_id];
        std::vector<std::string> field_names = storage_manager.get_table(table_name)->column_names();

        // std::cout << "===================\n";
        // std::cout << "Name: " << table_name << "\n";
        // Now print out all the segments in each table
        // Iterate over all chunks
        auto num_chunks = storage_manager.get_table(table_name)->chunk_count();
        for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; chunk_id++)
        {
            auto chunk_ptr = storage_manager.get_table(table_name)->get_chunk(chunk_id);
            // Iterate over all segments
            auto col_count = chunk_ptr->column_count();
            // std::cout << "-------------------\n";
            // std::cout << "Rows : " << chunk_ptr->size() << "\n";
            for (auto col_id = ColumnID{0}; col_id < col_count; col_id++)
            {
                // #ifdef GEM5_RUN
                uniq_id = table_id * 10000000 + col_id * 100000 + chunk_id;
                if (uniq_id_table.find(uniq_id) == uniq_id_table.end())
                {
                    uniq_id_table.insert({uniq_id, std::make_pair(table_name, field_names[col_id])});
                }
                // #endif
                // log_to_file(field_names[col_id]);
                // std::cout << "Chunk: " << chunk_id << "\n";
                // std::cout << "Segmt: " << col_id << "\n";
                auto segment_ptr = chunk_ptr->get_segment(col_id);
                // std::cout << "Type : " << Print::_segment_type(segment_ptr) << "\n";
                // Print the datatype for now
                // std::cout << "Data : " << segment_ptr->data_type() << "\n";
                if (const auto &encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment_ptr))
                {
                    // Get access to attribute vector and print out the data ranges
                    auto attr_vector_ptr = dynamic_pointer_cast<BaseDictionarySegment>(encoded_segment)->attribute_vector();
                    switch (attr_vector_ptr->type())
                    {
                    case CompressedVectorType::FixedWidthInteger1Byte:
                    {
                        auto attr_vec_1B_ptr = dynamic_pointer_cast<const FixedWidthIntegerVector<uint8_t>>(attr_vector_ptr);
                        if (!attr_vec_1B_ptr)
                        {
                            std::cerr << "Conversion to FixedWidthIntegerVector<UnsignedIntType::uint8_t> failed" << std::endl;
                            exit(2);
                        }
                        const auto &vector = attr_vec_1B_ptr->data();
                        // const uint8_t* raw_ptr = vector.data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(vector.data()) << "," << reinterpret_cast<uint64_t>(vector.data()) + vector.size() * sizeof(uint8_t) << "\n";
                    }
                    break;
                    case CompressedVectorType::FixedWidthInteger2Byte:
                    {
                        auto attr_vec_2B_ptr = dynamic_pointer_cast<const FixedWidthIntegerVector<uint16_t>>(attr_vector_ptr);
                        if (!attr_vec_2B_ptr)
                        {
                            std::cerr << "Conversion to FixedWidthIntegerVector<UnsignedIntType::uint16_t> failed" << std::endl;
                            exit(2);
                        }
                        const auto &vector = attr_vec_2B_ptr->data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(vector.data()) << "," << reinterpret_cast<uint64_t>(vector.data()) + vector.size() * sizeof(uint16_t) << "\n";
                    }
                    break;
                    case CompressedVectorType::FixedWidthInteger4Byte:
                    {
                        auto attr_vec_4B_ptr = dynamic_pointer_cast<const FixedWidthIntegerVector<uint32_t>>(attr_vector_ptr);
                        if (!attr_vec_4B_ptr)
                        {
                            std::cerr << "Conversion to FixedWidthIntegerVector<UnsignedIntType::uint132_t> failed" << std::endl;
                            exit(2);
                        }
                        const auto &vector = attr_vec_4B_ptr->data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(vector.data()) << "," << reinterpret_cast<uint64_t>(vector.data()) + vector.size() * sizeof(uint32_t) << "\n";
                    }
                    break;
                    default:
                        std::cerr << "Unknown attribute vector type" << std::endl;
                        exit(2);
                    }

                    // Get access to dictionary and print out the data ranges
                    switch (encoded_segment->encoding_type())
                    {
                    case EncodingType::Unencoded:
                    {
                        Fail("An actual segment should never have this type");
                    }
                    case EncodingType::Dictionary:
                    {
                        switch (segment_ptr->data_type())
                        {
                        case detail::DataType::Null:
                            Fail("Incorrect type");
                            break;
                        case detail::DataType::Int:
                        {
                            auto dict = std::dynamic_pointer_cast<DictionarySegment<int>>(std::dynamic_pointer_cast<BaseDictionarySegment>(encoded_segment))->dictionary();
                            // const int32_t *T = dict->data();
                            // std::ostringstream oss;
                            // oss << std::dec << "-1:0x" << std::hex << reinterpret_cast<uintptr_t>(T) << ",0x" << reinterpret_cast<uintptr_t>(T + dict->size()) << "\n";
                            // std::cout<<oss.str();
#ifdef GEM5_RUN
                            m5_add_mem_region(uniq_id, reinterpret_cast<uint64_t>(dict->data()), reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(int));
#else
                            fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(dict->data()) << "," << reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(int) << "\n";
                            // m5_add_mem_region(uniq_id, reinterpret_cast<uint64_t>(dict->data()), reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(int));
#endif
                            break;
                        }
                        case detail::DataType::Long:
                        {
                            auto dict = std::dynamic_pointer_cast<DictionarySegment<long int>>(std::dynamic_pointer_cast<BaseDictionarySegment>(encoded_segment))->dictionary();
                            // const long *T = dict->data();
                            // std::ostringstream oss;
                            // oss << std::dec << "-2:0x" << std::hex << reinterpret_cast<uintptr_t>(T) << ",0x" << reinterpret_cast<uintptr_t>(T + dict->size()) << "\n";
                            // std::cout<<oss.str();
#ifdef GEM5_RUN
                            m5_add_mem_region(uniq_id, reinterpret_cast<uint64_t>(dict->data()), reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(long));
#else
                            fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(dict->data()) << "," << reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(int) << "\n";
#endif
                            break;
                        }
                        case detail::DataType::Float:
                        {
                            auto dict = std::dynamic_pointer_cast<DictionarySegment<float>>(std::dynamic_pointer_cast<BaseDictionarySegment>(encoded_segment))->dictionary();
                            // const float *T = dict->data();
                            // std::ostringstream oss;
                            // oss << std::dec << "-3:0x" << std::hex << reinterpret_cast<uintptr_t>(T) << ",0x" << reinterpret_cast<uintptr_t>(T + dict->size()) << "\n";
                            // std::cout<<oss.str();
#ifdef GEM5_RUN
                            m5_add_mem_region(uniq_id, reinterpret_cast<uint64_t>(dict->data()), reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(float));
#else
                            fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(dict->data()) << "," << reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(int) << "\n";
#endif
                            break;
                        }
                        case detail::DataType::Double:
                        {
                            auto dict = std::dynamic_pointer_cast<DictionarySegment<double>>(std::dynamic_pointer_cast<BaseDictionarySegment>(encoded_segment))->dictionary();
                            // const double *T = dict->data();
                            // std::ostringstream oss;
                            // oss << std::dec << "-4:0x" << std::hex << reinterpret_cast<uintptr_t>(T) << ",0x" << reinterpret_cast<uintptr_t>(T + dict->size()) << "\n";
                            // std::cout<<oss.str();
#ifdef GEM5_RUN
                            m5_add_mem_region(uniq_id, reinterpret_cast<uint64_t>(dict->data()), reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(double));
#else
                            fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(dict->data()) << "," << reinterpret_cast<uint64_t>(dict->data()) + (dict->size() + 1) * sizeof(int) << "\n";
#endif
                            break;
                        }
                        case detail::DataType::String:
                        {
                            auto dict = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(std::dynamic_pointer_cast<BaseDictionarySegment>(encoded_segment))->dictionary();
                            // const pmr_string *T = dict->data();
                            for (unsigned long i = 0; i < dict->size(); i++)
                            {
                                // #ifdef GEM5_RUN
                                auto data = dict->data();
                                // #endif
                                std::ostringstream oss;
                                // std::cout << "String " << i << "(" <<

                                // data[i].size() << "): " << static_cast<const
                                // void*>(data[i].data()) << "\n";
                                // oss << std::dec << "-5:0x" << std::hex << reinterpret_cast<uint64_t>(data[i].data()) << ",0x" << reinterpret_cast<uint64_t>(data[i].data()) + data[i].size()+1 << "\n";
                                // std::cout<<oss.str();
#ifdef GEM5_RUN
                                m5_add_mem_region(uniq_id, reinterpret_cast<uint64_t>(data[i].data()), reinterpret_cast<uint64_t>(data[i].data()) + data[i].size() + 1);
#else
                                fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(data[i].data()) << "," << reinterpret_cast<uint64_t>(data[i].data()) + data[i].size() + 1 << "\n";
#endif
                            }
                            break;
                        }
                        }
                        break;
                    }
                    default:
                        break;
                    }
                }
                else if (const auto &base_value_segment_ptr = std::dynamic_pointer_cast<BaseValueSegment>(segment_ptr))
                {
                    // Its a value segment, uncoded
                    // Cast to correct data type and print addresses
                    // Print out the data ranges
                    switch (base_value_segment_ptr->data_type())
                    {
                    case detail::DataType::Null:
                        Fail("Incorrect type");
                        break;
                    case detail::DataType::Int:
                    {
                        const auto &value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<int32_t>>(base_value_segment_ptr);
                        Assertf(value_segment_ptr != nullptr, "BaseValueSegment to ValueSegment<int32_t> conversion failed\n");
                        const auto &vector = value_segment_ptr->values();
                        const int32_t *raw_ptr = vector.data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(raw_ptr) << "," << reinterpret_cast<uint64_t>(raw_ptr) + (vector.size() + 1) * sizeof(int32_t) << "\n";
                        break;
                    }
                    case detail::DataType::Long:
                    {
                        const auto &value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<int64_t>>(base_value_segment_ptr);
                        Assertf(value_segment_ptr != nullptr, "BaseValueSegment to ValueSegment<int64_t> conversion failed\n");
                        const auto &vector = value_segment_ptr->values();
                        const int64_t *raw_ptr = vector.data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(raw_ptr) << "," << reinterpret_cast<uint64_t>(raw_ptr) + (vector.size() + 1) * sizeof(int64_t) << "\n";
                        break;
                    }
                    case detail::DataType::Float:
                    {
                        const auto &value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<float>>(base_value_segment_ptr);
                        Assertf(value_segment_ptr != nullptr, "BaseValueSegment to ValueSegment<float> conversion failed\n");
                        const auto &vector = value_segment_ptr->values();
                        const float *raw_ptr = vector.data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(raw_ptr) << "," << reinterpret_cast<uint64_t>(raw_ptr) + (vector.size() + 1) * sizeof(float) << "\n";
                        break;
                    }
                    case detail::DataType::Double:
                    {
                        const auto &value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<double>>(base_value_segment_ptr);
                        Assertf(value_segment_ptr != nullptr, "BaseValueSegment to ValueSegment<double> conversion failed\n");
                        const auto &vector = value_segment_ptr->values();
                        const double *raw_ptr = vector.data();
                        fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(raw_ptr) << "," << reinterpret_cast<uint64_t>(raw_ptr) + (vector.size() + 1) * sizeof(double) << "\n";
                        break;
                    }
                    case detail::DataType::String:
                    {
                        const auto &value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<pmr_string>>(base_value_segment_ptr);
                        Assertf(value_segment_ptr != nullptr, "BaseValueSegment to ValueSegment<pmr_string> conversion failed\n");
                        const auto &vector = value_segment_ptr->values();
                        for (auto &s : vector)
                        {
                            const char *raw_ptr = s.data();
                            fout << std::dec << uniq_id << "," << std::hex << reinterpret_cast<uint64_t>(raw_ptr) << "," << reinterpret_cast<uint64_t>(raw_ptr) + s.size() + 1 << "\n";
                        }
                        break;
                    }
                    }
                }
                else
                {
                    Fail("Unknown segment type");
                }
                // std::cout << "-------------------\n";
            }
            // std::cout << "===================\n";
        }
    }

    std::string filename = "mem_regions.csv";
    std::ofstream ofs;
    ofs.open(filename);
    // // std::cout<<"###########\n";
    for (auto &ele : uniq_id_table)
    {
        ofs << ele.first << ":" << ele.second.first << "," << ele.second.second << "\n";
    }
    ofs.close();
    // // std::cout<<"###########\n";
#ifdef GEM5_RUN
    m5_mem_region_cmd(0);
#endif
    return ReturnCode::Ok;
}

void Console::handle_signal(int sig)
{
    if (sig == SIGINT)
    {
        auto &console = Console::get();
        // When in pagination mode, just quit pagination. Otherwise, reset console.
        if (console._pagination_active)
        {
            Pagination::push_ctrl_c();
        }
        else
        {
            // Reset console state
            console._out << "\n";
            console._multiline_input = "";
            console.set_prompt("!> ");
            console._verbose = false;
            // Restore program state stored in jmp_env set with sigsetjmp(2). See comment on jmp_env for details.
            siglongjmp(jmp_env, 1);
        }
    }
}

int Console::_print_transaction_info()
{
    if (!_explicitly_created_transaction_context)
    {
        out("Console is in auto-commit mode. Type `begin` to start a manual transaction.\n");
        return ReturnCode::Error;
    }

    const auto transaction_id = std::to_string(_explicitly_created_transaction_context->transaction_id());
    const auto snapshot_commit_id = std::to_string(_explicitly_created_transaction_context->snapshot_commit_id());
    out("Active transaction: { transaction id = " + transaction_id + ", snapshot commit id = " + snapshot_commit_id +
        " }\n");
    return ReturnCode::Ok;
}

int Console::_print_current_working_directory()
{
    out(std::filesystem::current_path().string() + "\n");
    return ReturnCode::Ok;
}

int Console::_print_current_process_id()
{
    out(std::to_string(getpid()) + "\n");
    return ReturnCode::Ok;
}

int Console::_load_plugin(const std::string &args)
{
    const auto arguments = trim_and_split(args);

    if (arguments.size() != 1)
    {
        out("Usage:\n");
        out("  load_plugin PLUGINPATH\n");
        return ReturnCode::Error;
    }

    const auto &plugin_path_str = arguments.at(0);

    const auto plugin_path = std::filesystem::path{plugin_path_str};
    const auto plugin_name = plugin_name_from_path(plugin_path);

    Hyrise::get().plugin_manager.load_plugin(plugin_path);

    out("Plugin (" + plugin_name + ") successfully loaded.\n");

    return ReturnCode::Ok;
}

int Console::_unload_plugin(const std::string &input)
{
    const auto arguments = trim_and_split(input);

    if (arguments.size() != 1)
    {
        out("Usage:\n");
        out("  unload_plugin NAME\n");
        return ReturnCode::Error;
    }

    const auto &plugin_name = arguments.at(0);

    Hyrise::get().plugin_manager.unload_plugin(plugin_name);

    // The presence of some plugins might cause certain query plans to be generated which will not work if the plugin
    // is stopped. Therefore, we clear the cache. For example, a plugin might create indexes which lead to query plans
    // using IndexScans, these query plans might become unusable after the plugin is unloaded.
    Hyrise::get().default_lqp_cache->clear();
    Hyrise::get().default_pqp_cache->clear();

    out("Plugin (" + plugin_name + ") stopped.\n");

    return ReturnCode::Ok;
}

int Console::_reset()
{
    _rollback();

    Hyrise::reset();
    Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
    Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

    // Restore default settings.
    _binary_caching = true;
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

    return ReturnCode::Ok;
}

void Console::_rollback()
{
    if (_explicitly_created_transaction_context)
    {
        _explicitly_created_transaction_context->rollback(RollbackReason::User);
        out("A transaction was still open and has been rolled back.\n");
    }
}

// GNU readline interface to our commands

char **Console::_command_completion(const char *text, const int start, const int /*end*/)
{
    char **completion_matches = nullptr;

    const auto input = std::string{rl_line_buffer};

    const auto tokens = tokenize(input);

    // Choose completion function depending on the input.
    const auto &first_word = tokens[0];
    if (first_word == "visualize")
    {
        // Completion only for three words, "visualize", and at most two options.
        if (tokens.size() <= 3)
        {
            completion_matches = rl_completion_matches(text, &Console::_command_generator_visualize);
        }
        // Turn off filepath completion.
        rl_attempted_completion_over = 1;
    }
    else if (first_word == "setting")
    {
        if (tokens.size() <= 2)
        {
            completion_matches = rl_completion_matches(text, &Console::_command_generator_setting);
        }
        else if (tokens.size() <= 3 && tokens[1] == "scheduler")
        {
            completion_matches = rl_completion_matches(text, &Console::_command_generator_setting_scheduler);
        }
        // Turn off filepath completion.
        rl_attempted_completion_over = 1;

        // NOLINTNEXTLINE(bugprone-branch-clone)
    }
    else if (first_word == "quit" || first_word == "exit" || first_word == "help")
    {
        // Turn off filepath completion.
        rl_attempted_completion_over = 1;
    }
    else if ((first_word == "load" || first_word == "script") && tokens.size() > 2)
    {
        // Turn off filepath completion after first argument for "load" and "script".
        rl_attempted_completion_over = 1;
    }
    else if (start == 0)
    {
        completion_matches = rl_completion_matches(text, &Console::_command_generator_default);
    }

    return completion_matches;
}

char *Console::_command_generator(const char *text, int state, const std::vector<std::string> &commands)
{
    static std::vector<std::string>::const_iterator it;
    if (state == 0)
    {
        it = commands.begin();
    }

    for (; it != commands.end(); ++it)
    {
        const auto &command = *it;
        if (command.find(text) != std::string::npos)
        {
            auto *completion = new char[command.size()]; // NOLINT(cppcoreguidelines-owning-memory)
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
            static_cast<void>(std::snprintf(completion, command.size() + 1, "%s", command.c_str()));
            return completion;
        }
    }
    return nullptr;
}

char *Console::_command_generator_default(const char *text, int state)
{
    auto commands = std::vector<std::string>();
    for (auto const &command : Console::get()._commands)
    {
        commands.emplace_back(command.first);
    }
    return _command_generator(text, state, commands);
}

char *Console::_command_generator_visualize(const char *text, int state)
{
    return _command_generator(text, state, {"exec", "noexec", "pqp", "lqp", "unoptlqp", "joins"});
}

char *Console::_command_generator_setting(const char *text, int state)
{
    return _command_generator(text, state, {"scheduler"});
}

char *Console::_command_generator_setting_scheduler(const char *text, int state)
{
    return _command_generator(text, state, {"on", "off"});
}

} // namespace hyrise

// int main(int argc, char** argv) {
//   // Make sure the TransactionManager is initialized before the console so that we don't run into destruction order
//   // problems (#1635).
//   hyrise::Hyrise::get();

//   using Return = hyrise::Console::ReturnCode;
//   auto& console = hyrise::Console::get();

//   // Bind CTRL-C to behaviour specified in Console::handle_signal.
//   static_cast<void>(std::signal(SIGINT, &hyrise::Console::handle_signal));

//   console.set_prompt("> ");
//   console.set_logfile("console.log");
//   console.set_console_path(argv[0]);

//   // Load command history
//   console.load_history(".repl_history");

//   // Timestamp dump only to logfile
//   console.out("--- Session start --- " + current_timestamp() + "\n", false);

//   // TODO(anyone): Use std::to_underlying(ReturnCode::Ok) once we use C++23.
//   auto return_code = magic_enum::enum_underlying(Return::Ok);

//   // Display usage if too many arguments are provided.
//   if (argc > 2) {
//     return_code = Return::Quit;
//     console.out("Usage:\n");
//     console.out("  ./hyriseConsole [SCRIPTFILE] - Start the interactive SQL interface.\n");
//     console.out("                                 Execute script if specified by SCRIPTFILE.\n");
//   }

//   // Execute .sql script if specified.
//   if (argc == 2) {
//     return_code = console.execute_script(std::string(argv[1]));
//     // Terminate Console if an error occured during script execution
//     if (return_code == Return::Error) {
//       return_code = Return::Quit;
//     }
//   }

//   // Display welcome message if console started normally.
//   if (argc == 1) {
//     console.out("HYRISE SQL Interface\n");
//     console.out("Type 'help' for more information.\n\n");

//     console.out("Hyrise is running a ");
//     if constexpr (HYRISE_DEBUG) {
//       console.out(ANSI_COLOR_RED "(debug)" ANSI_COLOR_RESET);
//     } else {
//       console.out(ANSI_COLOR_GREEN "(release)" ANSI_COLOR_RESET);
//     }
//     console.out(" build.\n\n");
//   }

//   // Set jmp_env to current program state in preparation for siglongjmp(2). See comment on jmp_env for details.
//   while (sigsetjmp(jmp_env, 1) != 0) {}

//   // Main REPL loop.
//   while (return_code != Return::Quit) {
//     return_code = console.read();
//     if (return_code == Return::Ok) {
//       console.set_prompt("> ");
//     } else if (return_code == Return::Multiline) {
//       console.set_prompt("... ");
//     } else {
//       console.set_prompt("!> ");
//     }
//   }
// }
