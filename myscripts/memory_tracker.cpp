#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <unistd.h>

// Function to extract value from a line like "Key:    value kB"
long get_value_from_line(const std::string& line) {
    std::stringstream ss(line);
    std::string key;
    long value;
    ss >> key >> value;
    return value;
}

bool print_memory_usage(const std::string& pid, std::ofstream& ostr, bool& started) {
    std::string status_path = "/proc/" + pid + "/status";
    std::ifstream status_file(status_path);

    if (!status_file.is_open() && !started) {
        std::cerr << "Error: Could not open " << status_path << ". Process with PID " << pid << " may not exist." << std::endl;
        return false;
    }
    else if (!status_file.is_open() && started) {
        // Process has ended
        std::cerr << "Process with PID " << pid << " has ended." << std::endl;
        ostr.close();
        return false;
    }

    started = true;

    std::string line;
    long vm_size = 0;
    long rss_size = 0;

    while (std::getline(status_file, line)) {
        if (line.rfind("VmSize:", 0) == 0) { // Check if the line starts with "VmSize:"
            vm_size = get_value_from_line(line);
        } else if (line.rfind("VmRSS:", 0) == 0) { // Check if the line starts with "VmRSS:"
            rss_size = get_value_from_line(line);
        }
    }

    if (vm_size > 0 && rss_size > 0) {
        ostr << vm_size << "," << rss_size << std::endl;
    } else {
        ostr << "Could not find VmSize or VmRSS for PID: " << pid << std::endl;
    }

    return true;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <pid> <output_file>" << std::endl;
        return 1;
    }

    std::string pid = argv[1];
    std::string output_file = argv[2] + std::string{".dat"};

    std::ofstream ostr(output_file);
    if (!ostr.is_open()) {
        std::cerr << "Error: Could not open output file " << output_file << std::endl;
        return 1;
    }

    // You can uncomment the while loop to monitor the process continuously
    bool started = false;
    while (print_memory_usage(pid, ostr, started)) {
        usleep(1000000); // Sleep for 1 second
    }

    return 0;
}
