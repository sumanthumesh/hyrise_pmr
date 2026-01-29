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

void print_memory_usage(const std::string& pid) {
    std::string status_path = "/proc/" + pid + "/status";
    std::ifstream status_file(status_path);

    if (!status_file.is_open()) {
        std::cerr << "Error: Could not open " << status_path << ". Process with PID " << pid << " may not exist." << std::endl;
        return;
    }

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
        std::cout << vm_size << "," << rss_size << std::endl;
    } else {
        std::cout << "Could not find VmSize or VmRSS for PID: " << pid << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <pid>" << std::endl;
        return 1;
    }

    std::string pid = argv[1];

    // You can uncomment the while loop to monitor the process continuously
    while (true) {
        print_memory_usage(pid);
        usleep(1000000); // Sleep for 1 second
    }

    return 0;
}
