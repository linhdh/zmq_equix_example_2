//
// Created by linhdh on 30/09/2025.
//

#include "pubcli6.hpp"

int main(int argc, char *argv[]) {
    // Create clone client
    auto clone_client = new clone_t();
    clone_client->subtree(SUBTREE);
    clone_client->connect("tcp://localhost", "5556");
    clone_client->connect("tcp://localhost", "5566");

    constexpr size_t TARGET_MESSAGES = 1000000;
    const std::string KEY_SUFFIX = "COS";
    size_t count = 0;

    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "[PUBCLI6] Ready to publish with key: " << SUBTREE << KEY_SUFFIX << std::endl;

    while (count < TARGET_MESSAGES) {
        std::string key = SUBTREE + KEY_SUFFIX;
        std::string value = std::to_string(count);
        clone_client->set(key, value, 30);
        count++;
    }
    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(now - start).count();
    double rate = count / elapsed;
    std::cout << "[PUBCLI6] Sent " << count << "/" << TARGET_MESSAGES << " (" << rate << " msg/s) in " << elapsed << " second(s)." << std::endl;
    std::cout << "[PUBCLI6] Completed sending " << count << " messages" << std::endl;
    delete clone_client;
}
