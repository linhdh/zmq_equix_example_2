//
// Created by linhdh on 30/09/2025.
//

#include "subcli6.hpp"

int main() {
    clone_t clone_client;
    //clone_client.set_receive_options(1000000, 16777216);
    clone_client.subtree(SUBTREE);
    clone_client.connect("tcp://localhost", "5556");
    clone_client.connect("tcp://localhost", "5566");

    std::cout << "[SUBCLI6] Clone client started, subscribing to: " << SUBTREE << std::endl;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
