#include <iostream>
#include "Server.hpp"

int main(int argc, char* argv[]) {
    short port = 8080;
    if (argc > 1) {
        port = static_cast<short>(std::stoi(argv[1]));
    }

    try {
        Server server(port);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "[SERVER ERROR] " << e.what() << std::endl;
        return 1;
    }

    return 0;
}