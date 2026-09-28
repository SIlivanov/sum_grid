#include <iostream>
#include <string>
#include "Worker.hpp"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Использование:\n";
        std::cout << "  Режим вычислителя: " << argv[0] << " --worker [host] [port]\n";
        std::cout << "  Режим заказчика:   " << argv[0] << " --request <type> <N> [host] [port]\n";
        return 1;
    }

    std::string mode = argv[1];

    if (mode == "--worker") {
        std::string host = (argc > 2) ? argv[2] : "127.0.0.1";
        int port = (argc > 3) ? std::stoi(argv[3]) : 8080;

        try {
            Worker worker(host, port);
            worker.start();
        } catch (const std::exception& e) {
            std::cerr << "[WORKER ERROR] " << e.what() << std::endl;
            return 1;
        }
    } 
    else if (mode == "--request") {
        std::cout << "[REQUESTER] ...\n"; // допилить
    } 
    else {
        std::cerr << "Неизвестный режим: " << mode << "\n";
        return 1;
    }

    return 0;
}