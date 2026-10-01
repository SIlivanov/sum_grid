#include <iostream>
#include <string>
#include <csignal>
#include "Worker.hpp"
#include "Requester.hpp"

// Обработчик сигнала Ctrl+C
void signal_handler(int signal) {
    if (signal == SIGINT) {
        std::cout << "\n[SIGNAL] SIGINT received! Graceful shutdown initiated...\n";
        g_stop_requested = true;
    }
}

int main(int argc, char* argv[]) {
    // Регистрируем перехват Ctrl+C
    std::signal(SIGINT, signal_handler);

    if (argc < 2) {
        std::cout << "Использование:\n";
        std::cout << "  Режим воркера:   " << argv[0] << " --worker [host] [port]\n";
        std::cout << "  Режим заказчика: " << argv[0] << " --request <type: 1|2> <N> [host] [port]\n";
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
        if (argc < 4) {
            std::cerr << "Укажите тип ряда (1=Pi, 2=Zeta) и N!\n";
            return 1;
        }

        uint32_t series_type = std::stoul(argv[2]);
        uint64_t n = std::stoull(argv[3]);
        std::string host = (argc > 4) ? argv[4] : "127.0.0.1";
        int port = (argc > 5) ? std::stoi(argv[5]) : 8080;

        try {
            Requester req(host, port);
            req.send_job(series_type, n);
        } catch (const std::exception& e) {
            std::cerr << "[REQUESTER ERROR] " << e.what() << std::endl;
            return 1;
        }
    }

    return 0;
}