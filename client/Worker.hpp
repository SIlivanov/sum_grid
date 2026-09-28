#pragma once
#include <iostream>
#include <thread>
#include <string>
#include <asio.hpp>
#include "Protocol.hpp"

using asio::ip::tcp;

class Worker {
public:
    Worker(const std::string& host, int port) 
        : host_(host), port_(port), socket_(io_context_) {}

    void start() {
        std::cout << "[WORKER] Connecting to " << host_ << ":" << port_ << "...\n";

        tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host_, std::to_string(port_));
        asio::connect(socket_, endpoints);

        std::cout << "[WORKER] Connected successfully!\n";

        // Регистрируемся на сервере
        register_on_server();

        // Основной цикл ожидания команд/задач от сервера (пока просто держим связь)
        listen_server();
    }

private:
    void register_on_server() {
        // определение доступных потоков CPU
        uint32_t cores = std::thread::hardware_concurrency();
        if (cores == 0) cores = 1; // запасной вариант

        std::cout << "[WORKER] Hardware concurrency: " << cores << " threads\n";

        // Формируем заголовок и тело
        Header header{MessageType::REGISTER_WORKER, sizeof(RegisterWorkerMsg)};
        RegisterWorkerMsg body{cores};

        // Отправляем серверу
        asio::write(socket_, asio::buffer(&header, sizeof(Header)));
        asio::write(socket_, asio::buffer(&body, sizeof(RegisterWorkerMsg)));

        std::cout << "[WORKER] Registration packet sent.\n";
    }

    void listen_server() {
        try {
            while (true) {
                //тут будет прием задач
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        } catch (const std::exception& e) {
            std::cout << "[WORKER] Disconnected: " << e.what() << "\n";
        }
    }

    std::string host_;
    int port_;
    asio::io_context io_context_;
    tcp::socket socket_;
};