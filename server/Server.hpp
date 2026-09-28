#pragma once
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <asio.hpp>
#include "Protocol.hpp"

using asio::ip::tcp;

class Server {
public:
    Server(short port) 
        : acceptor_(io_context_, tcp::endpoint(tcp::v4(), port)) 
    {
        std::cout << "[SERVER] Started on port " << port << std::endl;
        accept_connections();
    }

    void run() {
        // Запуск цикла обработки сетевых событий
        io_context_.run();
    }

private:
    void accept_connections() {
        // Асинхронно ждем подключения нового клиента
        acceptor_.async_accept(
            [this](std::error_code ec, tcp::socket socket) {
                if (!ec) {
                    std::cout << "[SERVER] New connection from: " 
                              << socket.remote_endpoint() << std::endl;
                    
                    // Запускаем сессию для клиента в отдельном потоке
                    std::thread(&Server::handle_client, this, std::move(socket)).detach();
                }
                accept_connections(); // Снова встаем на ожидание
            });
    }

    void handle_client(tcp::socket socket) {
        try {
            while (true) {
                // Читаем фиксированный заголовок (Header)
                Header header;
                asio::read(socket, asio::buffer(&header, sizeof(Header)));

                // В зависимости от типа сообщения читаем данные
                if (header.type == MessageType::REGISTER_WORKER) {
                    RegisterWorkerMsg msg;
                    asio::read(socket, asio::buffer(&msg, sizeof(RegisterWorkerMsg)));

                    std::cout << "[SERVER] Registered WORKER: " 
                              << socket.remote_endpoint() 
                              << " with " << msg.cpu_cores << " CPU cores\n";
                } 
                else {
                    std::cout << "[SERVER] Unknown message type: " 
                              << static_cast<int>(header.type) << "\n";
                    break;
                }
            }
        } catch (const std::exception& e) {
            std::cout << "[SERVER] Client disconnected (" << e.what() << ")\n";
        }
    }

    asio::io_context io_context_;
    tcp::acceptor acceptor_;
};