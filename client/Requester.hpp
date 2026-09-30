#pragma once
#include <iostream>
#include <iomanip>
#include <string>
#include <chrono>
#include <asio.hpp>
#include "Protocol.hpp"

using asio::ip::tcp;

class Requester {
public:
    Requester(const std::string& host, int port) 
        : host_(host), port_(port), socket_(io_context_) {}

    void send_job(uint32_t series_type, uint64_t total_n) {
        std::cout << "[REQUESTER] Connecting to " << host_ << ":" << port_ << "...\n";
        
        tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host_, std::to_string(port_));
        asio::connect(socket_, endpoints);

        std::cout << "[REQUESTER] Sending job: SeriesType=" << series_type 
                  << ", N=" << total_n << "...\n";

        // Отправляем запрос на расчет
        Header header{MessageType::NEW_JOB_REQUEST, sizeof(JobRequestMsg)};
        JobRequestMsg msg{series_type, total_n};

        asio::write(socket_, asio::buffer(&header, sizeof(Header)));
        asio::write(socket_, asio::buffer(&msg, sizeof(JobRequestMsg)));

        std::cout << "[REQUESTER] Job submitted. Waiting for calculation results...\n";

        // Ждем ответ от сервера
        Header resp_header;
        asio::read(socket_, asio::buffer(&resp_header, sizeof(Header)));

        if (resp_header.type == MessageType::JOB_FINISHED) {
            JobFinishedMsg resp;
            asio::read(socket_, asio::buffer(&resp, sizeof(JobFinishedMsg)));

            std::cout << "\n==========================================\n";
            std::cout << ">>> РЕЗУЛЬТАТ ВЫЧИСЛЕНИЙ <<<\n";
            std::cout << "Итоговое значение: " << std::setprecision(12) << resp.result << "\n";
            std::cout << "Затраченное время: " << resp.elapsed_seconds << " сек.\n";
            std::cout << "==========================================\n";
        }
    }

private:
    std::string host_;
    int port_;
    asio::io_context io_context_;
    tcp::socket socket_;
};