#pragma once
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <future>
#include <asio.hpp>
#include "Protocol.hpp"
#include "MathSeries.hpp"
#include "ThreadPool.hpp"

using asio::ip::tcp;

class Worker {
public:
    Worker(const std::string& host, int port) 
        : host_(host), port_(port), socket_(io_context_),
          cores_(std::max(1u, std::thread::hardware_concurrency())),
          pool_(cores_) {}

    void start() {
        std::cout << "[WORKER] Connecting to " << host_ << ":" << port_ << "...\n";

        tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host_, std::to_string(port_));
        asio::connect(socket_, endpoints);

        std::cout << "[WORKER] Connected. CPU threads available: " << cores_ << "\n";

        register_on_server();
        listen_server();
    }

private:
    void register_on_server() {
        Header header{MessageType::REGISTER_WORKER, sizeof(RegisterWorkerMsg)};
        RegisterWorkerMsg body{cores_};

        asio::write(socket_, asio::buffer(&header, sizeof(Header)));
        asio::write(socket_, asio::buffer(&body, sizeof(RegisterWorkerMsg)));
    }

    void listen_server() {
        try {
            while (true) {
                Header header;
                asio::read(socket_, asio::buffer(&header, sizeof(Header)));

                if (header.type == MessageType::TASK_ASSIGN) {
                    TaskAssignMsg task;
                    asio::read(socket_, asio::buffer(&task, sizeof(TaskAssignMsg)));

                    std::cout << "\n[WORKER] Received Task #" << task.task_id 
                              << " Range: [" << task.k_start << " .. " << task.k_end << "]\n";

                    compute_task(task);
                }
            }
        } catch (const std::exception& e) {
            std::cout << "[WORKER] Connection closed: " << e.what() << "\n";
        }
    }

    void compute_task(const TaskAssignMsg& task) {
        uint64_t total_elements = task.k_end - task.k_start + 1;
        uint64_t chunk_per_thread = total_elements / cores_;

        std::vector<std::future<double>> futures;

        auto start_time = std::chrono::high_resolution_clock::now();

        // Распределяем работу потоками
        for (uint32_t i = 0; i < cores_; ++i) {
            uint64_t sub_start = task.k_start + i * chunk_per_thread;
            uint64_t sub_end = (i == cores_ - 1) ? task.k_end : (sub_start + chunk_per_thread - 1);

            futures.push_back(pool_.enqueue([task, sub_start, sub_end]() {
                return MathSeries::calculate_range(task.series_type, sub_start, sub_end);
            }));
        }

        // Собираем сумму со всех ядер
        double task_sum = 0.0;
        for (auto& f : futures) {
            task_sum += f.get();
        }

        auto elapsed = std::chrono::duration<double>(
            std::chrono::high_resolution_clock::now() - start_time).count();

        std::cout << "[WORKER] Task #" << task.task_id 
                  << " done in " << elapsed << "s! Sending partial sum: " << task_sum << "\n";

        // Отправляем результат серверу
        Header resp_header{MessageType::TASK_COMPLETED, sizeof(TaskCompletedMsg)};
        TaskCompletedMsg resp_body{task.task_id, task_sum};

        asio::write(socket_, asio::buffer(&resp_header, sizeof(Header)));
        asio::write(socket_, asio::buffer(&resp_body, sizeof(TaskCompletedMsg)));
    }

    std::string host_;
    int port_;
    uint32_t cores_;
    ThreadPool pool_;
    asio::io_context io_context_;
    tcp::socket socket_;
};