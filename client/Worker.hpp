#pragma once
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <future>
#include <atomic>
#include <csignal>
#include <asio.hpp>
#include "Protocol.hpp"
#include "MathSeries.hpp"
#include "ThreadPool.hpp"

using asio::ip::tcp;

// Глобальный флаг для перехвата сигнала Ctrl+C
inline std::atomic<bool> g_stop_requested{false};

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
            while (!g_stop_requested) {
                Header header;
                asio::read(socket_, asio::buffer(&header, sizeof(Header)));

                if (header.type == MessageType::TASK_ASSIGN) {
                    TaskAssignMsg task;
                    asio::read(socket_, asio::buffer(&task, sizeof(TaskAssignMsg)));

                    std::cout << "\n[WORKER] Accepted Task #" << task.task_id 
                              << " Range: [" << task.k_start << " .. " << task.k_end << "]\n";

                    compute_task(task);
                }
            }
        } catch (const std::exception& e) {
            if (!g_stop_requested) {
                std::cout << "[WORKER] Disconnected: " << e.what() << "\n";
            }
        }
    }

    void compute_task(const TaskAssignMsg& task) {
        // Размер микро-батча для проверки Ctrl+C
        const uint64_t BATCH_SIZE = 10'000'000;
        double total_task_sum = 0.0;
        uint64_t last_processed_k = task.k_start;

        for (uint64_t batch_start = task.k_start; batch_start <= task.k_end; batch_start += BATCH_SIZE) {
            // Проверяем, не нажал ли пользователь Ctrl+C
            if (g_stop_requested) {
                std::cout << "\n[WORKER] Interrupt detected! Halting computation...\n";
                std::cout << "[WORKER] Sending checkpoint to server: reached k=" 
                          << last_processed_k << ", partial_sum=" << total_task_sum << "\n";

                // Отправляем серверу контрольную точку с флагом отмены
                Header chk_hdr{MessageType::CHECKPOINT, sizeof(CheckpointMsg)};
                CheckpointMsg chk_msg{task.task_id, last_processed_k, total_task_sum, 1 /* is_aborted */};

                asio::write(socket_, asio::buffer(&chk_hdr, sizeof(Header)));
                asio::write(socket_, asio::buffer(&chk_msg, sizeof(CheckpointMsg)));
                
                socket_.close();
                return;
            }

            uint64_t batch_end = std::min(batch_start + BATCH_SIZE - 1, task.k_end);
            
            // Распараллеливаем текущий батч по ядрам
            uint64_t total_elements = batch_end - batch_start + 1;
            uint64_t chunk_per_thread = total_elements / cores_;
            std::vector<std::future<double>> futures;

            for (uint32_t i = 0; i < cores_; ++i) {
                uint64_t sub_start = batch_start + i * chunk_per_thread;
                uint64_t sub_end = (i == cores_ - 1) ? batch_end : (sub_start + chunk_per_thread - 1);

                futures.push_back(pool_.enqueue([task, sub_start, sub_end]() {
                    return MathSeries::calculate_range(task.series_type, sub_start, sub_end);
                }));
            }

            for (auto& f : futures) {
                total_task_sum += f.get();
            }

            last_processed_k = batch_end;
        }

        // Если дошли сюда — задача завершена полностью
        std::cout << "[WORKER] Task #" << task.task_id << " 100% completed! Sending result.\n";

        Header resp_header{MessageType::TASK_COMPLETED, sizeof(TaskCompletedMsg)};
        TaskCompletedMsg resp_body{task.task_id, total_task_sum};

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