#pragma once
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <asio.hpp>
#include "Protocol.hpp"
#include "MathSeries.hpp"

using asio::ip::tcp;

struct WorkerSession {
    uint32_t id;
    std::shared_ptr<tcp::socket> socket;
    uint32_t cores;
};

class Server {
public:
    Server(short port) 
        : acceptor_(io_context_, tcp::endpoint(tcp::v4(), port)) 
    {
        std::cout << "[SERVER] Listening on port " << port << std::endl;
        accept_connections();
    }

    void run() {
        io_context_.run();
    }

private:
    void accept_connections() {
        acceptor_.async_accept(
            [this](std::error_code ec, tcp::socket socket) {
                if (!ec) {
                    auto sock_ptr = std::make_shared<tcp::socket>(std::move(socket));
                    std::thread(&Server::handle_client, this, sock_ptr).detach();
                }
                accept_connections();
            });
    }

    void handle_client(std::shared_ptr<tcp::socket> socket) {
        try {
            Header header;
            asio::read(*socket, asio::buffer(&header, sizeof(Header)));

            if (header.type == MessageType::REGISTER_WORKER) {
                RegisterWorkerMsg msg;
                asio::read(*socket, asio::buffer(&msg, sizeof(RegisterWorkerMsg)));

                uint32_t worker_id;
                {
                    std::lock_guard<std::mutex> lock(workers_mtx_);
                    worker_id = next_worker_id_++;
                    workers_.push_back({worker_id, socket, msg.cpu_cores});
                }
                std::cout << "[SERVER] Worker #" << worker_id 
                          << " registered with " << msg.cpu_cores << " CPU cores.\n";

                // Держим поток сессии открытым, пока сокет жив
                while (socket->is_open()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            } 
            else if (header.type == MessageType::NEW_JOB_REQUEST) {
                JobRequestMsg job;
                asio::read(*socket, asio::buffer(&job, sizeof(JobRequestMsg)));

                std::cout << "\n[SERVER] New job received: Type=" << job.series_type 
                          << ", N=" << job.total_n << "\n";

                process_job(socket, job);
            }
        } catch (const std::exception& e) {
            // Клиент отключился
        }
    }

    void process_job(std::shared_ptr<tcp::socket> requester_sock, const JobRequestMsg& job) {
        std::vector<WorkerSession> available_workers;
        {
            std::lock_guard<std::mutex> lock(workers_mtx_);
            available_workers = workers_;
        }

        if (available_workers.empty()) {
            std::cerr << "[SERVER] Error: No workers available to calculate!\n";
            return;
        }

        auto start_time = std::chrono::high_resolution_clock::now();

        // Считаем общее количество доступных ядер во всей сети
        uint32_t total_network_cores = 0;
        for (const auto& w : available_workers) {
            total_network_cores += w.cores;
        }

        std::cout << "[SERVER] Distributing job across " << available_workers.size() 
                  << " worker(s) (total " << total_network_cores << " cores)...\n";

        // Делим диапазон [0, total_n] пропорционально ядрам воркеров
        uint64_t current_k = 0;
        double accumulated_sum = 0.0;

        for (size_t i = 0; i < available_workers.size(); ++i) {
            auto& worker = available_workers[i];
            
            // Доля элементов на воркера
            uint64_t worker_chunk = (job.total_n * worker.cores) / total_network_cores;
            uint64_t k_start = current_k;
            uint64_t k_end = (i == available_workers.size() - 1) ? job.total_n : (k_start + worker_chunk - 1);
            current_k = k_end + 1;

            // Отправляем задачу воркеру
            Header task_hdr{MessageType::TASK_ASSIGN, sizeof(TaskAssignMsg)};
            TaskAssignMsg task_body{static_cast<uint32_t>(i + 1), job.series_type, k_start, k_end};

            asio::write(*worker.socket, asio::buffer(&task_hdr, sizeof(Header)));
            asio::write(*worker.socket, asio::buffer(&task_body, sizeof(TaskAssignMsg)));

            // Ждем завершения от воркера (в этом базовом шаге синхронно)
            Header resp_hdr;
            asio::read(*worker.socket, asio::buffer(&resp_hdr, sizeof(Header)));
            if (resp_hdr.type == MessageType::TASK_COMPLETED) {
                TaskCompletedMsg comp_msg;
                asio::read(*worker.socket, asio::buffer(&comp_msg, sizeof(TaskCompletedMsg)));
                accumulated_sum += comp_msg.final_sum;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(end_time - start_time).count();

        // Финализируем формулу (например, домножаем на 4 для числа Пи)
        double final_result = MathSeries::finalize(job.series_type, accumulated_sum);

        std::cout << "[SERVER] Job finished in " << elapsed 
                  << "s. Result: " << final_result << ". Notifying requester...\n";

        // Отправляем ответ заказчику
        Header finish_hdr{MessageType::JOB_FINISHED, sizeof(JobFinishedMsg)};
        JobFinishedMsg finish_msg{final_result, elapsed};

        asio::write(*requester_sock, asio::buffer(&finish_hdr, sizeof(Header)));
        asio::write(*requester_sock, asio::buffer(&finish_msg, sizeof(JobFinishedMsg)));
    }

    asio::io_context io_context_;
    tcp::acceptor acceptor_;
    std::mutex workers_mtx_;
    std::vector<WorkerSession> workers_;
    uint32_t next_worker_id_ = 1;
};