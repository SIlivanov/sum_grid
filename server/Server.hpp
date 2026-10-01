#pragma once
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <asio.hpp>
#include "Protocol.hpp"
#include "MathSeries.hpp"

using asio::ip::tcp;

// TODO: РАЗБИТЬ ПРОГРАММУ НА ФУНКЦИИ, ЧТОБЫ НЕБЫЛО СЛИШКОМ МНОГО ВЛОЖЕНИЙ

struct SubTask {
    uint32_t task_id;
    uint32_t series_type;
    uint64_t k_start;
    uint64_t k_end;
};

struct WorkerInfo {
    uint32_t id;
    std::shared_ptr<tcp::socket> socket;
    uint32_t cores;
    bool is_busy = false;
    bool is_alive = true;
};

class Server {
public:
    Server(short port) 
        : acceptor_(io_context_, tcp::endpoint(tcp::v4(), port)) 
    {
        std::cout << "[SERVER] Resilient Server listening on port " << port << std::endl;
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

                uint32_t id;
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    id = next_worker_id_++;
                    workers_.push_back({id, socket, msg.cpu_cores, false, true});
                }
                std::cout << "[SERVER] Worker #" << id << " connected (" << msg.cpu_cores << " threads)\n";

                // Поток держит соединение открытым
                while (socket->is_open()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            } 
            else if (header.type == MessageType::NEW_JOB_REQUEST) {
                JobRequestMsg job;
                asio::read(*socket, asio::buffer(&job, sizeof(JobRequestMsg)));

                std::cout << "\n[SERVER] New Job: Type=" << job.series_type 
                          << ", N=" << job.total_n << "\n";

                execute_job_resilient(socket, job);
            }
        } catch (const std::exception& e) {
            // Отключение клиента
        }
    }

    void execute_job_resilient(std::shared_ptr<tcp::socket> req_socket, const JobRequestMsg& job) {
        std::vector<WorkerInfo*> active_workers;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto& w : workers_) {
                if (w.is_alive) active_workers.push_back(&w);
            }
        }

        if (active_workers.empty()) {
            std::cerr << "[SERVER] Error: No workers available!\n";
            return;
        }

        auto start_time = std::chrono::high_resolution_clock::now();

        // Формируем очередь подзадач для воркеров
        std::queue<SubTask> task_queue;
        uint32_t num_chunks = active_workers.size() * 2; // Режем на порции с запасом
        uint64_t chunk_size = (job.total_n + 1) / num_chunks;
        
        uint32_t tid = 1;
        for (uint32_t i = 0; i < num_chunks; ++i) {
            uint64_t start = i * chunk_size;
            uint64_t end = (i == num_chunks - 1) ? job.total_n : (start + chunk_size - 1);
            task_queue.push({tid++, job.series_type, start, end});
        }

        double accumulated_sum = 0.0;
        std::mutex job_mtx;
        std::condition_variable job_cv;
        size_t tasks_in_progress = 0;

        // Поток-диспетчер задач
        std::thread scheduler([&]() {
            while (true) {
                SubTask cur_task;
                WorkerInfo* chosen_worker = nullptr;

                {
                    std::unique_lock<std::mutex> lock(job_mtx);
                    if (task_queue.empty() && tasks_in_progress == 0) {
                        job_cv.notify_all();
                        break; // Все задачи выполнены!
                    }

                    if (!task_queue.empty()) {
                        // Ищем свободного живого воркера
                        std::lock_guard<std::mutex> wlock(mtx_);
                        for (auto* w : active_workers) {
                            if (w->is_alive && !w->is_busy) {
                                chosen_worker = w;
                                break;
                            }
                        }

                        if (chosen_worker) {
                            cur_task = task_queue.front();
                            task_queue.pop();
                            chosen_worker->is_busy = true;
                            tasks_in_progress++;
                        }
                    }
                }

                if (chosen_worker) {
                    // Запускаем обслуживание задачи на выбранном воркере
                    std::thread([&, chosen_worker, cur_task]() {
                        try {
                            Header hdr{MessageType::TASK_ASSIGN, sizeof(TaskAssignMsg)};
                            TaskAssignMsg msg{cur_task.task_id, cur_task.series_type, cur_task.k_start, cur_task.k_end};

                            asio::write(*chosen_worker->socket, asio::buffer(&hdr, sizeof(Header)));
                            asio::write(*chosen_worker->socket, asio::buffer(&msg, sizeof(TaskAssignMsg)));

                            // Ждем ответа от воркера
                            Header resp_hdr;
                            asio::read(*chosen_worker->socket, asio::buffer(&resp_hdr, sizeof(Header)));

                            if (resp_hdr.type == MessageType::TASK_COMPLETED) {
                                TaskCompletedMsg cmsg;
                                asio::read(*chosen_worker->socket, asio::buffer(&cmsg, sizeof(TaskCompletedMsg)));

                                std::lock_guard<std::mutex> lock(job_mtx);
                                accumulated_sum += cmsg.final_sum;
                                tasks_in_progress--;
                                chosen_worker->is_busy = false;
                                std::cout << "[SERVER] Task #" << cur_task.task_id << " successfully completed.\n";
                            } 
                            else if (resp_hdr.type == MessageType::CHECKPOINT) {
                                CheckpointMsg chk;
                                asio::read(*chosen_worker->socket, asio::buffer(&chk, sizeof(CheckpointMsg)));

                                std::lock_guard<std::mutex> lock(job_mtx);
                                accumulated_sum += chk.partial_sum;
                                chosen_worker->is_alive = false; // Воркер выключился
                                chosen_worker->is_busy = false;

                                std::cout << "\n>>> [FAULT TOLERANCE] Worker #" << chosen_worker->id 
                                          << " aborted Task #" << cur_task.task_id << "!\n";
                                std::cout << ">>> Saved partial sum: " << chk.partial_sum << " up to k=" << chk.current_k << "\n";

                                // Если остался недосчитанный хвост - кладем его обратно в очередь!
                                if (chk.current_k < cur_task.k_end) {
                                    SubTask remainder{cur_task.task_id, cur_task.series_type, chk.current_k + 1, cur_task.k_end};
                                    std::cout << ">>> Reassigning remainder [" << remainder.k_start 
                                              << " .. " << remainder.k_end << "] to queue!\n\n";
                                    task_queue.push(remainder);
                                }
                                tasks_in_progress--;
                            }
                        } catch (...) {
                            // Аварийный обрыв соединения воркера
                            std::lock_guard<std::mutex> lock(job_mtx);
                            chosen_worker->is_alive = false;
                            chosen_worker->is_busy = false;
                            task_queue.push(cur_task); // Возвращаем задачу целиком
                            tasks_in_progress--;
                        }
                    }).detach();
                } else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
        });

        // Ждем завершения работы планировщика
        scheduler.join();

        auto end_time = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(end_time - start_time).count();
        double final_result = MathSeries::finalize(job.series_type, accumulated_sum);

        std::cout << "\n[SERVER] All tasks finished! Final Result: " << final_result 
                  << " (" << elapsed << " sec)\n";

        // Отправляем ответ заказчику
        Header finish_hdr{MessageType::JOB_FINISHED, sizeof(JobFinishedMsg)};
        JobFinishedMsg finish_msg{final_result, elapsed};

        asio::write(*req_socket, asio::buffer(&finish_hdr, sizeof(Header)));
        asio::write(*req_socket, asio::buffer(&finish_msg, sizeof(JobFinishedMsg)));
    }

    asio::io_context io_context_;
    tcp::acceptor acceptor_;
    std::mutex mtx_;
    std::vector<WorkerInfo> workers_;
    uint32_t next_worker_id_ = 1;
};