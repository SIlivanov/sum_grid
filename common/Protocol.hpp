#pragma once
#include <cstdint>

enum class MessageType : uint8_t {
    REGISTER_WORKER = 1,
    TASK_ASSIGN     = 2,
    CHECKPOINT      = 3,
    TASK_COMPLETED  = 4,
    NEW_JOB_REQUEST = 5,
    JOB_FINISHED    = 6
};

#pragma pack(push, 1)

// Заголовок любого сообщения (всегда 5 байт)
struct Header {
    MessageType type;
    uint32_t payload_size;
};

// Клиент как воркер
struct RegisterWorkerMsg {
    uint32_t cpu_cores;
};

struct TaskAssignMsg {
    uint32_t task_id;
    uint32_t series_type; // 1 = Лейбниц, 2 = базельская задача
    uint64_t k_start;
    uint64_t k_end;
};

// Контрольная точка при выходе клиента(воркера) за работой
struct CheckpointMsg {
    uint32_t task_id;
    uint64_t current_k;
    double partial_sum; // Накопленная сумма
    uint8_t is_aborted; // 1 = воркер аварийно выходит (Ctrl+C), 0 = просто пульс
};

// Воркер успешно закончил
struct TaskCompletedMsg {
    uint32_t task_id;
    double final_sum;
};

// Заказчик просит сервер посчитать ряд
struct JobRequestMsg {
    uint32_t series_type;
    uint64_t total_n;
};

// Сервер возвращает заказчику финальный результат
struct JobFinishedMsg {
    double result;
    double elapsed_seconds;
};

#pragma pack(pop)