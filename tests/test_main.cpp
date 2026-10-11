#include <gtest/gtest.h>
#include <cmath>
#include <numeric>
#include <vector>
#include "MathSeries.hpp"
#include "ThreadPool.hpp"
#include "Protocol.hpp"

// Тест Сходимость ряда Лейбница (проверка приближения числа Pi при расчете 1 млн шагов)
TEST(MathSeriesTest, LeibnizPiConvergence) {
    uint64_t n = 1'000'000;
    double raw_sum = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 0, n);
    double pi_approx = MathSeries::finalize(MathSeries::LEIBNIZ_PI, raw_sum);

    EXPECT_NEAR(pi_approx, M_PI, 0.0001);
}

// Тест Базельская задача (сумма обратных квадратов должна сходиться к pi^2 / 6)
TEST(MathSeriesTest, BaselProblemConvergence) {
    uint64_t n = 1'000'000;
    double sum = MathSeries::calculate_range(MathSeries::RIEMANN_ZETA_2, 1, n);
    double expected = (M_PI * M_PI) / 6.0;

    EXPECT_NEAR(sum, expected, 0.0001);
}

// Тест Аддитивность диапазонов (сумма от 0 до 1000 равна сумме 0..500 + 51..1000)
TEST(MathSeriesTest, RangeAdditivity) {
    double total = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 0, 1000);
    double part1 = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 0, 500);
    double part2 = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 501, 1000);

    EXPECT_DOUBLE_EQ(total, part1 + part2);
}

// Тест Расчет единичного элемента (диапазон где k_start равен k_end)
TEST(MathSeriesTest, SingleElementRange) {
    // При k = 0 член ряда Лейбница равен 1.0 / (2*0 + 1) = 1.0
    double single_first = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 0, 0);
    EXPECT_DOUBLE_EQ(single_first, 1.0);

    // При k = 1 член ряда Лейбница равен -1.0 / (2*1 + 1) = -1.0 / 3.0
    double single_second = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 1, 1);
    EXPECT_DOUBLE_EQ(single_second, -1.0 / 3.0);
}

// Тест Разбиение на произвольное число частей (сумма 5 кусков равна целому диапазону)
TEST(MathSeriesTest, MultiPartRangeAdditivity) {
    uint64_t total_n = 50'000;
    double total_sum = MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, 0, total_n);

    // Делим отрезок на 5 равных кусков
    double chunk_sum = 0.0;
    const uint64_t CHUNKS = 5;
    uint64_t step = (total_n + 1) / CHUNKS;

    for (uint64_t i = 0; i < CHUNKS; ++i) {
        uint64_t start = i * step;
        uint64_t end = (i == CHUNKS - 1) ? total_n : (start + step - 1);
        chunk_sum += MathSeries::calculate_range(MathSeries::LEIBNIZ_PI, start, end);
    }

    EXPECT_NEAR(total_sum, chunk_sum, 1e-12);
}

// Тест Финализация формул (проверка корректного масштабирования промежуточных сумм)
TEST(MathSeriesTest, FinalizeLogic) {
    // Для ряда Лейбница сумма умножается на 4
    double leibniz_raw = 0.5;
    EXPECT_DOUBLE_EQ(MathSeries::finalize(MathSeries::LEIBNIZ_PI, leibniz_raw), 2.0);

    // Для ряда Дзеты сумма возвращается без изменений
    double zeta_raw = 1.2345;
    EXPECT_DOUBLE_EQ(MathSeries::finalize(MathSeries::RIEMANN_ZETA_2, zeta_raw), 1.2345);
}

// Тест Фиксированный размер сетевого заголовка (проверка выравнивания структуры Header без padding)
TEST(ProtocolTest, HeaderPackingSize) {
    // Header состоит из uint8_t (1 байт) + uint32_t (4 байта). 
    // Благодаря #pragma pack(1) размер обязан быть ровно 5 байт, без скрытых байтов выравнивания.
    EXPECT_EQ(sizeof(Header), 5);
}

// Тест Параллельное накопление счетчика (проверка потокобезопасности очередей пула потоков)
TEST(ThreadPoolTest, ConcurrentAccumulation) {
    ThreadPool pool(4);
    std::atomic<int> counter{0};
    std::vector<std::future<void>> futures;

    const int TASKS_COUNT = 100;
    for (int i = 0; i < TASKS_COUNT; ++i) {
        futures.push_back(pool.enqueue([&counter]() {
            counter.fetch_add(1, std::memory_order_relaxed);
        }));
    }

    for (auto& f : futures) {
        f.get();
    }

    EXPECT_EQ(counter.load(), TASKS_COUNT);
}

// Тест Возврат значений из пула потоков (проверка передачи чисел double через std::future)
TEST(ThreadPoolTest, FutureReturnValues) {
    ThreadPool pool(2);
    
    auto f1 = pool.enqueue([]() { return 3.14; });
    auto f2 = pool.enqueue([]() { return 2.71; });

    EXPECT_DOUBLE_EQ(f1.get(), 3.14);
    EXPECT_DOUBLE_EQ(f2.get(), 2.71);
}