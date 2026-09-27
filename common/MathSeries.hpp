#pragma once
#include <cstdint>

class MathSeries {
public:
    enum Type : uint32_t {
        LEIBNIZ_PI = 1,     // Приближение числа Pi
        RIEMANN_ZETA_2 = 2  // Сумма 1 / k^2 = pi^2 / 6
    };

    // Вычисляет кусок ряда от k_start до k_end включительно
    static double calculate_range(uint32_t type, uint64_t k_start, uint64_t k_end) {
        double sum = 0.0;
        if (type == LEIBNIZ_PI) {
            for (uint64_t k = k_start; k <= k_end; ++k) {
                double term = 1.0 / (2.0 * k + 1.0);
                sum += (k % 2 == 0) ? term : -term;
            }
        } else if (type == RIEMANN_ZETA_2) {
            for (uint64_t k = k_start; k <= k_end; ++k) {
                if (k == 0) continue;
                sum += 1.0 / (static_cast<double>(k) * static_cast<double>(k));
            }
        }
        return sum;
    }

    static double finalize(uint32_t type, double raw_sum) {
        if (type == LEIBNIZ_PI) {
            return raw_sum * 4.0;
        }
        return raw_sum;
    }
};