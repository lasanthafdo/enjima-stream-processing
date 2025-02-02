//
// Created by m34ferna on 17/01/24.
//

#ifndef ENJIMA_MEMORY_UTIL_H
#define ENJIMA_MEMORY_UTIL_H

#include <cstddef>
#include <type_traits>

namespace enjima::memory {
    constexpr size_t KiloBytes(const size_t val)
    {
        return val << 10;
    }

    constexpr size_t MegaBytes(const size_t val)
    {
        return val << 20;
    }

    constexpr size_t GigaBytes(const size_t val)
    {
        return val << 30;
    }

    template<typename T>
        requires std::is_integral_v<T>
    constexpr T ToKiloBytes(const T bytes)
    {
        return bytes >> 10;
    }

}// namespace enjima::memory

#endif//ENJIMA_MEMORY_UTIL_H
