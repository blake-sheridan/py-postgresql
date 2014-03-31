#ifndef B_ENDIAN_HPP_
#define B_ENDIAN_HPP_

#include <cstdint>

namespace b {
namespace endian {

static const bool BIG =
#ifdef __BYTE_ORDER
#  if (__BYTE_ORDER == __BIG_ENDIAN)
    true;
#  else
#    if (__BYTE_ORDER == __LITTLE_ENDIAN)
    false;
#    else
#      error neither big nor little
#    endif
#  endif
#else
#  error TODO - byte order
#endif

#ifdef __GNUC__
#  include <byteswap.h>
#endif

template <typename TYPE>
static inline TYPE
swap(TYPE x)
{
    static_assert(sizeof(TYPE) == 2 || sizeof(TYPE) == 4 || sizeof(TYPE) == 8, "only 16/32/64 supported");

    if (sizeof(TYPE) == 2) {
#ifdef __GNUC__
        return bswap_16(x);
#else
        return (TYPE)(
            (((uint16_t)x & 0x00ff) << 8) |
            (((uint16_t)x & 0xff00) >> 8)
            );
#endif
    }


    if (sizeof(TYPE) == 4) {
#ifdef __GNUC__
        return bswap_32(x);
#else
        return (TYPE)(
            (((uint32_t)x & 0x000000ff) << 24) |
            (((uint32_t)x & 0x0000ff00) <<  8) |
            (((uint32_t)x & 0x00ff0000) >>  8) |
            (((uint32_t)x & 0xff000000) >> 24)
            );
#endif
    }

    else {
#ifdef __GNUC__
        return bswap_64(x);
#else
        return (TYPE)(
            (((uint64_t)x & 0x00000000000000ff) << 56) |
            (((uint64_t)x & 0x000000000000ff00) << 40) |
            (((uint64_t)x & 0x0000000000ff0000) << 24) |
            (((uint64_t)x & 0x00000000ff000000) <<  8) |
            (((uint64_t)x & 0x000000ff00000000) >>  8) |
            (((uint64_t)x & 0x0000ff0000000000) >> 24) |
            (((uint64_t)x & 0x00ff000000000000) >> 40) |
            (((uint64_t)x & 0xff00000000000000) >> 56)
            );
#endif
    }
}

} // namespace endian
} // namespace b

#endif
