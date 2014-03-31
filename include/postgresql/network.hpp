#ifndef POSTGRESQL_NETWORK_HPP_
#define POSTGRESQL_NETWORK_HPP_

#include "b/endian.hpp"

namespace postgresql {
namespace network {

template <typename TYPE>
static inline TYPE
order(TYPE x)
{
    if (b::endian::BIG)
        return x;
    else
        return b::endian::swap(x);
}

} // namespace network
} // namespace postgresql

#endif
