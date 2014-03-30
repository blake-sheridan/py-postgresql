#ifndef POSTGRESQL_DECODE_HPP_
#define POSTGRESQL_DECODE_HPP_

#include "Python.h"
#include <netinet/in.h> // htonl

#include "libpq-fe.h"

namespace postgresql {
namespace decode {

static inline PyObject *
bool_(PGresult *r, int i, int j)
{
    if (*PQgetvalue(r, i, j))
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

static inline PyObject *
bytea(PGresult *r, int i, int j)
{
    if (*PQgetvalue(r, i, j))
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}

static inline PyObject *
char_(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
date(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
float4(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
float4_array(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
float8(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
int2(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
int4(PGresult *r, int i, int j)
{
    char   *bytes      = PQgetvalue(r, i, j);
    int32_t host_value = *(int32_t *)bytes;
    int32_t value      = htonl(host_value);

    return PyLong_FromLong(value);
}

static inline PyObject *
int4_array(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
int8(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
interval(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
record(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
text(PGresult *r, int i, int j)
{
    int   length = PQgetlength(r, i, j);
    char *value  = PQgetvalue (r, i, j);

    return PyUnicode_FromStringAndSize(value, length);
}

static inline PyObject *
text_array(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
time(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
timetz(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
timestamp(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
timestamptz(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
uuid(PGresult *r, int i, int j)
{
    TODO();
    return NULL;
}

static inline PyObject *
decode(PGresult *r, int i, int j)
{
    switch (PQftype(r, j)) {
      case 16:   return         bool_(r, i, j);
      case 17:   return         bytea(r, i, j);
      case 18:   return         char_(r, i, j);
      case 20:   return          int8(r, i, j);
      case 21:   return          int2(r, i, j);
      case 23:   return          int4(r, i, j);
      case 25:   return          text(r, i, j);
      case 700:  return        float4(r, i, j);
      case 701:  return        float8(r, i, j);
      case 1007: return    int4_array(r, i, j);
      case 1009: return    text_array(r, i, j);
      case 1021: return  float4_array(r, i, j);
      case 1082: return          date(r, i, j);
      case 1083: return          time(r, i, j);
      case 1114: return     timestamp(r, i, j);
      case 1184: return   timestamptz(r, i, j);
      case 1186: return      interval(r, i, j);
      case 1266: return        timetz(r, i, j);
      case 2249: return        record(r, i, j);
      case 2950: return          uuid(r, i, j);
      default:
          PyErr_Format(PyExc_NotImplementedError, "%zd", PQftype(r, j));
          return NULL;
    }
}

} // namespace decode
} // namespace postgresql

#endif
