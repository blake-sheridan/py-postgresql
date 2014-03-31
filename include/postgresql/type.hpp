#ifndef POSTGRESQL_TYPE_HPP_
#define POSTGRESQL_TYPE_HPP_

#include "Python.h"
#include <netinet/in.h> // htonl

#include "libpq-fe.h"

namespace postgresql {

class BOOL
{
  public:
    static const Oid OID = 16;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        if (*PQgetvalue(r, i, j))
            Py_RETURN_TRUE;
        else
            Py_RETURN_FALSE;
    }
};

class BYTEA
{
  public:
    static const Oid OID = 17;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class CHAR
{
  public:
    static const Oid OID = 18;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class DATE
{
  public:
    static const Oid OID = 1082;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class FLOAT4
{
  public:
    static const Oid OID = 700;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class FLOAT8
{
  public:
    static const Oid OID = 701;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class INT2
{
  public:
    static const Oid OID = 21;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        char   *bytes = PQgetvalue(r, i, j);
        int16_t value = ntohs(*(int16_t *)bytes);

        return PyLong_FromLong(value);
    }
};

class INT4
{
  public:
    static const Oid OID = 23;
    static const Oid OID_ARRAY = 1007;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        char   *bytes = PQgetvalue(r, i, j);
        int32_t value = ntohl(*(int32_t *)bytes);

        return PyLong_FromLong(value);
    }

    static inline PyObject *
    decode_array(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class INT8
{
  public:
    static const Oid OID = 20;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class INTERVAL
{
  public:
    static const Oid OID = 1186;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class RECORD
{
  public:
    static const Oid OID = 2249;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class TEXT
{
  public:
    static const Oid OID = 25;
    static const Oid OID_ARRAY = 1009;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        int   length = PQgetlength(r, i, j);
        char *value  = PQgetvalue (r, i, j);

        return PyUnicode_FromStringAndSize(value, length);
    }

    static inline PyObject *
    decode_array(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class TIME
{
  public:
    static const Oid OID = 1083;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class TIMESTAMP
{
  public:
    static const Oid OID = 1114;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class TIMESTAMPTZ
{
  public:
    static const Oid OID = 1184;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class TIMETZ
{
  public:
    static const Oid OID = 1266;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

class UUID
{
  public:
    static const Oid OID = 2950;

    static inline PyObject *
    decode(PGresult *r, int i, int j)
    {
        TODO();
        return NULL;
    }
};

static inline PyObject *
decode(PGresult *r, int i, int j)
{
    switch (PQftype(r, j)) {
      case BOOL       ::OID      : return BOOL       ::decode      (r, i, j);
      case BYTEA      ::OID      : return BYTEA      ::decode      (r, i, j);
      case CHAR       ::OID      : return CHAR       ::decode      (r, i, j);
      case DATE       ::OID      : return DATE       ::decode      (r, i, j);
      case FLOAT4     ::OID      : return FLOAT4     ::decode      (r, i, j);
      case FLOAT8     ::OID      : return FLOAT8     ::decode      (r, i, j);
      case INT2       ::OID      : return INT2       ::decode      (r, i, j);
      case INT4       ::OID      : return INT4       ::decode      (r, i, j);
      case INT4       ::OID_ARRAY: return INT4       ::decode_array(r, i, j);
      case INT8       ::OID      : return INT8       ::decode      (r, i, j);
      case INTERVAL   ::OID      : return INTERVAL   ::decode      (r, i, j);
      case RECORD     ::OID      : return RECORD     ::decode      (r, i, j);
      case TEXT       ::OID      : return TEXT       ::decode      (r, i, j);
      case TEXT       ::OID_ARRAY: return TEXT       ::decode_array(r, i, j);
      case TIME       ::OID      : return TIME       ::decode      (r, i, j);
      case TIMESTAMP  ::OID      : return TIMESTAMP  ::decode      (r, i, j);
      case TIMESTAMPTZ::OID      : return TIMESTAMPTZ::decode      (r, i, j);
      case TIMETZ     ::OID      : return TIMETZ     ::decode      (r, i, j);
      case UUID       ::OID      : return UUID       ::decode      (r, i, j);
    }

    PyErr_Format(PyExc_NotImplementedError, "%zd", PQftype(r, j));
    return NULL;
}

} // namespace postgresql

#endif
