#ifndef POSTGRESQL_PARAMETERS_HPP_
#define POSTGRESQL_PARAMETERS_HPP_

#include "Python.h"

#include "libpq-fe.h"

#include "postgresql/network.hpp"
#include "postgresql/type.hpp"

namespace postgresql {

template <size_t N, size_t BYTES_PER = 8>
class Parameters
{
  public:
    Oid   types[N];
    char *values[N];
    int   lengths[N];
    int   formats[N];

    char  scratch[N * BYTES_PER];

    // Constants

    inline bool
    set_true(size_t i)
    {
        TODO();
        return false;
    }

    inline bool
    set_false(size_t i)
    {
        TODO();
        return false;
    }

    inline bool
    set(size_t i, PyObject *x)
    {
        PyTypeObject *cls = Py_TYPE(x);

        // Priority to builtin exact types
        if (cls == &PyUnicode_Type) return this->set(i, (PyUnicodeObject *)x);
        if (cls ==    &PyLong_Type) return this->set(i,    (PyLongObject *)x);
        if (cls ==   &PyFloat_Type) return this->set(i,   (PyFloatObject *)x);
        if (cls ==    &PyBool_Type) return (x == Py_True) ? this->set_true(i) : this->set_false(i);
        if (cls ==   &PyBytes_Type) return this->set(i,   (PyBytesObject *)x);

        PyErr_Format(PyExc_NotImplementedError, "encode(%R)", x);
        return false;
    }

    inline bool
    set(size_t i, int16_t x)
    {
        char *data = &this->scratch[i * BYTES_PER];

        this->types  [i] = INT2::OID;
        this->values [i] = data;
        this->lengths[i] = 2;
        this->formats[i] = 1;

        *(int16_t *)data = postgresql::network::order(x);

        return true;
    }

    inline bool
    set(size_t i, int32_t x)
    {
        char *data = &this->scratch[i * BYTES_PER];

        this->types  [i] = INT4::OID;
        this->values [i] = data;
        this->lengths[i] = 4;
        this->formats[i] = 1;

        *(int32_t *)data = postgresql::network::order(x);

        return true;
    }

    inline bool
    set(size_t i, int64_t x)
    {
        char *data = &this->scratch[i * BYTES_PER];

        this->types  [i] = INT8::OID;
        this->values [i] = data;
        this->lengths[i] = 8;
        this->formats[i] = 1;

        *(int64_t *)data = postgresql::network::order(x);

        return true;
    }

    inline bool
    set(size_t i, PyBytesObject *x)
    {
        TODO();
        return false;
    }

    inline bool
    set(size_t i, PyFloatObject *x)
    {
        TODO();
        return false;
    }

    inline bool
    set(size_t i, PyLongObject *x)
    {
        // Implementation details
        Py_ssize_t size = Py_SIZE(x);

        digit d;

        switch (size) {
          case 1:
              d = x->ob_digit[0];

              if (d < 32768)
                  return this->set(i, (int16_t)d);
              else
                  return this->set(i, (int32_t)d);

          case 0:
              return this->set(i, (int16_t)0);

          case -1:
              d = x->ob_digit[0];

              if (d <= 32768)
                  return this->set(i, -(int16_t)d);
              else
                  return this->set(i, -(int32_t)d);
        }

        bool negative = size < 0;
        if (negative)
            size = -size;

        uint64_t total = 0;

        while (--size >= 0)
            total = (total << PyLong_SHIFT) | x->ob_digit[size];

        if (negative) {
            if (total <= 2147483648)
                return this->set(i, -(int32_t)total);
            else
                return this->set(i, -(int64_t)total);
        } else {
            if (total < 2147483648)
                return this->set(i, (int32_t)total);
            else
                return this->set(i, (int64_t)total);
        }
    }

    inline bool
    set(size_t i, PyUnicodeObject *x)
    {
        Py_ssize_t size;

        char *utf8 = PyUnicode_AsUTF8AndSize((PyObject *)x, &size);
        if (utf8 == NULL)
            return false;

        this->types  [i] = TEXT::OID;
        this->values [i] = utf8;
        this->lengths[i] = size;
        this->formats[i] = 1;

        return true;
    }

    inline PGresult *
    execute(PGconn *conn, PyObject *command)
    {
        if (!PyUnicode_Check(command)) {
            PyErr_Format(PyExc_TypeError, "command must be a string, got: %R", command);
            return false;
        }

        return this->execute(conn, (PyUnicodeObject *)command);
    }

    inline PGresult *
    execute(PGconn *conn, PyUnicodeObject *command)
    {
        if (PyUnicode_READY(command) == -1)
            return false;

        if (PyUnicode_IS_COMPACT_ASCII(command)) {
            // Inline fast path for ASCII strings
            return this->execute(conn, (char *)((PyASCIIObject *)command + 1));
        }

        TODO();
        return false;
    }

    inline PGresult *
    execute(PGconn *conn, char *command)
    {
        return PQexecParams(
            conn,
            command,
            N,
            this->types,
            this->values,
            this->lengths,
            this->formats,
            1);
    }
};

} // namespace postgresql

#endif
