#ifndef POSTGRESQL_PARAMETERS_HPP_
#define POSTGRESQL_PARAMETERS_HPP_

#include "Python.h"

#include "libpq-fe.h"

#include "postgresql/network.hpp"
#include "postgresql/type.hpp"

namespace postgresql {
namespace parameters {

static const size_t MAX_BYTES_PER = 8;

class Parameters
{
  public:
    Oid   *types_i;
    char **values_i;
    int   *lengths_i;
    int   *formats_i;
    char  *scratch_i;

    // Constants

    inline bool
    append_true()
    {
        TODO();
        return false;
    }

    inline bool
    append_false()
    {
        TODO();
        return false;
    }

    inline bool
    append(PyObject *x)
    {
        PyTypeObject *cls = Py_TYPE(x);

        // Priority to builtin exact types
        if (cls == &PyUnicode_Type) return this->append((PyUnicodeObject *)x);
        if (cls ==    &PyLong_Type) return this->append(   (PyLongObject *)x);
        if (cls ==   &PyFloat_Type) return this->append(  (PyFloatObject *)x);
        if (cls ==    &PyBool_Type) return (x == Py_True) ? this->append_true() : this->append_false();
        if (cls ==   &PyBytes_Type) return this->append(  (PyBytesObject *)x);

        PyErr_Format(PyExc_NotImplementedError, "encode(%R)", x);
        return false;
    }

    inline bool
    append(int16_t x)
    {
        char *scratch = this->scratch_i;

        *this->types_i++   = INT2::OID;
        *this->values_i++  = scratch;
        *this->lengths_i++ = 2;
        *this->formats_i++ = 1;

        *(int16_t *)scratch = postgresql::network::order(x);

        this->scratch_i += 2;

        return true;
    }

    inline bool
    append(int32_t x)
    {
        char *scratch = this->scratch_i;

        *this->types_i++   = INT4::OID;
        *this->values_i++  = scratch;
        *this->lengths_i++ = 4;
        *this->formats_i++ = 1;

        *(int32_t *)scratch = postgresql::network::order(x);

        this->scratch_i += 4;

        return true;
    }

    inline bool
    append(int64_t x)
    {
        char *scratch = this->scratch_i;

        *this->types_i++   = INT8::OID;
        *this->values_i++  = scratch;
        *this->lengths_i++ = 8;
        *this->formats_i++ = 1;

        *(int64_t *)scratch = postgresql::network::order(x);

        this->scratch_i += 8;

        return true;
    }

    inline bool
    append(PyBytesObject *x)
    {
        TODO();
        return false;
    }

    inline bool
    append(PyFloatObject *x)
    {
        TODO();
        return false;
    }

    inline bool
    append(PyLongObject *x)
    {
        // Implementation details
        Py_ssize_t size = Py_SIZE(x);

        digit d;

        switch (size) {
          case 1:
              d = x->ob_digit[0];

              if (d < 32768)
                  return this->append((int16_t)d);
              else
                  return this->append((int32_t)d);

          case 0:
              return this->append((int16_t)0);

          case -1:
              d = x->ob_digit[0];

              if (d <= 32768)
                  return this->append(-(int16_t)d);
              else
                  return this->append(-(int32_t)d);
        }

        bool negative = size < 0;
        if (negative)
            size = -size;

        uint64_t total = 0;

        while (--size >= 0)
            total = (total << PyLong_SHIFT) | x->ob_digit[size];

        if (negative) {
            if (total <= 2147483648)
                return this->append(-(int32_t)total);
            else
                return this->append(-(int64_t)total);
        } else {
            if (total < 2147483648)
                return this->append((int32_t)total);
            else
                return this->append((int64_t)total);
        }
    }

    inline bool
    append(PyUnicodeObject *x)
    {
        Py_ssize_t size;

        char *utf8 = PyUnicode_AsUTF8AndSize((PyObject *)x, &size);
        if (utf8 == NULL)
            return false;

        *this->types_i++   = TEXT::OID;
        *this->values_i++  = utf8;
        *this->lengths_i++ = size;
        *this->formats_i++ = 1;

        return true;
    }
};

template <size_t N>
class Static : public Parameters
{
  public:
    Oid   types[N];
    char *values[N];
    int   lengths[N];
    int   formats[N];

    char  scratch[N * MAX_BYTES_PER];

    Static()
    {
        this->types_i   = this->types;
        this->values_i  = this->values;
        this->lengths_i = this->lengths;
        this->formats_i = this->formats;
        this->scratch_i = this->scratch;
    }
};

class Dynamic : public Parameters
{
  public:
    Oid   *types;
    char **values;
    int   *lengths;
    int   *formats;
    char  *scratch;

    Dynamic(size_t n)
    {
        size_t types_size   = n * sizeof(Oid);
        size_t values_size  = n * sizeof(char *);
        size_t lengths_size = n * sizeof(int);
        size_t formats_size = n * sizeof(int);
        size_t scratch_size = n * MAX_BYTES_PER;

        size_t total_size   = (types_size   +
                               values_size  +
                               lengths_size +
                               formats_size +
                               scratch_size);

        // Must be OK if malloc fails

        this->types   = this->types_i   =   (Oid *)PyMem_MALLOC(total_size);
        this->values  = this->values_i  = (char **)((size_t)this->types   + types_size);
        this->lengths = this->lengths_i =   (int *)((size_t)this->values  + values_size);
        this->formats = this->formats_i =   (int *)((size_t)this->lengths + lengths_size);
        this->scratch = this->scratch_i =  (char *)((size_t)this->formats + formats_size);
    }

    ~Dynamic()
    {
        PyMem_FREE(this->types);
    }
};

} // namespace parameters
} // namespace postgresql

#endif
