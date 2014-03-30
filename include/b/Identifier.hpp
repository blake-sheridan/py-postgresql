#ifndef B_IDENTIFIER_HPP_
#define B_IDENTIFIER_HPP_

#include "Python.h"

namespace b {

class Identifier
{
    PyUnicodeObject *_string;

  public:
    const char * const ascii;

    Identifier(const char * const ascii) : _string(NULL)
                                         , ascii(ascii)
    {
    }

    ~Identifier()
    {
        Py_XDECREF(this->_string);
    }

    // Properties

    inline PyUnicodeObject *
    string()
    {
        if (this->_string == NULL)
            this->_string = (PyUnicodeObject *)PyUnicode_InternFromString(this->ascii);
        return this->_string;
    }

    // Methods

    inline PyObject *
    get(PyDictObject *dict)
    {
        return PyDict_GetItemWithError((PyObject *)dict, (PyObject *)this->string());
    }
};

} // namespace hart

#endif
