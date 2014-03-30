#ifndef B_TYPE_HPP_
#define B_TYPE_HPP_

namespace b {
namespace type {

static inline bool
is_abstract(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_IS_ABSTRACT;
}

static inline bool
is_heap_type(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_HEAPTYPE;
}

static inline bool
is_ready(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_READY;
}

/* flags - fast subclass */

static inline bool
subclasses_bytes(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_BYTES_SUBCLASS;
}

static inline bool
subclasses_dict(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_DICT_SUBCLASS;
}

static inline bool
subclasses_base_exception(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_BASE_EXC_SUBCLASS;
}

static inline bool
subclasses_int(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_LONG_SUBCLASS;
}

static inline bool
subclasses_list(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_LIST_SUBCLASS;
}

static inline bool
subclasses_str(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_UNICODE_SUBCLASS;
}

static inline bool
subclasses_tuple(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_TUPLE_SUBCLASS;
}

static inline bool
subclasses_type(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_TYPE_SUBCLASS;
}

static inline bool
supports_cyclic_garbage_collection(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_HAVE_GC;
}

static inline bool
supports_subclassing(PyTypeObject *type)
{
    return type->tp_flags & Py_TPFLAGS_BASETYPE;
}

static inline bool
supports_weak_references(PyTypeObject *type)
{
    return type->tp_weaklistoffset > 0;
}

static inline bool
ensure_ready(PyTypeObject *type)
{
    return is_ready(type) || PyType_Ready(type) == 0;
}

} // namespace type
} // namespace b

#endif
