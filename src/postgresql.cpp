#include "Python.h"
#include "libpq-fe.h"

#include "b/Identifier.hpp"
#include "b/python.h"
#include "b/type.hpp"
#include "postgresql/Parameters.hpp"
#include "postgresql/type.hpp"

typedef struct {
    PyObject_HEAD
    PGconn *pg_conn;
    // Properties, cached upon first access
    PyObject *host;
    PyUnicodeObject *name;
    PyUnicodeObject *user;
} Database;

typedef struct {
    PyBaseExceptionObject base;
    PGconn *pg_conn;
} ConnectionError;

typedef struct {
    PyBaseExceptionObject base;
    PGresult *pg_result;
} ExecutionError;

typedef struct {
    PyObject_HEAD
    PGresult *pg_result;
    int       row_count;    // Cached from PQntuples
    int       column_count; // Cached from PQnfields
} Result;

typedef struct {
    PyObject_HEAD
    Result *result;
    int     index;
} ResultIterator;

typedef struct Row {
    PyObject_HEAD
    Result *result;
    int     index;
} Row;

typedef struct {
    PyObject_HEAD
    Row *row;
    int  index;
} RowIterator;

typedef struct {
    PyObject_HEAD
    Database *database;
} Schema;

typedef struct {
    PyObject_HEAD
    Database *database;
} Transaction;

/* Forward */

static inline Row *Result_row(Result *, int);
static inline bool Row_check(PyObject *);

/* ConnectionError */

PyDoc_STRVAR(
ConnectionError___doc__,
"Exception raised when failing to establish a connection.");

static void
ConnectionError___del__(ConnectionError *self)
{
    PQfinish(self->pg_conn);
    ((PyTypeObject *)PyExc_Exception)->tp_dealloc((PyObject *)self);
}

static PyObject *
ConnectionError___str__(ConnectionError *self)
{
    return PyUnicode_FromString(PQerrorMessage(self->pg_conn));
}

static PyTypeObject
ConnectionError_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.ConnectionError",
    /* tp_basicsize       */ sizeof(ConnectionError),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)ConnectionError___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ (reprfunc)ConnectionError___str__,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ ConnectionError___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ 0,
    /* tp_iternext        */ 0,
    /* tp_methods         */ 0,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ (PyTypeObject *)PyExc_Exception,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

static void
ConnectionError_set(PGconn *pg_conn)
{
    if (!b::type::ensure_ready(&ConnectionError_type)) {
        PyErr_SetString(PyExc_SystemError, "Failed to ready ConnectionError");
        return;
    }

    ConnectionError *self = (ConnectionError *)ConnectionError_type.tp_alloc(&ConnectionError_type, 0);
    if (self == NULL) {
        PyErr_SetObject((PyObject *)&ConnectionError_type, NULL);
        return;
    }

    self->pg_conn = pg_conn;

    PyErr_SetObject((PyObject *)&ConnectionError_type, (PyObject *)self);
}

/* ExecutionError */

PyDoc_STRVAR(
ExecutionError___doc__,
"Exception raised when failing to establish a connection.");

static void
ExecutionError___del__(ExecutionError *self)
{
    PQclear(self->pg_result);
    ((PyTypeObject *)PyExc_Exception)->tp_dealloc((PyObject *)self);
}

static PyObject *
ExecutionError___str__(ExecutionError *self)
{
    const char * const SEP = "\n  * ";

    char *primary = PQresultErrorField(self->pg_result, PG_DIAG_MESSAGE_PRIMARY);
    char *detail  = PQresultErrorField(self->pg_result, PG_DIAG_MESSAGE_DETAIL);
    char *hint    = PQresultErrorField(self->pg_result, PG_DIAG_MESSAGE_HINT);

    assert(primary != NULL);

    if (detail == NULL)
        return PyUnicode_FromString(primary);
    if (hint == NULL)
        return PyUnicode_FromFormat("%s%sDETAIL: %s", primary, SEP, detail);
    else
        return PyUnicode_FromFormat("%s%sDETAIL: %s%SHINT: %s", primary, SEP, detail, SEP, hint);
}

static PyTypeObject
ExecutionError_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.ExecutionError",
    /* tp_basicsize       */ sizeof(ExecutionError),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)ExecutionError___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ (reprfunc)ExecutionError___str__,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ ExecutionError___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ 0,
    /* tp_iternext        */ 0,
    /* tp_methods         */ 0,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ (PyTypeObject *)PyExc_Exception,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

static void
ExecutionError_set(PGresult *pg_result)
{
    if (!b::type::ensure_ready(&ExecutionError_type)) {
        PyErr_SetString(PyExc_SystemError, "Failed to ready ExecutionError");
        return;
    }

    ExecutionError *self = (ExecutionError *)ExecutionError_type.tp_alloc(&ExecutionError_type, 0);
    if (self == NULL) {
        PyErr_SetObject((PyObject *)&ExecutionError_type, NULL);
        return;
    }

    self->pg_result = pg_result;

    PyErr_SetObject((PyObject *)&ExecutionError_type, (PyObject *)self);
}

/* RowIterator */

PyDoc_STRVAR(
RowIterator___doc__,
"Iterator over a Row's columns");

static void
RowIterator___del__(RowIterator *self)
{
    Py_DECREF(self->row);
    return Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyLongObject *
RowIterator___length_hint__(RowIterator *self)
{
    TODO();
    return NULL;
}

static Row *
RowIterator___next__(RowIterator *self)
{
    TODO();
    return NULL;
}

static PyMethodDef
RowIterator_methods[] = {
    {"__length_hint__", (PyCFunction)RowIterator___length_hint__, METH_NOARGS, NULL},
    {NULL}
};

static PyTypeObject
RowIterator_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.RowIterator",
    /* tp_basicsize       */ sizeof(RowIterator),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)RowIterator___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ RowIterator___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ PyObject_SelfIter,
    /* tp_iternext        */ (iternextfunc)RowIterator___next__,
    /* tp_methods         */ RowIterator_methods,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

/* Row */

PyDoc_STRVAR(
Row___doc__,
"Object representing a PostgreSQL command result row.");

static void
Row___del__(Row *self)
{
    Py_DECREF(self->result);
    return Py_TYPE(self)->tp_free((PyObject *)self);
}

/* Row_as_sequence */

static Py_ssize_t
Row___len__(Row *self)
{
    return self->result->column_count;
}

static PyObject *
Row___getitem__(Row *self, Py_ssize_t index)
{
    return postgresql::decode(self->result->pg_result, self->index, index);
}

static PyObject *
Row_richcompare(Row *self, PyObject *other, int op)
{
    if (!Row_check(other))
        Py_RETURN_NOTIMPLEMENTED;

    bool invert;

    if (op == Py_EQ) {
        invert = false;
    } else if (op == Py_NE) {
        invert = true;
    } else {
        Py_RETURN_NOTIMPLEMENTED;
    }

    TODO();
    return NULL;

    if (invert) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static Py_hash_t
Row___hash__(Row *self)
{
    TODO();
    return -1;
}

static RowIterator *
Row___iter__(Row *self)
{
    if (!b::type::ensure_ready(&RowIterator_type))
        return NULL;

    RowIterator *iterator = (RowIterator *)RowIterator_type.tp_alloc(&RowIterator_type, 0);
    if (iterator == NULL)
        return NULL;

    Py_INCREF(self);

    iterator->row   = self;
    iterator->index = 0;

    return iterator;
}

static PyUnicodeObject *
Row___repr__(Row *self)
{
    TODO();
    return NULL;
}

static PySequenceMethods
Row_as_sequence = {
    /* sq_length         */ (lenfunc)Row___len__,
    /* sq_concat         */ 0,
    /* sq_repeat         */ 0,
    /* sq_item           */ (ssizeargfunc)Row___getitem__,
    /* sq_ass_item       */ 0,
    /* sq_contains       */ 0,
    /* sq_inplace_concat */ 0,
    /* sq_inplace_repeat */ 0,
};

static PyTypeObject
Row_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.Row",
    /* tp_basicsize       */ sizeof(Row),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)Row___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ (reprfunc)Row___repr__,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ &Row_as_sequence,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ (hashfunc)Row___hash__,
    /* tp_call            */ 0,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ Row___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ (richcmpfunc)Row_richcompare,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ (getiterfunc)Row___iter__,
    /* tp_iternext        */ 0,
    /* tp_methods         */ 0,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

static inline bool
Row_check(PyObject *x)
{
    return Py_TYPE(x) == &Row_type;
}

/* ResultIterator */

PyDoc_STRVAR(
ResultIterator___doc__,
"Iterator over the Rows of a Result");

static void
ResultIterator___del__(ResultIterator *self)
{
    Py_XDECREF(self->result);
    return Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyLongObject *
ResultIterator___length_hint__(ResultIterator *self)
{
    size_t length;

    Result *result = self->result;

    if (result == NULL) {
        length = 0;
    } else {
        length = result->row_count - self->index;
    }

    return (PyLongObject *)PyLong_FromSize_t(length);
}

static Row *
ResultIterator___next__(ResultIterator *self)
{
    Result *result = self->result;
    if (result == NULL)
        return NULL;

    int index = self->index;
    int length = result->row_count;

    if (index == length) {
        Py_DECREF(result);
        self->result = NULL;
        return NULL;
    }

    Row *row = Result_row(result, index);
    self->index = index + 1;
    return row;
}

static PyMethodDef
ResultIterator_methods[] = {
    {"__length_hint__", (PyCFunction)ResultIterator___length_hint__, METH_NOARGS, NULL},
    {NULL}
};

static PyTypeObject
ResultIterator_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.ResultIterator",
    /* tp_basicsize       */ sizeof(ResultIterator),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)ResultIterator___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ ResultIterator___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ PyObject_SelfIter,
    /* tp_iternext        */ (iternextfunc)ResultIterator___next__,
    /* tp_methods         */ ResultIterator_methods,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

/* Result */

PyDoc_STRVAR(
Result___doc__,
"Object representing a PostgreSQL command result.");

static void
Result___del__(Result *self)
{
    PQclear(self->pg_result);
    return Py_TYPE(self)->tp_free((PyObject *)self);
}

static inline Row *
Result_row(Result *self, int i)
{
    assert(i < self->row_count);

    if (!b::type::ensure_ready(&Row_type))
        return NULL;

    Row *row = (Row *)Row_type.tp_alloc(&Row_type, 0);
    if (row == NULL)
        return NULL;

    Py_INCREF(self);

    row->index  = i;
    row->result = self;

    return row;
}

static Py_ssize_t
Result___len__(Result *self)
{
    return self->row_count;
}

static Row *
Result___getitem__(Result *self, Py_ssize_t i)
{
    if (i >= self->row_count)
        return NULL;
    return Result_row(self, i);
}

static ResultIterator *
Result___iter__(Result *self)
{
    if (!b::type::ensure_ready(&ResultIterator_type))
        return NULL;

    ResultIterator *iterator = (ResultIterator *)ResultIterator_type.tp_alloc(&ResultIterator_type, 0);
    if (iterator == NULL)
        return NULL;

    Py_INCREF(self);

    iterator->result = self;
    iterator->index  = 0;

    return iterator;
}

static PySequenceMethods
Result_as_sequence = {
    /* sq_length         */ (lenfunc)Result___len__,
    /* sq_concat         */ 0,
    /* sq_repeat         */ 0,
    /* sq_item           */ (ssizeargfunc)Result___getitem__,
    /* sq_ass_item       */ 0,
    /* sq_contains       */ 0,
    /* sq_inplace_concat */ 0,
    /* sq_inplace_repeat */ 0,
};

static PyTypeObject
Result_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.Result",
    /* tp_basicsize       */ sizeof(Result),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)Result___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ &Result_as_sequence,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ Result___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ (getiterfunc)Result___iter__,
    /* tp_iternext        */ 0,
    /* tp_methods         */ 0,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

static inline Result *
Result_new(PGresult *pg_result)
{
    ExecStatusType status = PQresultStatus(pg_result);

    // Fast path the common cases outside of a switch

    if (status != PGRES_TUPLES_OK) {
        if (status == PGRES_COMMAND_OK) {
            // TODO: own type?
            Py_INCREF(Py_None);
            return (Result *)Py_None;
        }

        ExecutionError_set(pg_result);
        return NULL;
    }

    if (!b::type::ensure_ready(&Result_type))
        return NULL;

    Result *self = (Result *)Result_type.tp_alloc(&Result_type, 0);
    if (self == NULL)
        return NULL;

    self->pg_result    = pg_result;
    self->column_count = PQnfields(pg_result);
    self->row_count    = PQntuples(pg_result);

    return self;
}

/* Transaction */

PyDoc_STRVAR(
Transaction___doc__,
"A transaction context manager");

static void
Transaction___del__(Transaction *self)
{
    Py_DECREF(self->database);
    return Py_TYPE(self)->tp_free((PyObject *)self);
}

static Transaction *
Transaction___enter__(Transaction *self)
{
    PGresult *pg_result = PQexec(self->database->pg_conn, "BEGIN");

    if (PQresultStatus(pg_result) != PGRES_COMMAND_OK) {
        ExecutionError_set(pg_result);
        return NULL;
    }

    PQclear(pg_result);
    Py_INCREF(self);
    return self;
}

static PyObject *
Transaction___exit__(Transaction *self, PyObject *args)
{
    if (PyTuple_GET_SIZE(args) == 3) { // Sanity
        PGresult *pg_result = PQexec(self->database->pg_conn,
                                     PyTuple_GET_ITEM(args, 0) == Py_None ? "COMMIT" : "ROLLBACK");

        if (PQresultStatus(pg_result) != PGRES_COMMAND_OK) {
            ExecutionError_set(pg_result);
            return NULL;
        }

        PQclear(pg_result);
    }

    Py_RETURN_NONE;
}

static PyMethodDef
Transaction_methods[] = {
    {"__enter__", (PyCFunction)Transaction___enter__, METH_NOARGS,  NULL},
    {"__exit__",  (PyCFunction)Transaction___exit__,  METH_VARARGS, NULL},
    {NULL}
};

static PyTypeObject
Transaction_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.Transaction",
    /* tp_basicsize       */ sizeof(Transaction),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)Transaction___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ Transaction___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ 0,
    /* tp_iternext        */ 0,
    /* tp_methods         */ Transaction_methods,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

/* Schema */

PyDoc_STRVAR(
Schema___doc__,
"A Database schema");

static PyMethodDef
Schema_methods[] = {
    {NULL}
};

static PyTypeObject
Schema_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.Schema",
    /* tp_basicsize       */ sizeof(Schema),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ 0,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ 0,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ Schema___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ 0,
    /* tp_iternext        */ 0,
    /* tp_methods         */ Schema_methods,
    /* tp_members         */ 0,
    /* tp_getset          */ 0,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ 0,
    /* tp_alloc           */ 0,
    /* tp_new             */ 0,
    /* tp_free            */ 0,
};

/* Database */

PyDoc_STRVAR(
Database___doc__,
"Object encapsulating a single PostgreSQL database.");

static int
Database___init__(Database *self, PyObject *args, PyDictObject *kwargs)
{
    static b::Identifier id_dbname("dbname");
    static b::Identifier id_host("host");
    static b::Identifier id_name("name");
    static b::Identifier id_password("password");
    static b::Identifier id_port("port");
    static b::Identifier id_user("user");

    char  *keywords[6];
    char  *values  [6];
    size_t i = 0;

    PGconn *pg_conn;

    if (PyTuple_GET_SIZE(args) != 0) {
        PyErr_Format(PyExc_TypeError, "'%s' takes no positional arguments, got: %R", Py_TYPE(self)->tp_name, args);
        return -1;
    }

    if (kwargs != NULL) {
        PyObject *o;

        o = id_host.get(kwargs);
        if (o != NULL) {
            if (!PyUnicode_Check(o)) {
                PyErr_Format(PyExc_TypeError, "expecting string, got: %s=%R", id_host.ascii, o);
                return -1;
            }

            if ((values[i] = PyUnicode_AsUTF8AndSize(o, NULL)) == NULL)
                return -1;

            keywords[i++] = (char *)id_host.ascii;
        }

        o = id_name.get(kwargs);
        if (o != NULL) {
            if (!PyUnicode_Check(o)) {
                PyErr_Format(PyExc_TypeError, "expecting string, got: %s=%R", id_name.ascii, o);
                return -1;
            }

            if ((values[i] = PyUnicode_AsUTF8AndSize(o, NULL)) == NULL)
                return -1;

            keywords[i++] = (char *)id_dbname.ascii;
        }

        o = id_password.get(kwargs);
        if (o != NULL) {
            if (!PyUnicode_Check(o)) {
                PyErr_Format(PyExc_TypeError, "expecting string, got: %s=%R", id_password.ascii, o);
                return -1;
            }

            if ((values[i] = PyUnicode_AsUTF8AndSize(o, NULL)) == NULL)
                return -1;

            keywords[i++] = (char *)id_password.ascii;
        }

        o = id_port.get(kwargs);
        if (o != NULL) {
            if (!PyLong_Check(o)) {
                PyErr_Format(PyExc_TypeError, "expecting integer, got: %s=%R", id_port.ascii, o);
                return -1;
            }

            TODO();
            return -1;

            keywords[i++] = (char *)id_port.ascii;
        }

        o = id_user.get(kwargs);
        if (o != NULL) {
            if (!PyUnicode_Check(o)) {
                PyErr_Format(PyExc_TypeError, "expecting string, got: %s=%R", id_user.ascii, o);
                return -1;
            }

            if ((values[i] = PyUnicode_AsUTF8AndSize(o, NULL)) == NULL)
                return -1;

            keywords[i++] = (char *)id_user.ascii;
        }
    }

    keywords[i] = NULL;
    values  [i] = NULL;

    pg_conn = PQconnectdbParams((const char **)keywords, (const char **)values, 0);

    if (PQstatus(pg_conn) != CONNECTION_OK) {
        ConnectionError_set(pg_conn);
        return -1;
    }

    // Re-initialized?
    if (self->pg_conn != NULL) {
        PQfinish(self->pg_conn);
    }

    self->pg_conn = pg_conn;

    return 0;
}

static void
Database___del__(Database *self)
{
    Py_XDECREF(self->host);
    Py_XDECREF(self->name);
    Py_XDECREF(self->user);

    if (self->pg_conn != NULL)
        PQfinish(self->pg_conn);

    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* Database_getset */

static PyTypeObject *
Database_ExecutionError(Database *self)
{
    PyTypeObject *cls = &ExecutionError_type;

    if (!b::type::ensure_ready(cls))
        return NULL;

    Py_INCREF(cls);
    return cls;
}

PyDoc_STRVAR(
Database_host___doc__,
"The server host name of the connection (or None)");

static PyObject *
Database_host(Database *self)
{
    PyObject *x = self->host;
    if (x == NULL) {
        char *bytes = PQhost(self->pg_conn);
        if (bytes == NULL)
            x = self->host = Py_None;
        else
            x = self->host = PyUnicode_FromString(bytes);
    }
    Py_INCREF(x);
    return x;
}

PyDoc_STRVAR(
Database_name___doc__,
"The database name of the connection");

static PyUnicodeObject *
Database_name(Database *self)
{
    PyUnicodeObject *x = self->name;
    if (x == NULL) {
        x = self->name = (PyUnicodeObject *)PyUnicode_FromString(PQdb(self->pg_conn));
        if (x == NULL)
            return NULL;
    }
    Py_INCREF(x);
    return x;
}

PyDoc_STRVAR(
Database_user___doc__,
"The user name of the connection");

static PyUnicodeObject *
Database_user(Database *self)
{
    PyUnicodeObject *x = self->user;
    if (x == NULL) {
        x = self->user = (PyUnicodeObject *)PyUnicode_FromString(PQuser(self->pg_conn));
        if (x == NULL)
            return NULL;
    }
    Py_INCREF(x);
    return x;
}

static PyGetSetDef
Database_getset[] = {
    {(char *)"ExecutionError",  (getter)Database_ExecutionError,  NULL, NULL},
    {(char *)"host",            (getter)Database_host,            NULL, Database_host___doc__},
    {(char *)"name",            (getter)Database_name,            NULL, Database_name___doc__},
    {(char *)"user",            (getter)Database_user,            NULL, Database_user___doc__},
    {NULL}
};

/* Methods */

PyDoc_STRVAR(
Database_schema___doc__,
"Return a named Schema...");

static Schema *
Database_schema(Database *self, PyObject *name)
{
    if (!PyUnicode_Check(name)) {
        PyErr_Format(PyExc_TypeError, "expecting string, got: %R", name);
        return NULL;
    }

    TODO();
    return NULL;

    if (!b::type::ensure_ready(&Schema_type))
        return NULL;

    Schema *schema = (Schema *)Schema_type.tp_alloc(&Schema_type, 0);
    if (schema == NULL)
        return NULL;

    Py_INCREF(self);

    schema->database = self;

    return schema;
}

PyDoc_STRVAR(
Database_transaction___doc__,
"Return a new Transaction for this Database.");

static Transaction *
Database_transaction(Database *self)
{
    if (!b::type::ensure_ready(&Transaction_type))
        return NULL;

    Transaction *transaction = (Transaction *)Transaction_type.tp_alloc(&Transaction_type, 0);
    if (transaction == NULL)
        return NULL;

    Py_INCREF(self);

    transaction->database = self;

    return transaction;
}

static PyMethodDef
Database_methods[] = {
    {"schema",      (PyCFunction)Database_schema,      METH_O,       Database_schema___doc__},
    {"transaction", (PyCFunction)Database_transaction, METH_NOARGS,  Database_transaction___doc__},
    {NULL}
};

static inline Result *
_Database_execute_n(Database *self, PyObject *args, size_t arity)
{
    TODO();
    return NULL;
}

static Result *
Database___call__(Database *self, PyObject *args, PyObject *kwargs)
{
    if (kwargs != NULL) {
        PyErr_SetString(PyExc_TypeError, "__call__ does not take keyword arguments");
        return NULL;
    }

    PGresult *pg_result;

    Py_ssize_t n = PyTuple_GET_SIZE(args);
    switch (n) {
      case 0:
          PyErr_SetString(PyExc_TypeError, "expecting at least 1 positional argument");
          return NULL;

      case 1: {
          postgresql::Parameters<0> parameters;

          pg_result = parameters.execute(self->pg_conn, PyTuple_GET_ITEM(args, 0));
      }
          break;

      case 2: {
          postgresql::Parameters<1> parameters;

          if (!parameters.set(0, PyTuple_GET_ITEM(args, 1)))
              return NULL;

          pg_result = parameters.execute(self->pg_conn, PyTuple_GET_ITEM(args, 0));
      }
          break;
      default:
          TODO();
          return NULL;
    }

    if (pg_result == NULL)
        return NULL;

    return Result_new(pg_result);
}

/* Database_type */

static PyTypeObject
Database_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name            */ "postgresql.Database",
    /* tp_basicsize       */ sizeof(Database),
    /* tp_itemsize        */ 0,
    /* tp_dealloc         */ (destructor)Database___del__,
    /* tp_print           */ 0,
    /* tp_getattr         */ 0,
    /* tp_setattr         */ 0,
    /* tp_reserved        */ 0,
    /* tp_repr            */ 0,
    /* tp_as_number       */ 0,
    /* tp_as_sequence     */ 0,
    /* tp_as_mapping      */ 0,
    /* tp_hash            */ 0,
    /* tp_call            */ (ternaryfunc)Database___call__,
    /* tp_str             */ 0,
    /* tp_getattro        */ 0,
    /* tp_setattro        */ 0,
    /* tp_as_buffer       */ 0,
    /* tp_flags           */ Py_TPFLAGS_DEFAULT,
    /* tp_doc             */ Database___doc__,
    /* tp_traverse        */ 0,
    /* tp_clear           */ 0,
    /* tp_richcompare     */ 0,
    /* tp_weaklist_offset */ 0,
    /* tp_iter            */ 0,
    /* tp_iternext        */ 0,
    /* tp_methods         */ Database_methods,
    /* tp_members         */ 0,
    /* tp_getset          */ Database_getset,
    /* tp_base            */ 0,
    /* tp_dict            */ 0,
    /* tp_descr_get       */ 0,
    /* tp_descr_set       */ 0,
    /* tp_dictoffset      */ 0,
    /* tp_init            */ (initproc)Database___init__,
    /* tp_alloc           */ 0,
    /* tp_new             */ PyType_GenericNew,
    /* tp_free            */ 0,
};

/* module */

PyDoc_STRVAR(
module___doc__,
"A Python PostgreSQL front end");

static struct PyModuleDef
module_definition = {
    PyModuleDef_HEAD_INIT,
    "postgresql",
    module___doc__,
    -1,
};

PyMODINIT_FUNC
PyInit_postgresql(void)
{
    if (!b::type::ensure_ready(&Database_type) ||
        !b::type::ensure_ready(&ConnectionError_type))
        return NULL;

    PyObject *module = PyModule_Create(&module_definition);
    if (module == NULL)
        return NULL;

    PyModule_AddObject(module, "ConnectionError", (PyObject *)&ConnectionError_type);
    PyModule_AddObject(module, "Database",        (PyObject *)&Database_type);

    return module;
};
