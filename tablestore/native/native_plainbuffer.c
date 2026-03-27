#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

// Union for safe type punning to avoid strict aliasing violations
typedef union {
    int64_t i64;
    double d;
} double_int64_union;

// PlainBuffer constants from C++ SDK
#define HEADER 0x75
#define TAG_ROW_PK 0x1
#define TAG_ROW_DATA 0x2
#define TAG_CELL 0x3
#define TAG_CELL_NAME 0x4
#define TAG_CELL_VALUE 0x5
#define TAG_CELL_TYPE 0x6
#define TAG_CELL_TIMESTAMP 0x7
#define TAG_DELETE_ROW_MARKER 0x8
#define TAG_ROW_CHECKSUM 0x9
#define TAG_CELL_CHECKSUM 0x0A

// Variant types from C++ SDK
#define VT_INTEGER 0x0
#define VT_DOUBLE 0x1
#define VT_BOOLEAN 0x2
#define VT_STRING 0x3
#define VT_DATETIME 0x4
#define VT_NULL 0x6
#define VT_BLOB 0x7
#define VT_INF_MIN 0x9
#define VT_INF_MAX 0xa
#define VT_AUTO_INCREMENT 0xb

// CRC8 table - exactly as in C++ SDK
static int8_t crc8_table[256];
static int crc8_table_initialized = 0;

static void init_crc8_table() {
    if (crc8_table_initialized) return;

    for (int i = 0; i < 256; ++i) {
        // Use unsigned arithmetic to avoid undefined behavior with signed left shift
        uint8_t x = (uint8_t)i;
        for (int j = 8; j > 0; --j) {
            x = (uint8_t)((x << 1) ^ (((x & 0x80) != 0) ? 0x07 : 0));
        }
        crc8_table[i] = (int8_t)x;
    }
    crc8_table_initialized = 1;
}

// CRC8 functions - exactly as in C++ SDK
static inline int8_t crc_int8(int8_t crc, int8_t in) {
    return crc8_table[(crc ^ in) & 0xff];
}

static int8_t crc_int32(int8_t crc, int32_t in) {
    for (int i = 0; i < 4; ++i) {
        crc = crc_int8(crc, (int8_t)(in & 0xff));
        in >>= 8;
    }
    return crc;
}

static int8_t crc_int64(int8_t crc, int64_t in) {
    for (int i = 0; i < 8; ++i) {
        crc = crc_int8(crc, (int8_t)(in & 0xff));
        in >>= 8;
    }
    return crc;
}

static int8_t crc_string(int8_t crc, const char* str, int32_t len) {
    for (int32_t i = 0; i < len; ++i) {
        crc = crc_int8(crc, str[i]);
    }
    return crc;
}

// Stream reading functions
typedef struct {
    const uint8_t* data;
    size_t size;
    size_t pos;
    int32_t last_tag;
} PlainBufferStream;

static inline int8_t read_raw_byte(PlainBufferStream* stream) {
    if (stream->pos >= stream->size) {
        PyErr_SetString(PyExc_ValueError, "Read raw byte encountered EOF.");
        return 0;
    }
    return (int8_t)stream->data[stream->pos++];
}

static inline int32_t read_raw_little_endian32(PlainBufferStream* stream) {
    if (stream->pos > stream->size - 4) {
        PyErr_SetString(PyExc_ValueError, "Unexpected end of buffer In read_raw_little_endian32");
        return 0;
    }
    // Use explicit bounds checking to prevent potential overflow
    if (stream->pos > SIZE_MAX - 4) {
        PyErr_SetString(PyExc_ValueError, "Stream position overflow");
        return 0;
    }
    int32_t result = 0;
    for (int i = 0; i < 4; i++) {
        result |= ((int32_t)stream->data[stream->pos++]) << (i * 8);
    }
    return result;
}

static inline int64_t read_raw_little_endian64(PlainBufferStream* stream) {
    if (stream->pos > stream->size - 8) {
        PyErr_SetString(PyExc_ValueError, "Unexpected end of buffer in read_raw_little_endian64");
        return 0;
    }
    // Use explicit bounds checking to prevent potential overflow
    if (stream->pos > SIZE_MAX - 8) {
        PyErr_SetString(PyExc_ValueError, "Stream position overflow");
        return 0;
    }
    int64_t result = 0;
    for (int i = 0; i < 8; i++) {
        result |= ((int64_t)stream->data[stream->pos++]) << (i * 8);
    }
    return result;
}

static inline double read_double(PlainBufferStream* stream) {
    int64_t bits = read_raw_little_endian64(stream);
    // Use union to safely convert int64_t to double, avoiding strict aliasing violations
    double_int64_union converter;
    converter.i64 = bits;
    return converter.d;
}

static inline int read_tag(PlainBufferStream* stream) {
    if (stream->pos >= stream->size) {
        return 0; // End of stream
    }
    stream->last_tag = read_raw_byte(stream);
    return stream->last_tag;
}

static inline int check_last_tag_was(PlainBufferStream* stream, int32_t tag) {
    return stream->last_tag == tag;
}

static inline int check_buffer_bounds(PlainBufferStream* stream, int32_t size, const char* context) {
    if (size < 0) {
        PyErr_Format(PyExc_ValueError, "Negative size in %s", context);
        return 0;
    }

    if (stream->pos > stream->size) {
        PyErr_Format(PyExc_ValueError, "Stream position overflow in %s", context);
        return 0;
    }

    if ((size_t)size > stream->size - stream->pos) {
        PyErr_Format(PyExc_ValueError, "Unexpected end of buffer in %s", context);
        return 0;
    }

    return 1;
}

static char* read_utf_string(PlainBufferStream* stream, int32_t size) {
    // Save current position for potential rollback
    size_t saved_pos = stream->pos;

    if (!check_buffer_bounds(stream, size, "read_utf_string")) {
        return NULL;
    }
    char* result = (char*)malloc(size + 1);
    if (!result) {
        // Rollback stream position on memory allocation failure
        stream->pos = saved_pos;
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate memory");
        return NULL;
    }
    memcpy(result, stream->data + stream->pos, size);
    result[size] = '\0';
    stream->pos += size;
    return result;
}

// Parse primary key value - exactly as in C++ SDK
static PyObject* read_primary_key_value(PlainBufferStream* stream, int8_t* cell_checksum) {
    if (!check_last_tag_was(stream, TAG_CELL_VALUE)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_VALUE but it was %d", stream->last_tag);
        return NULL;
    }

    read_raw_little_endian32(stream); // Skip size
    int8_t type = read_raw_byte(stream);

    switch (type) {
        case VT_INTEGER: {
            int64_t value = read_raw_little_endian64(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            *cell_checksum = crc_int8(*cell_checksum, VT_INTEGER);
            *cell_checksum = crc_int64(*cell_checksum, value);
            read_tag(stream);
            return PyLong_FromLongLong(value);
        }
        case VT_STRING: {
            int32_t size = read_raw_little_endian32(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            char* str = read_utf_string(stream, size);
            if (!str) return NULL;
            *cell_checksum = crc_int8(*cell_checksum, VT_STRING);
            *cell_checksum = crc_int32(*cell_checksum, size);
            *cell_checksum = crc_string(*cell_checksum, str, size);
            read_tag(stream);
            PyObject* result = PyUnicode_FromStringAndSize(str, size);
            free(str);
            return result;
        }
        case VT_BLOB: {
            int32_t size = read_raw_little_endian32(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            if (!check_buffer_bounds(stream, size, "read_primary_key_value")) {
                return NULL;
            }
            *cell_checksum = crc_int8(*cell_checksum, VT_BLOB);
            *cell_checksum = crc_int32(*cell_checksum, size);
            *cell_checksum = crc_string(*cell_checksum, (const char*)stream->data + stream->pos, size);
            PyObject* bytes_obj = PyBytes_FromStringAndSize((const char*)stream->data + stream->pos, size);
            if (bytes_obj == NULL) {
                return NULL;
            }
            PyObject* result = PyByteArray_FromObject(bytes_obj);
            Py_DECREF(bytes_obj);
            stream->pos += size;
            read_tag(stream);
            return result;
        }
        default:
            PyErr_Format(PyExc_ValueError, "Unsupported primary key type: %d", type);
            return NULL;
    }
}

// Parse column value - exactly as in C++ SDK
static PyObject* read_column_value(PlainBufferStream* stream, int8_t* cell_checksum) {
    if (!check_last_tag_was(stream, TAG_CELL_VALUE)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_VALUE but it was %d", stream->last_tag);
        return NULL;
    }

    read_raw_little_endian32(stream); // Skip size
    int8_t type = read_raw_byte(stream);

    switch (type) {
        case VT_INTEGER: {
            int64_t value = read_raw_little_endian64(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            *cell_checksum = crc_int8(*cell_checksum, VT_INTEGER);
            *cell_checksum = crc_int64(*cell_checksum, value);
            read_tag(stream);
            return PyLong_FromLongLong(value);
        }
        case VT_STRING: {
            int32_t size = read_raw_little_endian32(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            char* str = read_utf_string(stream, size);
            if (!str) return NULL;
            *cell_checksum = crc_int8(*cell_checksum, VT_STRING);
            *cell_checksum = crc_int32(*cell_checksum, size);
            *cell_checksum = crc_string(*cell_checksum, str, size);
            read_tag(stream);
            PyObject* result = PyUnicode_FromStringAndSize(str, size);
            free(str);
            return result;
        }
        case VT_BLOB: {
            int32_t size = read_raw_little_endian32(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            if (!check_buffer_bounds(stream, size, "read_column_value")) {
                return NULL;
            }
            *cell_checksum = crc_int8(*cell_checksum, VT_BLOB);
            *cell_checksum = crc_int32(*cell_checksum, size);
            *cell_checksum = crc_string(*cell_checksum, (const char*)stream->data + stream->pos, size);
            PyObject* bytes_obj = PyBytes_FromStringAndSize((const char*)stream->data + stream->pos, size);
            if (bytes_obj == NULL) {
                return NULL;
            }
            PyObject* result = PyByteArray_FromObject(bytes_obj);
            Py_DECREF(bytes_obj);
            stream->pos += size;
            read_tag(stream);
            return result;
        }
        case VT_BOOLEAN: {
            int8_t value = read_raw_byte(stream);
            *cell_checksum = crc_int8(*cell_checksum, VT_BOOLEAN);
            int8_t bool_int8 = value ? 1 : 0;
            *cell_checksum = crc_int8(*cell_checksum, bool_int8);
            read_tag(stream);
            return PyBool_FromLong(value);
        }
        case VT_DOUBLE: {
            double value = read_double(stream);
            if (PyErr_Occurred()) {
                return NULL;
            }
            // Use union to safely convert double to int64_t, avoiding strict aliasing violations
            double_int64_union converter;
            converter.d = value;
            int64_t double_bits = converter.i64;
            *cell_checksum = crc_int8(*cell_checksum, VT_DOUBLE);
            *cell_checksum = crc_int64(*cell_checksum, double_bits);
            read_tag(stream);
            return PyFloat_FromDouble(value);
        }
        default:
            PyErr_Format(PyExc_ValueError, "Unsupported column type: %d", type);
            return NULL;
    }
}

// Parse primary key column - exactly as in C++ SDK
static PyObject* read_primary_key_column(PlainBufferStream* stream, int8_t* row_checksum) {
    if (!check_last_tag_was(stream, TAG_CELL)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL but it was %d", stream->last_tag);
        return NULL;
    }
    read_tag(stream);

    if (!check_last_tag_was(stream, TAG_CELL_NAME)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_NAME but it was %d", stream->last_tag);
        return NULL;
    }

    int8_t cell_checksum = 0;
    int32_t name_size = read_raw_little_endian32(stream);
    if (PyErr_Occurred()) {
        return NULL;
    }
    char* column_name = read_utf_string(stream, name_size);
    if (!column_name) return NULL;

    cell_checksum = crc_string(cell_checksum, column_name, name_size);
    read_tag(stream);

    if (!check_last_tag_was(stream, TAG_CELL_VALUE)) {
        free(column_name);
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_VALUE but it was %d", stream->last_tag);
        return NULL;
    }

    PyObject* value = read_primary_key_value(stream, &cell_checksum);
    if (!value) {
        free(column_name);
        return NULL;
    }

    if (stream->last_tag == TAG_CELL_CHECKSUM) {
        int8_t checksum = read_raw_byte(stream);
        if (checksum != cell_checksum) {
            free(column_name);
            Py_DECREF(value);
            PyErr_SetString(PyExc_ValueError, "Cell checksum mismatch");
            return NULL;
        }
        read_tag(stream);
    } else {
        free(column_name);
        Py_DECREF(value);
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_CHECKSUM but it was %d", stream->last_tag);
        return NULL;
    }

    *row_checksum = crc_int8(*row_checksum, cell_checksum);

    PyObject* name_obj = PyUnicode_FromString(column_name);
    free(column_name);
    if (!name_obj) {
        Py_DECREF(value);
        return NULL;
    }

    PyObject* result = PyTuple_Pack(2, name_obj, value);
    Py_DECREF(name_obj);
    Py_DECREF(value);
    return result;
}

// Parse column - exactly as in C++ SDK
static PyObject* read_column(PlainBufferStream* stream, int8_t* row_checksum) {
    if (!check_last_tag_was(stream, TAG_CELL)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL but it was %d", stream->last_tag);
        return NULL;
    }
    read_tag(stream);

    if (!check_last_tag_was(stream, TAG_CELL_NAME)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_NAME but it was %d", stream->last_tag);
        return NULL;
    }

    int8_t cell_checksum = 0;
    int32_t name_size = read_raw_little_endian32(stream);
    if (PyErr_Occurred()) {
        return NULL;
    }
    char* column_name = read_utf_string(stream, name_size);
    if (!column_name) return NULL;

    cell_checksum = crc_string(cell_checksum, column_name, name_size);
    read_tag(stream);

    PyObject* value = NULL;
    if (stream->last_tag == TAG_CELL_VALUE) {
        value = read_column_value(stream, &cell_checksum);
        if (!value) {
            free(column_name);
            return NULL;
        }
    }

    // Skip CELL_TYPE
    if (stream->last_tag == TAG_CELL_TYPE) {
        int8_t cell_type = read_raw_byte(stream);
        cell_checksum = crc_int8(cell_checksum, cell_type);
        read_tag(stream);
    }

    PyObject* timestamp = NULL;
    if (stream->last_tag == TAG_CELL_TIMESTAMP) {
        int64_t ts = read_raw_little_endian64(stream);
        if (PyErr_Occurred()) {
            free(column_name);
            Py_XDECREF(value);
            return NULL;
        }
        cell_checksum = crc_int64(cell_checksum, ts);
        timestamp = PyLong_FromLongLong(ts);
        if (!timestamp) {
            free(column_name);
            Py_XDECREF(value);
            return NULL;
        }
        read_tag(stream);
    }

    if (stream->last_tag == TAG_CELL_CHECKSUM) {
        int8_t checksum = read_raw_byte(stream);
        if (checksum != cell_checksum) {
            free(column_name);
            Py_XDECREF(value);
            Py_XDECREF(timestamp);
            PyErr_SetString(PyExc_ValueError, "Cell checksum mismatch");
            return NULL;
        }
        read_tag(stream);
    } else {
        free(column_name);
        Py_XDECREF(value);
        Py_XDECREF(timestamp);
        PyErr_Format(PyExc_ValueError, "Expect TAG_CELL_CHECKSUM but it was %d", stream->last_tag);
        return NULL;
    }

    *row_checksum = crc_int8(*row_checksum, cell_checksum);

    PyObject* name_obj = PyUnicode_FromString(column_name);
    free(column_name);
    if (!name_obj) {
        Py_XDECREF(value);
        Py_XDECREF(timestamp);
        return NULL;
    }

    PyObject* result = NULL;
    PyObject* value_or_none = value ? value : Py_None;
    if (!value) {
        Py_INCREF(Py_None);
    }

    if (timestamp) {
        result = PyTuple_Pack(3, name_obj, value_or_none, timestamp);
        Py_DECREF(timestamp);
    } else {
        result = PyTuple_Pack(2, name_obj, value_or_none);
    }

    Py_DECREF(name_obj);
    Py_DECREF(value_or_none);

    return result;
}

// Parse row without header - exactly following C++ SDK logic
static PyObject* read_row_without_header(PlainBufferStream* stream) {
    int8_t row_checksum = 0;

    // Follow C++ SDK logic exactly: only check for TAG_ROW_PK or TAG_ROW_DATA
    if (!check_last_tag_was(stream, TAG_ROW_PK) && !check_last_tag_was(stream, TAG_ROW_DATA)) {
        PyErr_Format(PyExc_ValueError, "Expect TAG_ROW_PK or TAG_ROW_DATA but it was %d", stream->last_tag);
        return NULL;
    }

    PyObject* primary_keys = PyList_New(0);
    if (!primary_keys) return NULL;

    // Parse primary key - exactly as in C++ SDK
    if (check_last_tag_was(stream, TAG_ROW_PK)) {  // no pk for GetRange agg without groupby
        read_tag(stream);
        while (check_last_tag_was(stream, TAG_CELL)) {
            PyObject* pk_column = read_primary_key_column(stream, &row_checksum);
            if (!pk_column) {
                Py_DECREF(primary_keys);
                return NULL;
            }
            PyList_Append(primary_keys, pk_column);
            Py_DECREF(pk_column);
        }
    }

    PyObject* columns = PyList_New(0);
    if (!columns) {
        Py_DECREF(primary_keys);
        return NULL;
    }

    // Parse columns - exactly as in C++ SDK
    if (check_last_tag_was(stream, TAG_ROW_DATA)) {
        read_tag(stream);
        while (check_last_tag_was(stream, TAG_CELL)) {
            PyObject* column = read_column(stream, &row_checksum);
            if (!column) {
                Py_DECREF(primary_keys);
                Py_DECREF(columns);
                return NULL;
            }
            PyList_Append(columns, column);
            Py_DECREF(column);
        }
    }

    // Skip row delete marker - exactly as in C++ SDK
    if (check_last_tag_was(stream, TAG_DELETE_ROW_MARKER)) {
        read_tag(stream);
        row_checksum = crc_int8(row_checksum, 1);
    } else {
        row_checksum = crc_int8(row_checksum, 0);
    }

    // Check row checksum - exactly as in C++ SDK
    if (check_last_tag_was(stream, TAG_ROW_CHECKSUM)) {
        int8_t checksum = read_raw_byte(stream);
        if (checksum != row_checksum) {
            Py_DECREF(primary_keys);
            Py_DECREF(columns);
            PyErr_SetString(PyExc_ValueError, "Checksum is mismatch");
            return NULL;
        }
        read_tag(stream);
    } else {
        Py_DECREF(primary_keys);
        Py_DECREF(columns);
        PyErr_Format(PyExc_ValueError, "Expect TAG_ROW_CHECKSUM but it was %d", stream->last_tag);
        return NULL;
    }

    PyObject* result = PyTuple_Pack(2, primary_keys, columns);
    Py_DECREF(primary_keys);
    Py_DECREF(columns);
    return result;
}

// Parse single row (equivalent to C++ ReadRow)
static PyObject* parse_single_row(PyObject* self, PyObject* args) {
    if (!crc8_table_initialized) {
        PyErr_SetString(PyExc_RuntimeError, "CRC8 table not initialized");
        return NULL;
    }
    const char* buffer;
    Py_ssize_t buffer_size;

    if (!PyArg_ParseTuple(args, "y#", &buffer, &buffer_size)) {
        return NULL;
    }

    PlainBufferStream stream = {
        .data = (const uint8_t*)buffer,
        .size = (size_t)buffer_size,
        .pos = 0,
        .last_tag = 0
    };

    // Read header
    int32_t header = read_raw_little_endian32(&stream);
    if (PyErr_Occurred()) {
        return NULL;
    }
    if (header != HEADER) {
        PyErr_Format(PyExc_ValueError, "Invalid header: expected 0x%x, got 0x%x", HEADER, header);
        return NULL;
    }

    // Read first tag
    read_tag(&stream);

    // Parse single row
    PyObject* row = read_row_without_header(&stream);
    if (!row) {
        return NULL;
    }

    return row;
}

// Parse multiple rows (equivalent to C++ ReadRows)
static PyObject* parse_multiple_rows(PyObject* self, PyObject* args) {
    if (!crc8_table_initialized) {
        PyErr_SetString(PyExc_RuntimeError, "CRC8 table not initialized");
        return NULL;
    }
    const char* buffer;
    Py_ssize_t buffer_size;

    if (!PyArg_ParseTuple(args, "y#", &buffer, &buffer_size)) {
        return NULL;
    }

    PlainBufferStream stream = {
        .data = (const uint8_t*)buffer,
        .size = (size_t)buffer_size,
        .pos = 0,
        .last_tag = 0
    };

    // Read header
    int32_t header = read_raw_little_endian32(&stream);
    if (PyErr_Occurred()) {
        return NULL;
    }
    if (header != HEADER) {
        PyErr_Format(PyExc_ValueError, "Invalid header: expected 0x%x, got 0x%x", HEADER, header);
        return NULL;
    }

    // Read first tag
    read_tag(&stream);

    PyObject* rows = PyList_New(0);
    if (!rows) return NULL;

    // Parse all rows - exactly as in C++ ReadRows
    while (stream.pos < stream.size) {
        PyObject* row = read_row_without_header(&stream);
        if (!row) {
            Py_DECREF(rows);
            return NULL;
        }

        PyList_Append(rows, row);
        Py_DECREF(row);
    }

    return rows;
}



// Method definitions
static PyMethodDef NativePlainBufferMethods[] = {
    {"parse_single_row", parse_single_row, METH_VARARGS, "Parse single row from PlainBuffer data (equivalent to C++ ReadRow)"},
    {"parse_multiple_rows", parse_multiple_rows, METH_VARARGS, "Parse multiple rows from PlainBuffer data (equivalent to C++ ReadRows)"},
    {NULL, NULL, 0, NULL}
};

// Module definition
static struct PyModuleDef native_plainbuffer_module = {
    PyModuleDef_HEAD_INIT,
    "native_plainbuffer",
    "Native PlainBuffer parser based on C++ SDK",
    -1,
    NativePlainBufferMethods
};

// Module initialization
PyMODINIT_FUNC PyInit_native_plainbuffer(void) {
    init_crc8_table();
    return PyModule_Create(&native_plainbuffer_module);
}