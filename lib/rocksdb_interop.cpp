#include "rocksdb_interop.h"


#include <random>
#include <stdlib.h>
#include <string.h>
#include "include/dart_api.h"
#include "include/dart_native_api.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include <unistd.h>
#include <iostream>
#include <inttypes.h>

using namespace rocksdb;


// Dartが全部uint64_tで渡してくるので、これが一番楽。
std::map<uint64_t, DB *> all_dbs;
uint64_t all_dbs_max = 0;

std::map<uint64_t, ColumnFamilyHandle *> all_column_family_handlers;
uint64_t all_column_family_handlers_max = 10000;

std::map<uint64_t, WriteBatch *> all_write_batches;
uint64_t all_write_batches_max = 20000;

std::map<uint64_t, Iterator *> all_iterators;
std::map<uint64_t, Slice *> all_prefix;
uint64_t all_iterators_max = 30000;


// Forward declaration of ResolveName function.
Dart_NativeFunction ResolveName(Dart_Handle name, int argc, bool *auto_setup_scope);

// The name of the initialization function is the extension name followed
// by _Init.
DART_EXPORT Dart_Handle rocksdb_interop_Init(Dart_Handle parent_library) {
    if (Dart_IsError(parent_library)) return parent_library;

    Dart_Handle result_code =
            Dart_SetNativeResolver(parent_library, ResolveName, NULL);
    if (Dart_IsError(result_code)) return result_code;

    return Dart_Null();
}

Dart_Handle HandleError(Dart_Handle handle) {
    if (Dart_IsError(handle)) Dart_PropagateError(handle);
    return handle;
}

std::string dart_str_to_string(Dart_Handle dart_str) {
    const char *name_c;
    Dart_StringToCString(dart_str, &name_c);
    std::string str = std::string(name_c);
    return str;
}


std::string dart_arg_str_to_string(Dart_NativeArguments arguments, int index) {
    Dart_Handle handle = Dart_GetNativeArgument(arguments, index);
    return dart_str_to_string(handle);
}

DB *dart_int_to_db(Dart_Handle dart_i) {
    uint64_t ptr;
    Dart_IntegerToUint64(dart_i, &ptr);
    return all_dbs[ptr];
}

DB *dart_arg_int_to_db(Dart_NativeArguments arguments, int index) {
    Dart_Handle handle = Dart_GetNativeArgument(arguments, index);
    return dart_int_to_db(handle);
}

Iterator *dart_int_to_iter(Dart_Handle dart_i) {
    uint64_t ptr;
    Dart_IntegerToUint64(dart_i, &ptr);
    return all_iterators[ptr];
}

Iterator *dart_arg_int_to_iter(Dart_NativeArguments arguments, int index) {
    Dart_Handle handle = Dart_GetNativeArgument(arguments, index);
    return dart_int_to_iter(handle);
}

Slice *dart_int_to_prefix(Dart_Handle dart_i) {
    uint64_t ptr;
    Dart_IntegerToUint64(dart_i, &ptr);
    return all_prefix[ptr];
}

Slice *dart_arg_int_to_prefix(Dart_NativeArguments arguments, int index) {
    Dart_Handle handle = Dart_GetNativeArgument(arguments, index);
    return dart_int_to_prefix(handle);
}

ColumnFamilyHandle *dart_int_to_cfh(Dart_Handle dart_i) {
    uint64_t ptr;
    Dart_IntegerToUint64(dart_i, &ptr);
    return all_column_family_handlers[ptr];
}

ColumnFamilyHandle *dart_arg_int_to_cfh(Dart_NativeArguments arguments, int index) {
    Dart_Handle handle = Dart_GetNativeArgument(arguments, index);
    return dart_int_to_cfh(handle);
}

uint8_t dart_int_lit_to_length(Dart_Handle bytes) {
    intptr_t length;
    Dart_ListLength(bytes, &length);
    return length;
}


WriteBatch *dart_int_to_wb(Dart_Handle dart_i) {
    uint64_t ptr;
    Dart_IntegerToUint64(dart_i, &ptr);
    return all_write_batches[ptr];
}

// void  CreateDB(String path, List<String> columnFamilyNames)
void CreateDB(Dart_NativeArguments arguments) {
    std::string db_path = dart_arg_str_to_string(arguments, 0);

    Options options;
    options.create_if_missing = true;
    DB *db;
    Status s = DB::Open(options, db_path, &db);
    assert(s.ok());

    Dart_Handle column_family_names = Dart_GetNativeArgument(arguments, 1);
    intptr_t length;
    Dart_ListLength(column_family_names, &length);
    for (intptr_t i = 0; i < length; i++) {
        std::string column_family_name = dart_str_to_string(Dart_ListGetAt(column_family_names, i));
        ColumnFamilyHandle *cf;
        s = db->CreateColumnFamily(ColumnFamilyOptions(), column_family_name, &cf);
        assert(s.ok());
        delete cf;
    }
    delete db;
}

// [ DB, ColumnFamilyHandler...]  Open(String path, List<String> columnFamilyNames)
void Open(Dart_NativeArguments arguments) {


    std::string db_path = dart_arg_str_to_string(arguments, 0);


    Dart_Handle column_family_names = Dart_GetNativeArgument(arguments, 1);
    intptr_t length;
    Dart_ListLength(column_family_names, &length);
    std::vector<ColumnFamilyDescriptor> column_families_vector;
    column_families_vector.push_back(ColumnFamilyDescriptor(
            kDefaultColumnFamilyName, ColumnFamilyOptions()));
    for (intptr_t i = 0; i < length; i++) {
        std::string column_family_name = dart_str_to_string(Dart_ListGetAt(column_family_names, i));
        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptor(column_family_name, ColumnFamilyOptions());
        column_families_vector.push_back(cfd);
    }

    DBOptions options = DBOptions();
    DB *db;
    std::vector<ColumnFamilyHandle *> handles;

    Status s = DB::Open(options, db_path, column_families_vector, &handles, &db);

    assert(s.ok());

    Dart_Handle result = Dart_NewListOf(Dart_CoreType_Int, length + 2);
    uint64_t db_ptr = all_dbs_max++;
    all_dbs[db_ptr] = db;
    Dart_ListSetAt(result, 0, Dart_NewInteger(db_ptr));

    for (int i = 0; i < handles.size(); i++) {
        ColumnFamilyHandle *handle = handles[i];
        uint64_t h_ptr = all_column_family_handlers_max++;
        all_column_family_handlers[h_ptr] = handle;
        Dart_ListSetAt(result, i + 1, Dart_NewInteger(h_ptr));
    }

    Dart_SetReturnValue(arguments, result);

}

// void Close(DB db, List<ColumnFamilyHandle> handles)
void Close(Dart_NativeArguments arguments) {

    Dart_Handle column_family_handlers = Dart_GetNativeArgument(arguments, 1);
    intptr_t length;
    Dart_ListLength(column_family_handlers, &length);

    for (intptr_t i = 0; i < length; i++) {
        uintptr_t ptr;
        Dart_IntegerToUint64(Dart_ListGetAt(column_family_handlers, i), &ptr);
        ColumnFamilyHandle *handle = all_column_family_handlers[ptr];
        all_column_family_handlers.erase(ptr);
        delete handle;
    }

    uintptr_t db_ptr;
    Dart_IntegerToUint64(Dart_GetNativeArgument(arguments, 0), &db_ptr);
    DB *db = all_dbs[db_ptr];
    all_dbs.erase(db_ptr);
    delete db;
}

// void Put(DB db, ColumnFamilyHandler handler, Uint8List key ,Uint8List value)
void Put(Dart_NativeArguments arguments) {
    DB *db = dart_arg_int_to_db(arguments, 0);
    ColumnFamilyHandle *handle = dart_arg_int_to_cfh(arguments, 1);

    Dart_Handle key_bytes = Dart_GetNativeArgument(arguments, 2);
    uint8_t key_length = dart_int_lit_to_length(key_bytes);
    auto *key = new uint8_t[key_length];
    Dart_ListGetAsBytes(key_bytes, 0, key, key_length);

    Dart_Handle value_bytes = Dart_GetNativeArgument(arguments, 3);
    uint8_t value_length = dart_int_lit_to_length(value_bytes);
    auto *value = new uint8_t[value_length];
    Dart_ListGetAsBytes(value_bytes, 0, value, value_length);

    db->Put(WriteOptions(), handle, Slice((char *) key, key_length), Slice((char *) value, value_length));

    delete[] key;
    delete[] value;
}

// Uint8List Get(DB db, ColumnFamilyHandler handler, Uint8List key)
void Get(Dart_NativeArguments arguments) {
    DB *db = dart_arg_int_to_db(arguments, 0);
    ColumnFamilyHandle *handle = dart_arg_int_to_cfh(arguments, 1);

    Dart_Handle key_bytes = Dart_GetNativeArgument(arguments, 2);
    uint8_t key_length = dart_int_lit_to_length(key_bytes);
    auto *key = new uint8_t[key_length];
    Dart_ListGetAsBytes(key_bytes, 0, key, key_length);

    PinnableSlice value;
    Status s = db->Get(ReadOptions(), handle, Slice((char *) key, key_length), &value);

    if (s.IsNotFound()) {
        return;
    }

    Dart_Handle results = Dart_NewTypedData(Dart_TypedData_kUint8, value.size());
    Dart_ListSetAsBytes(results, 0, (uint8_t *) value.data(), value.size());
    value.Reset();
    delete[] key;

    Dart_SetReturnValue(arguments, results);

}

void CreateWriteBatch(Dart_NativeArguments arguments) {
    auto *batch = new WriteBatch();
    uint64_t ptr = all_write_batches_max++;
    all_write_batches[ptr] = batch;
    Dart_SetReturnValue(arguments, Dart_NewInteger(ptr));
}

void WriteBatch_Put(Dart_NativeArguments arguments) {
    Dart_Handle wbp = Dart_GetNativeArgument(arguments, 0);
    WriteBatch *batch = dart_int_to_wb(wbp);

    ColumnFamilyHandle *handle = dart_arg_int_to_cfh(arguments, 1);


    Dart_Handle key_bytes = Dart_GetNativeArgument(arguments, 2);
    uint8_t key_length = dart_int_lit_to_length(key_bytes);
    auto *key = new uint8_t[key_length];
    Dart_ListGetAsBytes(key_bytes, 0, key, key_length);

    Dart_Handle value_bytes = Dart_GetNativeArgument(arguments, 3);
    uint8_t value_length = dart_int_lit_to_length(value_bytes);
    auto *value = new uint8_t[value_length];
    Dart_ListGetAsBytes(value_bytes, 0, value, value_length);

    batch->Put(handle, Slice((char *) key, key_length), Slice((char *) value, value_length));

    delete[] key;
    delete[] value;
}

void WriteBatch_Delete(Dart_NativeArguments arguments) {
    Dart_Handle wbp = Dart_GetNativeArgument(arguments, 0);
    WriteBatch *batch = dart_int_to_wb(wbp);

    ColumnFamilyHandle *handle = dart_arg_int_to_cfh(arguments, 1);


    Dart_Handle key_bytes = Dart_GetNativeArgument(arguments, 2);
    uint8_t key_length = dart_int_lit_to_length(key_bytes);
    auto *key = new uint8_t[key_length];
    Dart_ListGetAsBytes(key_bytes, 0, key, key_length);


    batch->Delete(handle, Slice((char *) key, key_length));

    delete[] key;
}


// void Write(DB db, List<ColumnFamilyHandler> handler, List<Uint8List> keys ,List<Uint8List> values)
void Write(Dart_NativeArguments arguments) {
    DB *db = dart_arg_int_to_db(arguments, 0);
    Dart_Handle wbp = Dart_GetNativeArgument(arguments, 1);
    WriteBatch *batch = dart_int_to_wb(wbp);

    Status s = db->Write(WriteOptions(), batch);
    assert(s.ok());

    delete batch;
}

// void Delete(DB db, ColumnFamilyHandler handler, Uint8List key ,Uint8List value)
void Delete(Dart_NativeArguments arguments) {
    DB *db = dart_arg_int_to_db(arguments, 0);
    ColumnFamilyHandle *handle = dart_arg_int_to_cfh(arguments, 1);

    Dart_Handle key_bytes = Dart_GetNativeArgument(arguments, 2);
    uint8_t key_length = dart_int_lit_to_length(key_bytes);
    auto *key = new uint8_t[key_length];
    Dart_ListGetAsBytes(key_bytes, 0, key, key_length);


    db->Delete(WriteOptions(), handle, Slice((char *) key, key_length));

    delete[] key;
}

// int Seek(DB db, ColumnFamilyHandler handler, Uint8List prefix)
void Seek_Start(Dart_NativeArguments arguments) {

    DB *db = dart_arg_int_to_db(arguments, 0);
    ColumnFamilyHandle *handle = dart_arg_int_to_cfh(arguments, 1);

    Dart_Handle prefix_bytes = Dart_GetNativeArgument(arguments, 2);
    uint8_t prefix_length = dart_int_lit_to_length(prefix_bytes);
    auto *prefix_b = new char[prefix_length];
    Dart_ListGetAsBytes(prefix_bytes, 0, (uint8_t *) prefix_b, prefix_length);

    auto prefix = new Slice(prefix_b, prefix_length);

    ReadOptions options;
    auto iter = db->NewIterator(options, handle);

    iter->Seek(*prefix);

    uint64_t iter_ptr = ++all_iterators_max;
    all_iterators[iter_ptr] = iter;
    all_prefix[iter_ptr] = prefix;

    Dart_SetReturnValue(arguments, Dart_NewInteger(iter_ptr));
}

// Uint8List Seek_Key(int seekPtr)
void Seek_Key(Dart_NativeArguments arguments) {
    Iterator *iter = dart_arg_int_to_iter(arguments, 0);

    auto key = iter->key();
    auto size = key.size();
    auto data = key.data();

    Dart_Handle results = Dart_NewTypedData(Dart_TypedData_kUint8, size);
    Dart_ListSetAsBytes(results, 0, (uint8_t *) data, size);

    Dart_SetReturnValue(arguments, results);
}

// Uint8List Seek_Value(int seekPtr)
void Seek_Value(Dart_NativeArguments arguments) {
    Iterator *iter = dart_arg_int_to_iter(arguments, 0);

    auto value = iter->value();
    auto size = value.size();
    auto data = value.data();

    Dart_Handle results = Dart_NewTypedData(Dart_TypedData_kUint8, size);
    Dart_ListSetAsBytes(results, 0, (uint8_t *) data, size);

    Dart_SetReturnValue(arguments, results);
}

// bool Seek_Value(int seekPtr)
void Seek_HasNext(Dart_NativeArguments arguments) {
    Iterator *iter = dart_arg_int_to_iter(arguments, 0);
    Slice *prefix = dart_arg_int_to_prefix(arguments, 0);

    bool next = iter->Valid() && iter->key().starts_with(*prefix);

    Dart_Handle results = Dart_NewBoolean(next);
    Dart_SetReturnValue(arguments, results);
}

void Seek_Next(Dart_NativeArguments arguments) {
    Iterator *iter = dart_arg_int_to_iter(arguments, 0);
    iter->Next();

}

// void Seek_End(int seekPtr)
void Seek_End(Dart_NativeArguments arguments) {
    uintptr_t seek_ptr;
    Dart_IntegerToUint64(Dart_GetNativeArgument(arguments, 0), &seek_ptr);

    Slice *pfx = all_prefix[seek_ptr];
    delete[] pfx;
    Iterator *iter = all_iterators[seek_ptr];

    iter->Reset();

    all_iterators.erase(seek_ptr);
    all_prefix.erase(seek_ptr);

}

Dart_NativeFunction ResolveName(Dart_Handle name, int argc, bool *auto_setup_scope) {
// If we fail, we return NULL, and Dart throws an exception.
    if (!Dart_IsString(name)) return NULL;
    Dart_NativeFunction result = NULL;
    const char *cname;
    HandleError(Dart_StringToCString(name, &cname));

    if (strcmp("CreateDB", cname) == 0) result = CreateDB;
    if (strcmp("Open", cname) == 0) result = Open;
    if (strcmp("Close", cname) == 0) result = Close;
    if (strcmp("Put", cname) == 0) result = Put;
    if (strcmp("Get", cname) == 0) result = Get;
    if (strcmp("Delete", cname) == 0) result = Delete;
    if (strcmp("CreateWriteBatch", cname) == 0) result = CreateWriteBatch;
    if (strcmp("WriteBatch_Put", cname) == 0) result = WriteBatch_Put;
    if (strcmp("WriteBatch_Delete", cname) == 0) result = WriteBatch_Delete;
    if (strcmp("Write", cname) == 0) result = Write;
    if (strcmp("Seek_Start", cname) == 0) result = Seek_Start;
    if (strcmp("Seek_Key", cname) == 0) result = Seek_Key;
    if (strcmp("Seek_Value", cname) == 0) result = Seek_Value;
    if (strcmp("Seek_Next", cname) == 0) result = Seek_Next;
    if (strcmp("Seek_HasNext", cname) == 0) result = Seek_HasNext;
    if (strcmp("Seek_End", cname) == 0) result = Seek_End;
    return result;
}
