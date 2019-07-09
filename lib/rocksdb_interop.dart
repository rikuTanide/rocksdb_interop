library rocksdb_interop;

import "dart:typed_data";
import "dart-ext:rocksdb_interop";

void createDB(String path, List<String> columnFamilyNames) native "CreateDB";

// [db, defaultColumnFamilyHandle , columnFamilyHandles...]
List<int> open(String path, List<String> columnFamilyNames) native "Open";

void close(int db, List<int> columnFamilyHandlers) native "Close";

void put(int db, int columnFamilyHandler, Uint8List key, Uint8List value)
    native "Put";

void delete(int db, int columnFamilyHandler, Uint8List key) native "Delete";

Uint8List get(int db, int columnFamilyHandle, Uint8List key) native "Get";

int createWriteBatch() native "CreateWriteBatch";

void writeBatch_Put(int columnFamilyHandle, int handle, Uint8List key,
    Uint8List value) native "WriteBatch_Put";

void writeBatch_Delete(int columnFamilyHandle, int handle, Uint8List key) native "WriteBatch_Delete";

void write(int db, int writeBatch) native "Write";

int seek_Start(int db, int handler, Uint8List prefix) native "Seek_Start";

Uint8List seek_Key(int seek) native "Seek_Key";

Uint8List seek_Value(int seek) native "Seek_Value";

bool seek_Next(int seek) native "Seek_Next";

bool seek_HasNext(int seek) native "Seek_HasNext";

void seek_End(int seek) native "Seek_End";
