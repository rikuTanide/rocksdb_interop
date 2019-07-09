import "package:rocksdb_interop/rocksdb_interop.dart";
import "package:test/test.dart";
import "dart:typed_data";
import "dart:convert";
import "dart:io";

const DB_PATH = "./db";
const COLUMN_FAMILY_NAME = "column1";

void main() {
  int db;
  int column1;

  setUp(() {
    var dbDir = new Directory(DB_PATH);
    if (dbDir.existsSync()) {
      dbDir.deleteSync(recursive: true);
    }
    createDB(DB_PATH, [COLUMN_FAMILY_NAME]);

    var result = open(DB_PATH, [COLUMN_FAMILY_NAME]);
    db = result[0];
    column1 = result[2];
  });

  test("Put/Get", () {
    put(db, column1, fromString("key1"), fromString("value1"));
    put(db, column1, fromString("key2"), fromString("value2"));
    put(db, column1, fromString("key3"), fromString("value3"));
    String value1 = toString(get(db, column1, fromString("key1")));
    String value2 = toString(get(db, column1, fromString("key2")));
    String value3 = toString(get(db, column1, fromString("key3")));
    Uint8List value4 = get(db, column1, fromString("key4"));

    expect(value1, equals("value1"));
    expect(value2, equals("value2"));
    expect(value3, equals("value3"));
    expect(value4, isNull);
  });

  test("Delete", () {
    put(db, column1, fromString("key1"), fromString("value1"));
    put(db, column1, fromString("key2"), fromString("value2"));
    put(db, column1, fromString("key3"), fromString("value3"));

    delete(db, column1, fromString("key2"));

    String value1 = toString(get(db, column1, fromString("key1")));
    Uint8List value2 = get(db, column1, fromString("key2"));
    String value3 = toString(get(db, column1, fromString("key3")));


    expect(value1, equals("value1"));
    expect(value2, isNull);
    expect(value3, equals("value3"));
  });

  test("Write", () {
    var batch = createWriteBatch();

    writeBatch_Put(batch, column1, fromString("key1"), fromString("value1"));
    writeBatch_Put(batch, column1, fromString("key2"), fromString("value2"));
    writeBatch_Put(batch, column1, fromString("key3"), fromString("value3"));

    write(db, batch);

    String value1 = toString(get(db, column1, fromString("key1")));
    String value2 = toString(get(db, column1, fromString("key2")));
    String value3 = toString(get(db, column1, fromString("key3")));
    Uint8List value4 = get(db, column1, fromString("key4"));

    expect(value1, equals("value1"));
    expect(value2, equals("value2"));
    expect(value3, equals("value3"));
    expect(value4, isNull);
  });

  test("DeleteBatch", () {

    put(db, column1, fromString("key1"), fromString("value1"));
    put(db, column1, fromString("key2"), fromString("value2"));
    put(db, column1, fromString("key3"), fromString("value3"));

    var batch = createWriteBatch();

    writeBatch_Delete(batch, column1, fromString("key2"));

    write(db, batch);

    String value1 = toString(get(db, column1, fromString("key1")));
    String value2 = toString(get(db, column1, fromString("key2")));
    String value3 = toString(get(db, column1, fromString("key3")));

    expect(value1, equals("value1"));
    expect(value2, isNull);
    expect(value3, equals("value3"));
  });



  test("Seek", () {
    put(db, column1, fromString("key110111"), fromString("value0"));
    put(db, column1, fromString("key111222"), fromString("value1"));
    put(db, column1, fromString("key111333"), fromString("value2"));
    put(db, column1, fromString("key112222"), fromString("value3"));

    var iter = seek_Start(db, column1, fromString("key111"));

    expect(seek_HasNext(iter), isTrue);
    expect(toString(seek_Key(iter)), "key111222");
    expect(toString(seek_Value(iter)), "value1");
    seek_Next(iter);
    expect(seek_HasNext(iter), isTrue);
    expect(toString(seek_Key(iter)), "key111333");
    expect(toString(seek_Value(iter)), "value2");
    seek_Next(iter);
    expect(seek_HasNext(iter), isFalse);

    seek_End(iter);
  });

  tearDown(() {
    close(db, [column1]);
  });
}

Uint8List fromString(String str) {
  return AsciiEncoder().convert(str);
}

String toString(Uint8List list) {
  if (list == null) {
    return null;
  }
  return AsciiDecoder().convert(list);
}
