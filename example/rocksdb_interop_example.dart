import "package:rocksdb_interop/rocksdb_interop.dart";

import "dart:io";
import "dart:typed_data";
import "dart:convert";

int db;
int column;
const DB_PATH = "./db";
const COLUMN_FAMILY_NAME = "column1";

main() async {
  var dbDir = new Directory(DB_PATH);
  if (dbDir.existsSync()) {
    dbDir.deleteSync(recursive: true);
  }
  createDB(DB_PATH, [COLUMN_FAMILY_NAME]);

  var result = open(DB_PATH, [COLUMN_FAMILY_NAME]);
  db = result[0];
  column = result[2];

  var server = await HttpServer.bind("0.0.0.0", 8080);

  server.listen(onRequest);

  print("http://localhost:8080/");
}

void onRequest(HttpRequest event) async {
  if (event.method == "GET") {
    event.response
      ..headers.add("content-type", "text/html; charset=utf-8")
      ..write("""
<html>
<body>

<form method='post' action='/' accept-charset="UTF-8">
<input type='text' placeholder='key' name='key' pattern="^[0-9A-Za-z]+\$"><br/>
<input type='text' placeholder='value' name='value' pattern="^[0-9A-Za-z]+\$">
<input type='submit' name='保存'/>
</form>

""");

    var iter = seek_Start(db, column, fromString("messege_"));
    var escape = new HtmlEscape();
    for (; seek_HasNext(iter); seek_Next(iter)) {
      event.response.write(
          "${escape.convert(toString(seek_Key(iter)))} ${escape.convert(toString(seek_Value(iter)))} <br/>\n");
    }
    seek_End(iter);

    event.response
      ..write("""
</body>
</html>
""")
      ..close();
  } else {
    var content = await event.transform(Utf8Decoder()).join();
    var queryParams = Uri(query: content).queryParameters;
    var key = queryParams["key"];
    var value = queryParams["value"];

    put(db, column, fromString("messege_" + key), fromString(value));

    await event.response.redirect(Uri.parse("/"), status: HttpStatus.seeOther);
  }
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
