# RocksDB Interop

DartでRocksDBを使うためのC++バインディングです。

## インストール方法

現在Ubuntuにしか対応していません。
あんまりローカル環境にライブラリとか色々入れるのはやだと思うので、VagrantとかAWSとか使って用意するといいと思います。

### 依存ライブラリ
```
sudo apt install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev make cmake g++
git clone git@github.com:facebook/rocksdb.git
cd rocksdb
make shared_lib
sudo make install-shared
```

### Dart

```
cd ~
sudo apt install apt-transport-https
sudo sh -c 'curl https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -'
sudo sh -c 'curl https://storage.googleapis.com/download.dartlang.org/linux/debian/dart_stable.list > /etc/apt/sources.list.d/dart_stable.list'
sudo apt update
sudo apt install dart

export PATH=/usr/lib/dart/bin:$PATH # これは.bashrcにも書いておこう
```

### このリポジトリ
```
git clone https://github.com/rikuTanide/rocksdb_interop
cd rocksdb_interop
pub get
pub run test
dart example/rocksdb_interop.dart
```


## このライブラリをビルドしたい人向け（不要）
cmakeが最新じゃないといけないっぽいです。

```
cd ~
wget https://github.com/Kitware/CMake/releases/download/v3.14.5/cmake-3.14.5.tar.gz
tar xvf cmake-3.14.5.tar.gz
cd cmake-3.14.5.tar.gz
./bootstrap && make && sudo make install
cd rocksdb_interop/lib
cmake .
make

```