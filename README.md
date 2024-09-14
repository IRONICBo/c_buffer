# Datenlord sdk demo

Current datenlord implementation is based on the fuse filesystem and support basic POSIX api for file operations, but it will introduce some overheads for the data transfer.
This demo is to show how to use the datenlord sdk to implement a user space client for datenlord, and serve datenlord as daemon process, current demo support c and python language.

### c language demo

Use `cargo build --release` to get dynamic library `libdatenlord.so` in `target/release/`.

Go to `examples/c` to run the c demo.
```bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../../target/release
g++ -o main test_datenlord_sdk.c -L../../target/release -ldatenlord -ldl
./main
```

### python language demo

##### pybind11

Use `pip install pybind11` to install pybind11.

```bash
python3 -m pip install pybind11
```

Build python library with pybind11.
```bash
g++ -O3 -Wall -shared -std=c++11 -fPIC $(python3 -m pybind11 --includes) bindings.cpp -o datenlord$(python3-config --extension-suffix) -L../../../target/release -ldatenlord -ldl
```

Run python demo.
```bash
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../../../target/release PYTHONPATH=.:$PYTHONPATH python3 test_datenlord_sdk.py
```