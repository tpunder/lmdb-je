LMDB Java Edition
=================

<a href="http://symas.com/mdb/">Lightning Memory-Mapped Database</a> (LMDB) wrappers for Java.

Features
--------

* Bundled LMDB binaries for Mac OSX x86_64 and Linux x86_64 (you can also provide your own)
* <a href="https://github.com/jnr/jnr-ffi">JNR</a> based wrappers for most of the <a href="http://symas.com/mdb/doc/group__mdb.html">LMDB API</a>
* Implementation of a Java <a href="http://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentNavigableMap.html">ConcurrentNavigableMap</a> backed by LMDB.
* A MultiMap implementation allowing multiple values per key

JavaDocs
--------

TODO

Native Library
--------------

LMDB-JE comes bundled with LMDB binaries for Mac OSX x86_64 and Linux x86_64 that have been compiled with "-DMDB_MAXKEYSIZE=0".  If you are using a different OS/CPU combo then you can provide your own LMDB library by specifying either an environment variable (LMDB_LIB_PATH) or a Java System Property (lmdb_lib_path) that points to the LMDB library (e.g. /path/to/liblmdb.so).

    CPPFLAGS="-DMDB_MAXKEYSIZE=0" make
    
Linux, and Windows Libaries are built using: [dockcross](https://github.com/dockcross/dockcross)

Helper runtime scripts were created via:

    docker run --rm dockcross/linux-x64 > ./dockcross-linux-x64
    chmod +x ./dockcross-linux-x64
    docker run --rm dockcross/windows-static-x64 > ./dockcross-windows-static-x64
    chmod +x ./dockcross-windows-static-x64

To rebuild the native lmdb libraries (from OSX) simply run `make all` from the top level project directory.  This does:

1. Checks out the current liblmdb sources from https://github.com/lmdb/lmdb
2. Runs `make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all` for Linux, OSX, and Windows
3. Copies the resulting `liblmdb.so` to the appropriate `src/main/resources/lmdb-je/[platform]_x86_64` folder.

API Usage
---------


ConcurrentNavigableMap Usage
----------------------------


Authors
-------

Tim Underwood (<a href="https://github.com/tpunder" rel="author">GitHub</a>, <a href="https://www.linkedin.com/in/tpunder" rel="author">LinkedIn</a>, <a href="https://twitter.com/tpunder" rel="author">Twitter</a>, <a href="https://plus.google.com/+TimUnderwood0" rel="author">Google Plus</a>)

Copyright
---------

* LMDB-JE - Copyright [Eluvio](http://www.eluvio.com)
* LMDB - Copyright Howard Chu, [Symas Corp.](http://symas.com/)

License
-------

* LMDB-JE is licensed under: [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt)
* LMDB is licensed under the [OpenLDAP License](http://www.openldap.org/software/release/license.html)

