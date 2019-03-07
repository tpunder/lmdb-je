dir := lmdblib/libraries/liblmdb/

all : clean linux windows osx

.PHONY : all

linux:
	cd $(dir) &&  ../../../dockcross-linux-x64 make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all
	cp lmdblib/libraries/liblmdb/liblmdb.so src/main/resources/lmdb-je/linux_x86_64/liblmdb.so

windows:
	cd $(dir) &&  ../../../dockcross-windows-static-x64 make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all
	cp lmdblib/libraries/liblmdb/liblmdb.so src/main/resources/lmdb-je/windows_x86_64/liblmdb.so

osx:
	cd $(dir) && make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean test;
	cp lmdblib/libraries/liblmdb/liblmdb.so src/main/resources/lmdb-je/darwin_x86_64/liblmdb.so
	
clean:
	rm -rf lmdblib/
	git clone git@github.com:LMDB/lmdb.git lmdblib