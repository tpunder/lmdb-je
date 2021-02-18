lmdb_version := 0.9.24
dir := lmdblib/libraries/liblmdb

all : code linux-x64 linux-arm64 windows osx version

.PHONY : all

version: 
	echo LMDB_$(lmdb_version) > src/main/resources/lmdb-je/VERSION

linux-x64:
	cd $(dir) &&  ../../../dockcross-linux-x64 make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all
	cp $(dir)/liblmdb.so src/main/resources/lmdb-je/liblmdb_linux_x86_64.so

linux-arm64:
	cd $(dir) &&  ../../../dockcross-linux-arm64 make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all
	cp $(dir)/liblmdb.so src/main/resources/lmdb-je/liblmdb_linux_aarch64.so

windows:
	cd $(dir) &&  ../../../dockcross-windows-static-x64 make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all
	cp $(dir)/liblmdb.so src/main/resources/lmdb-je/liblmdb_windows_x86_64.so

osx:
	# Note: Explicitly set the path to make sure we use the Apple toolchain and not anything installed from brew
	cd $(dir) && PATH="/bin:/usr/bin" make CPP_FLAGS="-DMDB_MAXKEYSIZE=0" -e clean all;
	cp $(dir)/liblmdb.so src/main/resources/lmdb-je/liblmdb_darwin_x86_64.so

code:
	rm -rf lmdblib
	mkdir lmdblib
	cd lmdblib && curl --location https://github.com/LMDB/lmdb/archive/LMDB_$(lmdb_version).tar.gz | tar --strip-components 1 -xzf -

clean:
	rm -rf lmdblib