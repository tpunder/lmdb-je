# As of 2021-05-25 this is the latest mdb.master commit.
# Note: We need to use the mdb.master branch since it has additional fixes needed
#       for Windows support to work right with the memory mapping to incrementally
#       grow the data file instead of allocating everything up front. This is needed
#       when we specify something like a 1TB Map Size.
lmdb_version := 4b6154340c27d03592b8824646a3bc4eb7ab61f5
dir := lmdblib/libraries/liblmdb

all : code linux-x64 linux-arm64 windows osx version

.PHONY : all

version: 
	echo $(lmdb_version) > src/main/resources/lmdb-je/VERSION

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
	cd lmdblib && curl --location https://github.com/LMDB/lmdb/archive/$(lmdb_version).tar.gz | tar --strip-components 1 -xzf -

clean:
	rm -rf lmdblib
