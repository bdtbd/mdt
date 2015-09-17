all:
	g++ -g -o mdt_test *.cc -I../src ../../libmdt.a ../../libtera.a -rdynamic -L../../../thirdparty/lib -lins_sdk -lsofa-pbrpc -lprotobuf -lsnappy -lzookeeper_mt -lgflags -lglog -ltcmalloc_minimal -lpthread -lrt -lz -ldl -I../../src -I../../../thirdparty/include

clean:
	rm -rf mdt_test
