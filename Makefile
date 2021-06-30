.PHONY: proto-compile
proto-compile:
	protoc -I/usr/local/lib/protobuf/include -Iexample/src/main/resources/com/example --java_out=example/src/main/java example/src/main/resources/com/example/schema.proto
