package ipfsnifferv1

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative --proto_path=. --proto_path=/usr/include envelope.proto discovery.proto fetch.proto extract.proto doc.proto index.proto stream.proto
