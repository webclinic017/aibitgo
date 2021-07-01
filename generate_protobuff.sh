#! bin/bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. execution/execution.proto
ls -l execution | grep pb2
