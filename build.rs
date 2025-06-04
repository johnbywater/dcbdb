use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Note: This requires the protoc compiler to be installed.
    // To install it on macOS, run `brew install protobuf`.
    // It is also available at https://github.com/protocolbuffers/protobuf/releases

    // Try to compile the proto files, but don't fail the build if protoc is not installed
    tonic_build::compile_protos("proto/eventstore.proto").expect("Failed to compile proto files");


    Ok(())
}
