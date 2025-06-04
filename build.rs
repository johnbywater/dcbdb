fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only compile proto files if the grpc feature is enabled
    #[cfg(feature = "grpc")]
    {
        // Note: This requires the protoc compiler to be installed.
        // To install it on macOS, run `brew install protobuf`.
        // It is also available at https://github.com/protocolbuffers/protobuf/releases

        // Try to compile the proto files, but don't fail the build if protoc is not installed
        match tonic_build::compile_protos("proto/eventstore.proto") {
            Ok(_) => {},
            Err(e) => {
                println!("Warning: Failed to compile proto files: {}", e);
                println!("The gRPC functionality will not be available.");
                println!("To enable gRPC, install the protoc compiler.");
            }
        }
    }

    Ok(())
}
