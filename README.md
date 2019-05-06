# StormTracer
Java classes used to interfacing a storm topology/cluster with the Tracer scaling system.

To compile this library you will need `protoc` installed. You also need to make sure that
the versions of `protobuf-java` and the `grpc` libraries support the installed `protoc`
version. Also check that the dependency versions are reflected in the
`protobuf-maven-plugin` settings.

Also make sure that the version of java targeted but the compiler plugin is installed.
Once all the above is checked run the commands below to build and install the library:

`$ mvn protobuf:compile` 
`$ mvn protobuf:compile-custom` 
`$ mvn clean install`
