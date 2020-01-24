module github.com/m-mizutani/minerva

go 1.13

require (
	github.com/apache/thrift v0.13.0 // indirect
	github.com/aws/aws-lambda-go v1.13.3
	github.com/aws/aws-sdk-go v1.28.8
	github.com/awslabs/aws-lambda-go-api-proxy v0.5.0
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/gin-gonic/gin v1.5.0
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/uuid v1.1.1
	github.com/guregu/dynamo v1.6.0
	github.com/json-iterator/go v1.1.9 // indirect
	github.com/klauspost/compress v1.9.8 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/leodido/go-urn v1.2.0 // indirect
	github.com/m-mizutani/rlogs v0.1.3
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli/v2 v2.1.1
	// github.com/xitongsys/parquet-go must be fixed on v1.3.0 to avoid zstd
	github.com/xitongsys/parquet-go v1.5.0
	github.com/xitongsys/parquet-go-source v0.0.0-20191104003508-ecfa341356a6
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa // indirect
	golang.org/x/sys v0.0.0-20200122134326-e047566fdf82 // indirect
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/go-playground/validator.v9 v9.31.0 // indirect
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

replace github.com/ugorji/go v1.1.4 => github.com/ugorji/go/codec v0.0.0-20190204201341-e444a5086c43
