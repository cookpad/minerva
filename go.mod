module github.com/m-mizutani/minerva

go 1.13

require (
	github.com/apache/thrift v0.13.0 // indirect
	github.com/aws/aws-lambda-go v1.13.3
	github.com/aws/aws-sdk-go v1.25.32
	github.com/awslabs/aws-lambda-go-api-proxy v0.5.0
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/gin-gonic/gin v1.4.0
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/uuid v1.1.1
	github.com/guregu/dynamo v1.4.1
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/m-mizutani/rlogs v0.1.3
	github.com/m-mizutani/task-kitchen v0.0.0-20190423132322-ea758b3cc3d0
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli/v2 v2.0.0
	// github.com/xitongsys/parquet-go must be fixed on v1.3.0 to avoid zstd
	github.com/xitongsys/parquet-go v1.3.0
	github.com/xitongsys/parquet-go-source v0.0.0-20191104003508-ecfa341356a6
	golang.org/x/net v0.0.0-20191109021931-daa7c04131f5 // indirect
	golang.org/x/sys v0.0.0-20191110163157-d32e6e3b99c4 // indirect
	golang.org/x/text v0.3.2 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.2.5 // indirect
)

replace github.com/ugorji/go v1.1.4 => github.com/ugorji/go/codec v0.0.0-20190204201341-e444a5086c43
