module github.com/m-mizutani/minerva

go 1.13

require (
	github.com/Netflix/go-env v0.0.0-20200803161858-92715955ff70
	github.com/apache/thrift v0.13.0 // indirect
	github.com/aws/aws-lambda-go v1.15.0
	github.com/aws/aws-sdk-go v1.34.5
	github.com/awslabs/aws-lambda-go-api-proxy v0.6.0
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/fastly/go-utils v0.0.0-20180712184237-d95a45783239 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/getsentry/sentry-go v0.5.1
	github.com/gin-gonic/gin v1.7.7
	github.com/golang/protobuf v1.3.5 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/uuid v1.1.1
	github.com/guregu/dynamo v1.6.1
	github.com/itchyny/gojq v0.9.0
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869 // indirect
	github.com/klauspost/compress v1.10.3
	github.com/kr/text v0.2.0 // indirect
	github.com/m-mizutani/rlogs v0.1.8
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.5.1
	github.com/tebeka/strftime v0.1.3 // indirect
	github.com/urfave/cli/v2 v2.2.0
	github.com/vmihailenco/msgpack/v5 v5.0.0-beta.1
	// github.com/xitongsys/parquet-go must be fixed on v1.3.0 to avoid zstd
	github.com/xitongsys/parquet-go v1.5.1
	github.com/xitongsys/parquet-go-source v0.0.0-20200326031722-42b453e70c3b
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e // indirect
	golang.org/x/sys v0.0.0-20200814200057-3d37ad5750ed // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/go-playground/assert.v1 v1.2.1
)

replace github.com/ugorji/go v1.1.4 => github.com/ugorji/go/codec v0.0.0-20190204201341-e444a5086c43
