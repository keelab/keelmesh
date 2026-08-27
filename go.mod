module github.com/keelab/keelmesh

go 1.26.7

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.10.0
	github.com/keelab/contrib v0.0.1
	github.com/keelab/keelith v0.0.3
	github.com/larksuite/oapi-sdk-go/v3 v3.10.0
	github.com/open-dingtalk/dingtalk-stream-sdk-go v0.9.1
	github.com/tencent-connect/botgo v0.2.1
	go.opentelemetry.io/otel v1.45.0
	golang.org/x/oauth2 v0.36.0
	google.golang.org/grpc v1.83.1
	google.golang.org/protobuf v1.36.12
	gorm.io/driver/postgres v1.6.2
	gorm.io/gorm v1.31.2
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260709200747-435963d16310.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coder/websocket v1.8.15 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-resty/resty/v2 v2.6.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	github.com/tidwall/gjson v1.9.3 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool (
	github.com/keelab/keelith/cmd/protoc-gen-go-keelith
	google.golang.org/protobuf/cmd/protoc-gen-go
)
