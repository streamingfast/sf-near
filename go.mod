module github.com/streamingfast/sf-near

go 1.16

require (
	cloud.google.com/go/iam v0.3.0 // indirect
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2
	github.com/lithammer/dedent v1.1.0
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mr-tron/base58 v1.2.0
	github.com/spf13/cobra v1.4.0
	github.com/spf13/viper v1.8.1
	github.com/streamingfast/bstream v0.0.2-0.20220831142019-0a0b0caa04c3
	github.com/streamingfast/cli v0.0.4-0.20220419231930-a555cea243fc
	github.com/streamingfast/dauth v0.0.0-20220404140613-a40f4cd81626
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20220526184630-695c21740145
	github.com/streamingfast/dgrpc v0.0.0-20220307180102-b2d417ac8da7
	github.com/streamingfast/dlauncher v0.0.0-20220510190546-3b2b932ceac8
	github.com/streamingfast/dmetering v0.0.0-20220307162406-37261b4b3de9
	github.com/streamingfast/dmetrics v0.0.0-20220811180000-3e513057d17c
	github.com/streamingfast/dstore v0.1.1-0.20220607202639-35118aeaf648
	github.com/streamingfast/firehose v0.1.1-0.20220812162323-b98d71964619
	github.com/streamingfast/firehose-acme/types v0.0.0-20220901005555-99c1df91c222
	github.com/streamingfast/index-builder v0.0.0-20220810183227-6de1a5d962c7
	github.com/streamingfast/logging v0.0.0-20220511154537-ce373d264338
	github.com/streamingfast/merger v0.0.3-0.20220811184329-ce81549bd619
	github.com/streamingfast/near-go v0.0.0-20220302163233-b638f5b48a2d
	github.com/streamingfast/node-manager v0.0.2-0.20220811195019-694930bda9cb
	github.com/streamingfast/pbgo v0.0.6-0.20220801202203-c32e42ac42a8
	github.com/streamingfast/relayer v0.0.2-0.20220811185139-02ee222c9277
	github.com/streamingfast/sf-near/types v0.0.0-20220901151747-ff41fd4070cb
	github.com/streamingfast/sf-tools v0.0.0-20220810183745-b514ffd4aa46
	github.com/streamingfast/snapshotter v0.0.0-20220511035139-9c8d907b1c49
	github.com/stretchr/testify v1.7.1
	github.com/tidwall/gjson v1.9.3
	go.uber.org/zap v1.21.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/ShinyTrinkets/overseer => github.com/dfuse-io/overseer v0.2.1-0.20210326144022-ee491780e3ef
