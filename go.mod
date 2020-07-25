module github.com/ipfs/go-ipfs

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	github.com/AndreasBriese/bbloom v0.0.0-20190825152654-46b345b51c96 // indirect
	github.com/blang/semver v3.5.1+incompatible
	github.com/bren2010/proquint v0.0.0-20160323162903-38337c27106d
	github.com/coreos/go-systemd/v22 v22.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/elgris/jsondiff v0.0.0-20160530203242-765b5c24c302
	github.com/fatih/color v1.9.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9
	github.com/gabriel-vasile/mimetype v1.1.0
	github.com/go-bindata/go-bindata/v3 v3.1.3
	github.com/gogo/protobuf v1.3.1
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/ipfs/go-bitswap v0.2.19
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-blockservice v0.1.3
	github.com/ipfs/go-cid v0.0.6
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-datastore v0.4.4
	github.com/ipfs/go-detect-race v0.0.1
	github.com/ipfs/go-ds-badger v0.2.4
	github.com/ipfs/go-ds-flatfs v0.4.4
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipfs/go-ds-measure v0.1.0
	github.com/ipfs/go-filestore v0.0.3
	github.com/ipfs/go-fs-lock v0.0.5
	github.com/ipfs/go-graphsync v0.0.5
	github.com/ipfs/go-ipfs-blockstore v0.1.4
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-cmds v0.2.9
	github.com/ipfs/go-ipfs-config v0.8.0
	github.com/ipfs/go-ipfs-ds-help v0.1.1
	github.com/ipfs/go-ipfs-exchange-interface v0.0.1
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-files v0.0.8
	github.com/ipfs/go-ipfs-pinner v0.0.4
	github.com/ipfs/go-ipfs-posinfo v0.0.1
	github.com/ipfs/go-ipfs-provider v0.4.3
	github.com/ipfs/go-ipfs-routing v0.1.0
	github.com/ipfs/go-ipfs-util v0.0.2
	github.com/ipfs/go-ipld-cbor v0.0.4
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-ipld-git v0.0.3
	github.com/ipfs/go-ipns v0.0.2
	github.com/ipfs/go-log v1.0.4
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-metrics-prometheus v0.0.2
	github.com/ipfs/go-mfs v0.1.2
	github.com/ipfs/go-path v0.0.7
	github.com/ipfs/go-unixfs v0.2.4
	github.com/ipfs/go-verifcid v0.0.1
	github.com/ipfs/interface-go-ipfs-core v0.3.0
	github.com/ipld/go-car v0.1.0
	github.com/jbenet/go-is-domain v1.0.5
	github.com/jbenet/go-random v0.0.0-20190219211222-123a90aedc0c
	github.com/jbenet/go-temp-err-catcher v0.1.0
	github.com/jbenet/goprocess v0.1.4
	github.com/libp2p/go-libp2p v0.9.6
	github.com/libp2p/go-libp2p-circuit v0.2.3
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.5.7
	github.com/libp2p/go-libp2p-discovery v0.4.0
	github.com/libp2p/go-libp2p-http v0.1.5
	github.com/libp2p/go-libp2p-kad-dht v0.8.2
	github.com/libp2p/go-libp2p-kbucket v0.4.2
	github.com/libp2p/go-libp2p-loggables v0.1.0
	github.com/libp2p/go-libp2p-mplex v0.2.3
	github.com/libp2p/go-libp2p-noise v0.1.1
	github.com/libp2p/go-libp2p-peerstore v0.2.6
	github.com/libp2p/go-libp2p-pubsub v0.3.1
	github.com/libp2p/go-libp2p-pubsub-router v0.3.0
	github.com/libp2p/go-libp2p-quic-transport v0.6.0
	github.com/libp2p/go-libp2p-record v0.1.3
	github.com/libp2p/go-libp2p-routing-helpers v0.2.3
	github.com/libp2p/go-libp2p-secio v0.2.2
	github.com/libp2p/go-libp2p-swarm v0.2.6
	github.com/libp2p/go-libp2p-testing v0.1.1
	github.com/libp2p/go-libp2p-tls v0.1.3
	github.com/libp2p/go-libp2p-yamux v0.2.8
	github.com/libp2p/go-reuseport-transport v0.0.4 // indirect
	github.com/libp2p/go-socket-activation v0.0.2
	github.com/libp2p/go-tcp-transport v0.2.0
	github.com/libp2p/go-ws-transport v0.3.1
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/miekg/dns v1.1.29 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mr-tron/base58 v1.1.3
	github.com/multiformats/go-multiaddr v0.2.2
	github.com/multiformats/go-multiaddr-dns v0.2.0
	github.com/multiformats/go-multiaddr-net v0.1.5
	github.com/multiformats/go-multibase v0.0.3
	github.com/multiformats/go-multihash v0.0.13
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/syndtr/goleveldb v1.0.0
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc
	github.com/whyrusleeping/go-sysinfo v0.0.0-20190219211824-4a357d4b90b1
	github.com/whyrusleeping/multiaddr-filter v0.0.0-20160516205228-e903e4adabd7
	github.com/whyrusleeping/tar-utils v0.0.0-20180509141711-8c6c8ba81d5c
	go.uber.org/fx v1.12.0
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200510223506-06a226fb4e37
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20200520182314-0ba52f642ac2 // indirect
	golang.org/x/sys v0.0.0-20200523222454-059865788121
	golang.org/x/tools v0.0.0-20200522201501-cb1345f3a375 // indirect
	gopkg.in/cheggaaa/pb.v1 v1.0.28
)

replace github.com/ipfs/go-ipfs-cmds => ../../ipfs/go-ipfs-cmds

replace github.com/ipfs/go-fs-lock => ../../ipfs/go-fs-lock

replace github.com/ipfs/go-ds-flatfs => ../../ipfs/go-ds-flatfs

replace gopkg.in/cheggaaa/pb.v1 => ../../cheggaaa/pb

go 1.13
