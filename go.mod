module github.com/podomy/concord

go 1.26.3

require (
	github.com/coreos/go-iptables v0.8.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/mdns v1.0.7
	github.com/hashicorp/memberlist v0.5.4
	github.com/miekg/dns v1.1.72
	github.com/opencontainers/cgroups v0.0.4
	github.com/opencontainers/runc v1.3.3
	github.com/podomy/concord/sdk v0.0.0-20261013105412-8b22d6c65b45
	github.com/spf13/cobra v1.10.2
	github.com/vishvananda/netlink v1.3.1
	go.etcd.io/bbolt v1.5.0
	go.uber.org/zap v1.28.0
	golang.org/x/crypto v0.53.0
	golang.zx2c4.com/wireguard/wgctrl v0.0.0-20241231184526-a9ab2273dd10
)

require (
	cyphar.com/go-pathrs v0.2.5 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/checkpoint-restore/go-criu/v6 v6.3.0 // indirect
	github.com/cilium/ebpf v0.17.3 // indirect
	github.com/containerd/console v1.0.5 // indirect
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/cyphar/filepath-securejoin v0.7.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/godbus/dbus/v5 v5.2.2 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/native v1.1.0 // indirect
	github.com/mdlayher/genetlink v1.3.2 // indirect
	github.com/mdlayher/netlink v1.7.2 // indirect
	github.com/mdlayher/socket v0.5.1 // indirect
	github.com/moby/sys/capability v0.4.0 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/mrunalp/fileutils v0.5.1 // indirect
	github.com/opencontainers/runtime-spec v1.3.0 // indirect
	github.com/opencontainers/selinux v1.13.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/seccomp/libseccomp-golang v0.10.0 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/vishvananda/netns v0.0.5 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/mod v0.37.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/tools v0.46.0 // indirect
	golang.zx2c4.com/wireguard v0.0.0-20231211153847-12269c276173 // indirect
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af // indirect
)

replace github.com/podomy/concord/sdk => ./sdk
