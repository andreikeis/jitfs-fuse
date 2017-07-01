# jitfs-fuse

## Installation

Build requires [Meson](http://mesonbuild.com/Manual.html) and
[ninja](https://ninja-build.org/).

1. Build and install libfuse.

2. In empty directory:

```
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig
export LD_RUN_PATH=/usr/local/lib64
meson <path to jitfs-fuse>
ninja-build
jitfsd --help
```

This assumes that libfuse is installed in /usr/local/lib64.

## Running jitfs-fuse

```
sudo ./jitfsd  -f -o allow_other     \
    -o cache=/var/tmp/jitfs/cache    \
    -o db=/var/tmp/jitfs/mirror.db   \
    -o sock=/tmp/jitfs.sock          \
    /var/tmp/jitfs/root/.jitfs
```

## Details of jitfs protocol

tbd.
