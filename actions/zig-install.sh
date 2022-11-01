#!/bin/bash
# set up sccache
set -euxo pipefail
MATRIX_OS=${1}
ZIG_VER=0.9.1
ARCH=$(rustc --print cfg | grep target_arch | cut -d '=' -f 2 | sed 's/"//g')
echo "installing zig matrix.os=$MATRIX_OS version=$ZIG_VER"

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    echo "installing zig on ubuntu"
    echo "LLVM is available on: $LLVM_PATH"
    wget https://ziglang.org/download/$ZIG_VER/zig-linux-$ARCH-$ZIG_VER.tar.xz && \
        tar -xf zig-linux-$ARCH-$ZIG_VER.tar.xz && \
        sudo mv zig-linux-$ARCH-$ZIG_VER /usr/local && \
        pushd /usr/local/bin && \
        sudo ln -s ../zig-linux-$ARCH-$ZIG_VER/zig . && \
        popd && \
        rm zig-linux-$ARCH-$ZIG_VER.tar.* && \
    echo "FLUVIO_BUILD_LLD=$LLVM_PATH/bin/lld" | tee -a $GITHUB_ENV
fi

if [[ "$MATRIX_OS" == "macos-11" ]]; then
    echo "installing zig on mac"
 #   brew update
    brew install zig && \
    echo "FLUVIO_BUILD_LLD=/opt/homebrew/opt/llvm@13/bin/lld" | tee -a $GITHUB_ENV
fi