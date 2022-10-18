#!/bin/bash
# set up sccache
set -euxo pipefail
MATRIX_OS=${1}
echo "installing zig matrix.os=$MATRIX_OS"

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    echo "installing zig on ubuntu"
    echo $LLVM_PATH
    sudo snap install --beta --classic zig && \
    echo "FLUVIO_BUILD_LLD=$LLVM_PATH/bin/lld" | tee -a $GITHUB_ENV
fi

if [[ "$MATRIX_OS" == "macos-11" ]]; then
    echo "installing zig on mac"
 #   brew update
    brew install zig && \
    echo "FLUVIO_BUILD_LLD=/opt/homebrew/opt/llvm@13/bin/lld" | tee -a $GITHUB_ENV
fi