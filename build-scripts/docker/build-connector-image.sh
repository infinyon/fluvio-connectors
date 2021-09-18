#!/usr/bin/env bash
set -ex


# The target platform - Ex: x86_64-unknown-linux-musl
readonly TARGET=${TARGET:?Set TARGET platform to build}
# The tag to build this Docker image with - Ex: 0.7.4-abcdef (where abcdef is a git commit)
readonly COMMIT_HASH${COMMIT_HASH:-}
# The name of the connector - Ex: test-connector
readonly CONNECTOR_NAME="${CONNECTOR_NAME:?Set CONNECTOR_NAME to build}"
# Default image name structure
readonly IMAGE_NAME="${IMAGE_NAME:infinyon/fluvio-connect-$CONNECTOR_NAME}"

function main() {

  if [ "$TARGET" = "aarch64-unknown-linux-musl" ]; then
    BUILD_ARGS="--build-arg CONNECTOR_NAME=${CONNECTOR_NAME} --build-arg ARCH=arm64v8/"
  else
    BUILD_ARGS="--build-arg CONNECTOR_NAME=${CONNECTOR_NAME}"
  fi

  # Tag the image with a commit hash if we provide it
  if [[ -z "$COMMIT_HASH" ]];
  then
    docker build -t "$IMAGE_NAME" $BUILD_ARGS .
  else
    docker build -t "$IMAGE_NAME" -t "$IMAGE_NAME:$COMMIT_HASH-$TARGET" $BUILD_ARGS .
  fi

}

main
