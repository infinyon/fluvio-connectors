#!/usr/bin/env bash
set -ex

# The target platform - Ex: x86_64-unknown-linux-musl
readonly TARGET="${TARGET:?Set TARGET platform to build}"
# The tag to build this Docker image with - Ex: 0.7.4-abcdef (where abcdef is a git commit)
readonly DOCKER_TAG="${DOCKER_TAG:-}"
# The name of the connector - Ex: test-connector
readonly CONNECTOR_NAME="${CONNECTOR_NAME:-test-connector}"
# Default image name structure
readonly IMAGE_NAME="infinyon/fluvio-connect-$CONNECTOR_NAME"

# Creates a staging Docker build directory
readonly BUILD_ROOT="$(realpath $( dirname ${BASH_SOURCE[0]})/../../container-build)"
readonly WORK_DIR="$(mktemp -d -p $BUILD_ROOT)"

# Default path to Dockerfile (for `docker build`) 
readonly DOCKERFILE_PATH="$(realpath ${DOCKERFILE_PATH:-$BUILD_ROOT/dockerfiles/default/Dockerfile})"

# check if tmp dir was created
if [[ ! "$WORK_DIR" || ! -d "$WORK_DIR" ]]; then
  echo "Could not create temp dir"
  exit 1
fi

function main() {

  # We need to make sure we always know what dir we are in
  pushd "$BUILD_ROOT"
  ls

  if [ "$TARGET" = "aarch64-unknown-linux-musl" ]; then
    BUILD_ARGS="--platform linux/arm64 --build-arg CONNECTOR_NAME=${CONNECTOR_NAME} --build-arg ARCH=arm64v8/"
  else
    BUILD_ARGS="--platform linux/amd64 --build-arg CONNECTOR_NAME=${CONNECTOR_NAME}"
  fi


  # Tag the image with a commit hash if we provide it
  if [[ -z "$DOCKER_TAG" ]];
  then
    IMAGE_TAGS="-t $IMAGE_NAME"
  else
    IMAGE_TAGS="-t $IMAGE_NAME -t $IMAGE_NAME:$DOCKER_TAG-${TARGET}"
  fi
  DOCKER_IMAGE_TAR=/tmp/infinyon-fluvio-connector-${CONNECTOR_NAME}-${TARGET}.tar

  # Copy Dockerfile and any other files copied to $BUILD_ROOT to $WORK_DIR
  cp $DOCKERFILE_PATH $WORK_DIR 
  find . -maxdepth 1 -type f -exec cp {} $WORK_DIR \;

  # Changes directory then runs appropriate docker build command
  pushd $WORK_DIR
  ls

  # The CI build should producer a tarball
  if [[ -z "$CI" ]];
  then
    # shellcheck disable=SC2086
    docker build $IMAGE_TAGS $BUILD_ARGS . 
    docker save ${IMAGE_NAME} > ${DOCKER_IMAGE_TAR}
    k3d image import -k -c fluvio ${DOCKER_IMAGE_TAR}
  else
    # shellcheck disable=SC2086
    docker buildx build -o type=docker,dest=- $IMAGE_TAGS $BUILD_ARGS $DOCKERFILE_PATH > ${DOCKER_IMAGE_TAR}
  fi
  popd


  ## Cleans up staging directory
  popd
  echo "Cleaning up work dir"
  rm -rf $WORK_DIR

}

main
