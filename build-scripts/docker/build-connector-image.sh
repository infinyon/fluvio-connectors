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
readonly BUILD_ROOT="$(realpath "$(dirname "${BASH_SOURCE[0]}")"/../../container-build)"
# https://stackoverflow.com/a/31397073
readonly WORK_DIR="$(mktemp -d "${BUILD_ROOT:-/tmp/}/tmp.XXXXXXXXX")"

# Default path to Dockerfile (for `docker build`)
readonly DOCKERFILE_PATH="$(realpath "${DOCKERFILE_PATH:-$BUILD_ROOT/dockerfiles/default/Dockerfile}")"

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
    IMAGE_TAGS="-t $IMAGE_NAME:latest"
  else
    IMAGE_TAGS="-t $IMAGE_NAME:latest -t $IMAGE_NAME:$DOCKER_TAG-${TARGET}"
  fi
  DOCKER_IMAGE_TAR=/tmp/infinyon-fluvio-connector-${CONNECTOR_NAME}-${TARGET}.tar

  # Copy Dockerfile and any other files copied to $BUILD_ROOT to $WORK_DIR
  cp "$DOCKERFILE_PATH" "$WORK_DIR"
  find . -maxdepth 1 -type f -exec cp {} "$WORK_DIR" \;

  # Changes directory then runs appropriate docker build command
  pushd "$WORK_DIR"
  ls

  # The CI build should producer a tarball
  if [[ -z "$CI" ]];
  then
    # shellcheck disable=SC2086
    docker build $IMAGE_TAGS $BUILD_ARGS .

    nodes=`kubectl get nodes -o=jsonpath='{.items[0].metadata.name}'`
    K8=$(if echo ${nodes} | grep -q minikube;  then
        echo 'minikube'
    elif echo ${nodes} | grep -q k3d; then
        echo "k3d"
    elif echo ${nodes} | grep -q control-plane; then
        echo "kind"
    elif echo ${nodes} | grep -q microk8s; then
        echo "microk8"
    elif echo ${nodes} | grep -q lima; then
        echo "lima"
    else
        echo "unknown"
    fi)
    if [ "$K8" = "lima" ]; then
      echo "no need to export image for lima"
    fi

    if [ "$K8" = "k3d" ]; then
      docker save "${IMAGE_NAME}" > "${DOCKER_IMAGE_TAR}"
      k3d image import -k -c fluvio "${DOCKER_IMAGE_TAR}"
      docker exec k3d-fluvio-server-0 sh -c "ctr image list -q"
    fi

    if [ "$K8" = "kind" ]; then
      echo "export image to kind cluster"
      docker image save "${IMAGE_NAME}" --output "${DOCKER_IMAGE_TAR}"
	  #docker image save "$docker_repo:$commit_hash" --output /tmp/infinyon-fluvio.tar
      kind load image-archive "${DOCKER_IMAGE_TAR}"
	  docker exec -it kind-control-plane crictl images
    fi
  else
    # shellcheck disable=SC2086
    docker buildx build -o type=docker,dest=- $IMAGE_TAGS $BUILD_ARGS . > ${DOCKER_IMAGE_TAR}
  fi
  popd


  ## Cleans up staging directory
  # TODO: convert this cleanup step to trap on EXIT. Opt-out when SKIP_CLEAN=true
  popd
  echo "Cleaning up work dir"
  rm -rf "$WORK_DIR"

}

main
