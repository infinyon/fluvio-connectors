ARG ARCH=
FROM ${ARCH}alpine:3.12
# Update to latest when https://github.com/alpinelinux/docker-alpine/issues/185 is fixed.

# Install tini for signal processing and zombie killing
# Install unzip to unpack fluvio-test
RUN if [ -z "$ARCH" ]; then \
        apk add --arch x86_64 --no-cache tini unzip ;\
    else \
        apk add --arch aarch64 --no-cache tini unzip ;\
    fi

# This env var is used in the entrypoint
ARG CONNECTOR_NAME=test-connector
ENV CONNECTOR_NAME=${CONNECTOR_NAME}

# Copy the connector somewhere the image's $PATH
COPY ${CONNECTOR_NAME} /usr/local/bin/${CONNECTOR_NAME}
RUN chmod +x /usr/local/bin/${CONNECTOR_NAME}

# Add `fluvio-test` from Fluvio's dev releases
ADD https://github.com/infinyon/fluvio/releases/download/dev/fluvio-test-x86_64-unknown-linux-musl.zip /tmp/
RUN unzip /tmp/fluvio-test-x86_64-unknown-linux-musl.zip && chmod +x fluvio-test && mv fluvio-test /usr/local/bin/fluvio-test && rm /tmp/fluvio-test-x86_64-unknown-linux-musl.zip

# Add non-privileged user
ENV USER=fluvio
RUN adduser \
    --disabled-password \
    --home "/home/$USER" \
    "$USER"
USER $USER

ENTRYPOINT [ "sh", "-c", "tini $CONNECTOR_NAME \"$@\"" ]
