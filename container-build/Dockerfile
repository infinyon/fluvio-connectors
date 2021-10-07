ARG ARCH=
FROM ${ARCH}alpine:3.12
# Update to latest when https://github.com/alpinelinux/docker-alpine/issues/185 is fixed.

# Install tini for signal processing and zombie killing
RUN if [ -z "$ARCH" ]; then \
        apk add --arch x86_64 --no-cache tini ;\
    else \
        apk add --arch aarch64 --no-cache tini ;\
    fi

# This env var is used in the entrypoint
ARG CONNECTOR_NAME=test-connector
ENV CONNECTOR_NAME=${CONNECTOR_NAME}

# Copy the connector somewhere the image's $PATH
COPY ${CONNECTOR_NAME} /usr/local/bin/${CONNECTOR_NAME}
RUN chmod +x /usr/local/bin/${CONNECTOR_NAME}

# Add non-privileged user
ENV USER=fluvio
RUN adduser \
    --disabled-password \
    --home "/home/$USER" \
    "$USER"
USER $USER

ENTRYPOINT [ "sh", "-c", "tini $CONNECTOR_NAME \"$@\"" ]
