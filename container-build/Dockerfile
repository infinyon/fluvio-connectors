FROM alpine 

# Install tini for signal processing and zombie killing
RUN apk add --no-cache tini

# This env var is used in the entrypoint
ARG CONNECTOR_NAME=test-connector
ENV CONNECTOR_NAME=${CONNECTOR_NAME}

# Copy the connector somewhere the image's $PATH
COPY ${CONNECTOR_NAME} /usr/local/bin/${CONNECTOR_NAME}
RUN chmod +x /usr/local/bin/${CONNECTOR_NAME}

# The startup script uses this file to get the connector binary name
RUN echo ${CONNECTOR_NAME} | tee /CONNECTOR_NAME

ENTRYPOINT [ "sh", "-c", "tini $CONNECTOR_NAME" ] 