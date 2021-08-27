FROM alpine 

ARG CONNECTOR_NAME=test-connector

COPY ./target/x86_64-unknown-linux-musl/release/${CONNECTOR_NAME} /${CONNECTOR_NAME}
RUN chmod +x /${CONNECTOR_NAME}
RUN echo ${CONNECTOR_NAME} | tee /CONNECTOR_NAME

COPY ./start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT [ "/start.sh" ] 