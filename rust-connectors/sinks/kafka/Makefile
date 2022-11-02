.PHONY: compose-up compose-down

compose-up:
	docker-compose up -d

compose-down:
	docker-compose rm -f -s

kafka-consume:
	docker exec -it kafka-connector-demo-kafka-1 kafka-console-consumer.sh -topic poc1 --bootstrap-server localhost:9092

test: compose-up
	$(MAKE) compose-down

generate-certs: setup
	openssl req -new -newkey rsa:4096 -days 365 -x509 -subj /C=US/ST=CA/L=Sunnyvale/O=kafka-sink/OU=Eng/CN=intermediate.kafka-sink.fluvio.io -keyout test-certs/kafka.keystore.key -out test-certs/kafka.keystore.pem -nodes

