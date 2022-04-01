DYNAMO_NAME=fluvio-dynamo

dynamodb-docker:
	docker run -p 8000:8000 --name $(DYNAMO_NAME) -d amazon/dynamodb-local

run-dev:
	cargo run --bin dynamodb-sink -- --fluvio-topic foobar --table-name foobar --column-types S,S --column-names col_1,col_2 --aws-endpoint http://localhost:8000

clean:
	docker stop $(DYNAMO_NAME)
	docker rm -f $(DYNAMO_NAME)
