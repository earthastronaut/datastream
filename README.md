# Data Stream


A simple publish-subscribe messaging system using postgres as a backend. 

The main motivation of this project was to create something similar to kafka
but with long-term storage in postgres and overall simpler without dealing with
distributed architecture for the message storage.

(Also was just a great way to really get the fundamentals of how this type of publish-subscription system works.)


# Quick Start


Can use docker and docker-compose to set up postgres backend and a container with python environment. 

Start the postgres backend.

```
docker-compose up database
```

Start a consumer.

```
docker-compose run datastream python scripts/consumer.py mytopic
```

Produce some messages.

```
docker-compose run --rm datastream python scripts/producer.py \
	mytopic --producer-name producer_turtle \
	--key 'testing' --data '{"hello": "world"}' --metadata '{"meta": "data"}' \
	--repeat 10
```

Watch the consumer process the messages. 

Then try spinning up a few consumers and send messages.
