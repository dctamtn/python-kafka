"""Shared settings for the learning samples."""

# Address your Python clients use to reach the broker (matches docker-compose port mapping).
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Sample 1 & 2: simple hello-world stream
TOPIC_HELLO = "hello-kafka"

# Sample 3: orders demo for consumer-group load balancing
TOPIC_ORDERS = "orders-demo"
