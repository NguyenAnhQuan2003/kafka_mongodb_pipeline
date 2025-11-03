#!/bin/bash

echo "Tạo topic..."

# Tạo topic partitions
for i in 1 2 3 4; do
  echo "Tạo part-$i..."
  docker exec kafka-0 bash -c "\
    kafka-topics --create --if-not-exists \
      --topic part-$i --partitions $i --replication-factor 1 \
      --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
      --command-config <(printf '%s\n' \
        'security.protocol=SASL_PLAINTEXT' \
        'sasl.mechanism=PLAIN' \
        'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"17022003\";' \
      ) \
  "
done

# Tạo topic replication
for rf in 1 2 3; do
  echo "Tạo repl-$rf..."
  docker exec kafka-0 bash -c "\
    kafka-topics --create --if-not-exists \
      --topic repl-$rf --partitions 3 --replication-factor $rf \
      --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
      --command-config <(printf '%s\n' \
        'security.protocol=SASL_PLAINTEXT' \
        'sasl.mechanism=PLAIN' \
        'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"17022003\";' \
      ) \
  "
done

echo "HOÀN TẤT! Mở AKHQ: http://localhost:8180"
echo "Login: admin / 17022003"