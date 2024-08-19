# Kafka Client Desktop Application
This project is in very early stage of development. The idea is to build a Kafka GUI client that is keyboard friendly, supporting the following features allowing to connect to any kafka cluster. 

- Admin
    - Topic
        - Create/Delete
        - Alter Configs
        - Increase Partitions
    - Consumer Groups
        - Create (offsets by start, end and timestamp).
        - Delete.
        - Reset Offsets (timestamp, latest, earliest, shift by offset)
- Consumers
    - Consume messages between timestamps, from earliest, and from latest.
    - Deserialisation support JSON/String (future: avro, protobuf).
    - Filtering messages by Key, JSON payload properties.
    - Export to file csv, json.
- Producers
    - Support addition of headers
    - Serialisation support JSON/String (future: avro, protobuf)
    - Produce on specific partition
    - History of produced messages, resend, edit and resend.
    - Produce from a file csv, json.

## Build & Run
This is built using Tauri Framework. Follow the [Tauri's Develop Guide](https://v2.tauri.app/develop/#developing-your-desktop-application) to build and run this project. Additionally, this project requires [librdkafka c/c++ library](https://github.com/confluentinc/librdkafka) installed on your system.

## Contibute
If you'd like to contribute to this project please create an issue.

## Todo
- [] Add User input Bootstrap servers (Cluster configs).
- [] Add Multiple Configs Selection while creating topics.
- [] Use pinia store to keep consuming messages in the store.
- [] Support Multiple Consumers 
- [] Add Prouducers feature
- [] Support Multiple Serialization

