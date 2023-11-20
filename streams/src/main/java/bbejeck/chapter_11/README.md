# ksqlDB

This contains the example `sql` files you can execute to try out `ksqlDB`

To run the examples in this chapter take the following steps:

1. If you have any existing Kafka brokers running in docker, stop them.
2. CD into this directory and run `docker compose up -d`
3. Open a ksqlDB CLI session with `docker exec -it ksqldb_cli ksql http://ksqldb_server:8088`
4. Run any of the example SQL files by copying and pasting individual commands into the CLI
5. When done, clean everything up with `docker compose down -v`