# Seq Data File

super simple sequential data format, to put multiple data chunks one after another

## Format

Each new data chunk is preceded by a 4 bytes little endian integer that represent the size of the chunk.

```
┌──────┬──────┬────┬─────┬─┬────┬─────┬─┬───────┐
│magic │header│len1│data1│#│len2│data2│#│.......│
└──────┴──────┴────┴─────┴─┴────┴─────┴─┴───────┘
```
