# Batch Processing

## Getting Started

```sh
docker run --detach -p 8888:8888 -p 4040:4040 -p 4041:4041 -v "${PWD}":/home/jovyan/work quay.io/jupyter/pyspark-notebook
docker exec -it <container-id> bash

(base) jovyan@xxxxxx:~$ jupyter server list
```

## Actions vs Transformations

- *Transformations* are lazy (not executed immediately)
  - select columns
  - filtering

- *Actions* are eager
  - Show, take, head
  - Write
