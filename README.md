# Force Directed Spark

## Requirements

- [Google Cloud SDK](https://cloud.google.com/sdk/)
- [Scala SBT](https://www.scala-sbt.org/)

## Run locally

Enter inside the project directory:

```
$ cd force-directed-spark
```

Run `sbt`:

```
$ sbt
```

Run the program giving the arguments, i.e.:

```
> run data/pietronostro.txt out/wordcount-out
```

## Default storage bucket

We have a bucket called `force-directed-bucket`, to reference it in `gcloud` or `gsutil` commands use `gs://force-directed-bucket`.

## How to deploy

### Generate `.jar`

```
$ sbt package
```

### Create Cluster

```
$ gcloud dataproc clusters create cluster-name
```

TODO: Check the available parameters.

### Copy the `.jar` to the bucket

```
$ gsutil cp target/scala-2.11/WordCount.jar gs://force-directed-bucket
```

### Submit Job (example)

```
$ gcloud dataproc jobs submit spark
    --cluster pietroster
    --jar gs://force-directed-bucket/WordCount.jar
    -- gs://force-directed-bucket/pietronostro.txt gs://force-directed-bucket/wordcount-out
```

Note: The parameters after `--` are the jar program's input arguments.

### Check the output (example)

#### List

```
$ gsutil ls gs://force-directed-bucket/wordcount-out/
```

#### Print

```
$ gsutil cat gs://force-directed-bucket/wordcount-out/part-00000
```

### Delete the Cluster

```
$ gcloud dataproc clusters delete cluster-name
```
