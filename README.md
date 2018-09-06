# Force Directed Spark

## Requirements

- [Google Cloud SDK](https://cloud.google.com/sdk/)
- [Scala SBT](https://www.scala-sbt.org/)

## VSCode Setup

- Make sure you have Java 8 installed and `JDK_HOME` or `JAVA_HOME` env variable correctly set
- Add this line to `~/.sbt/1.0/plugins/plugins.sbt` (create it if necessary):

```
addSbtPlugin("org.ensime" % "sbt-ensime" % "2.5.1")
```

- Enter inside the project directory and run `sbt`

```
$ sbt
```

- Run `ensimeConfig`:

```
> ensimeConfig
```

- Install [this](https://marketplace.visualstudio.com/items?itemName=dragos.scala-lsp) extension
- Say goodbye to IntelliJ Idea and uninstall it

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
> run algorithm inFile [outFile, isCloud, nCPUs]

    - algorithm | SPRING-M, SPRING-S, FR-M, FR-S, FA2-M
    - inFile | input file name, picked from the "data" folder
    - outFile | optional output file name, saved in the "out" folder
    - isCloud | optional whether or not is executing on GCloud
    - nCPUs | optional CPUs number to use
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
$ gcloud dataproc clusters create cluster-name \
    --master-machine-type n1-standard-4 \
    --worker-machine-type n1-standard-4 \
    --num-workers 4 \
    --zone europe-west3-a
```
Note: `n1-standard-4` machines have 4 vCPU and 15GB of memory

### Upload the `.jar` to the bucket

```
$ gsutil cp target/scala-2.11/force-directed-spark_2.11-0.1.jar gs://force-directed-bucket/force-directed-spark.jar
```
### Upload the input data to the bucket

```
$ gsutil cp data/simple-graph.net gs://force-directed-bucket/data/simple-graph.net
```

### Submit Job (example)

```
$ gcloud dataproc jobs submit spark --cluster cluster-name \
    --jar gs://force-directed-bucket/force-directed-spark.jar \
    -- FR-S infile.net outfile.net cloud
```

Note: The parameters after `--` are the jar program's input arguments.

### Check the output (example)

#### List

```
$ gsutil ls gs://force-directed-bucket/out/
```

#### Print

```
$ gsutil cat gs://force-directed-bucket/wordcount-out/part-00000
```

### Delete the Cluster

```
$ gcloud dataproc clusters delete cluster-name
```
