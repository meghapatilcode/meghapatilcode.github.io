---
layout: post
title:  "Use Auto Loader with Binary Files"
date:   2021-04-15 11:07:24 +0530
categories: jekyll update
---

## Introduction

Auto Loader feature of Databricks is used to detect incremental files as they arrive in Azure Blob storage and Azure Data Lake Storage Gen1 and Gen2. It provides a Structured Streaming source called `cloudFiles`. Given an input directory path on the cloud file storage, the cloudFiles source automatically processes new files as they arrive, with the option of also processing existing files in that directory.

cloudFiles source directly supports the following Apache Spark data sources in Azure Databricks: CSV, JSON, ORC, Parquet, Avro.
In this article we will demonstrate how to use Auto Loader for data sources which are not directly supported such as zip and compressed tar archives (tar.gz).

- **Zip Files**

    Hadoop does not have support for zip files as a compression codec. While a text file in GZip, BZip2 and other supported compression formats can be configured to be automatically decompressed in Apache Spark as long as it has the right file extension, you must perform additional steps to read zip files.

- **Tar Archives**

    Databricks does not directly support tar archives as a data source. Members from the archive must be extracted first.

For raw files in the format of zip/tar.gz files, we can use Auto Loader to stream metadata about the raw files rather than the actual data within the files due to constraints described above for zip/tar.gz file formats. Zip/tar.gz files will be read as whole binary files using Spark's binary file data source `binaryFile`.

> On Databricks Runtime 7.3 LTS and above, if the file format is text or binaryFile you don’t need to provide the schema.

## Implementation

Auto Loader using binary file format can be implemented as follows:

1. Configure Auto Loader `cloudFiles` source.

    ```python
        cloudfileconfig = {
            "cloudFiles.format": "binaryFile",
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.useNotifications": "false",
        }
    ```

    - `cloudFiles.format` - `binaryFile` converts each file into a single record that contains the raw content and metadata of the file. The resulting DataFrame contains the following columns:
        - path: StringType
        - modificationTime: TimestampType
        - length: LongType
        - content: BinaryType
    - `cloudFiles.includeExistingFiles` - Option `true` enables us to process any existing files in the directory before a stream is started.
    - `cloudFiles.useNotifications` - Option `false` uses Auto Loader in directory listing mode. Directory listing mode identifies new files by parallel listing of the input directory. It allows you to quickly start Auto Loader streams without any permission configuration and is suitable for scenarios where only a few files need to be streamed in on a regular basis.

1. Create dataframe representing the stream of incremental files.

    ```python
    df_autoloader = (
        spark.readStream.format("cloudFiles")
        .options(**cloudfileconfig)
        .load(input_path)
    )
    ```

    - `input_path` - Landing path for raw files. Auto Loader monitors this directory (and sub-directories) for new files. Let's call this directory "raw".

1. Optionally, we can drop the field `content` which contains raw content of the files from the dataframe and retain only file metadata fields - `path`, `modificationTime` and `length`. Additionally, a new column `epoch_id` representing current timestamp in unix format can be added.

    ```python
    from datetime import datetime

    epoch_id = int(datetime.now().timestamp())

    df_autoloader = df_autoloader.drop("content").withColumn(
        "epoch_id", lit(epoch_id)
    )
    ```

1. Start the streaming job and write the list of incremental files detected by Auto Loader to Delta Lake.

    ```python
    write_query = (
        df_autoloader.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)
        .start(output_path)
    )
    write_query.awaitTermination()
    ```

    - `output_path` - Output from Auto Loader is written as a Delta table in this path.
    - `checkpoint_path` - Checkpoint maintains information about what files were processed and what files are new. This folder is created and updated automatically when Auto Loader process runs.
    - `trigger(once=True)` - This option is used to stop the streaming job once Auto Loader output is written into Delta Lake.
    - `awaitTermination()` - This prevents execution of subsequent cells in Databricks notebook when the Auto Loader streaming query is still active.

1. Query the Delta table to get list of files to process for current run. `epoch_id` is used to filter out current run files in this step.

    ```python
    df_autoloader_output = spark.read.format("delta").load(output_path).filter(epoch_id == epoch_id)
    ```

1. Extract members from tar archives in "raw" directory into an "extract" directory.

```python
incremental_files = df_autoloader_output.select("path").rdd.flatMap(lambda x: x).collect()

local_raw_file_path = [file.replace("dbfs:", "/dbfs") for file in incremental_files]
local_extract_file_path = extract_path.replace("dbfs:", "/dbfs")

for file in local_raw_file_path:
    with tarfile.open(file, "r:gz") as tar:
        tar.extractall(path=local_extract_file_path)
```

> Since the extract from tar process (or unzip) runs only on the driver node and not in a distributed manner, the driver node may need to be resized accordingly.

1. Read and process the files extracted from tar archive in the "extract" directory.

```python
df = spark.read.format("csv").load(extract_path)
```

## Conclusion

In this article we have demonstrated how to use Auto Loader for data sources which are not directly supported such as compressed tar archives (tar.gz). As opposed to directly supported data sources such as CSV, JSON, ORC, Parquet, Avro etc, in the case of zip or tar archives, Auto Loader can be used to detect path and names of new and unprocessed incremental files. With this knowledge, we are able to extract data files from the detected files and further read and process them as regular data files.
