# digdag-plugin-livy

## Description
digdag-plugin-livy is a plugin for submitting [Apache Sark](http://spark.apache.org/) jobs to 
the [Apache Livy](https://livy.incubator.apache.org/).

## Requirements

- [Digdag](https://www.digdag.io/)
- [Apache Livy](https://livy.incubator.apache.org/)

## Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - com.github.platform-lunar:digdag-plugin-livy:0.1.1
  # Set livy host
  livy:
    host: http://livy.cluster.internal
    port: 8998

+livy_step:
  livy>: Spark application
  name: Spark application name
  class_name: com.example.some.ClassName
  driver_memory: 1024mb
  conf:
    spark.yarn.appMasterEnv.PARAM: example
```

Submission example:

```
digdag run --project sample plugin.dig -p spark_file=s3://<bucket>/<file>.jar -p spark_class=some.class.Class -p livy_host=<livy_host> -p repos=`pwd`/build/repo --rerun
```

Testing changes locally:

```
rm -fr build/repo && ./gradlew clean publishMavenJavaPublicationToMavenRepository
rm -fr .digdag/ && digdag run --project sample plugin.dig -p spark_file=s3://<bucket>/<file>.jar -p spark_class=some.class.Class -p livy_host=<livy_host> -p repos=`pwd`/build/repo --rerun
```

## License

[Apache License 2.0](LICENSE)
