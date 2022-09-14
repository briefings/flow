<br>

An experiment.

<br>

### Snippets

```scala
val year: String = FilenameUtils.getBaseName(Paths.get(dataString).getParent.toString)
val month: String = FilenameUtils.getBaseName(dataString)

val T: Try[Unit] = Exception.allCatch.withTry(
  baseline.write.format("csv")
    .option("header", value = true)
    .option("encoding", value = "UTF-8")
    .option("timestampFormat", value = dataConfiguration.sourceTimeStamp)
    .option("dateFormat", value = "yyy-MM-dd")
    .save(path = Paths.get(localSettings.warehouseDirectory, year, month).toString)
)
if (T.isFailure) {
  sys.error(T.failed.get.getMessage)
}
```

<br>

```scala
T.reduce(_ union _).write.format("csv")
  .option("header", value = true)
  .option("encoding", value = "UTF-8")
  .option("timestampFormat", value = dataConfiguration.sourceTimeStamp)
  .option("dateFormat", value = "yyy-MM-dd")
  .save(path = Paths.get(localSettings.warehouseDirectory, "data").toString)
```


<br>
<br>

<br>
<br>

<br>
<br>

<br>
<br>