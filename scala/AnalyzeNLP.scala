/*
# Peeking Under the Covers

### The EmberSession

After a non-genomic DataSource is ingested through Ember, it is written to the specified output path
in the parquet file format. We can read these files into the Spark environment using our `EmberSession` API.

Since we're communicating with Ember, we'll need to instantiate an EmberSession again.
*/

import com.metistream.ember.embersession.EmberSession

val es = (EmberSession
          .builder
          .host("http://ruduv-kmedge001")
          .port("9876")
          .user("training@metistream.com")
          .password("Password1@")
          .getOrCreate(spark))

val datasourceName = "DATASOURCE_NAME"
val esd = es.readEmberSparkDataset(datasourceName)
val df = esd.ds.toDF()

df.printSchema
df.filter($"ROW_ID" > 5).show