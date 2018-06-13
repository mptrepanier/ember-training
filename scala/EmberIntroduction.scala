/*
# An Introduction to Viewing Processed Files

### The EmberSession

After a non-genomic DataSource is ingested through Ember, it is written to the specified output path
in the parquet file format. We can read these files into the Spark environment using our `EmberSession` API.

The `EmberSession` is our way of communicating with the Ember client. We'll instantiate one now.
*/
import com.metistream.ember.embersession.EmberSession

val es = (EmberSession
          .builder
          .host("http://ruduv-kmedge001")
          .port("9876")
          .user("training@metistream.com")
          .password("Password1@")
          .getOrCreate(spark))

/*
### EmberDatasets 
 
An `EmberDataset` is Ember's abstraction of either a Spark `Dataset` (non-genomic) or a Hail `VariantDataset`.
In fact, these two data types have their own unique EmberDataset APIs, an `EmberSparkDataset` and and `EmberHailDataset`.
These APIs provide an entry-point for interacting with your data. As well, you can always extract the underlying Spark `Dataset`
or Hail `VariantDataset` using the `emberDataset.ds` `emberDataset.vds` methods.

Let's read in one of our DataSources as an `EmberSparkDataset` and take a look at its schema.
*/

val datasourceName = "ENTER_DATASOURCE_NAME"
val esd = es.readEmberSparkDataset(datasourceName)
esd.printSchema

/*
### Building Models
*/
val continuousFeatures = Array(
      "is_male",
      "on_iv",
      "bu-nal",
      "obese",
      "smoker",
      "anticoagulants",
      "narcotics",
      "narc-ans",
      "antipsychotics",
      "chemo",
      "DBP_mean",
      "DBP_recent",
      "SBP_mean",
      "SBP_recent",
      "MAP_mean",
      "MAP_recent",
      "temp_mean",
      "temp_recent",
      "SPO2_mean",
      "SPO2_recent",
      "RR_mean",
      "RR_recent",
      "pulse_mean",
      "pulse_recent"
    )

import com.metistream.ember.embersession.ml.services.EmberMLSuite
val mls = new EmberMLSuite()
val (model, schema) = mls.trainClassifier(esd,
                               splitRatio = 0.65,
                                convergenceTolerance = 0.2,
                               continuousFeatures = continuousFeatures,
                              labelColumn = "is_rrt")

es.saveModel(model, schema, datasourceName, "demo")