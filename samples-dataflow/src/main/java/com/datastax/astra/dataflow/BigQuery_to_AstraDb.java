package com.datastax.astra.dataflow;

import com.datastax.astra.dataflow.utils.GoogleSecretManagerUtils;
import com.google.api.services.bigquery.Bigquery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.astra.db.AstraDbConnectionManager;
import org.apache.beam.sdk.io.astra.db.options.AstraDbWriteOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Big Query to Astra
 *

 mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.dtx.astra.pipelines.dataflow.LoadBigQueryTableInAstraDataflow \
 -Dexec.args="\
 --project=integrations-379317 \
 --bigQueryDataset=dataflow_input_us \
 --bigQueryTable=languages \
 --region=us-central1 \
 --astraToken=projects/747469159044/secrets/astra-token/versions/2 \
 --secureConnectBundle=projects/747469159044/secrets/cedrick-demo-scb/versions/2 \
 --keyspace=gcp_integrations \
 --runner=DataflowRunner"

 */
public class BigQuery_to_AstraDb {

    /**
     * Logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Gcs_To_AstraDb.class);

    /**
     * BigQuery to Astra
     */
    public interface BigQueryToAstraDbOptions extends GcpOptions, AstraDbWriteOptions {

        @Description("BigQuery dataset name")
        @Default.String("dataflow_input_tiny")
        String getBigQueryDataset();
        void setBigQueryDataset(String dataset);

        @Description("BigQuery table name")
        @Default.String("table_language_codes")
        String getBigQueryTable();
        void setBigQueryTable(String table);

    }

    /**
     * Big Query Client
     */
    private Bigquery bigQueryClient = null;

    /**
     * Main.
     */
    public static void main(String[] args) {

        BigQueryToAstraDbOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(BigQueryToAstraDbOptions.class);

        try {

            // Read Input From Google Secrets
            String astraToken = GoogleSecretManagerUtils.
                    readTokenSecret(options.getAstraToken());
            byte[] astraSecureBundle = GoogleSecretManagerUtils.
                    readSecureBundleSecret(options.getAstraSecureConnectBundle());

            Pipeline bq2AstraPipeline = Pipeline.create(options);
            LOGGER.info("+ Pipeline Created");

        bq2AstraPipeline
                        // 1. READ From BigQuery
                        .apply("Read from BigQuery query", BigQueryIO
                           .readTableRows()
                           .from( new TableReference()
                                   .setProjectId(bq2AstraOptions.getProject())
                                   .setDatasetId(bq2AstraOptions.getBigQueryDataset())
                                   .setTableId(bq2AstraOptions.getBigQueryTable())))

                        // 2. Marshal the beans
                        .apply(MapElements
                                .into(TypeDescriptor.of(LanguageCodeEntity.class))
                                .via(LanguageCodeEntity::fromTableRow))

                        // 3. Create Table if needed with a CQL Statement
                        .apply("Create Table " + LanguageCodeEntity.TABLE_NAME, new AstraCqlQueryPTransform<LanguageCodeEntity>(
                                astraToken, astraSecureBundle, bq2AstraOptions.getKeyspace(),
                                LanguageCodeEntity.createTableStatement().toString()))

                        // 4. Write into Astra
                        .apply("Write into Astra", AstraIO.<LanguageCodeEntity>write()
                           .withToken(astraToken)
                           .withKeyspace(bq2AstraOptions.getKeyspace())
                           .withSecureConnectBundleData(astraSecureBundle)
                           .withEntity(LanguageCodeEntity.class));

        bq2AstraPipeline.run().waitUntilFinish();
        } finally {
            AstraDbConnectionManager.cleanup();
        }
    }


}
