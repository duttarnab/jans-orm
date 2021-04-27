package io.jans.orm.cloud.spanner.persistence;

import java.io.FileInputStream;
import java.util.Arrays;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
//Imports the Google Cloud client library
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
;

/**
 * A quick start code for Cloud Spanner. It demonstrates how to setup the Cloud
 * Spanner client and execute a simple query using it against an existing
 * database.
 */
public class QuickstartSample {
	public static void main(String... args) throws Exception {
		// Instantiates a client
		Builder options = SpannerOptions.newBuilder();
		String connectionCredentialsFile = "V:\\Documents\\spanner\\full_access_spanner_sa.json";
    	options.setCredentials(GoogleCredentials.fromStream(new FileInputStream(connectionCredentialsFile)));

    	Spanner spanner = options.build().getService();

		// Name of your instance & database.
		String instanceId = "movchan";
		String databaseId = "test_array";
		try {
			DatabaseId database = DatabaseId.of("gluu-server-184620", instanceId, databaseId);
/*
			DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
			OperationFuture<Database, CreateDatabaseMetadata> op = dbAdminClient.createDatabase(dbAdminClient.newDatabaseBuilder(database).build(), Arrays.asList(
"CREATE TABLE temp (SingerId   INT64 NOT NULL) PRIMARY KEY (SingerId)"));
			Database db = op.get();
			System.out.println(db);
*/
			// Creates a database client
			DatabaseClient dbClient = spanner
					.getDatabaseClient(database);
			// Queries the database
			ResultSet resultSet = dbClient.singleUse().executeQuery(Statement.of("SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM information_schema.columns WHERE table_catalog = '' and table_schema = ''"));

			System.out.println("\n\nResults:");
			// Prints the results
			while (resultSet.next()) {
				System.out.printf("%d\n\n", resultSet.getString("IS_NULLABLE"));
			}
		} finally {
			// Closes the client which will free up the resources used
			spanner.close();
		}
	}
}