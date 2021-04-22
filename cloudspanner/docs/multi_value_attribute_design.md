### Multi valued attributes storing mechanisms

Cloud Spanner DB supports ARRAY attributes but in same time it's not allows to index them. This led to full table scan when query has filter with these attributes. ALternative of this is to use interleave child tables support. These tables can increase queries performance but in parallel with this this approach requires additional storage space for child table and index. As result Spanner ORM should mix of both approaches.

## ARRAY attribute and interleave child table comparision

# DB structures and sample data

1. DB Structure which uses ARRAY

```
CREATE TABLE jansClnt_Array (
  doc_id STRING(64) NOT NULL,
  objectClass STRING(48),
  dn STRING(128),
  jansRedirectURI ARRAY<STRING(MAX)>,
) PRIMARY KEY(doc_id)

```

![](./img/array_data.png) <!-- .element height="50%" width="50%" -->

2. DB Structure which child interleave table

```
CREATE TABLE jansClnt_Array (
  doc_id STRING(64) NOT NULL,
  objectClass STRING(48),
  dn STRING(128),
  jansRedirectURI ARRAY<STRING(MAX)>,
) PRIMARY KEY(doc_id)

```

![](./img/interleave_data.png) <!-- .element height="50%" width="50%" -->

# Java code which inserts 1M records into both DB structures
```
package io.jans.orm.cloud.spanner.operation.impl.test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;

import io.jans.orm.cloud.spanner.operation.SpannerOperationService;
import io.jans.orm.cloud.spanner.operation.impl.SpannerConnectionProvider;

public class SpannerMultiValuedDataPopulatorTest {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("connection.project", "projectId");
		props.setProperty("connection.instance", "insanceId");
		props.setProperty("connection.database", "db_name");
		props.setProperty("connection.client.create-max-wait-time-millis", "15");
		props.setProperty("connection.credentials-file", "path_to_creds_file");

		SpannerConnectionProvider connectionProvider = new SpannerConnectionProvider(props);
		connectionProvider.create();
		DatabaseClient client = connectionProvider.getClient();

		List<Mutation> mutations = new LinkedList<>();
		for (int i = 1; i <= 1000000; i++) {
			if (i % 50000 == 0) {
				System.out.println("Added: " + i);
			}

			String redirectURIs[] = new String[4];
			for (int j = 0; j < redirectURIs.length; j++) {
				redirectURIs[j] = String.valueOf(Math.round(Math.random() * 10));
			}

			if (i % 10000 == 0) {
				redirectURIs[3] = String.valueOf(Math.round(Math.random() * 2));
			}

			// Change to false to insert data into parent-child tables
			if (true) {
				WriteBuilder insertMutation = Mutation.newInsertOrUpdateBuilder("jansClnt_Array")
						.set(SpannerOperationService.DOC_ID).to(String.valueOf(i))
						.set(SpannerOperationService.OBJECT_CLASS).to("jansClnt").set("jansRedirectURI")
						.toStringArray(Arrays.asList(redirectURIs));

				mutations.add(insertMutation.build());
			} else {

				WriteBuilder insertMutation = Mutation.newInsertOrUpdateBuilder("jansClnt_Interleave")
						.set(SpannerOperationService.DOC_ID).to(String.valueOf(i))
						.set(SpannerOperationService.OBJECT_CLASS).to("jansClnt");

				mutations.add(insertMutation.build());

				for (int j = 0; j < redirectURIs.length; j++) {
					WriteBuilder insertDictMutation = Mutation
							.newInsertOrUpdateBuilder("jansClnt_Interleave_jansRedirectURI")
							.set(SpannerOperationService.DOC_ID).to(String.valueOf(i))
							.set(SpannerOperationService.DICT_DOC_ID).to(String.valueOf(j)).set("jansRedirectURI")
							.to(redirectURIs[j]);

					mutations.add(insertDictMutation.build());
				}
				client.write(mutations);
			}
			mutations.clear();
		}
		connectionProvider.destroy();
	}

}

```
