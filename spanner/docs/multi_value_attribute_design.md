Multi valued attributes storing mechanisms
=======================

Cloud Spanner DB supports ARRAY attributes but in same time it's not allows to index them. This led to full table scan when query has filter with these attributes. ALternative of this is to use interleave child tables support. These tables can increase queries performance but in parallel with this this approach requires additional storage space for child table and index. As result Spanner ORM should mix of both approaches.

ARRAY attribute and interleave child table comparision
-----------------------

DB Structure which uses ARRAY

```
CREATE TABLE jansClnt_Array (
  doc_id STRING(64) NOT NULL,
  objectClass STRING(48),
  dn STRING(128),
  jansRedirectURI ARRAY<STRING(MAX)>,
) PRIMARY KEY(doc_id)

```

DB Structure which child interleave table

```
CREATE TABLE jansClnt_Array (
  doc_id STRING(64) NOT NULL,
  objectClass STRING(48),
  dn STRING(128),
  jansRedirectURI ARRAY<STRING(MAX)>,
) PRIMARY KEY(doc_id)

```
