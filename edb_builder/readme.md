# External Database Builder

This package contains all the tools to construct the universal metabolite database.

### Tables:
- `edb_tmp` External DB downloaders & inserters
- `edb` Indexed and filtered version of `edb_tmp`. This table serves as the database for the discovery algorithm.
- `mdb` Universal Metabolite DB. The end result of the discovery algorithm. Also available as a jsonlines file.
- `mdb_ddb` DynamoDB-style NoSQL table (still in postgres) that contains the same data as `mdb` and 
is used for debugging the AWS-based gateway and frontend app using Tomcru.

## Constructing:

