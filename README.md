# NFL Data System

## Workflow
#### Research and Development
R&D work is done in jupyter notebooks:
```bash
make notebook
```
Notebooks are used to perform EDA, check for bugs/gotchas, and develop and test out queries. The goal is to create a pandas dataframe that mirrors the desired output table.

#### Creating New Tables and Jobs
1. Once the output table has been built with pandas, write the table to a csv. Then, get the corresponding `CREATE TABLE` statement by running:
```bash
csvsql -i postgresql path/to/file.csv
```
2. Create the alembic revision for the new table:
```bash
alembic revision -m "create table_name table"
```
3. Run the migration to create the table in the database:
```bash
make migrate
```

4. Create the job script in `app/jobs` and add it to the pipeline in `app/__main__.py`. 


## Database Management

The database is managed with [alembic](https://alembic.sqlalchemy.org/en/latest/tutorial.html). Alembic commands must be run within the Docker container.

#### Creating Schemas from CSVs
```bash
csvsql -i postgresql data/play_by_play/play_by_play
csvsql -i postgresql path/to/csv
```

#### Creating a Revision
```bash
make shell
root@a9d76ffd19da:/opt/nfl# alembic revision -m "create play by play table"
```
This creates a new revision in the `alembic/versions` folder. Update the `upgrade()` and `downgrade()` functions accordingly. Generally, the `upgrade()` contains the `CREATE TABLE` statement, and the `downgrade()` is the corresponding `DROP TABLE` statement.

#### Running a Migration
```bash
make shell
root@a9d76ffd19da:/opt/nfl# alembic upgrade head
```

#### Reverting a Migration
`alembic downgrade base` - Downgrade back to nothing.

`alembic downgrade -1` - Downgrade to the previous version.

#### Useful commands
```bash
alembic current
alembic history --verbose
```