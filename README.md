# nfl-data


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
`alembic downgrade base` - Downgrade back to nothing: 

`alembic downgrade -1` - Downgrade to the previous version: 

#### Useful commands
```bash
alembic current
alembic history --verbose
```