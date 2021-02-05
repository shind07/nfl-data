"""create receiving by player by year

Revision ID: d670a0e93945
Revises: b92b80aa7fa3
Create Date: 2021-01-26 08:50:19.380589

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd670a0e93945'
down_revision = 'b92b80aa7fa3'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE receiving_by_player_by_year (
            year DECIMAL NOT NULL,
            season_type VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            gsis_id VARCHAR NOT NULL,
            receiver VARCHAR,
            position VARCHAR,
            games SMALLINT NOT NULL,
            receptions SMALLINT NOT NULL,
            targets SMALLINT NOT NULL,
            yards SMALLINT NOT NULL,
            air_yards_intended SMALLINT NOT NULL,
            air_yards_completed SMALLINT NOT NULL,
            td SMALLINT NOT NULL,
            int SMALLINT NOT NULL,
            fumbles SMALLINT NOT NULL,
            epa DECIMAL NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS receiving_by_player_by_year""")
