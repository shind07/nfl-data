"""create passing by player by year

Revision ID: ba3fad0c46be
Revises: 9e50ee302025
Create Date: 2021-01-22 11:32:10.757586

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ba3fad0c46be'
down_revision = '9e50ee302025'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(""" 
        CREATE TABLE passing_by_player_by_year (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            passer VARCHAR NOT NULL,
            games SMALLINT NOT NULL,
            completions SMALLINT NOT NULL,
            attempts SMALLINT NOT NULL,
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
    op.execute("""
        DROP TABLE IF EXISTS passing_by_player_by_year;
    """)
