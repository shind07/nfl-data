"""create rushing by player by year

Revision ID: 37011a3a3d14
Revises: c0cea8505ac5
Create Date: 2020-12-17 09:46:10.711684

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '37011a3a3d14'
down_revision = 'c0cea8505ac5'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE rushing_by_player_by_year (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            rusher VARCHAR NOT NULL,
            rush_type VARCHAR NOT NULL,
            games SMALLINT NOT NULL,
            attempts INTEGER NOT NULL,
            yards INTEGER NOT NULL,
            td INTEGER NOT NULL,
            fumbles INTEGER NOT NULL,
            fumbles_lost INTEGER NOT NULL,
            fumbles_out_of_bounds INTEGER NOT NULL,
            epa INTEGER NOT NULL
        );
    """)


def downgrade():
    op.execute("""
        DROP TABLE IF EXISTS rushing_by_player_by_year;
    """)
