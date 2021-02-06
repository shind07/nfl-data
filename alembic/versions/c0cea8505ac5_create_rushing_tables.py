"""create rushing tables

Revision ID: c0cea8505ac5
Revises: c2019f5f4de1
Create Date: 2020-12-15 05:52:47.386708

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c0cea8505ac5'
down_revision = 'c2019f5f4de1'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE rushing_by_player_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            rusher VARCHAR NOT NULL,
            rush_type VARCHAR NOT NULL,
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
        DROP TABLE IF EXISTS rushing_by_player_by_game;
    """)
