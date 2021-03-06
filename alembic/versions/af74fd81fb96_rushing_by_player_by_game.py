"""rushing_by_player_by_game

Revision ID: af74fd81fb96
Revises: 2cf1471dab07
Create Date: 2021-02-17 21:13:02.474905

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'af74fd81fb96'
down_revision = '2cf1471dab07'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DROP TABLE IF EXISTS rushing_by_player_by_game;
        CREATE TABLE rushing_by_player_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            player VARCHAR NOT NULL,
            attempts_total SMALLINT NOT NULL,
            yards_total SMALLINT NOT NULL,
            td_total SMALLINT NOT NULL,
            fumbles_total SMALLINT NOT NULL,
            fumbles_lost_total SMALLINT NOT NULL,
            fumbles_out_of_bounds_total SMALLINT NOT NULL,
            epa_total DECIMAL NOT NULL,
            attempts_designed SMALLINT NOT NULL,
            yards_designed SMALLINT NOT NULL,
            td_designed SMALLINT NOT NULL,
            fumbles_designed SMALLINT NOT NULL,
            fumbles_lost_designed SMALLINT NOT NULL,
            fumbles_out_of_bounds_designed SMALLINT NOT NULL,
            epa_designed DECIMAL NOT NULL,
            attempts_scramble SMALLINT NOT NULL,
            yards_scramble SMALLINT NOT NULL,
            td_scramble SMALLINT NOT NULL,
            fumbles_scramble SMALLINT NOT NULL,
            fumbles_lost_scramble SMALLINT NOT NULL,
            fumbles_out_of_bounds_scramble SMALLINT NOT NULL,
            epa_scramble DECIMAL NOT NULL,
            attempts_kneel SMALLINT NOT NULL,
            yards_kneel SMALLINT NOT NULL,
            td_kneel SMALLINT NOT NULL,
            fumbles_kneel SMALLINT NOT NULL,
            fumbles_lost_kneel SMALLINT NOT NULL,
            fumbles_out_of_bounds_kneel SMALLINT NOT NULL,
            epa_kneel DECIMAL NOT NULL,
            attempts SMALLINT NOT NULL,
            yards SMALLINT NOT NULL,
            td SMALLINT NOT NULL,
            fumbles SMALLINT NOT NULL,
            fumbles_lost SMALLINT NOT NULL,
            fumbles_out_of_bounds SMALLINT NOT NULL,
            epa DECIMAL NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS rushing_by_player_by_game""")