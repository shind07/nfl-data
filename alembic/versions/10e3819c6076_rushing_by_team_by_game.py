"""rushing_by_team_by_game

Revision ID: 10e3819c6076
Revises: 885cf8bdabae
Create Date: 2021-02-18 14:26:52.352864

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '10e3819c6076'
down_revision = '885cf8bdabae'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DROP TABLE IF EXISTS rushing_by_team_by_game;
        CREATE TABLE rushing_by_team_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
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
            epa DECIMAL NOT NULL,
            attempts_rb SMALLINT NOT NULL,
            yards_rb SMALLINT NOT NULL,
            td_rb SMALLINT NOT NULL,
            epa_rb DECIMAL NOT NULL,
            attempts_qb_designed SMALLINT NOT NULL,
            yards_qb_designed SMALLINT NOT NULL,
            td_qb_designed SMALLINT NOT NULL,
            epa_qb_designed DECIMAL NOT NULL,
            attempts_qb_scramble SMALLINT NOT NULL,
            yards_qb_scramble SMALLINT NOT NULL,
            td_qb_scramble SMALLINT NOT NULL,
            epa_qb_scramble DECIMAL NOT NULL,
            attempts_qb_kneel SMALLINT NOT NULL,
            yards_qb_kneel SMALLINT NOT NULL,
            epa_qb_kneel DECIMAL NOT NULL,
            attempts_wr SMALLINT NOT NULL,
            yards_wr SMALLINT NOT NULL,
            td_wr SMALLINT NOT NULL,
            epa_wr DECIMAL NOT NULL,
            attempts_other SMALLINT NOT NULL,
            yards_other SMALLINT NOT NULL,
            td_other SMALLINT NOT NULL,
            epa_other DECIMAL NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS rushing_by_team_by_game""")
