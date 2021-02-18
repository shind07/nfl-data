"""rushing_by_player_by_year

Revision ID: 885cf8bdabae
Revises: af74fd81fb96
Create Date: 2021-02-18 05:46:18.261659

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '885cf8bdabae'
down_revision = 'af74fd81fb96'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DROP TABLE IF EXISTS rushing_by_player_by_year;
        CREATE TABLE rushing_by_player_by_year (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            player VARCHAR NOT NULL,
            games SMALLINT NOT NULL,
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
    op.execute("""DROP TABLE IF EXISTS rushing_by_player_by_year""")
