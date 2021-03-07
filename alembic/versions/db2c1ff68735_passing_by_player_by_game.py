"""passing_by_player_by_game

Revision ID: db2c1ff68735
Revises: c48f200f6fda
Create Date: 2021-02-20 16:19:57.977193

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'db2c1ff68735'
down_revision = 'c48f200f6fda'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DROP TABLE IF EXISTS passing_by_player_by_game;
        CREATE TABLE passing_by_player_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            player VARCHAR NOT NULL,
            completions SMALLINT NOT NULL,
            attempts SMALLINT NOT NULL,
            yards SMALLINT NOT NULL,
            air_yards_intended SMALLINT NOT NULL,
            air_yards_completed SMALLINT NOT NULL,
            td SMALLINT NOT NULL,
            int SMALLINT NOT NULL,
            sacks SMALLINT NOT NULL,
            sack_yards SMALLINT NOT NULL,
            fumbles SMALLINT NOT NULL,
            fumbles_lost SMALLINT NOT NULL,
            spikes SMALLINT NOT NULL,
            epa DECIMAL NOT NULL,
            epa_pass DECIMAL NOT NULL,
            epa_int DECIMAL NOT NULL,
            epa_sack DECIMAL NOT NULL,
            epa_spike DECIMAL NOT NULL,
            cpoe DECIMAL NOT NULL,
            completions_wr SMALLINT NOT NULL,
            attempts_wr SMALLINT NOT NULL,
            yards_wr SMALLINT NOT NULL,
            air_yards_intended_wr SMALLINT NOT NULL,
            air_yards_completed_wr SMALLINT NOT NULL,
            td_wr SMALLINT NOT NULL,
            int_wr SMALLINT NOT NULL,
            epa_wr DECIMAL NOT NULL,
            cpoe_wr DECIMAL NOT NULL,
            completions_te SMALLINT NOT NULL,
            attempts_te SMALLINT NOT NULL,
            yards_te SMALLINT NOT NULL,
            air_yards_intended_te SMALLINT NOT NULL,
            air_yards_completed_te SMALLINT NOT NULL,
            td_te SMALLINT NOT NULL,
            int_te SMALLINT NOT NULL,
            epa_te DECIMAL NOT NULL,
            cpoe_te DECIMAL NOT NULL,
            completions_rb SMALLINT NOT NULL,
            attempts_rb SMALLINT NOT NULL,
            yards_rb SMALLINT NOT NULL,
            air_yards_intended_rb SMALLINT NOT NULL,
            air_yards_completed_rb SMALLINT NOT NULL,
            td_rb SMALLINT NOT NULL,
            int_rb SMALLINT NOT NULL,
            epa_rb DECIMAL NOT NULL,
            cpoe_rb DECIMAL NOT NULL,
            completions_null SMALLINT NOT NULL,
            attempts_null SMALLINT NOT NULL,
            yards_null SMALLINT NOT NULL,
            air_yards_intended_null SMALLINT NOT NULL,
            air_yards_completed_null SMALLINT NOT NULL,
            td_null SMALLINT NOT NULL,
            int_null SMALLINT NOT NULL,
            epa_null DECIMAL NOT NULL,
            cpoe_null DECIMAL NOT NULL,
            completions_other SMALLINT NOT NULL,
            attempts_other SMALLINT NOT NULL,
            yards_other SMALLINT NOT NULL,
            air_yards_intended_other SMALLINT NOT NULL,
            air_yards_completed_other SMALLINT NOT NULL,
            td_other SMALLINT NOT NULL,
            int_other SMALLINT NOT NULL,
            epa_other DECIMAL NOT NULL,
            cpoe_other DECIMAL NOT NULL,
            target_share_wr DECIMAL NOT NULL,
            target_share_te DECIMAL NOT NULL,
            target_share_rb DECIMAL NOT NULL,
            target_share_other DECIMAL NOT NULL,
            air_yards_intended_share_wr DECIMAL NOT NULL,
            air_yards_intended_share_te DECIMAL NOT NULL,
            air_yards_intended_share_rb DECIMAL NOT NULL,
            air_yards_completed_share_wr DECIMAL NOT NULL,
            air_yards_completed_share_te DECIMAL NOT NULL,
            air_yards_completed_share_rb DECIMAL NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS passing_by_player_by_game;""")
