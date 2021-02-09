"""create passing by player by game

Revision ID: 9e50ee302025
Revises: 39c12e1512fa
Create Date: 2021-01-22 11:29:12.412116

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9e50ee302025'
down_revision = '39c12e1512fa'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(""" 
        CREATE TABLE passing_by_player_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            passer VARCHAR NOT NULL,
            completions SMALLINT NOT NULL,
            attempts SMALLINT NOT NULL,
            yards SMALLINT NOT NULL,
            air_yards_intended SMALLINT NOT NULL,
            air_yards_completed SMALLINT NOT NULL,
            td SMALLINT NOT NULL,
            int SMALLINT NOT NULL,
            fumbles SMALLINT NOT NULL,
            spikes SMALLINT NOT NULL,
            epa DECIMAL NOT NULL,
            epa_spikes DECIMAL NOT NULL,
            epa_total DECIMAL NOT NULL,
            cpoe DECIMAL NOT NULL
        ); 
    """)


def downgrade():
    op.execute("""
        DROP TABLE IF EXISTS passing_by_player_by_game;
    """)
