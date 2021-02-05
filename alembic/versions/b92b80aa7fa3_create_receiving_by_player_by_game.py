"""create receiving by player by game

Revision ID: b92b80aa7fa3
Revises: ba3fad0c46be
Create Date: 2021-01-26 08:42:50.486437

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b92b80aa7fa3'
down_revision = 'ba3fad0c46be'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE receiving_by_player_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            gsis_id VARCHAR NOT NULL,
            receiver VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            opp VARCHAR NOT NULL,
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
    op.execute("""DROP TABLE IF EXISTS receiving_by_player_by_game""")
