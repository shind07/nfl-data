"""receiving_by_team_by_game

Revision ID: 755551daa423
Revises: 124984a15c6e
Create Date: 2021-02-09 18:18:57.972529

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '755551daa423'
down_revision = '124984a15c6e'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE receiving_by_team_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            pos VARCHAR NOT NULL,
            receptions SMALLINT NOT NULL,
            targets SMALLINT NOT NULL,
            yards SMALLINT NOT NULL,
            air_yards_intended SMALLINT NOT NULL,
            air_yards_completed SMALLINT NOT NULL,
            td SMALLINT NOT NULL,
            int SMALLINT NOT NULL,
            fumbles SMALLINT NOT NULL,
            fumbles_lost SMALLINT NOT NULL,
            epa DECIMAL NOT NULL,
            cpoe DECIMAL NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS receiving_by_team_by_game""")
