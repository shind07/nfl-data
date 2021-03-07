"""two_point_conversions_by_game

Revision ID: 8bf5262a7e17
Revises: 63f41f9aa2dd
Create Date: 2021-02-28 16:03:09.882465

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8bf5262a7e17'
down_revision = '63f41f9aa2dd'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE two_point_conversions_by_player_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            player VARCHAR NOT NULL,
            two_point_conversions SMALLINT NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS two_point_conversions_by_player_by_game;""")
