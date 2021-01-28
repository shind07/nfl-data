"""create lateral receiving yards

Revision ID: 124984a15c6e
Revises: d670a0e93945
Create Date: 2021-01-27 05:10:08.883295

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '124984a15c6e'
down_revision = 'd670a0e93945'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE lateral_receiving_yards (
            game_id VARCHAR NOT NULL,
            play_id SMALLINT NOT NULL,
            stat_id SMALLINT NOT NULL,
            lateral_rec_yards SMALLINT NOT NULL,
            team_abbr VARCHAR NOT NULL,
            player_name VARCHAR NOT NULL,
            gsis_player_id VARCHAR NOT NULL
    );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS lateral_receiving_yards""")
