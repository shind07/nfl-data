"""create rushing by team by game

Revision ID: 9337ebfa676f
Revises: 37011a3a3d14
Create Date: 2020-12-17 10:00:00.462405

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9337ebfa676f'
down_revision = '37011a3a3d14'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE rushing_by_team_by_game (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            game_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            opp VARCHAR NOT NULL,
            week SMALLINT NOT NULL,
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
        DROP TABLE IF EXISTS rushing_by_team_by_game;
    """)
