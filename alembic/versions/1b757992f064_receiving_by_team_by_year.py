"""receiving_by_team_by_year

Revision ID: 1b757992f064
Revises: 755551daa423
Create Date: 2021-02-09 18:31:36.118254

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1b757992f064'
down_revision = '755551daa423'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE receiving_by_team_by_year (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
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
    op.execute("""DROP TABLE IF EXISTS receiving_by_team_by_year""")
