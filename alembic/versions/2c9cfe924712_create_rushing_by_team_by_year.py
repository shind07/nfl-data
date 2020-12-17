"""create rushing by team by year

Revision ID: 2c9cfe924712
Revises: 9337ebfa676f
Create Date: 2020-12-17 10:11:43.720403

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2c9cfe924712'
down_revision = '9337ebfa676f'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE rushing_by_team_by_year (
            team VARCHAR NOT NULL,
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
        DROP TABLE IF EXISTS rushing_by_team_by_year;
    """)
