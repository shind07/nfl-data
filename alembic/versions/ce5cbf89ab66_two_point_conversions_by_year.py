"""two_point_conversions_by_year

Revision ID: ce5cbf89ab66
Revises: 8bf5262a7e17
Create Date: 2021-02-28 16:08:45.896072

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ce5cbf89ab66'
down_revision = '8bf5262a7e17'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE two_point_conversions_by_player_by_year (
            year SMALLINT NOT NULL,
            season_type VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            gsis_id VARCHAR NOT NULL,
            pos VARCHAR NOT NULL,
            player VARCHAR NOT NULL,
            two_point_conversions SMALLINT NOT NULL
        );
    """)


def downgrade():
    op.execute("""DROP TABLE IF EXISTS two_point_conversions_by_player_by_year;""")