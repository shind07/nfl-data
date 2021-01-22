"""update roster

Revision ID: 73dea82a55a9
Revises: 2c9cfe924712
Create Date: 2020-12-22 05:21:43.922800

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '73dea82a55a9'
down_revision = '2c9cfe924712'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        DROP TABLE IF EXISTS roster;
        CREATE TABLE roster (
            season DECIMAL NOT NULL,
            team VARCHAR,
            position VARCHAR,
            depth_chart_position VARCHAR,
            jersey_number DECIMAL,
            status VARCHAR NOT NULL,
            full_name VARCHAR NOT NULL,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            birth_date DATE,
            height VARCHAR,
            weight DECIMAL,
            college VARCHAR,
            high_school VARCHAR,
            gsis_id VARCHAR,
            espn_id DECIMAL,
            sportradar_id VARCHAR,
            yahoo_id DECIMAL,
            rotowire_id DECIMAL,
            update_dt TIMESTAMP WITHOUT TIME ZONE,
            headshot_url VARCHAR
        );
    """)


def downgrade():
    op.execute("""
        DROP TABLE IF EXISTS roster;
    """)
