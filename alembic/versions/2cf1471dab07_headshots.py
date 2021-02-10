"""headshots

Revision ID: 2cf1471dab07
Revises: 1b757992f064
Create Date: 2021-02-09 19:54:46.909770

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2cf1471dab07'
down_revision = '1b757992f064'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE headshots (
            season SMALLINT NOT NULL,
            team VARCHAR,
            full_name VARCHAR NOT NULL,
            gsis_id VARCHAR,
            update_dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            headshot_url VARCHAR NOT NULL,
            local_path VARCHAR NOT NULL,
            success BOOLEAN NOT NULL
    );
    """)

def downgrade():
    op.execute("""DROP TABLE IF EXISTS headshots""")
