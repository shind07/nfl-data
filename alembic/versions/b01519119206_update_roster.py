"""update_roster

Revision ID: b01519119206
Revises: c6b1a6d2485a
Create Date: 2021-02-21 23:55:11.463303

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b01519119206'
down_revision = 'c6b1a6d2485a'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE roster ADD COLUMN fantasy_data_id VARCHAR;
        ALTER TABLE roster ADD COLUMN sleeper_id VARCHAR;
    """)


def downgrade():
    op.execute("""
        ALTER TABLE roster DROP COLUMN fantasy_data_id;
        ALTER TABLE roster DROP COLUMN sleeper_id;
    """)
