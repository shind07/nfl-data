"""create roster table

Revision ID: c2019f5f4de1
Revises: 098e039174cf
Create Date: 2020-12-14 09:36:12.566105

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c2019f5f4de1'
down_revision = '098e039174cf'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE roster (
            season DECIMAL NOT NULL,
            abbr VARCHAR NOT NULL,
            jerseyNumber DECIMAL,
            displayName VARCHAR NOT NULL,
            firstName VARCHAR,
            middleName VARCHAR,
            lastName VARCHAR,
            suffix VARCHAR,
            status VARCHAR,
            positionGroup VARCHAR,
            position VARCHAR,
            nflId DECIMAL,
            esbId VARCHAR,
            gsisId VARCHAR,
            birthDate DATE,
            homeTown VARCHAR,
            collegeId DECIMAL,
            collegeName VARCHAR NOT NULL,
            height VARCHAR,
            weight DECIMAL,
            teamId DECIMAL,
            cityState VARCHAR,
            fullName VARCHAR NOT NULL,
            nick VARCHAR,
            conferenceAbbr VARCHAR,
            divisionAbbr VARCHAR,
            headshot_url VARCHAR,
            profile_url VARCHAR,
            scrape_dt TIMESTAMP WITHOUT TIME ZONE,
            join VARCHAR,
            pbp_id VARCHAR,
            pbp_name VARCHAR
        );
    """)


def downgrade():
    op.execute("""
        DROP TABLE IF EXISTS roster;
    """)
