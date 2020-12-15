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
            "team.season" DECIMAL NOT NULL,
            "team.abbr" VARCHAR NOT NULL,
            "teamPlayers.jerseyNumber" DECIMAL,
            "teamPlayers.displayName" VARCHAR NOT NULL,
            "teamPlayers.firstName" VARCHAR,
            "teamPlayers.middleName" VARCHAR,
            "teamPlayers.lastName" VARCHAR,
            "teamPlayers.suffix" VARCHAR,
            "teamPlayers.status" VARCHAR,
            "teamPlayers.positionGroup" VARCHAR,
            "teamPlayers.position" VARCHAR,
            "teamPlayers.nflId" DECIMAL,
            "teamPlayers.esbId" VARCHAR,
            "teamPlayers.gsisId" VARCHAR,
            "teamPlayers.birthDate" DATE,
            "teamPlayers.homeTown" VARCHAR,
            "teamPlayers.collegeId" DECIMAL,
            "teamPlayers.collegeName" VARCHAR NOT NULL,
            "teamPlayers.height" VARCHAR,
            "teamPlayers.weight" DECIMAL,
            "team.teamId" DECIMAL,
            "team.cityState" VARCHAR,
            "team.fullName" VARCHAR NOT NULL,
            "team.nick" VARCHAR,
            "team.conferenceAbbr" VARCHAR,
            "team.divisionAbbr" VARCHAR,
            "teamPlayers.headshot_url" VARCHAR,
            "teamPlayers.profile_url" VARCHAR,
            scrape_dt TIMESTAMP WITHOUT TIME ZONE,
            "join" VARCHAR,
            pbp_id VARCHAR,
            pbp_name VARCHAR
        );
    """)


def downgrade():
    op.execute("""
        DROP TABLE IF EXISTS roster;
    """)
