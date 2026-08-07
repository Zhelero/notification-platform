"""add unique constraint for correlation_id

Revision ID: a751ecd7407a
Revises: 
Create Date: 2026-08-07 14:14:27.026593

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a751ecd7407a'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_unique_constraint(
        "uq_notifications_correlation_id",
        "notifications",
        ["correlation_id"],
    )


def downgrade():
    op.drop_constraint(
        "uq_notifications_correlation_id",
        "notifications",
        type_="unique",
    )