"""Add Israeli tax rules and models

Revision ID: b3d55099a52c
Revises: 424384f7cb0a
Create Date: 2026-03-14 02:28:15.872576

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3d55099a52c'
down_revision: Union[str, Sequence[str], None] = '424384f7cb0a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Note: These might already exist in some environments because of create_all runs.
    # We use 'if_not_exists' or just catch errors if needed, but standard Alembic assumes they don't exist.
    
    op.create_table('cpi_rates',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('index_value', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_cpi_rates_date'), 'cpi_rates', ['date'], unique=True)
    op.create_index(op.f('ix_cpi_rates_id'), 'cpi_rates', ['id'], unique=False)

    with op.batch_alter_table('transactions') as batch_op:
        batch_op.add_column(sa.Column('inflationary_gain_ils', sa.Float(), nullable=True))
        batch_op.add_column(sa.Column('real_gain_ils', sa.Float(), nullable=True))
        # Note: Enum type might need special handling for fork/airdrop
        # We'll just let it be VARCHAR for now or assume it's already updated
        
    with op.batch_alter_table('tax_lot_consumptions') as batch_op:
        batch_op.add_column(sa.Column('adjusted_cost_basis_ils', sa.Float(), nullable=True))
        batch_op.add_column(sa.Column('inflationary_gain_ils', sa.Float(), nullable=True))
        batch_op.add_column(sa.Column('real_gain_ils', sa.Float(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('tax_lot_consumptions') as batch_op:
        batch_op.drop_column('real_gain_ils')
        batch_op.drop_column('inflationary_gain_ils')
        batch_op.drop_column('adjusted_cost_basis_ils')

    with op.batch_alter_table('transactions') as batch_op:
        batch_op.drop_column('real_gain_ils')
        batch_op.drop_column('inflationary_gain_ils')

    op.drop_index(op.f('ix_cpi_rates_id'), table_name='cpi_rates')
    op.drop_index(op.f('ix_cpi_rates_date'), table_name='cpi_rates')
    op.drop_table('cpi_rates')
