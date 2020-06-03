# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 10:51:28 2019

@author: William
"""

# Third-party imports
from sqlalchemy import inspect

# Local application imports
from model import Base


def create_tables(engine, rollback=False):
    """Create all database tables. If rollback is True, then all changes will
    be undone."""
    with engine.connect() as conn:
        trans = conn.begin()
        Base.metadata.create_all(conn)
        inspector = inspect(conn)
        table_names = inspector.get_table_names()
        print('tables added: {!s}'.format(table_names))
        if rollback:
            trans.rollback()
            inspector = inspect(conn)
            rolled_back_table_names = inspector.get_table_names()
            print('tables added: {!s}'.format(rolled_back_table_names))
        else:
            trans.commit()


def drop_tables(engine, rollback=False):
    """Drop all database tables.
    
    Parameters
    ----------
    rollback : bool
        If rollback is True, then all changes will be undone before commiting.
    """
    with engine.connect() as conn:
        trans = conn.begin()
        Base.metadata.drop_all(conn)
        if rollback:
            trans.rollback()
        else:
            trans.commit()
