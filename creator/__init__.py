# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 14:03:00 2019

@author: William
"""

# Standard library imports
from contextlib import contextmanager

# Third-party imports
from pint import UnitRegistry
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

# Local application imports
import config

# Create a common UnitRegistry for performing unit conversions with pint module
ureg = UnitRegistry()
Q_ = ureg.Quantity

# SQLAlchemy setup
db_url = URL('postgresql',
             **config.config(filename='database.ini',
                             section='sqlalchemy postgresql')
             )
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        # TODO: Told by flake8 not to use 'bare except' i.e. without
        # specifying an Exception, but if an error of any kind is raised, I
        # think it is best to rollback work done for database? But if error is
        # raised and not handled, the finally clause will still get executed
        # and if nothing is commited yet, the session will closed and an
        # implicit rollback will ensue. I think using 'except Exception' is
        # fine.
        session.rollback()
        raise
    finally:
        session.close()