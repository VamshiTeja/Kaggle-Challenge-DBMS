# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-04-14 11:07:32
# @Last Modified by:   vamshi
# @Last Modified time: 2018-04-14 12:41:23

import sys
import os
import numpy as np
import pandas as pd
import csv

import psycopg2
from config import config

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (

        """
        CREATE TABLE Ratings (
            UserID INTEGER ,
            ProfileID INTEGER,
            Rating INTEGER
            )
        """,

        """
        CREATE TABLE Gender(
            UserID INTEGER,
            Gender VARCHAR(1)
        )
        """,
        )

    copy_commands = ("""COPY Ratings FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Kaggle/kaggle_dbms/data/train.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY Gender FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Kaggle/kaggle_dbms/data/gender.csv' DELIMITER '\t' CSV HEADER""")

    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
        	cur.execute(command)
        for cmd in copy_commands:
        	cur.execute(cmd)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
 
if __name__ == '__main__':
    create_tables()