# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-04-13 19:33:47
# @Last Modified by:   vamshi
# @Last Modified time: 2018-04-14 12:39:51

#code to read and push data to postgres

import sys
import os
import numpy as np
import pandas as pd
import csv

import psycopg2
from config import config


train_dir = "../data/train_user_ratings.csv"
test_dir  = "../data/test_user_ratings.csv"
gender_data_dir = "../data/gender.dat"

#read train_data
train_data = pd.read_csv(train_dir,names=['UserId','ForUserId','Rating'],sep=',',header=0,index_col=None)
gender_data = pd.read_table(gender_data_dir, sep=",",names=['UserId','Gender'])

train_data['UserId'] = train_data['UserId'].astype('int')
train_data['ForUserId'] = train_data['ForUserId'].astype('int')
train_data['Rating'] = train_data['Rating'].astype('int')

gender_data = pd.DataFrame(gender_data)
gender_data['UserId'] = gender_data['UserId'].astype('int')


#get unique users
users = np.unique(train_data.as_matrix())
print("Total number of users are %s"%len(users))

#group by	 
#a = pd.groupby(train_data,'UserId')

#save frienship and gender
train_data.to_csv("../data/train.csv",sep='\t',index=False,header=None)
gender_data.to_csv("../data/gender.csv",sep='\t',index=False,header=None)
