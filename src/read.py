# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-04-13 19:33:47
# @Last Modified by:   vamshi
# @Last Modified time: 2018-04-13 19:41:05

#code to read and push data to postgres

import os,sys
import pandas as pd
import numpy as np


train_dir = "../data/train_user_ratings.csv"
test_dir  = "../data/test_user_ratings.csv"

train = pd.read_csv(train_dir,names=['UserId','ForUserId','Rating'])

print(train)