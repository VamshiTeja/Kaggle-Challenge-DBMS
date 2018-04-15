# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-04-14 18:16:55
# @Last Modified by:   vamshi
# @Last Modified time: 2018-04-15 22:06:59
import sys
import os
import numpy as np
import pandas as pd
import csv
from sklearn.metrics import mean_squared_error
import psycopg2
from config import config
import pickle
import tqdm
test_dir  = "../data/test_user_ratings.csv"
def get_ratings():
    """ query data from the vendors table """
    sim_queries = ("""
    	WITH sim(c1,c2) AS
		(SELECT r1.rating-avg1.avg ,r2.rating-avg2.avg  
		FROM ratings as r1,ratings as r2,avg_ratings as avg1,avg_ratings as avg2
		WHERE  r1.userid='%d' AND r2.userid='%d' AND r1.profileid=r2.profileid AND avg1.userid='%d' AND avg2.userid='%d')  
		SELECT (SELECT SUM(c1*c2)  FROM sim)/(select SQRT(SUM(c1*c1))*sqrt(SUM(c2*c2))FROM sim);
    	""",
    	"""
    	CREATE FUNCTION pearson( p1 INTEGER,p2 INTEGER)
		RETURNS FLOAT AS $simi$
		DECLARE
		simi FLOAT;
		BEGIN
		WITH sim(c1,c2) AS
		(SELECT r1.rating-avg1.avg ,r2.rating-avg2.avg  
		FROM ratings as r1,ratings as r2,avg_ratings as avg1,avg_ratings as avg2
		WHERE  r1.userid=p1 AND r2.userid=p2 AND r1.profileid=r2.profileid AND avg1.userid=p1 AND avg2.userid=p2)  
		SELECT (SELECT SUM(c1*c2)  FROM sim)/(select SQRT(SUM(c1*c1))*sqrt(SUM(c2*c2))FROM sim) into simi;
		RETURN simi;
		END;
		$simi$ LANGUAGE plpgsql;""",
		
		"""
		SELECT pearson(110866,128);
    	""",
    	)

    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        def get_similarity(user1,user2):
        	cur.execute(sim_queries[0]%(user1,user2,user1,user2))
        	rows = cur.fetchone()      
        	a = rows[0]
        	return float(a)

        def get_rating():
        	query = ("""
        			select (select avg from avg_ratings where userid='%d') + 
        			(select avg(ratings.rating-avg_ratings.avg) from ratings,avg_ratings where ratings.profileid='%d' and avg_ratings.userid='%d');
        		""",
        		"""
        		select userid,avg from avg_ratings 
        		""")
        	cur.execute(query[1])
        	rows = cur.fetchall()
        	return pd.DataFrame(rows)

        def get_item_avg():
        	query = ("""
        			select ratings.profileid, avg(ratings.rating-avg_ratings.avg) from ratings,avg_ratings
					where ratings.userid = avg_ratings.userid
					group by ratings.profileid
					order by ratings.profileid

        		""",
        		

        		)
        	cur.execute(query[0])
        	rows = cur.fetchall()
        	return pd.DataFrame(rows)

		
        #cur.execute("SELECT userid, profileid,rating FROM ratings ORDER BY userid")
        #print("The number of parts: ", cur.rowcount)
        
        #rows = cur.fetchall()
        #data_df = pd.DataFrame(rows[:-20000],columns=['userid','profileid','rating'])
        test_data = pd.read_csv(test_dir,names=['UserId','ForUserId'],sep=',',header=0,index_col=None)
        test_userid = test_data['UserId'].as_matrix()
        test_profileid = test_data['ForUserId'].as_matrix()

        print(len(test_profileid))
        
        test_ratings = []
       	avg_table = get_rating().as_matrix()
       	item_avg_table = get_item_avg().as_matrix()

       	avg_user_dict = dict(zip(avg_table[:,0],avg_table[:,1]))
        avg_item_dict = dict(zip(item_avg_table[:,0],item_avg_table[:,1]))


        #print(item_avg_dict)
       	for i in range(test_userid.shape[0]):
       		try:
       			test_ratings.append(float(avg_user_dict[test_userid[i]])+float(avg_item_dict[test_profileid[i]]))
       		except KeyError,e:
				test_ratings.append(float(avg_user_dict[test_userid[i]]))


       	#print(test_ratings)

       	test_ratings = pd.DataFrame(test_ratings)
       	test_ratings.to_csv("./test_avg_1.csv",sep='\t',index=False,header=['Rating'])
 
       	#test_data = pd.DataFrame(test_ratings)
        cur.close()

        '''
        mean_ratings = data_df.groupby('userid')[['userid','profileid','rating']]
        mean_ratings = mean_ratings.rename(columns={'rating':'rating_mean'})
        data_df = pd.merge(data_df, mean_ratings,on='userid',how='left',sort=False)
        data_df['rating_adjusted'] = data_df['ratings'] - data_df['rating_mean']
		'''
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
 
if __name__ == '__main__':
    get_ratings()