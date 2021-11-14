#!/usr/bin/python
import pymysql
from datetime import datetime
from abc import ABCMeta

# global constants
ONLY_LAST = True
USE_LIMIT = False
N_ZMD = 4


class abstract_item(object):
    _metaclass__ = ABCMeta

    def __init__(self, t, nom_iu, sn):
        self.t_rosp = int(t)
        self.nom_iu = int(nom_iu)
        self.sn = int(sn)


# common item
class Item(abstract_item):

    def __init__(self, t, nom_iu, sn, t_ev, msec_ev, v_upr, kzp):
        super().__init__(t, nom_iu, sn)
        self.t_ev_double = float(t_ev) + float(msec_ev) / 1000.0
        self.v = float(v_upr) / 32.0
        self.kzp = kzp
        self.mass = 0

    def increment_mass(self, mass):
        self.mass = self.mass + mass


# #item mass
# class Mass_item():
#     def __init__(self):


items = list()
db = pymysql.connect(host="127.0.0.1",  # your host, usually laocalhost
                     user="kgm",  # your username
                     passwd="ellipsoid",  # your password
                     db="kgm_new",
                     port=3301)  # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

tab_prefix = "merge_"
tab_postfix = ""
limit_str = ""
if ONLY_LAST:
    # Use all the SQL you like
    cur.execute("show tables like 'gr_zmd%'")

    tab_postfix = cur.fetchall()[-1][0][-10:]
    time_real = datetime.utcfromtimestamp(int(tab_postfix)).strftime('%Y-%m-%d %H:%M:%S')
    print("used time is {0}".format(time_real))
    tab_prefix = ""
    tab_postfix = "_" + tab_postfix

if USE_LIMIT:
    limit_str = " limit 10000"

# get v_upr
query_str = "select time, nom_iu, v_pmin, time_ev, msec_ev, v_upr, kzp  " \
            "from {0}gr_zmd{1} where n_zmd = {3} and n_mes=1{2}".format(tab_prefix, tab_postfix, limit_str, N_ZMD)
print(query_str)
cur.execute(query_str)
# print(cur.fetchall())
for res in cur.fetchall():
    items.append(Item(res[0], res[1], res[2], res[3], res[4],res[5],res[6]))

# get mass and k_vag
items_mass = list()
query_str = "select time, time_ev, msec_ev, nom_iu, v_pmin, v_upr, kzp  " \
            "from {0}gr_zmd{1} where n_zmd = {3} and n_mes=1{2}".format(tab_prefix, tab_postfix, limit_str, N_ZMD)
print(query_str)
cur.execute(query_str)
for res in cur.fetchall():
    items_mass.append(Item(res[0], res[1], res[2], res[3], res[4],res[5],res[6]))
# print(cur.fetchall())
# for res in cur.fetchall():


db.close()
