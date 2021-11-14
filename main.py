#!/usr/bin/python
import pymysql
from datetime import datetime
from abc import ABCMeta

# global constants
ONLY_LAST = True
USE_LIMIT = False
N_ZMD = 4


class AbstractItem(object):
    _metaclass__ = ABCMeta

    def __init__(self, t, iu, sn):
        self.tRosp = int(t)
        self.iu = int(iu)
        self.sn = int(sn)


# common item
class Item(AbstractItem):

    def __init__(self, t, nom_iu, sn, tEv, msecEv, vUpr, kzp):
        super().__init__(t, nom_iu, sn)
        self.tEvDouble = float(tEv) + float(msecEv) / 1000.0
        self.v = float(vUpr) / 32.0
        self.kzp = kzp
        self.mass = 0


# item mass
class MassItem(AbstractItem):
    def __init__(self, t, nomIu, sn, mass):
        super().__init__(t, nomIu, sn)
        self.mass = mass
        self.kVag = 1

    def increment_mass(self, mass):
        self.mass += mass
        self.kVag += 1


items = list()
db = pymysql.connect(host="127.0.0.1",  # your host, usually laocalhost
                     user="kgm",  # your username
                     passwd="ellipsoid",  # your password
                     db="kgm_new",
                     port=3301)  # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

tabPrefix = "merge_"
tabPostfix = ""
limit_str = ""
if ONLY_LAST:
    # Use all the SQL you like
    cur.execute("show tables like 'gr_zmd%'")

    tabPostfix = cur.fetchall()[-1][0][-10:]
    time_real = datetime.utcfromtimestamp(int(tabPostfix)).strftime('%Y-%m-%d %H:%M:%S')
    print("used time is {0}".format(time_real))
    tabPrefix = ""
    tabPostfix = "_" + tabPostfix
if USE_LIMIT:
    limit_str = " limit 10000"

# get v_upr
query_str = "select time, nom_iu, v_pmin, time_ev, msec_ev, v_upr, kzp  " \
            "from {0}gr_zmd{1} where n_zmd = {3} and n_mes=1{2}".format(tabPrefix, tabPostfix, limit_str, N_ZMD)
print(query_str)
cur.execute(query_str)
# print(cur.fetchall())
for res in cur.fetchall():
    items.append(Item(res[0], res[1], res[2], res[3], res[4], res[5], res[6]))

# get mass and k_vag
massItems = list()
query_str = "select time, nom_iu, sn, n_vag, ves_vm, ves_sl, err  " \
            "from {0}sves{1}".format(tabPrefix, tabPostfix)
print(query_str)
cur.execute(query_str)
for res in cur.fetchall():
    ves = int(res[4])
    if int(res[6]) > 0:
        ves = int(res[5])
    t_rosp = int(res[0])
    iu = int(res[1])
    sn = int(res[2])
    if len(massItems)>0 \
            or t_rosp != massItems[-1].t \
            or iu != massItems[-1].iu \
            or sn != massItems[-1].sn:
            massItems.append(MassItem(res[0], res[1], res[2], res[3], res[4], res[5], res[6]))
# print(cur.fetchall())
# for res in cur.fetchall():


db.close()
