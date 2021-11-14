#!/usr/bin/python
from typing import List, Any

import pymysql
from datetime import datetime
from abc import ABCMeta
from enum import Enum
from copy import deepcopy

# global constants
ONLY_LAST = True
USE_LIMIT = False
N_ZMD = 4


class TypeOfVal(Enum):
    V_INP = 1,
    V_OUTP = 2,
    V_CALC = 3,
    STUP1 = 4,
    STUP2 = 5,
    NAGON = 6

class StupInfo(object):
    def __init__(self,value,duration):
        self.value = value
        self.duration = duration

class AbstractItem(object):
    _metaclass__ = ABCMeta

    def __init__(self, tRosp, iu, sn):
        self.tRosp = int(tRosp)
        self.iu = int(iu)
        self.sn = int(sn)


# abstract item with time
class AbstractTimeItem(AbstractItem):
    _metaclass__ = ABCMeta

    def __init__(self, t, nom_iu, sn, tEv, msecEv):
        super().__init__(t, nom_iu, sn)
        self.tEvDouble = float(tEv) + float(msecEv) / 1000.0


# common item from gr_zmd
class ItemGrZmd(AbstractTimeItem):
    def __init__(self, t, iu, sn, tEv, msecEv, typeOfVal: TypeOfVal, val):
        super().__init__(t, iu, sn, tEv, msecEv)
        self.tEvDouble = float(tEv) + float(msecEv) / 1000.0
        self.typeOfVal = typeOfVal
        self.val = int(val)


# common item
class Item(AbstractTimeItem):

    def __init__(self, t, nom_iu, sn, tEv, msecEv, vUpr, kzp):
        super().__init__(t, nom_iu, sn, tEv, msecEv)
        self.vInp = float(vUpr) / 32.0
        self.kzp = kzp
        self.mass = 0.0
        self.kVag = 0
        self.vRec = 0.0
        self.vOutp = 0.0
        self.vCalc = 0.0
        self.stups = list()

    def set_v_outp(self,v:int):
        self.vOutp = float(v) / 32.0
    def set_v_calc(self,v:int):
        self.vCalc = float(v) / 32.0
    #
    # @classmethod
    # def from_gr_zmd_item(cls, AbstractTimeItem, vUpr, kzp):
    #     return cls(AbstractTimeItem.tRosp,AbstractTimeItem.iu,
    #                AbstractTimeItem.sn, int(AbstractTimeItem.tEvDouble,
    #                int(AbstractTimeItem.tEvDouble*1000)%1000,vUpr,kzp)


# item mass
class MassItem(AbstractItem):
    def __init__(self, t, nomIu, sn, massKg10):
        super().__init__(t, nomIu, sn)
        self.mass = round(float(massKg10) / 100.0, 2)
        self.kVag = 1

    def increment_mass(self, massKg10):
        self.mass += round(float(massKg10) / 100.0, 2)
        self.kVag += 1


def from_unixtime(timestamp: int):
    return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')


if __name__ == "__main__":
    items: List[Item] = list()
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
        time_real = from_unixtime(tabPostfix)
        print("used time is {0}".format(time_real))
        tabPrefix = ""
        tabPostfix = "_" + tabPostfix
    if USE_LIMIT:
        limit_str = " limit 10000"

    # get v_upr
    itemsGrZmd: List[ItemGrZmd] = list()
    query_str = "select time, nom_iu, v_pmin, time_ev, msec_ev, v_upr, kzp, v_ust, v_pmax, n_mes, n_zmd, inf2  " \
                "from {0}gr_zmd{1} where n_zmd in ({3},{4}) and n_mes between 1 and 7{2}".format(tabPrefix, tabPostfix,
                                                                                                 limit_str, N_ZMD,
                                                                                                 N_ZMD + 1)
    print(query_str)
    cur.execute(query_str)
    # print(cur.fetchall())
    prev_mess = 0  # to check if after n_mes=1 is n_mes=7
    sn = -1
    for res in cur.fetchall():
        [time, nom_iu, v_pmin, time_ev, msec_ev, v_upr, kzp, v_ust, v_pmax, n_mes, n_zmd, inf2] = res

        if n_mes == 1 and n_zmd == N_ZMD:  # input into retarder (v_pmin=sn)
            sn = v_pmin
            if prev_mess != 1:
                items.append(Item(time, nom_iu, v_pmin, time_ev, msec_ev, v_upr, kzp))
            else:
                print("nagon at {0}.{1}".format(time_ev, msec_ev))
                itemsGrZmd.append(ItemGrZmd(time, nom_iu, v_pmin, time_ev, msec_ev, TypeOfVal.NAGON, 1))
            prev_mess = 1
        if n_mes == 7 and n_zmd == N_ZMD + 1:  # output from retarder (v_pmin=sn)
            sn = -1
            itemsGrZmd.append(ItemGrZmd(time, nom_iu, v_pmin, time_ev, msec_ev, TypeOfVal.V_OUTP, v_ust))
            itemsGrZmd.append(ItemGrZmd(time, nom_iu, v_pmin, time_ev, msec_ev, TypeOfVal.V_CALC, v_pmax))
            prev_mess = 7
        if 2 < n_mes < 5 and sn >= 0:
            type_of_val_tmp = TypeOfVal.STUP1
            if n_zmd == N_ZMD + 1:
                type_of_val_tmp = TypeOfVal.STUP2
            itemsGrZmd.append(ItemGrZmd(time, nom_iu, sn, time_ev, msec_ev, type_of_val_tmp, inf2 & 0xF))

    # items_tmp = [item for item in itemsGrZmd if item.typeOfVal == TypeOfVal.STUP]
    # get mass and k_vag
    massItems: List[MassItem] = list()
    query_str = "select time, nom_iu, sn, n_vag, ves_vm, ves_sl, err  " \
                "from {0}sves{1}".format(tabPrefix, tabPostfix)
    print(query_str)
    cur.execute(query_str)
    for res in cur.fetchall():
        ves = int(res[4])
        if int(res[6]) > 0:
            ves = int(res[5])
        tRosp = int(res[0])
        iu = int(res[1])
        sn = int(res[2])-1
        if not massItems \
                or tRosp != massItems[-1].tRosp \
                or iu != massItems[-1].iu \
                or sn != massItems[-1].sn:
            massItems.append(MassItem(tRosp, iu, sn, ves))
        else:
            massItems[-1].increment_mass(ves)

    for i in range(len(items)):
        try:
            # set weight to item
            tmp_val_weight = [[mItem.mass,mItem.kVag] for mItem in massItems if
                   mItem.tRosp == items[i].tRosp and
                   mItem.iu == items[i].iu and
                   mItem.sn == items[i].sn][0]
            items[i].mass = tmp_val_weight[0]
            items[i].kVag = tmp_val_weight[1]
            # set v_outp to item
            tmp_val_v_outp = [grItem.val for grItem in itemsGrZmd if
                              grItem.tRosp == items[i].tRosp and
                              grItem.iu == items[i].iu and
                              grItem.sn == items[i].sn and
                              grItem.typeOfVal == TypeOfVal.V_OUTP][0]
            items[i].set_v_outp(tmp_val_v_outp)
            # set v_calc to item
            tmp_val_v_calc = [grItem.val for grItem in itemsGrZmd if
                              grItem.tRosp == items[i].tRosp and
                              grItem.iu == items[i].iu and
                              grItem.sn == items[i].sn and
                              grItem.typeOfVal == TypeOfVal.V_CALC][0]
            items[i].set_v_calc(tmp_val_v_calc)

            # set stup to item
            tmp_val_stup1 = [[grItem.tEvDouble, grItem.val] for grItem in itemsGrZmd if
                              grItem.tRosp == items[i].tRosp and
                              grItem.iu == items[i].iu and
                              grItem.sn == items[i].sn and
                              grItem.typeOfVal == TypeOfVal.STUP1]
            tmp_val_stup2 = [[grItem.tEvDouble, grItem.val] for grItem in itemsGrZmd if
                              grItem.tRosp == items[i].tRosp and
                              grItem.iu == items[i].iu and
                              grItem.sn == items[i].sn and
                              grItem.typeOfVal == TypeOfVal.STUP2]
            tmp_val=1
        except:
            items.pop(i)

    # print(cur.fetchall())
    # for res in cur.fetchall():

    db.close()
