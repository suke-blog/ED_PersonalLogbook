# Personal logger script for Elite:Dangerous
# TestVersion

import json
import sqlite3
import datetime
import re
from bidict import bidict
from typing import List, Dict, Tuple
from watchfiles import watch
from pprint import pformat, pprint
from pathlib import Path
from logging import basicConfig, getLogger, DEBUG, ERROR

basicConfig(level=DEBUG)
logger = getLogger(__name__)

PATH_EDLOG_DIR='/mnt/DATA/SteamLibrary/steamapps/compatdata/359320/pfx/drive_c/users/steamuser/Saved Games/Frontier Developments/Elite Dangerous/'
DB_NAME='ed_personallog.db'

# sqlite3 adapter and converter
# https://docs.python.org/3/library/sqlite3.html#sqlite3-adapter-converter-recipes
def adapt_date_iso(val):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()

def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.replace(tzinfo=None).isoformat()

def adapt_datetime_epoch(val):
    """Adapt datetime.datetime to Unix timestamp."""
    return int(val.timestamp())

sqlite3.register_adapter(datetime.date, adapt_date_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)

def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return datetime.date.fromisoformat(val.decode())

def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val.decode())

def convert_timestamp(val):
    """Convert Unix epoch timestamp to datetime.datetime object."""
    return datetime.datetime.fromtimestamp(int(val))

sqlite3.register_converter("date", convert_date)
sqlite3.register_converter("datetime", convert_datetime)
sqlite3.register_converter("timestamp", convert_timestamp)

# Query to create tables
#  system_tbl: StarSystem
#  body_tbl: body_type=(star/planet/belt/station)
#  market_tbl
#  faction_tbl
#  faction_relation_tbl
#    'CREATE TABLE IF NOT EXISTS body_star_tbl(body_id INTEGER, startype_id INTEGER, detail JSONB, timestamp INTEGER)',
    # 'CREATE TABLE IF NOT EXISTS body_planet_tbl(body_id INTEGER, planettype_id INTEGER, atmospheretype_id INTEGER, landable INTEGER, detail JSONB, timestamp INTEGER)',
    # 'CREATE TABLE IF NOT EXISTS body_ring_tbl(body_id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, ringclass_id INTEGER)',
    # "CREATE TABLE IF NOT EXISTS commodity_category_tbl(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
    # "CREATE TABLE IF NOT EXISTS startype_tbl(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)",
    # "CREATE TABLE IF NOT EXISTS planettype_tbl(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, terraformable BOOLEAN)",
    # "CREATE TABLE IF NOT EXISTS atmospheretype_tbl(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)",
    # "CREATE TABLE IF NOT EXISTS ringclass_tbl(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)",
QUERY_CREATE_TABLE = [
    "CREATE TABLE IF NOT EXISTS system_tbl(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, posx REAL, posy REAL, posz REAL, startype TEXT, systemfaction_id INTEGER, allegiance TEXT, economy TEXT, economysecond TEXT, government TEXT, security TEXT, population INTEGER, detail BLOB NOT NULL DEFAULT (jsonb('{}')), lastarrived_at INTEGER, updated_at INTEGER) WITHOUT ROWID",

    "CREATE TABLE IF NOT EXISTS body_tbl(system_id INTEGER, body_id INTEGER, name TEXT NOT NULL, type TEXT, wasdiscovered BOOLEAN, wasmapped BOOLEAN, detail BLOB NOT NULL DEFAULT (jsonb('{}')), updated_at INTEGER, PRIMARY KEY(system_id, body_id)) WITHOUT ROWID",

    "CREATE TABLE IF NOT EXISTS market_tbl(id INTEGER PRIMARY KEY, name TEXT NOT NULL, system_id INTEGER, body_id INTEGER, type TEXT, goverment TEXT, economy TEXT, distfromstarls REAL, pads INTEGER, padm INTEGER, padl INTEGER, stationfaction_id INTEGER, detail BLOB NOT NULL DEFAULT (jsonb('{}')), updated_at INTEGER)",

    "CREATE TABLE IF NOT EXISTS market_price_tbl(market_id INTEGER, commodity_id INTEGER, buyprice INTEGER, sellprice INTEGER, stockbracket INTEGER, demandbracket INTEGER, stock INTEGER, demand INTEGER, updated_at INTEGER, PRIMARY KEY(market_id, commodity_id)) WITHOUT ROWID",

    "CREATE TABLE IF NOT EXISTS faction_tbl(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, allegiance TEXT, government TEXT, myreputation REAL, updated_at INTEGER)",

    "CREATE INDEX IF NOT EXISTS faction_tbl_name_idx ON faction_tbl(name)",

    "CREATE TABLE IF NOT EXISTS system_faction_tbl(faction_id INTEGER, system_id INTEGER, state BLOB NOT NULL DEFAULT (jsonb('{}')), influence REAL, happiness TEXT, updated_at INTEGER, PRIMARY KEY(faction_id, system_id)) WITHOUT ROWID",

    "CREATE TABLE IF NOT EXISTS commodity_tbl(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, category TEXT, israre INTEGER)",

    "CREATE TABLE IF NOT EXISTS statistics_tbl(id INTEGER PRIMARY KEY, updated_at INTEGER UNIQUE NOT NULL, detail BLOB NOT NULL DEFAULT (jsonb('{}')) )"
    ];

def main():
    print("Personal logger script for Elite:Dangerous")
    print("ver 0.01")

    conn = getConnection()
    if conn is None:
        exit()

    edlogs = getEdLogList(PATH_EDLOG_DIR)
    edjournalBulkReadLogs(edlogs)

    readMarketJson()
    commoditytbl = getCommodityBidict()
    # pprint(commoditytbl[1])

    closeConnection()

_dbconnection = None
def getConnection():
    global _dbconnection

    if _dbconnection is None:
        _dbconnection = sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)
        cur = _dbconnection.cursor()

        cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
        table_cnt = cur.fetchone()[0]
        cur.close()

        if table_cnt == 0:
            # create tables
            print("Create Tables...")
            _dbconnection.execute("BEGIN")
            for query in QUERY_CREATE_TABLE:
                _dbconnection.execute(query)
            _dbconnection.execute("END")
            _dbconnection.commit()
    return _dbconnection

def closeConnection():
    global _dbconnection
    _dbconnection.commit()
    _dbconnection.close()


def updateMarketPrice(jsondata:dict):
    KEY_MAPPING = {"Name_Localised":"name", "Name":"name", "BuyPrice":"buyprice", "SellPrice":"sellprice", "StockBracket":"stockbracket", "DemandBracket":"demandbracket", "Stock":"stock", "Demand":"demand"}
    
    idnametbl = getCommodityBidict()
    pricedata = [{KEY_MAPPING[k]:v for k,v in item.items() if k in KEY_MAPPING} for item in jsondata["Items"] ]
    for item in pricedata:
        item["market_id"] = jsondata["MarketID"]
        item["commodity_id"] = idnametbl.inverse[item["name"]]
        item["updated_at"] = jsondata["timestamp"]

    conn = getConnection()
    conn.execute("BEGIN")
    conn.executemany("REPLACE INTO market_price_tbl(market_id, commodity_id, buyprice, sellprice, stockbracket, demandbracket, stock, demand, updated_at) VALUES(:market_id, :commodity_id, :buyprice, :sellprice, :stockbracket, :demandbracket, :stock, :demand, :updated_at)", pricedata)
    conn.execute("END")
    conn.commit()
    return

def readMarketJson():
    filepath = f"{PATH_EDLOG_DIR}/Market.json"
    with open(filepath, "r") as f:
        text = f.read()
        jsondata = json.loads(text)
    updateCommodity(jsondata)
    updateMarketPrice(jsondata)
    return

def updateCommodity(jsondata:dict):
    COMMODITY_KEY = {"Name":"name", "Name_Localised":"name", "Category":"category", "Category_Localised":"category", "Rare":"israre"}

    commodities = [{COMMODITY_KEY[k]: v for k,v in item.items() if k in COMMODITY_KEY} for item in jsondata["Items"]]

    conn = getConnection()
    conn.execute("BEGIN")
    conn.executemany("INSERT INTO commodity_tbl(name, category, israre) VALUES(:name, :category, :israre) ON CONFLICT(name) DO NOTHING", commodities)
    conn.execute("END")
    conn.commit()
    return


_CommodityBidict = None
def getCommodityBidict(isUpdate:bool=False) -> bidict:
    global _CommodityBidict

    if _CommodityBidict == None or isUpdate:
        conn = getConnection()
        cur = conn.cursor()
        res_value = cur.execute("SELECT id, name FROM commodity_tbl")

        dictCommodity = {v[0]:v[1] for v in res_value}
        _CommodityBidict = bidict(dictCommodity)

        res_value.close()
        cur.close()
    return _CommodityBidict

def edjournalBulkReadLogs(path_to_logs: List):
    conn = getConnection()
    conn.execute("BEGIN")
    for logfile in path_to_logs:
        edjournalReadLog(logfile)
    conn.execute("END")
    conn.commit()

def edjournalReadLog(path_log:str, isWaitforupdate:bool=False):
    with open(path_log, mode='r') as f:
        while True:
            for line in f:
                try:
                    data = json.loads(line)
                    checkEvent(data)
                except json.JSONDecodeError as e:
                    logger.error(f"failed to parse json: {e}")
            if isWaitforupdate:
                for changes in watch(path_log):
                    break
            else:
                break


def checkEvent(jsondata):
    event = jsondata["event"]
    match event:
        case "ApproachSettlement":
            updateMarket(jsondata)
        case "Docked":
            updateMarket(jsondata)
        case "Location":
            updateMarket(jsondata)
        case "Scan":
            eventScan(jsondata)
        case "StartJump":
            updateSystem(jsondata)
        case "Statistics":
            updateStatistics(jsondata)
        case "SupercruiseExit":
            updateBody(jsondata)
        case "FSDJump":
            updateSystem(jsondata)
            updateBody(jsondata)
            updateFaction(jsondata)
            updateSystemFaction(jsondata)
        case _:
            # logger.info(f"no handling event: {event}")
            pass


def eventScan(jsondata:dict):
    scanType = jsondata["ScanType"]
    match scanType:
        case "AutoScan":
            updateSystem(jsondata)
            updateBody(jsondata)
        case "Detailed":
            updateBody(jsondata)
        case "NavBeaconDetail":
            updateBody(jsondata)
        case _:
            logger.info(f"no handling ScanType: {scanType}")
            pass


def updateSystem(jsondata:dict):
    KEY_SYSTEM = {"SystemAddress":"id", "StarSystem":"name", "StarPos":"pos", "StarClass":"startype", "SystemAllegiance":"allegiance", "SystemEconomy_Localised":"economy", "SystemSecondEconomy_Localised":"economysecond", "SystemGovernment_Localised":"government", "SystemSecurity_Localised":"security", "Population":"population", "SystemFaction":"systemfaction_id", "timestamp":"updated_at"}
    LIST_SYSTEM_DETAIL = ["ControllingPower", "Powers", "PowerplayState", "PowerplayStateControlProgress", "PowerplayStateReinforcement", "PowerplayStateUndermining","Factions", "SystemFaction"]

    data = {k: v for k, v in jsondata.items() if k in KEY_SYSTEM and v != ""}
    detail = {k: v for k, v in jsondata.items() if k in LIST_SYSTEM_DETAIL and v != ""}

    if len(detail) > 0:
        data["detail"] = json.dumps(detail, ensure_ascii=False)

    if jsondata["event"] == "FSDJump":
        data["lastarrived_at"] = jsondata["timestamp"]
        KEY_SYSTEM.update(lastarrived_at="lastarrived_at")

    if "StarPos" in data:
        x,y,z = data.pop("StarPos")
        data["posx"] = x
        data["posy"] = y
        data["posz"] = z
        KEY_SYSTEM.update(posx="posx",posy="posy",posz="posz")

    if "SystemFaction" in data:
        data["SystemFaction"] = data["SystemFaction"]["Name"]

    if "SystemAddress" in data and len(data) >= 2:
        tmp1 = "INSERT INTO system_tbl(id,"
        tmp2 = " SELECT :SystemAddress,"
        tmpextbl = ""
        tmp3 = " ON CONFLICT(id) DO UPDATE SET"
        for k,v in data.items():
            match k:
                case "SystemAddress":
                    pass
                case "SystemFaction":
                    tmp1 += f" {KEY_SYSTEM[k]},"
                    tmp2 += f" faction_tbl.id,"
                    tmpextbl = f" FROM faction_tbl WHERE faction_tbl.name=:{k} "
                    tmp3 += f" {KEY_SYSTEM[k]}=excluded.{KEY_SYSTEM[k]},"
                case "detail":
                    tmp1 += f" detail,"
                    tmp2 += f" jsonb(:detail),"
                    tmp3 += f" detail=jsonb_patch(detail, excluded.detail),"
                case _:
                    tmp1 += f" {KEY_SYSTEM[k]},"
                    tmp2 += f" :{k},"
                    tmp3 += f" {KEY_SYSTEM[k]}=excluded.{KEY_SYSTEM[k]},"
        query = re.sub(',$',') ',tmp1) + re.sub(',$',' ',tmp2) + tmpextbl + re.sub(',$',';',tmp3)
        logger.debug(query)
        db = getConnection()
        cur = db.cursor()
        cur.execute(query, data)
        cur.close()


def updateBody(jsondata:dict):
    KEY_BODY = {"SystemAddress":"system_id", "BodyID":"body_id", "BodyName":"name", "Body":"name", "BodyType":"type","WasDiscovered":"wasdiscovered", "WasMapped":"wasmapped", "timestamp":"updated_at"}
    LIST_BODY_DETAIL = ['Parents', 'DistanceFromArrivalLS', 'TidalLock', 'TerraformState', 'PlanetClass', 'Atmosphere', 'AtmosphereType', 'Volcanism', 'MassEM', 'Radius', 'SurfaceGravity', 'SurfaceTemperature', 'SurfacePressure', 'Landable', 'Materials', 'Composition', 'SemiMajorAxis', 'Eccentricity', 'OrbitalInclination', 'Periapsis', 'OrbitalPeriod', 'AscendingNode', 'MeanAnomaly', 'RotationPeriod', 'AxialTilt']

    data = {k: v for k,v in jsondata.items() if k in KEY_BODY and v != ""}
    detail = {k: v for k,v in jsondata.items() if k in LIST_BODY_DETAIL and v != ""}

    if len(detail) > 0:
        data["detail"] = json.dumps(detail, ensure_ascii=False)

    if data.keys() >= {"SystemAddress", "BodyID"}:
        tmp1 = "INSERT INTO body_tbl(system_id, body_id,"
        tmp2 = " VALUES(:SystemAddress, :BodyID,"
        tmp3 = " ON CONFLICT(system_id, body_id) DO UPDATE SET"
        for k,v in data.items():
            match k:
                case "SystemAddress":
                    pass
                case "BodyID":
                    pass
                case "detail":
                    tmp1 += f" detail,"
                    tmp2 += f" jsonb(:detail),"
                    tmp3 += f" detail=jsonb_patch(detail, excluded.detail),"
                case _:
                    tmp1 += f" {KEY_BODY[k]},"
                    tmp2 += f" :{k},"
                    tmp3 += f" {KEY_BODY[k]}=excluded.{KEY_BODY[k]},"
        query = re.sub(',$',') ',tmp1) + re.sub(',$',') ',tmp2) + re.sub(',$',';',tmp3)
        # logger.debug(query)
        db = getConnection()
        cur = db.cursor()
        cur.execute(query,data)
        cur.close()


def updateMarket(jsondata:dict):
    KEY_MARKET = {"MarketID":"id", "StationName":"name", "Name":"name", "SystemAddress":"system_id", "BodyID":"body_id", "StationType":"type", "StationGovernment_Localised":"goverment", "StationEconomy_Localised":"economy", "StationFaction":"stationfaction_id", "DistFromStarLS":"distfromstarls", "pads":"pads", "padm":"padm", "padl":"padl", "timestamp":"updated_at"}
    LIST_MARKET_DETAIL = ['StationServices']
    
    data = {k:v for k,v in jsondata.items() if k in KEY_MARKET and v != ""}
    detail = {k:v for k,v in jsondata.items() if k in LIST_MARKET_DETAIL and v != ""}

    if "StationFaction" in data:
        data["StationFaction"] = data["StationFaction"]["Name"]

    KEY_PADS = {"Small":"pads", "Medium":"padm", "Large":"padl"}
    if "LandingPads" in jsondata:
        data.update({KEY_PADS[k]:v for k,v in jsondata["LandingPads"].items() if k in KEY_PADS})

    if len(detail) > 0:
        data['detail'] = json.dumps(detail, ensure_ascii=False)

    if "MarketID" in data:
        tmp1 = "INSERT INTO market_tbl(id,"
        tmp2 = " SELECT :MarketID,"
        tmpextbl = ""
        tmp3 = " ON CONFLICT(id) DO UPDATE SET"
        for k,v in data.items():
            match k:
                case "MarketID":
                    pass
                case "StationFaction":
                    tmp1 += f" {KEY_MARKET[k]},"
                    tmp2 += f" faction_tbl.id,"
                    tmpextbl = f" FROM faction_tbl WHERE faction_tbl.name=:{k} "
                    tmp3 += f" {KEY_MARKET[k]}=excluded.{KEY_MARKET[k]},"
                case "detail":
                    tmp1 += f" detail,"
                    tmp2 += f" jsonb(:detail),"
                    tmp3 += f" detail=jsonb_patch(detail, excluded.detail),"
                case _:
                    tmp1 += f" {KEY_MARKET[k]},"
                    tmp2 += f" :{k},"
                    tmp3 += f" {KEY_MARKET[k]}=excluded.{KEY_MARKET[k]},"
        query = re.sub(',$',') ',tmp1) + re.sub(',$',' ',tmp2) + tmpextbl + re.sub(',$',';',tmp3)
        logger.debug(query)
        db = getConnection()
        cur = db.cursor()
        cur.execute(query,data)
        cur.close()


def updateFaction(jsondata:dict):
    KEY_FACTION = {"Name":"name", "Government":"government", "Allegiance":"allegiance", "MyReputation":"myreputation"}

    if "Factions" in jsondata:
        factions = jsondata["Factions"]
        data = [{KEY_FACTION[k]:v for k,v in fac.items() if k in KEY_FACTION} for fac in factions]
        for d in data:
            d["updated_at"] = jsondata["timestamp"]

        if len(data) > 0:
            tmp1 = "INSERT INTO faction_tbl(name,"
            tmp2 = " VALUES(:name,"
            tmp3 = " ON CONFLICT(name) DO UPDATE SET"
            for k,v in data[0].items():
                match k:
                    case "name":
                        pass
                    case _:
                        tmp1 += f" {k},"
                        tmp2 += f" :{k},"
                        tmp3 += f" {k}=excluded.{k},"
            query = re.sub(',$',') ',tmp1) + re.sub(',$',') ',tmp2) + re.sub(',$',';',tmp3)
            conn = getConnection()
            conn.executemany(query, data)


def updateSystemFaction(jsondata:dict):
    KEY_SYSTEM_FACTION = {"SystemAddress":"system_id","Name":"name", "FactionState":"state", "Influence":"influence", "Happiness":"happiness", "timestamp":"updated_at"}
    LIST_STATE = ["ActiveStates", "PendingStates"]

    if "Factions" in jsondata:
        factions = mergeLocalizedArray(jsondata["Factions"])
        for fac in factions:
            # merge ActiveState/PendingState to state field
            state = {k:v for k, v in fac.items() if k in LIST_STATE}
            fac["FactionState"] = json.dumps(state, ensure_ascii=False)
            fac["timestamp"] = jsondata["timestamp"]

        data = [{k:v for k,v in fac.items() if k in KEY_SYSTEM_FACTION} for fac in factions]
        
        for v in data:
            v["SystemAddress"] = jsondata["SystemAddress"]

        if len(data) > 0:
            tmp1 = "INSERT INTO system_faction_tbl(system_id, faction_id,"
            tmp2 = " SELECT :SystemAddress, faction_tbl.id,"
            tmp3 = " FROM faction_tbl WHERE name=:Name "
            tmp4 = " ON CONFLICT(system_id, faction_id) DO UPDATE SET"
            for k,v in data[0].items():
                match k:
                    case "SystemAddress":
                        pass
                    case "Name":
                        pass
                    case "FactionState":
                        tmp1 += f" state,"
                        tmp2 += f" jsonb(:FactionState),"
                        tmp4 += f" state=excluded.state,"
                    case _:
                        tmp1 += f" {KEY_SYSTEM_FACTION[k]},"
                        tmp2 += f" :{k},"
                        tmp4 += f" {KEY_SYSTEM_FACTION[k]}=excluded.{KEY_SYSTEM_FACTION[k]},"
            query = re.sub(',$',') ',tmp1) + re.sub(',$',' ',tmp2) + tmp3 + re.sub(',$',';',tmp4)
            conn = getConnection()
            conn.executemany(query, data)

def mergeLocalizedArray(jsonarray:list):
    for item in jsonarray:
        mergeLocalized(item)
    return jsonarray

def mergeLocalized(jsondata:dict) -> dict:
    listLocaliedKey = [k for k in jsondata.keys() if "_Localised" in k]
    for k in listLocaliedKey:
        jsondata[k.replace("_Localised", "")] = jsondata[k]
    return jsondata

def updateStatistics(jsondata:dict):
    KEY_STATISTICS = {"timestamp":"updated_at"}
    LIST_STATISTICS_DETAIL = ['Bank_Account', 'Combat', 'Crime', 'Smuggling', 'Trading', 'Mining', 'Exploration', 'Passengers', 'Search_And_Rescue', 'Squadron', 'Crafting', 'Crew', 'Multicrew', 'Material_Trader_Stats', 'Exobiology']

    data = {k:v for k,v in jsondata.items() if k in KEY_STATISTICS and v != ""}
    detail = {k:v for k,v in jsondata.items() if k in LIST_STATISTICS_DETAIL and v != ""}

    if len(detail) > 0:
        data['detail'] = json.dumps(detail, ensure_ascii=False)

        tmp1 = "INSERT INTO statistics_tbl(updated_at,"
        tmp2 = " VALUES(:timestamp,"
        tmp3 = " ON CONFLICT(updated_at) DO UPDATE SET"
        for k,v in data.items():
            match k:
                case "timestamp":
                    pass
                case "detail":
                    tmp1 += f" detail,"
                    tmp2 += f" jsonb(:detail),"
                    tmp3 += f" detail=jsonb_patch(detail, excluded.detail),"
                case _:
                    tmp1 += f" {KEY_STATISTICS[k]},"
                    tmp2 += f" :{k},"
                    tmp3 += f" {KEY_STATISTICS[k]}=excluded.{KEY_STATISTICS[k]},"
        query = re.sub(',$',') ',tmp1) + re.sub(',$',') ',tmp2) + re.sub(',$',';',tmp3)
        logger.debug(query)
        db = getConnection()
        cur = db.cursor()
        cur.execute(query,data)
        cur.close()


def getEdLogList(dir:str):
    p = Path(dir)
    edlogs = list(p.glob('**/*.log'))
    edlogs.sort()
    return edlogs


if __name__ == "__main__":
    main()


