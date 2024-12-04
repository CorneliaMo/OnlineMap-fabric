#coding=utf-8
from json import loads
from requests import post
from time import time, sleep, strftime, localtime
from PIL import Image, ImageDraw
import sqlite3
import logging
from sys import path
from math import floor
from threading import Thread
import atexit
import os
import shutil
from flask import Flask, request
from waitress import serve

class HttpAPI:
    addr = None
    lastCacheTime = {}
    cache = {}
    debug = None
    headers = None
    lastOnline = None
    logger = logging.getLogger("HttpAPI")

    def __init__(self, addr: str, debug: bool, headers: dict = {"Connection": "close"}):
        self.addr = addr
        self.headers = headers
        self.lastCacheTime["dimensions"] = None
        self.cache["dimensions"] = None
        self.lastCacheTime["serverDistance"] = None
        self.cache["viewDistance"] = None
        self.cache["simulationDistance"] = None
        self.debug = debug

    def defaultRequest(self, operation: str):
        return {"operation": operation}

    def postJson(self, json: dict):
        return post(self.addr, headers=self.headers, json=json)
    
    def getDimensions(self, reload: bool = False) -> list:
        if (self.lastCacheTime["dimensions"] != None):
            if (time() - self.lastCacheTime["dimensions"] > 600):
                reload = True
        else:
            reload = True
        if reload:
            json = self.defaultRequest("getDimensions")
            for tries in range(3):
                try:
                    re = self.postJson(json)
                    if self.debug:
                        self.logger.info("Try " + str(tries) + " getDimensions")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                    data = loads(re.text)
                    if (data["status"]!=0):
                        self.cache["dimensions"] = data["dimensions"]
                        self.lastCacheTime["dimensions"] = time()
                        break
                except BaseException as e:
                    self.logger.error(e)
        return self.cache["dimensions"]

    def getPlayers(self, dimension: str = ""):
        json = self.defaultRequest("getPlayers")
        if (len(dimension)!=0):
            json["dimension"] = dimension
        players = None
        for tries in range(3):
            try:
                re = self.postJson(json)
                if self.debug:
                        self.logger.info("Try " + str(tries) + " getPlayers")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                data = loads(re.text)
                if (data["status"]!=0):
                    players = data["players"]
                    break
            except BaseException as e:
                self.logger.error(e)
        return players

    def getPlayerPos(self, player: str):
        pos = None
        if (player in self.getPlayers()):
            json = self.defaultRequest("getPlayerPos")
            json["player"] = player
            for tries in range(3):
                try:
                    re = self.postJson(json)
                    if self.debug:
                        self.logger.info("Try " + str(tries) + " getPlayerPos")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                    data = loads(re.text)
                    if (data["status"]!=0):
                        pos = [(data["x"], data["y"], data["z"]), data["dimension"]]
                        break
                except BaseException as e:
                    self.logger.error(e)
        return pos

    """
    def getPlayerViewDistanceResponse(self, player: str, reload: bool = False):
        if (self.lastCacheTime["playerViewDistance"] != None):
            if (time() - self.lastCacheTime["dimensions"] > 3600 or player not in self.cache["playerViewDistance"].keys()):
                reload = True
        else:
            reload = True
        if reload:
            players = self.getPlayers()
            for forplayer in players:
                json = self.defaultRequest("getPlayerViewDistance")
                json["player"] = forplayer
                for tries in range(3):
                    try:
                        re = self.postJson(json)
                        if self.debug:
                            self.logger.info("Try " + str(tries) + " getPlayerViewDistance")
                            self.logger.info("Send " + str(json))
                            self.logger.info("Get "+re.text)
                        data = loads(re.text)
                        if (data["status"]!=0):
                            self.cache["playerViewDistance"][forplayer] = data["viewDistance"]
                            self.lastCacheTime["playerViewDistance"] = time()
                            break
                    except BaseException as e:
                        self.logger.error(e)
        return self.cache["playerViewDistance"][player]
    """

    def getServerDistance(self, reload: bool = False):
        if (self.lastCacheTime["serverDistance"] != None):
            if (time() - self.lastCacheTime["serverDistance"] > 3600):
                reload = True
        else:
            reload = True
        if reload:
            json = self.defaultRequest("getServerDistance")
            for tries in range(3):
                try:
                    re = self.postJson(json)
                    if self.debug:
                        self.logger.info("Try " + str(tries) + " getServerDistance")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                    data = loads(re.text)
                    if (data["status"]!=0):
                        self.cache["viewDistance"] = data["viewDistance"]
                        self.cache["simulationDistance"] = data["simulationDistance"]
                        self.lastCacheTime["serverDistance"] = time()
                        break
                except BaseException as e:
                    self.logger.error(e)
        return {"viewDistance": self.cache["viewDistance"], "simulationDistance": self.cache["simulationDistance"]}
    
    def getLoadedChunks(self, dimension: str):
        self.getDimensions()
        loadedChunks = []
        if (dimension in self.cache["dimensions"]):
            json = self.defaultRequest("getLoadedChunks")
            json["dimension"] = dimension
            for tries in range(3):
                try:
                    re = self.postJson(json)
                    if self.debug:
                        self.logger.info("Try " + str(tries) + " getLoadedChunks")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                    data = loads(re.text)
                    if (data["status"]!=0):
                        for chunk in data["loadedChunkPos"]:
                            loadedChunks.append((chunk["x"], chunk["z"]))
                        break
                except BaseException as e:
                    self.logger.error(e)
        return loadedChunks

    def getChunkColors(self, dimension: str, chunkX: int, chunkZ: int):
        colors = None
        if (dimension in self.getDimensions()):
            json = self.defaultRequest("getChunkColors")
            json["dimension"] = dimension
            json["x"] = chunkX
            json["z"] = chunkZ
            for tries in range(3):
                try:
                    re = self.postJson(json)
                    if self.debug:
                        self.logger.info("Try " + str(tries) + " getChunkColors")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                    data = loads(re.text)
                    if (data["status"]!=0):
                        colorMap = data["colormap"]
                        colors = []
                        for color in colorMap:
                            r = color & 0xFF
                            g = color >> 8 & 0xFF
                            b = color >> 16 & 0xFF
                            colors.append((r, g, b))
                        break
                    elif (data["errMsg"]=="Chunk isn't loaded"):
                        break
                except BaseException as e:
                    self.logger.error(e)
        return colors
    
    def isChunkLoaded(self, dimension: str, x: int, z: int):
        loaded = False
        if dimension in self.getDimensions():
            json = self.defaultRequest("isChunkLoaded")
            json["dimension"] = dimension
            json["x"] = x
            json["z"] = z
            for tries in range(3):
                try:
                    re = self.postJson(json)
                    if self.debug:
                        self.logger.info("Try " + str(tries) + " getChunkColors")
                        self.logger.info("Send " + str(json))
                        self.logger.info("Get "+re.text)
                    data = loads(re.text)
                    if (data["status"]!=0):
                        loaded = data["isLoaded"]
                        break
                    elif (data["errMsg"]=="Chunk isn't loaded"):
                        break
                except BaseException as e:
                    self.logger.error(e)
        return loaded

    def getSpawnChunk(self):
        pos = {"x":0,"z": 0}
        json = self.defaultRequest("getSpawnChunk")
        for tries in range(3):
            try:
                re = self.postJson(json)
                if self.debug:
                    self.logger.info("Try " + str(tries) + " getChunkColors")
                    self.logger.info("Send " + str(json))
                    self.logger.info("Get "+re.text)
                data = loads(re.text)
                if (data["status"]!=0):
                    pos["x"] = data["x"]
                    pos["z"] = data["z"]
                    break
            except BaseException as e:
                self.logger.error(e)
        return pos

class Util:
    logger = logging.getLogger("Util")

    def drawColors(self, img: Image, colors: list, factor: int, startX: int, startY: int):
        draw = ImageDraw.Draw(img)
        for Y in range(16):
            for X in range(16):
                draw.rectangle((startX+X*factor, startY+Y*factor, startX+X*factor+factor, startY+Y*factor+factor), fill=colors[Y*16+X])
        
    def getChunksRange(self, chunks: list):
        minX = chunks[0][0]
        maxX = minX
        minY = chunks[0][1]
        maxY = minY
        for pos in chunks:
            minX = min(minX, pos[0])
            maxX = max(maxX, pos[0])
            minY = min(minY, pos[1])
            maxY = max(maxY, pos[1])
        return minX, minY, maxX, maxY

    def drawLoadedChunks(self, httpApi: HttpAPI, dimension: str, factor: int) -> Image:
        chunks = httpApi.getLoadedChunks(dimension)
        if (len(chunks) == 0):
            return None
        else:
            minX, minY, maxX, maxY = self.getChunksRange(chunks)
            self.logger.info("Draw from " + str((minX, minY)) + " to " + str((maxX, maxY)))
            img = Image.new("RGB", ((maxX-minX+1)*16*factor, (maxY-minY+1)*16*factor), (255, 255, 255))
            for pos in chunks:
                self.logger.info("Draw " + str(pos))
                colors = httpApi.getChunkColors(dimension, pos[0], pos[1])
                if colors != None:
                    self.drawColors(img, colors, factor, (pos[0]-minX)*16*factor, (pos[1]-minY)*16*factor)
            return img
    
    def getAllLoadedChunkColor(httpApi: HttpAPI, dimension: str):
        chunks = httpApi.getLoadedChunks(dimension)
        chunkColors = []
        if (len(chunks) != 0):
            for pos in chunks:
                chunkColors.append([pos, httpApi.getChunkColors(dimension, pos[0], pos[1])])
        return chunkColors
    
    def getCorrespondingChunkPos(playerPos: tuple):
        return (int(floor(float(playerPos[0]) / 16.0)), int(floor(float(playerPos[2]) / 16.0)))
    
    def getPlayerLoadChunks(playerPos: tuple, viewDistance: int):
        centerChunk = Util.getCorrespondingChunkPos(playerPos)
        minX = centerChunk[0] - viewDistance - 1
        maxX = centerChunk[0] + viewDistance + 1
        minZ = centerChunk[1] - viewDistance - 1
        maxZ = centerChunk[1] + viewDistance + 1
        chunks = []
        for x in range(minX, maxX+1):
            for z in range(minZ, maxZ+1):
                chunks.append((x, z))
        return chunks
        
class SQLCache:
    httpApi = None
    connection = None
    debug = False
    processThread = None
    localfile = None
    tablesExist = {}
    lastdumptime = None
    logger = logging.getLogger("SQLCache")

    def __init__(self, httpApi: HttpAPI, localFile: str, debug: bool = False):
        self.httpApi = httpApi
        self.localfile = localFile
        self.loadFromFile()
        atexit.register(self.dumpToFile)
        self.debug = debug
        self.interval = 10
        self.lastdumptime = time()
        #self.dimensions = [dimension.replace(":", "_") for dimension in httpApi.getDimensions()]
        self.initTables()
        #self.updateAll()

    def dumpToFile(self):
        self.logger.info("start dump memory db")
        if os.path.exists(self.localfile):
            shutil.copy(self.localfile, self.localfile+".bak-"+strftime('%Y-%m-%d-%H-%M-%S', localtime()))
            shutil.os.remove(self.localfile)
        sqldump = list(self.connection.iterdump())
        localconn = sqlite3.connect(self.localfile, check_same_thread=False)
        for line in sqldump:
            localconn.execute(line)
        localconn.commit()
        localconn.close()
        self.logger.info("dump memory db to " + self.localfile)

    def loadFromFile(self):
        self.logger.info("load db from " + self.localfile + " to memory")
        self.connection = sqlite3.connect(':memory:', check_same_thread=False)
        if os.path.exists(self.localfile):
            localconn = sqlite3.connect(self.localfile, check_same_thread=False)
            sqldump = list(localconn.iterdump())
            for line in sqldump:
                self.connection.execute(line)
            self.connection.commit()

    def getDimensions(self):
        return [dimension.replace(":", "_") for dimension in self.httpApi.getDimensions()]
    
    def execute(self, command: str, commit: bool = False):
        if self.debug:
            self.logger.info("SQL command> " + command)
        result = self.connection.execute(command)
        if commit:
            self.connection.commit()
        if self.debug:
            self.logger.info("SQL result> success" if result!=None else "SQL result> No result")
            #self.logger.info(("SQL result>\n"+self.cursorToString("SQL result> ", self.connection.execute(command))) if result!=None else "SQL result> null")
        return result
        
    def cursorToString(self, prefix: str, cursor: sqlite3.Cursor) -> str:
        result = ""
        if cursor != None and cursor.description != None:
            if len(cursor.description)!=0:
                columns = []
                result += prefix
                for description in cursor.description:
                    columns.append(description[0])
                    result += description[0] + "   "
                result += "\n"
                for row in cursor:
                    result += prefix
                    for i in range(len(columns)):
                        result += str(row[i])+"  "
                    result += "\n"
            return result[:-2] if result!="" else result
        else:
            return prefix+" null"

    def convertFromSpecialChar(self, string: str):
        convertTable = [("/", "//"), ("'", "\""), ("[", "/["), ("]", "/]"), ("%", "/%"), ("&", "/&"), ("_", "/_"), ("(", "/("), (")", "/)")]
        converted = str(string)
        for convert in convertTable:
            converted.replace(convert[0], convert[1])
        return converted
    
    def insertMapColor(self, table: str, pos: tuple, colors: list):
        prevDebug = self.debug
        self.debug = False
        if self.isColumnExist(table, {"x": pos[0], "z": pos[1]}):
            command = "UPDATE " + table + " SET " + "colors=\'" + str(colors) + "\' WHERE x=" + str(pos[0]) + " AND z=" + str(pos[1]) + ";"
            self.execute(command, True)
        else:
            columns = "(x, z, colors)"
            values = "(" + str(pos[0]) + ", " + str(pos[1]) + ", \'" + str(colors) + "\')"
            self.insert(table, columns, values)
        self.debug = prevDebug

    def insert(self, table: str, columns: str, values: str):
        command = "INSERT INTO " + table + " " + columns + " VALUES " + values + ";"
        self.execute(command, True)

    def createTable(self, name: str, colomns: str):
        cursor = self.connection.cursor()
        command = "CREATE TABLE " + name + " " + colomns + ";"
        self.execute(command, True)

    def isTableExist(self, name: str):
        command = "select count(*) from sqlite_master where type='table' and name = '" + name + "';"
        result = list(self.execute(command))
        return result[0][0] != 0

    def isColorExist(self, dimension: str, pos: tuple):
        if self.isTableExist(dimension):
            return self.isColumnExist(dimension, {"x":pos[0], "z":pos[1]})
        else:
            return False
        
    def isColumnExist(self, table: str, keys: dict):
        command = ""
        for key in keys.keys():
            command += " AND "
            command += str(key) + "=" + (str(keys[key])) if not isinstance(keys[key], str) else ("'" + keys[key]+"'")
        command = command[4:]
        command = "SELECT COUNT(*) FROM " + table + " WHERE " + command
        result = list(self.execute(command))
        return result[0][0] != 0

    def isValueExist(self, table: str, value: str):
        command = "SELECT CASE WHEN EXISTS (SELECT 1 FROM " + table + " WHERE condition) THEN 1 ELSE 0 END AS exists_flag;"
        return self.execute(command) != ""
    
    def initTables(self):
        for dimension in self.getDimensions():
            if not self.isTableExist(dimension):
                self.createTable(dimension, "(x INT NOT NULL, z INT NOT NULL, colors TEXT NOT NULL, PRIMARY KEY(x, z))")
    
    def getColors(self, dimension: str, pos: tuple):
        if self.isColorExist(dimension, pos):
            command = "SELECT colors FROM " + dimension + " WHERE x=" + str(pos[0]) + " AND z=" + str(pos[1]) + ";"
            result = list(self.execute(command))
            return eval(result[0][0])
        else:
            return None
        
    def updateAll(self):
        for dimension in self.getDimensions():
            allLoadedChunkColor = Util.getAllLoadedChunkColor(self.httpApi, dimension.replace("_", ":"))
            for loadedChunkColor in allLoadedChunkColor:
                self.insertMapColor(dimension, loadedChunkColor[0], loadedChunkColor[1])

    def updateProcess(self):
        self.updateAll()
        while True:
            onlinePlayers = self.httpApi.getPlayers()
            if self.debug:
                self.logger.info(str(onlinePlayers))
            updated = {}
            for dimension in self.httpApi.getDimensions():
                updated[dimension] = []
            for player in onlinePlayers:
                getResult = self.httpApi.getPlayerPos(player)
                playerPos = getResult[0]
                dimension = getResult[1]
                chunks = Util.getPlayerLoadChunks(playerPos, self.httpApi.getServerDistance()["viewDistance"])
                for chunk in chunks:
                    if chunk not in updated[dimension]:
                        if self.httpApi.isChunkLoaded(dimension, chunk[0], chunk[1]):
                            colors = self.httpApi.getChunkColors(dimension, chunk[0], chunk[1])
                            if colors != None:
                                updated[dimension].append(chunk)
                                self.insertMapColor(dimension.replace(":", "_"), chunk, colors)
                                if self.debug:
                                    self.logger.info("Update " + str(chunk) + " at " + dimension)
            if time()-self.lastdumptime >= 300:
                self.dumpToFile()
                self.lastdumptime = time()
            sleep(self.interval)
    
    def startProcess(self, interval: int):
        self.interval = interval
        if self.processThread == None:
            self.processThread = Thread(target=self.updateProcess)
            self.processThread.start()
        elif not self.processThread.is_alive():
            self.processThread = Thread(target=self.updateProcess)
            self.processThread.start()

    def getChunks(self, dimension: str):
        chunks = []
        if dimension in self.getDimensions() and self.isTableExist(dimension):
            command = "SELECT x, z FROM " + dimension + ";"
            queryResult = self.execute(command)
            if queryResult != None:
                chunks = list(queryResult)
        return chunks
    
app = Flask(__name__)
sqlcache = None
flaskLogger = logging.getLogger("flask")

class HttpService:

    def errorMsg(errMsg):
        return {"errMsg": errMsg, "status": 0}

    def successMsg():
        return {"errMsg": "", "status": 1}

    def sendResponse(data):
        return (data, 200, [("Access-Control-Allow-Origin", "*")])

    @app.route("/getDimensions")
    def getDimensions():
        if sqlcache == None:
            return HttpService.sendResponse(HttpService.errorMsg("SQLcache not init"))
        else:
            result = HttpService.successMsg()
            result["dimensions"] = sqlcache.getDimensions()
            return HttpService.sendResponse(result)

    @app.route("/getChunks", methods=['GET'])
    def getChunks():
        dimension = request.args.get("dimension", default=None)
        if sqlcache == None:
            return HttpService.sendResponse(HttpService.errorMsg("SQLcache not init"))
        elif dimension == None:
            return HttpService.sendResponse(HttpService.errorMsg("Provide dimension"))
        elif dimension not in sqlcache.getDimensions():
            return HttpService.sendResponse(HttpService.errorMsg("Dimension not exist"))
        else:
            result = HttpService.successMsg()
            result["chunks"] = sqlcache.getChunks(dimension)
            return HttpService.sendResponse(result)

    @app.route("/getChunkColors")
    def getChunkColors():
        dimension = request.args.get("dimension", default=None)
        x = None
        z = None
        try:
            x = request.args.get("x", type=int, default=None)
            z = request.args.get("z", type=int, default=None)
        except BaseException as e:
            flaskLogger.error(e)
        if sqlcache==None:
            return HttpService.sendResponse(HttpService.errorMsg("SQLcache not init"))
        elif dimension==None or x==None or z==None:
            return HttpService.sendResponse(HttpService.errorMsg("Provide dimension(str), x(int), z(int)"))
        elif dimension not in sqlcache.getDimensions():
            return HttpService.sendResponse(HttpService.errorMsg("Dimension not exist"))
        elif not sqlcache.isColorExist(dimension, (x, z)):
            return HttpService.sendResponse(HttpService.errorMsg("Chunk not cached"))
        else:
            result = HttpService.successMsg()
            result["colors"] = sqlcache.getColors(dimension, (x, z))
            return HttpService.sendResponse(result)

    @app.route("/getSpawnChunk")
    def getSpawnChunk():
        if sqlcache==None:
            return HttpService.sendResponse(HttpService.errorMsg("SQLcache not init"))
        else:
            result = HttpService.successMsg()
            pos = sqlcache.httpApi.getSpawnChunk()
            result["x"] = pos["x"]
            result["z"] = pos["z"]
            return HttpService.sendResponse(result)

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sqlcache = SQLCache(HttpAPI("http://127.0.0.1:10090/", False), path[0]+"//map.db", False)
    sqlcache.startProcess(5)
    serve(app, host="0.0.0.0", port=5000, threads=32)
    