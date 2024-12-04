package cn.corneliamo;

import cn.corneliamo.mixin.ThreadedAnvilChunkStorageMixin;
import cn.corneliamo.responses.*;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import net.minecraft.block.BlockState;
import net.minecraft.block.Blocks;
import net.minecraft.block.MapColor;
import net.minecraft.fluid.FluidState;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.PlayerManager;
import net.minecraft.server.network.ServerPlayerEntity;
import net.minecraft.server.world.ChunkHolder;
import net.minecraft.server.world.ServerWorld;
import net.minecraft.server.world.ThreadedAnvilChunkStorage;
import net.minecraft.util.math.BlockPos;
import net.minecraft.util.math.Direction;
import net.minecraft.world.Heightmap;
import net.minecraft.world.World;
import net.minecraft.world.chunk.WorldChunk;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HttpMapServer {
    private MinecraftServer server;
    private HashMap<String, ServerWorld> worlds;
    private HttpServer httpServer;
    private Logger LOGGER;
    private static boolean debug;
    class MapHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String responseContent;
            Response responseObject;
            //LOGGER.info("Get " +exchange.getRequestMethod()+" request from " + exchange.getRemoteAddress());
            if (exchange.getRequestMethod().equals("POST")){
                // Read post data
                InputStream inputStream = exchange.getRequestBody();
                int requestLength = 0;
                Instant startTime = Instant.now();
                while (requestLength == 0 && Duration.between(startTime, Instant.now()).toSeconds() < 3) requestLength = inputStream.available();
                if (requestLength == 0) responseObject = errorResponse("Empty request");
                else {
                    byte[] requestBytes = new byte[requestLength];
                    inputStream.read(requestBytes, 0, requestLength);
                    inputStream.close();
                    String requestString = new String(requestBytes);
                    //LOGGER.info("POST data "+requestString);
                    JSONObject jsonObject = JSON.parseObject(requestString);
                    if (jsonObject.containsKey("operation")) {
                        // operation route
                        String operation = jsonObject.getString("operation");
                        if (HttpMapServer.debug) LOGGER.info("Get " +operation+" request from " + exchange.getRemoteAddress());
                        switch (operation) {
                            case "getDimensions" -> responseObject = getDimensions();
                            case "getPlayers" -> responseObject = getPlayers(jsonObject);
                            case "getPlayerPos" -> responseObject = getPlayerPos(jsonObject);
                            case "getPlayerViewDistance" -> responseObject = getPlayerViewDistance(jsonObject);
                            case "getLoadedChunks" -> responseObject = getLoadedChunks(jsonObject);
                            case "getChunkColors" -> responseObject = getChunkColors(jsonObject);
                            case "isChunkLoaded" -> responseObject = isChunkLoaded(jsonObject);
                            case "getServerDistance" -> responseObject = getServerDistance();
                            case "getSpawnChunk" -> responseObject = getSpawnChunk();
                            default -> responseObject = errorResponse("Wrong operation " + operation);
                        }
                    } else responseObject = errorResponse("Provide operation");
                }
            } else responseObject = errorResponse("Should use POST method");
            // send response object by JSON
            responseContent = JSON.toJSONString(responseObject);
            byte[] responseBytes = responseContent.getBytes(StandardCharsets.UTF_8);
            //LOGGER.info("Send response " + responseContent);
            exchange.getResponseHeaders().add("Content-type", "application/json");
            exchange.getResponseHeaders().add("Connection", "close");
            exchange.sendResponseHeaders(200, responseBytes.length);
            OutputStream outputStream = exchange.getResponseBody();
            outputStream.write(responseBytes);
            outputStream.flush();
            outputStream.close();
            exchange.close();
        }
        private @NotNull Response getDimensions(){
            GetDimensionsResponse response = (GetDimensionsResponse) successResponse(new GetDimensionsResponse());
            response.dimensions = new ArrayList<>(worlds.keySet());
            return response;
        }
        private @NotNull Response getPlayers(JSONObject jsonObject){
            if (jsonObject.containsKey("dimension")){
                if (worlds.containsKey(jsonObject.getString("dimension"))){
                    GetPlayersResponse getPlayersResponse = (GetPlayersResponse) successResponse(new GetPlayersResponse());
                    getPlayersResponse.players = new ArrayList<>();
                    List<ServerPlayerEntity> serverPlayerEntityList = worlds.get(jsonObject.getString("dimension")).getPlayers();
                    for (ServerPlayerEntity serverPlayerEntity : serverPlayerEntityList) getPlayersResponse.players.add(serverPlayerEntity.getName().getString());
                    return getPlayersResponse;
                } else return errorResponse("Dimension not exist");
            } else {
                GetPlayersResponse getPlayersResponse = (GetPlayersResponse) successResponse(new GetPlayersResponse());
                getPlayersResponse.players = new ArrayList<>();
                for (String dimension : worlds.keySet()){
                    List<ServerPlayerEntity> serverPlayerEntityList = worlds.get(dimension).getPlayers();
                    for (ServerPlayerEntity serverPlayerEntity : serverPlayerEntityList) getPlayersResponse.players.add(serverPlayerEntity.getName().getString());
                }
                return getPlayersResponse;
            }
        }
        private @NotNull Response getPlayerPos(JSONObject jsonObject){
            if (jsonObject.containsKey("player")){
                String name = jsonObject.getString("player");
                String dimension = null;
                ServerPlayerEntity player = null;
                for (String key : worlds.keySet()){
                    ServerWorld world = worlds.get(key);
                    for (ServerPlayerEntity playerEntity : world.getPlayers())
                        if (playerEntity.getName().getString().equals(name)){
                            dimension = key;
                            player = playerEntity;
                            break;
                        }
                    if (player != null) break;
                }
                if (player != null){
                    GetPlayerPosResponse getPlayerPosResponse = (GetPlayerPosResponse) successResponse(new GetPlayerPosResponse());
                    getPlayerPosResponse.x = player.getBlockX();
                    getPlayerPosResponse.y = player.getBlockY();
                    getPlayerPosResponse.z = player.getBlockZ();
                    getPlayerPosResponse.dimension = dimension;
                    return getPlayerPosResponse;
                } else return errorResponse("Player " + name + " not found");
            } else return errorResponse("Provide player name");
        }
        private @NotNull Response getPlayerViewDistance(JSONObject jsonObject){
            if (jsonObject.containsKey("player")){
                String name = jsonObject.getString("player");
                String dimension = null;
                ServerPlayerEntity player = null;
                for (String key : worlds.keySet()){
                    ServerWorld world = worlds.get(key);
                    for (ServerPlayerEntity playerEntity : world.getPlayers())
                        if (playerEntity.getName().getString().equals(name)){
                            dimension = key;
                            player = playerEntity;
                            break;
                        }
                    if (player != null) break;
                }
                if (player != null){
                    GetPlayerViewDistanceResponse getPlayerViewDistanceResponse = (GetPlayerViewDistanceResponse) successResponse(new GetPlayerViewDistanceResponse());
                    getPlayerViewDistanceResponse.viewDistance = player.getViewDistance();
                    return getPlayerViewDistanceResponse;
                } else return errorResponse("Player " + name + " not found");
            } else return errorResponse("Provide player name");
        }
        private @NotNull Response getServerDistance(){
            GetServerDistanceResponse getServerDistanceResponse = (GetServerDistanceResponse) successResponse(new GetServerDistanceResponse());
            PlayerManager playerManager = server.getPlayerManager();
            getServerDistanceResponse.simulationDistance = playerManager.getSimulationDistance();
            getServerDistanceResponse.viewDistance = playerManager.getViewDistance();
            return getServerDistanceResponse;
        }
        private @NotNull Response getLoadedChunks(JSONObject jsonObject){
            if (jsonObject.containsKey("dimension")){
                String dimension = jsonObject.getString("dimension");
                if (worlds.containsKey(dimension)) {
                    GetLoadedChunksResponse getLoadedChunksResponse = (GetLoadedChunksResponse) successResponse(new GetLoadedChunksResponse());
                    getLoadedChunksResponse.loadedChunkPos = new ArrayList<>();
                    ServerWorld serverWorld = worlds.get(dimension);
                    ThreadedAnvilChunkStorage threadedAnvilChunkStorage = serverWorld.getChunkManager().threadedAnvilChunkStorage;
                    for (ChunkHolder chunkHolder : ((ThreadedAnvilChunkStorageMixin) threadedAnvilChunkStorage).invokeEntryIterator()) {
                        WorldChunk worldChunk = chunkHolder.getWorldChunk();
                        if (worldChunk != null) getLoadedChunksResponse.loadedChunkPos.add(new HashMap<>(){{put('x', worldChunk.getPos().x);put('z', worldChunk.getPos().z);}});
                    }
                    return getLoadedChunksResponse;
                } else return errorResponse("Dimension not exist");
            } else return errorResponse("Provide dimension");
        }
        private @NotNull Response getChunkColors(JSONObject jsonObject){
            if (!jsonObject.containsKey("dimension")) return errorResponse("Provide dimension");
            if (!jsonObject.containsKey("x")) return errorResponse("Provide x");
            if (!jsonObject.containsKey("z")) return errorResponse("Provide z");
            if (!worlds.containsKey(jsonObject.getString("dimension"))) return errorResponse("Dimension " + jsonObject.getString("dimension") + " not exist");
            String dimension = jsonObject.getString("dimension");
            int x = jsonObject.getIntValue("x"), z = jsonObject.getIntValue("z");
            ServerWorld serverWorld = worlds.get(dimension);
            if (!serverWorld.isChunkLoaded(x, z)) return errorResponse("Chunk isn't loaded");
            GetChunkColorsResponse getChunkColorsResponse = (GetChunkColorsResponse) successResponse(new GetChunkColorsResponse());
            getChunkColorsResponse.colormap = MapRenderer.calculateColormap(serverWorld, x, z);
            return getChunkColorsResponse;
        }
        private @NotNull Response isChunkLoaded(JSONObject jsonObject){
            if (jsonObject.containsKey("dimension")){
                if (jsonObject.containsKey("x") && jsonObject.containsKey("z")){
                    if (worlds.containsKey(jsonObject.getString("dimension"))){
                        ServerWorld serverWorld = worlds.get(jsonObject.getString("dimension"));
                        IsChunkLoadedResponse isChunkLoadedResponse = (IsChunkLoadedResponse) successResponse(new IsChunkLoadedResponse());
                        isChunkLoadedResponse.isLoaded = serverWorld.isChunkLoaded(jsonObject.getIntValue("x", 0), jsonObject.getIntValue("z", 0));
                        return isChunkLoadedResponse;
                    } else return errorResponse("Dimension not exist");
                } else return errorResponse("Provide x and z");
            } else return errorResponse("Provide dimension");
        }
        private @NotNull Response errorResponse(String errMsg){
            Response response = new Response();
            response.status = 0;
            response.errMsg = errMsg;
            return response;
        }
        private @NotNull Response getSpawnChunk(){
            BlockPos blockPos = worlds.get("minecraft:overworld").getSpawnPos();
            int chunkX = (int) Math.floor((double) blockPos.getX() / 16.0F);
            int chunkZ = (int) Math.floor((double) blockPos.getZ() / 16.0F);
            GetSpawnChunkResponse getSpawnChunkResponse = (GetSpawnChunkResponse) this.successResponse(new GetSpawnChunkResponse());
            getSpawnChunkResponse.x = chunkX;
            getSpawnChunkResponse.z = chunkZ;
            return getSpawnChunkResponse;
        }

        private @NotNull Response successResponse(Response response){
            response.status = 1;
            response.errMsg = "";
            return response;
        }
    }
    HttpMapServer(String host, int port, MinecraftServer minecraftServer, Logger LOGGER, boolean debug) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(host, port), 0);
        httpServer.createContext("/", new MapHttpHandler());
        ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 32, 15, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(300));
        httpServer.setExecutor(executor);
        server = minecraftServer;
        worlds = new HashMap<>();
        this.LOGGER = LOGGER;
        HttpMapServer.debug = debug;
        for (ServerWorld world : server.getWorlds()) worlds.put(world.getDimensionKey().getValue().toString(), world);
        for (String key : worlds.keySet()) LOGGER.info(key);
        httpServer.start();
        LOGGER.info("HttpMapServer listening on " + host + " at " + port + ", debug " + (debug?"on":"off"));
    }
}

class MapRenderer {
    public static int[] calculateColormap(ServerWorld serverWorld, int chunkX, int chunkZ){
        int[] colormap = new int[256];
        int blocksPerPixel = 1;
        BlockPos.Mutable mutable = new BlockPos.Mutable();
        BlockPos.Mutable mutable2 = new BlockPos.Mutable();
        for (int sampleX=0;sampleX<16;sampleX++){
            double prevHeightSum = 0.0;
            for (int sampleZ=0;sampleZ<16;sampleZ++){
                int worldX = chunkX*16 + sampleX;
                int worldZ = chunkZ*16 + sampleZ;
                WorldChunk worldChunk = serverWorld.getChunk(chunkX, chunkZ);
                // if (!worldChunk.isEmpty())
                // should judge when invoke
                int fluidDepth = 0;
                double heightSum = 0.0;
                mutable.set(worldX, 0, worldZ);
                int height;
                BlockState blockState;
                if (serverWorld.getDimension().hasCeiling()){
                    // skip render ceiling
                    height = 70;
                    do {
                        mutable.setY(--height);
                        blockState = worldChunk.getBlockState(mutable);
                    } while (blockState.getMapColor(serverWorld, mutable) != MapColor.CLEAR && height > serverWorld.getBottomY());
                } else {
                    height = worldChunk.sampleHeightmap(Heightmap.Type.WORLD_SURFACE, worldX, worldZ) + 1;
                }
                // get a renderable blockstate
                if (height <= serverWorld.getBottomY() +1){
                    // is at bedrock layer
                    blockState = Blocks.BEDROCK.getDefaultState();
                } else {
                    // find the top renderable blockstate
                    do {
                        mutable.setY(--height);
                        blockState = worldChunk.getBlockState(mutable);
                    } while (blockState.getMapColor(serverWorld, mutable) == MapColor.CLEAR && height > serverWorld.getBottomY());
                    if (height > serverWorld.getBottomY() && !blockState.getFluidState().isEmpty()){
                        // is fluid, count fluid depth
                        int y = height - 1;
                        mutable2.set(mutable);
                        BlockState blockState2;
                        do {
                            mutable2.setY(y--);
                            blockState2 = worldChunk.getBlockState(mutable2);
                            fluidDepth++;
                        } while (y > serverWorld.getBottomY() && !blockState2.getFluidState().isEmpty());
                        blockState = getFluidStateIfVisible(serverWorld, blockState, mutable);
                    }
                }
                // because we sample one block for each pixel
                heightSum = (double) height / (double) (blocksPerPixel * blocksPerPixel);
                fluidDepth /= blocksPerPixel;
                MapColor mapColor = blockState.getMapColor(serverWorld, mutable);
                MapColor.Brightness brightness;
                if (mapColor == MapColor.WATER_BLUE) {
                    // meaning unclear formula, referring to original minecraft FilledMapItem.updateColor()
                    double f = (double) fluidDepth * 0.1 + (double)(sampleX + sampleZ & 1) * 0.2;
                    if (f < 0.5) brightness = MapColor.Brightness.HIGH;
                    else if (f > 0.9) brightness = MapColor.Brightness.LOW;
                    else brightness = MapColor.Brightness.NORMAL;
                } else {
                    double f = (heightSum - prevHeightSum) * 4.0 / (double) (blocksPerPixel + 4) + ((double)(sampleX + sampleZ & 1) - 0.5) * 0.4;
                    if (f > 0.6) brightness = MapColor.Brightness.HIGH;
                    else if (f < -0.6) brightness = MapColor.Brightness.LOW;
                    else brightness = MapColor.Brightness.NORMAL;
                }
                prevHeightSum = heightSum;
                colormap[sampleZ*16+sampleX] = mapColor.getRenderColor(brightness);
            }
        }
        return colormap;
    }
    private static BlockState getFluidStateIfVisible(World world, BlockState state, BlockPos pos) {
        FluidState fluidState = state.getFluidState();
        return !fluidState.isEmpty() && !state.isSideSolidFullSquare(world, pos, Direction.UP) ? fluidState.getBlockState() : state;
    }
}