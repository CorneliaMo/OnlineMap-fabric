package cn.corneliamo.responses;

import java.util.HashMap;
import java.util.List;

public class GetLoadedChunksResponse extends Response{
    public List<HashMap<Character, Integer>> loadedChunkPos;
}
