package cn.corneliamo;

import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OnlineMap implements ModInitializer {
	public static final String MOD_ID = "onlinemap";
	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);
	private HttpMapServer httpMapServer;
	private String host;
	private int port;
	private boolean debug;

	@Override
	public void onInitialize() {
		this.host = System.getProperty("MapHost")==null? "0.0.0.0":System.getProperty("MapHost");
		this.port = System.getProperty("MapPort")==null? 8080:Integer.valueOf(System.getProperty("MapPort"));
		this.debug = System.getProperty("MapDebug")==null? false:Boolean.valueOf(System.getProperty("MapDebug"));
		ServerLifecycleEvents.SERVER_STARTED.register((server) -> {
            try {
                httpMapServer = new HttpMapServer(this.host, this.port, server, LOGGER, this.debug);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
	}
}