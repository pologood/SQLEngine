package com.baidu.sqlengine;

import com.baidu.sqlengine.config.model.SystemConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class SqlEngineStartup {
    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlEngineStartup.class);

    public static void main(String[] args) {

        try {
            String home = SystemConfig.getHomePath();
            if (home == null) {
                System.out.println(SystemConfig.SYS_HOME + "  is not set.");
                System.exit(-1);
            }
            // init
            SqlEngineServer server = SqlEngineServer.getInstance();
            server.beforeStart();

            // startup
            server.startup();
            System.out.println("SqlEngine Server startup successfully. see logs in logs/sqlEngine.log");
            while (true) {
                Thread.sleep(300 * 1000);
            }
        } catch (Exception e) {
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            LOGGER.error(sdf.format(new Date()) + " startup error", e);
            System.exit(-1);
        }
    }
}
