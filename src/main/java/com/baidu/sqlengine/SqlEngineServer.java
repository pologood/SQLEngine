package com.baidu.sqlengine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.buffer.BufferPool;
import com.baidu.sqlengine.buffer.DirectByteBufferPool;
import com.baidu.sqlengine.cache.CacheService;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.manager.ManagerConnectionFactory;
import com.baidu.sqlengine.memory.SqlEngineMemory;
import com.baidu.sqlengine.net.NIOAcceptor;
import com.baidu.sqlengine.net.NIOConnector;
import com.baidu.sqlengine.net.NIOProcessor;
import com.baidu.sqlengine.net.NIOReactorPool;
import com.baidu.sqlengine.net.SocketAcceptor;
import com.baidu.sqlengine.net.SocketConnector;
import com.baidu.sqlengine.route.RouteService;
import com.baidu.sqlengine.route.SqlEngineSequnceProcessor;
import com.baidu.sqlengine.route.factory.RouteStrategyFactory;
import com.baidu.sqlengine.server.ServerConnectionFactory;
import com.baidu.sqlengine.server.interceptor.SQLInterceptor;
import com.baidu.sqlengine.statistic.SQLRecorder;
import com.baidu.sqlengine.statistic.stat.SqlResultSizeRecorder;
import com.baidu.sqlengine.statistic.stat.UserStat;
import com.baidu.sqlengine.statistic.stat.UserStatAnalyzer;
import com.baidu.sqlengine.util.ExecutorUtil;
import com.baidu.sqlengine.util.TimeUtil;
import com.baidu.sqlengine.util.tool.NameableExecutor;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class SqlEngineServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlEngineServer.class);
    private static final SqlEngineServer INSTANCE = new SqlEngineServer();

    public static final String FLAG = "SqlEngine";
    private static final long TIME_UPDATE_PERIOD = 20L;
    private static final long DEFAULT_SQL_STAT_RECYCLE_PERIOD = 5 * 1000L;
    private static final long DEFAULT_OLD_CONNECTION_CLEAR_PERIOD = 5 * 1000L;


    private final RouteService routerService;
    private final CacheService cacheService;

    //全局序列号
    private final SqlEngineSequnceProcessor sequnceProcessor = new SqlEngineSequnceProcessor();
    private final SQLInterceptor sqlInterceptor;
    private volatile int nextProcessor;

    // System Buffer Pool Instance
    private BufferPool bufferPool;

    //XA事务全局ID生成
    private final AtomicLong xaIDInc = new AtomicLong();

    // SqlEngine 内存管理类
    private SqlEngineMemory sqlEngineMemory = null;

    private final SqlEngineConfig config;
    private final ScheduledExecutorService scheduler;
    private final SQLRecorder sqlRecorder;
    private final AtomicBoolean isOnline;
    private final long startupTime;
    private NIOProcessor[] processors;
    private SocketConnector connector;
    private NameableExecutor businessExecutor;
    private NameableExecutor timerExecutor;
    private ListeningExecutorService listeningExecutorService;

    public static final SqlEngineServer getInstance() {
        return INSTANCE;
    }

    private SqlEngineServer() {

        //读取文件配置
        this.config = new SqlEngineConfig();

        //定时线程池，单线程线程池
        scheduler = Executors.newSingleThreadScheduledExecutor();

        //SQL记录器
        this.sqlRecorder = new SQLRecorder(config.getSystem().getSqlRecordCount());

        /**
         * 是否在线，SqlEngine manager中有命令控制
         * | offline | Change SqlEngine status to OFF |
         * | online | Change SqlEngine status to ON |
         */
        this.isOnline = new AtomicBoolean(true);

        //缓存服务初始化
        cacheService = new CacheService();

        //路由计算初始化
        routerService = new RouteService(cacheService);

        try {
            //SQL解析器
            sqlInterceptor = (SQLInterceptor) Class.forName(config.getSystem().getSqlInterceptor()).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //记录启动时间
        this.startupTime = TimeUtil.currentTimeMillis();
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public NameableExecutor getTimerExecutor() {
        return timerExecutor;
    }

    public SqlEngineSequnceProcessor getSequnceProcessor() {
        return sequnceProcessor;
    }

    public SQLInterceptor getSqlInterceptor() {
        return sqlInterceptor;
    }

    public String genXATXID() {
        long seq = this.xaIDInc.incrementAndGet();
        if (seq < 0) {
            synchronized(xaIDInc) {
                if (xaIDInc.get() < 0) {
                    xaIDInc.set(0);
                }
                seq = xaIDInc.incrementAndGet();
            }
        }
        return "'SqlEngine." + this.getConfig().getSystem().getSqlEngineNodeId() + "." + seq + "'";
    }

    public SqlEngineMemory getSqlEngineMemory() {
        return sqlEngineMemory;
    }

    public SqlEngineConfig getConfig() {
        return config;
    }

    public void beforeStart() {
        String home = SystemConfig.getHomePath();
    }

    public void shutdown() {
        scheduler.shutdown();
        timerExecutor.shutdown();
        businessExecutor.shutdown();
    }

    public void startup() throws IOException {

        SystemConfig system = config.getSystem();
        int processorCount = system.getProcessors();

        // server startup
        LOGGER.info("===============================================");
        LOGGER.info(FLAG + " is ready to startup ...");
        LOGGER.info(this.getServerInfo());
        LOGGER.info("sysconfig params:" + system.toString());

        // startup manager
        ManagerConnectionFactory mf = new ManagerConnectionFactory();
        ServerConnectionFactory sf = new ServerConnectionFactory();
        SocketAcceptor manager = null;
        SocketAcceptor server = null;

        // startup processors
        int threadPoolSize = system.getProcessorExecutor();
        processors = new NIOProcessor[processorCount];
        // a page size
        int bufferPoolPageSize = system.getBufferPoolPageSize();
        // total page number
        short bufferPoolPageNumber = system.getBufferPoolPageNumber();
        //minimum allocation unit
        short bufferPoolChunkSize = system.getBufferPoolChunkSize();


        bufferPool = new DirectByteBufferPool(bufferPoolPageSize, bufferPoolChunkSize,
                bufferPoolPageNumber, system.getFrontSocketSoRcvbuf());
        long totalNetWorkBufferSize = bufferPoolPageSize * bufferPoolPageNumber;

        /**
         * Off Heap For Merge/Order/Group/Limit 初始化
         */
        if (system.getUseOffHeapForMerge() == 1) {
            try {
                sqlEngineMemory = new SqlEngineMemory(system, totalNetWorkBufferSize);
            } catch (NoSuchFieldException e) {
                LOGGER.error("NoSuchFieldException", e);
            } catch (IllegalAccessException e) {
                LOGGER.error("Error", e);
            }
        }
        businessExecutor = ExecutorUtil.create("BusinessExecutor", threadPoolSize);
        timerExecutor = ExecutorUtil.create("Timer", system.getTimerExecutor());
        listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);

        for (int i = 0; i < processors.length; i++) {
            processors[i] = new NIOProcessor("Processor" + i, bufferPool, businessExecutor);
        }

        LOGGER.info("using nio network handler ");

        NIOReactorPool reactorPool =
                new NIOReactorPool(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR", processors.length);
        connector = new NIOConnector(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", reactorPool);
        ((NIOConnector) connector).start();

        manager = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + FLAG
                + "Manager", system.getBindIp(), system.getManagerPort(), mf, reactorPool);

        server = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + FLAG
                + "Server", system.getBindIp(), system.getServerPort(), sf, reactorPool);

        // manager start
        manager.start();
        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());
        server.start();

        // server started
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());

        LOGGER.info("===============================================");

        // init datahost
        Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
        LOGGER.info("Initialize dataHost ...");
        for (PhysicalDBPool node : dataHosts.values()) {
            node.init(0);
            node.startHeartbeat();
        }

        long dataNodeIldeCheckPeriod = system.getDataNodeIdleCheckPeriod();

        scheduler.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(processorCheck(), 0L, system.getProcessorCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataNodeConHeartBeatCheck(dataNodeIldeCheckPeriod), 0L, dataNodeIldeCheckPeriod,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataNodeHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod(),
                TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataSourceOldConsClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD,
                TimeUnit.MILLISECONDS);

        if (system.getUseSqlStat() == 1) {
            scheduler.scheduleAtFixedRate(recycleSqlStat(), 0L, DEFAULT_SQL_STAT_RECYCLE_PERIOD, TimeUnit.MILLISECONDS);
        }

        //定期清理结果集排行榜，控制拒绝策略
        scheduler.scheduleAtFixedRate(resultSetMapClear(), 0L, system.getClearBigSqLResultSetMapMs(),
                TimeUnit.MILLISECONDS);

        RouteStrategyFactory.init();
    }

    private String getServerInfo() {
        SystemConfig system = config.getSystem();
        String info = "Startup processors ...,total processors:" + system.getProcessors()
                + ",nio thread pool size:" + system.getProcessorExecutor()
                + "\r\n each process allocated socket buffer pool bytes ,a page size:" + system.getBufferPoolPageSize()
                + " a page's chunk number(PageSize/ChunkSize) is:"
                + (system.getBufferPoolPageSize() / system.getBufferPoolChunkSize())
                + "  buffer page's number is:" + system.getBufferPoolPageNumber();
        return info;
    }

    /**
     * 清理 reload @@config_all 后，老的 connection 连接
     *
     * @return
     */
    private Runnable dataSourceOldConsClear() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        long sqlTimeout =
                                SqlEngineServer.getInstance().getConfig().getSystem().getSqlExecuteTimeout() * 1000L;

                        //根据 lastTime 确认事务的执行， 超过 sqlExecuteTimeout 阀值 close connection
                        long currentTime = TimeUtil.currentTimeMillis();
                        Iterator<BackendConnection> iter = NIOProcessor.backends_old.iterator();
                        while (iter.hasNext()) {
                            BackendConnection con = iter.next();
                            long lastTime = con.getLastTime();
                            if (currentTime - lastTime > sqlTimeout) {
                                con.close("clear old backend connection ...");
                                iter.remove();
                            }
                        }
                    }
                });
            }

            ;
        };
    }

    /**
     * 在bufferpool使用率大于使用率阈值时不清理
     * 在bufferpool使用率小于使用率阈值时清理大结果集清单内容
     */
    private Runnable resultSetMapClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    BufferPool bufferPool = getBufferPool();
                    long bufferSize = bufferPool.size();
                    long bufferCapacity = bufferPool.capacity();
                    long bufferUsagePercent = (bufferCapacity - bufferSize) * 100 / bufferCapacity;
                    if (bufferUsagePercent < config.getSystem().getBufferUsagePercent()) {
                        Map<String, UserStat> map = UserStatAnalyzer.getInstance().getUserStatMap();
                        Set<String> userSet = config.getUsers().keySet();
                        for (String user : userSet) {
                            UserStat userStat = map.get(user);
                            if (userStat != null) {
                                SqlResultSizeRecorder recorder = userStat.getSqlResultSizeRecorder();
                                //System.out.println(recorder.getSqlResultSet().size());
                                recorder.clearSqlResultSet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("resultSetMapClear err " + e);
                }
            }

            ;
        };
    }


    public RouteService getRouterService() {
        return routerService;
    }

    public CacheService getCacheService() {
        return cacheService;
    }

    public NameableExecutor getBusinessExecutor() {
        return businessExecutor;
    }

    public RouteService getRouterservice() {
        return routerService;
    }

    public NIOProcessor nextProcessor() {
        int i = ++nextProcessor;
        if (i >= processors.length) {
            i = nextProcessor = 0;
        }
        return processors[i];
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public SocketConnector getConnector() {
        return connector;
    }

    public SQLRecorder getSqlRecorder() {
        return sqlRecorder;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    // 系统时间定时更新任务
    private Runnable updateTime() {
        return new Runnable() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    // 处理器定时检查任务
    private Runnable processorCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : processors) {
                                p.checkBackendCons();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("checkBackendCons caught err:" + e);
                        }

                    }
                });
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : processors) {
                                p.checkFrontCons();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("checkFrontCons caught err:" + e);
                        }
                    }
                });
            }
        };
    }

    // 数据节点定时连接空闲超时检查任务
    private Runnable dataNodeConHeartBeatCheck(final long heartPeriod) {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                        for (PhysicalDBPool node : nodes.values()) {
                            node.heartbeatCheck(heartPeriod);
                        }

                    }
                });
            }
        };
    }

    // 数据节点定时心跳任务
    private Runnable dataNodeHeartbeat() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                        for (PhysicalDBPool node : nodes.values()) {
                            node.doHeartbeat();
                        }
                    }
                });
            }
        };
    }

    //定时清理保存SqlStat中的数据
    private Runnable recycleSqlStat() {
        return new Runnable() {
            @Override
            public void run() {
                Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
                for (UserStat userStat : statMap.values()) {
                    userStat.getSqlLastStat().recycle();
                    userStat.getSqlRecorder().recycle();
                    userStat.getSqlHigh().recycle();
                    userStat.getSqlLargeRowStat().recycle();
                }
            }
        };
    }

    public ListeningExecutorService getListeningExecutorService() {
        return listeningExecutorService;
    }
}
