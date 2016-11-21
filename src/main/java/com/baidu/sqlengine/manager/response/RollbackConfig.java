package com.baidu.sqlengine.manager.response;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.config.SqlEngineCluster;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.model.FirewallConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.UserConfig;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;

public final class RollbackConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(RollbackConfig.class);

	public static void execute(ManagerConnection c) {
		final ReentrantLock lock = SqlEngineServer.getInstance().getConfig().getLock();
		lock.lock();
		try {
			if (rollback()) {
				StringBuilder s = new StringBuilder();
				s.append(c).append("Rollback config success by manager");
				LOGGER.warn(s.toString());
				OkPacket ok = new OkPacket();
				ok.packetId = 1;
				ok.affectedRows = 1;
				ok.serverStatus = 2;
				ok.message = "Rollback config success".getBytes();
				ok.write(c);
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Rollback config failure");
			}
		} finally {
			lock.unlock();
		}
	}

	private static boolean rollback() {
		SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
		Map<String, UserConfig> users = conf.getBackupUsers();
		Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
		Map<String, PhysicalDBNode> dataNodes = conf.getBackupDataNodes();
		Map<String, PhysicalDBPool> dataHosts = conf.getBackupDataHosts();
		SqlEngineCluster cluster = conf.getBackupCluster();
		FirewallConfig firewall = conf.getBackupFirewall();

		// 检查可回滚状态
		if (!conf.canRollback()) {
			return false;
		}

		// 如果回滚已经存在的pool
		boolean rollbackStatus = true;
		Map<String, PhysicalDBPool> cNodes = conf.getDataHosts();
		for (PhysicalDBPool dn : dataHosts.values()) {
			dn.init(dn.getActivedIndex());
			if (!dn.isInitSuccess()) {
				rollbackStatus = false;
				break;
			}
		}
		// 如果回滚不成功，则清理已初始化的资源。
		if (!rollbackStatus) {
			for (PhysicalDBPool dn : dataHosts.values()) {
				dn.clearDataSources("rollbackup config");
				dn.stopHeartbeat();
			}
			return false;
		}

		// 应用回滚
		conf.rollback(users, schemas, dataNodes, dataHosts, cluster, firewall);

		// 处理旧的资源
		for (PhysicalDBPool dn : cNodes.values()) {
			dn.clearDataSources("clear old config ");
			dn.stopHeartbeat();
		}

		//清理缓存
		 SqlEngineServer.getInstance().getCacheService().clearCache();
		return true;
	}

}