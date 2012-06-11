package org.kit.tecs;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZKConnector extends ZKConnection {

	private static final Charset CHARSET = Charset.forName("UTF-8");

	public void writeNodeData(String _path, String _value) {

		Stat stat;

		try {

			stat = zk.exists(_path, false);

			if (stat == null) {
				zk.create(_path, _value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else {
				zk.setData(_path, _value.getBytes(CHARSET), -1);
			}

		} catch (KeeperException e ) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}

	}

	public void createNode(String _path, String _value, boolean _isPersistent,
			boolean _isOverwrite) {

		CreateMode cm;

		if (_isPersistent) {
			cm = CreateMode.PERSISTENT;
		} else {
			cm = CreateMode.EPHEMERAL;
		}

		try {

			if (zk.exists(_path, null) == null) {
				zk.create(_path, _value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
						cm);
			} else {

				if (_isOverwrite) {
					zk.setData(_path, _value.getBytes(CHARSET), -1);
				} else {
					System.out.println("Node: " + _path + " allready exists!");
				}

			}

		} catch (KeeperException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}

	}

	public void deleteNode(String _path) {

		try {

			if(zk.exists(_path, null) != null) {
				zk.delete(_path, -1);
			}
			
			

		} catch (InterruptedException e) {

			e.printStackTrace();
		} catch (KeeperException e) {
			
			e.printStackTrace();
		}

	}

	public void checkAndCreatePath(String _path, boolean isPersistent) {

		String[] subPath = _path.split("\\/");
		String createPath = "";
		CreateMode cm;

		if (isPersistent) {
			cm = CreateMode.PERSISTENT;
		} else {
			cm = CreateMode.EPHEMERAL;
		}

		for (int i = 1; i < subPath.length; i++) {

			createPath = createPath + "/" + subPath[i];

			try {
				if (zk.exists(createPath, false) == null) {
					zk.create(createPath, null, Ids.OPEN_ACL_UNSAFE, cm);

				}
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}

		}

	}

}

class ZKConnection implements Watcher {

	ZooKeeperLogUtil zkLogger = new ZooKeeperLogUtil(this.getClass());

	private static final int SESSION_TIMEOUT = 5000;

	protected ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {

		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();

	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}

	public void close() throws InterruptedException {
		zk.close();
	}
}

class ZooKeeperLogUtil {

	static Logger logger;

	public ZooKeeperLogUtil(Class callingClass) {

		logger = Logger.getLogger(callingClass);

		Properties props = new Properties();
		props.setProperty("log4j.rootLogger", "DEBUG, file");
		props.setProperty("log4j.appender.file",
				"org.apache.log4j.RollingFileAppender");
		props.setProperty("log4j.appender.file.File", "zkDebug.log");
		props.setProperty("log4j.appender.file.MaxFileSize", "100KB");
		props.setProperty("log4j.appender.file.MaxBackupIndex", "1");
		props.setProperty("log4j.appender.file.layout",
				"org.apache.log4j.PatternLayout");
		props.setProperty("log4j.appender.file.layout.ConversionPattern",
				"%p %t %c - %m%n");

		PropertyConfigurator pc = new PropertyConfigurator();
		pc.configure(props);

	}

}
