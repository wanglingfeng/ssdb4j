package org.nutz.ssdb4j.pool2;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.impl.SocketSSDBStream;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.SSDBStream;

public class Pool2s {
	public static SSDBStream pool(final String host, final int port, final int timeout, Object cnf) {
		return pool(host, port, timeout, cnf, null);
	}

	public static SSDBStream pool(final String host, final int port, final int timeout, Object cnf, final byte[] auth) {
		if (cnf == null) {
			GenericObjectPoolConfig config = new GenericObjectPoolConfig();
			config.setMaxTotal(20);
			config.setMaxIdle(10);
			config.setTestWhileIdle(true);
			cnf = config;
		}
		return new Pool2SSDBStream(new GenericObjectPool<SSDBStream>(new BasePooledObjectFactory<SSDBStream>() {
			@Override
			public SSDBStream create() throws Exception {
				return new SocketSSDBStream(host, port, timeout, auth);
			}

			@Override
			public PooledObject<SSDBStream> wrap(SSDBStream arg0) {
				return new DefaultPooledObject<SSDBStream>(arg0);
			}

			@Override
			public PooledObject<SSDBStream> makeObject() throws Exception {
				return wrap(create());
			}

			@Override
			public void destroyObject(PooledObject<SSDBStream> p) throws Exception {
				p.getObject().close();
			}

			@Override
			public boolean validateObject(PooledObject<SSDBStream> p) {
				try {
					return p.getObject().req(Cmd.ping).ok();
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}

			@Override
			public void activateObject(PooledObject<SSDBStream> p) throws Exception {
			}

			@Override
			public void passivateObject(PooledObject<SSDBStream> p) throws Exception {
			}
		}, (GenericObjectPoolConfig) cnf));
	}
}
