package org.nutz.ssdb4j.pool2;

import java.io.IOException;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pool2SSDBStream implements SSDBStream {

	static final Logger LOG = LoggerFactory.getLogger(Pool2SSDBStream.class);

	protected GenericObjectPool<SSDBStream> pool;

	public Pool2SSDBStream(GenericObjectPool<SSDBStream> pool) {
		this.pool = pool;
	}

	public Response req(Cmd cmd, byte[]... vals) {
		SSDBStream steam = null;
		try {
			steam = pool.borrowObject();
			Response resp = steam.req(cmd, vals);
			pool.returnObject(steam);
			LOG.debug("【" + cmd.getName() + "】" + resp.stat
					+ (vals.length > 0 ? "【" + new String(vals[0], SSDBs.DEFAULT_CHARSET) + "】" : "") + "："
					+ resp.asString());
			return resp;
		} catch (Exception e) {
			if (steam != null)
				try {
					pool.invalidateObject(steam);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			throw new SSDBException(e);
		}
	}

	public void callback(SSDBStreamCallback callback) {
		try {
			SSDBStream steam = pool.borrowObject();
			try {
				steam.callback(callback);
			} finally {
				pool.returnObject(steam);
			}
		} catch (Exception e) {
			throw new SSDBException(e);
		}
	}

	public void close() throws IOException {
		try {
			pool.close();
		} catch (Exception e) {
			if (e instanceof IOException)
				throw (IOException) e;
			throw new IOException(e);
		}
	}
}
