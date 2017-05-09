package org.nutz.ssdb4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.nutz.ssdb4j.impl.BatchClient;
import org.nutz.ssdb4j.impl.DefaultObjectConv;
import org.nutz.ssdb4j.impl.SocketSSDBStream;
import org.nutz.ssdb4j.pool2.Pool2s;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.KeyValue;
import org.nutz.ssdb4j.spi.ObjectConv;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author wr
 *
 */

public class SSDBClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(SSDBClient.class);

	protected SSDBStream stream;

	protected ObjectConv conv;

	public SSDBClient(String host, int port, int timeoutSeconds, String pass) {
		this.stream = Pool2s.pool(host, port, timeoutSeconds, null, isBlank(pass) ? null : pass.getBytes());
		this.conv = DefaultObjectConv.me;
	}

	public SSDBClient(SSDBStream stream) {
		this.stream = stream;
		this.conv = DefaultObjectConv.me;
	}

	public SSDBClient(String host, int port, int timeout) {
		stream = new SocketSSDBStream(host, port, timeout);
		this.conv = DefaultObjectConv.me;
	}

	protected byte[] bytes(Object obj) {
		return conv.bytes(obj);
	}

	protected byte[][] bytess(Object... objs) {
		return conv.bytess(objs);
	}

	public static boolean isBlank(String str) {
		int strLen;
		if ((str == null) || ((strLen = str.length()) == 0))
			return true;
		for (int i = 0; i < strLen; ++i) {
			if (!(Character.isWhitespace(str.charAt(i)))) {
				return false;
			}
		}
		return true;
	}

	protected Response req(Cmd cmd, byte[] first, byte[][] lots) {
		byte[][] vals = new byte[lots.length + 1][];
		vals[0] = first;
		for (int i = 0; i < lots.length; i++) {
			vals[i + 1] = lots[i];
		}
		try {
			return req(cmd, vals);
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public Response req(Cmd cmd, byte[]... vals) {
		return stream.req(cmd, vals);
	}

	public SSDB batch() {
		return new BatchClient(stream, 60, TimeUnit.SECONDS);
	}

	public SSDB batch(int timeout, TimeUnit timeUnit) {
		return new BatchClient(stream, timeout, timeUnit);
	}

	public List<Response> exec() {
		throw new SSDBException("not batch!");
	}

	public void setObjectConv(ObjectConv conv) {
		this.conv = conv;
	}

	public void changeObjectConv(ObjectConv conv) {
		this.setObjectConv(conv);
	}

	public void setSSDBStream(SSDBStream stream) {
		this.stream = stream;
	}

	// ----------------------------------------------------------------------------------

	public String get(Object key) {
		try {
			return req(Cmd.get, bytes(key)).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int set(Object key, Object val) {
		try {
			return req(Cmd.set, bytes(key), bytes(val)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int setx(Object key, Object val, int ttl) {
		try {
			return req(Cmd.setx, bytes(key), bytes(val), Integer.toString(ttl).getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int del(Object key) {
		try {
			return req(Cmd.del, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public long incr(Object key, int val) {
		try {
			return req(Cmd.incr, bytes(key), Integer.toString(val).getBytes()).asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public boolean exists(Object key) {
		try {
			return req(Cmd.exists, bytes(key)).asInt() > 0;
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public List<String> keys(Object start, Object end, int limit) {
		try {
			return req(Cmd.keys, bytes(start), bytes(end), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int multiSet(Object... pairs) {
		try {
			return req(Cmd.multi_set, bytess(pairs)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<String> multiGet(Object... keys) {
		try {
			return req(Cmd.multi_get, bytess(keys)).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int multiDel(Object... keys) {
		try {
			return req(Cmd.multi_del, bytess(keys)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> scan(Object start, Object end, int limit) {
		try {
			return req(Cmd.scan, bytes(start), bytes(end), Integer.toString(limit).getBytes()).asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> rscan(Object start, Object end, int limit) {
		try {
			return req(Cmd.rscan, bytes(start), bytes(end), Integer.toString(limit).getBytes()).asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int hset(Object key, Object hkey, Object hval) {
		try {
			return req(Cmd.hset, bytes(key), bytes(hkey), bytes(hval)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int hdel(Object key, Object hkey) {
		try {
			return req(Cmd.hdel, bytes(key), bytes(hkey)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public String hget(Object key, Object hkey) {
		try {
			return req(Cmd.hget, bytes(key), bytes(hkey)).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int hsize(Object key) {
		try {
			return req(Cmd.hsize, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> hlist(Object key, Object hkey, int limit) {
		try {
			return req(Cmd.hlist, bytes(key), bytes(hkey), Integer.toString(limit).getBytes()).asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public long hincr(Object key, Object hkey, int val) {
		try {
			return req(Cmd.hincr, bytes(key), bytes(hkey), Integer.toString(val).getBytes()).asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> hscan(Object key, Object start, Object end, int limit) {
		try {
			return req(Cmd.hscan, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes())
					.asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> hrscan(Object key, Object start, Object end, int limit) {
		try {
			return req(Cmd.hrscan, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes())
					.asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int zset(Object key, Object zkey, long score) {
		try {
			return req(Cmd.zset, bytes(key), bytes(zkey), Long.toString(score).getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public long zget(Object key, Object zkey) {
		try {
			return req(Cmd.zget, bytes(key), bytes(zkey)).asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int zdel(Object key, Object zkey) {
		try {
			return req(Cmd.zdel, bytes(key), bytes(zkey)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public long zincr(Object key, Object zkey, int val) {
		try {
			return req(Cmd.zincr, bytes(key), bytes(zkey), Integer.toString(val).getBytes()).asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int zsize(Object key) {
		try {
			return req(Cmd.zsize, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<String> zlist(Object zkey_start, Object zkey_end, int limit) {
		try {
			return req(Cmd.zlist, bytes(zkey_start), bytes(zkey_end), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int zrank(Object key, Object zkey) {
		try {
			return req(Cmd.zrank, bytes(key), bytes(zkey)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int zrrank(Object key, Object zkey) {
		try {
			return req(Cmd.zrrank, bytes(key), bytes(zkey)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> zscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
		try {
			return req(Cmd.zscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end),
					Integer.toString(limit).getBytes()).asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> zrscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
		try {
			return req(Cmd.zrscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end),
					Integer.toString(limit).getBytes()).asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> zpopfront(Object key, int limit) {
		try {
			return req(Cmd.zpopfront, bytes(key), Integer.toString(limit).getBytes()).asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> zpopback(Object key, int limit) {
		try {
			return req(Cmd.zpopback, bytes(key), Integer.toString(limit).getBytes()).asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int qsize(Object key) {
		try {
			return req(Cmd.qsize, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public String qfront(Object key) {
		try {
			return req(Cmd.qfront, bytes(key)).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public String qback(Object key) {
		try {
			return req(Cmd.qback, bytes(key)).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int qpush(Object key, Object value) {
		try {
			return req(Cmd.qpush, bytes(key), bytes(value)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<String> qpop(Object key) {
		try {
			return req(Cmd.qpop, bytes(key)).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<String> qlist(Object key_start, Object key_end, int limit) {
		try {
			return req(Cmd.qlist, bytes(key_start), bytes(key_end), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public boolean qclear(Object key) {
		try {
			req(Cmd.qclear, bytes(key)).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public List<String> hkeys(Object key, Object start, Object end, int limit) {
		try {
			return req(Cmd.hkeys, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes())
					.listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public boolean hexists(Object key, Object hkey) {
		try {
			return req(Cmd.hexists, bytes(key), bytes(hkey)).asInt() > 0;
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public int hclear(Object key) {
		try {
			return req(Cmd.hclear, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> multiHget(Object key, Object... hkeys) {
		try {
			return req(Cmd.multi_hget, bytes(key), bytess(hkeys)).asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public boolean multiHset(Object key, Object... pairs) {
		try {
			return req(Cmd.multi_hset, bytes(key), bytess(pairs)).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public int multiHdel(Object key, Object... hkeys) {
		try {
			return req(Cmd.multi_hdel, bytes(key), bytess(hkeys)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public boolean zexists(Object key, Object zkey) {
		try {
			return req(Cmd.zexists, bytes(key), bytes(zkey)).asInt() > 0;
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public int zclear(Object key) {
		try {
			return req(Cmd.zclear, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<String> zkeys(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
		try {
			return req(Cmd.zkeys, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end),
					Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> zrange(Object key, int offset, int limit) {
		try {
			return req(Cmd.zrange, bytes(key), Integer.toString(offset).getBytes(), Integer.toString(limit).getBytes())
					.asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<KeyValue> zrrange(Object key, int offset, int limit) {
		try {
			return req(Cmd.zrrange, bytes(key), Integer.toString(offset).getBytes(), Integer.toString(limit).getBytes())
					.asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public boolean multiZset(Object key, Object... pairs) {
		try {
			return req(Cmd.multi_zset, bytes(key), bytess(pairs)).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public List<KeyValue> multiZget(Object key, Object... zkeys) {
		try {
			return req(Cmd.multi_zget, bytes(key), bytess(zkeys)).asKeyScores();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int multiZdel(Object key, Object... zkeys) {
		try {
			return req(Cmd.multi_zdel, bytes(key), bytess(zkeys)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public long flushdb(String type) {
		if (type == null || type.length() == 0) {
			long count = 0;
			count += flushdb_kv();
			count += flushdb_hash();
			count += flushdb_zset();
			count += flushdb_list();
			return count;
		}
		if ("kv".equals(type)) {
			return flushdb_kv();
		}
		if ("hash".equals(type)) {
			return flushdb_hash();
		}
		if ("zset".equals(type)) {
			return flushdb_zset();
		}
		if ("list".equals(type)) {
			return flushdb_list();
		}
		throw new IllegalArgumentException("not such flushdb mode=" + type);
	}

	protected long flushdb_kv() {
		long count = 0;
		while (true) {
			List<String> keys = keys("", "", 1000);
			if (null == keys || keys.isEmpty())
				return count;
			count += keys.size();
			multiDel(keys.toArray());
		}
	}

	protected long flushdb_hash() {
		long count = 0;
		while (true) {
			List<KeyValue> keys = hlist("", "", 1000);
			if (null == keys || keys.isEmpty())
				return count;
			count += keys.size();
			for (KeyValue key : keys) {
				hclear(key.getKey());
			}
		}
	}

	protected long flushdb_zset() {
		long count = 0;
		while (true) {
			List<String> keys = zlist("", "", 1000);
			if (null == keys || keys.isEmpty())
				return count;
			count += keys.size();
			for (String key : keys) {
				zclear(key);
			}
		}
	}

	protected long flushdb_list() {
		long count = 0;
		while (true) {
			List<String> keys = qlist("", "", 1000);
			if (null == keys || keys.isEmpty())
				return count;
			count += keys.size();
			for (String key : keys) {
				qclear(key);
			}
		}
	}

	public String info() {
		try {
			return req(Cmd.info).asBlocks('\n');
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	// ------------------------------------------

	public int setnx(Object key, Object val) {
		try {
			return req(Cmd.setnx, bytes(key), bytes(val)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public String getset(Object key, Object val) {
		try {
			return req(Cmd.getset, bytes(key), bytes(val)).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<String> qslice(Object key, int start, int end) {
		try {
			return req(Cmd.qslice, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes())
					.listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public String qget(Object key, int index) {
		try {
			return req(Cmd.qget, bytes(key), Integer.toString(index).getBytes()).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int zcount(Object key, int start, int end) {
		try {
			return req(Cmd.zcount, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes())
					.asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public long zsum(Object key, int start, int end) {
		try {
			return req(Cmd.zsum, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes())
					.asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public long zavg(Object key, int start, int end) {
		try {
			return req(Cmd.zavg, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes())
					.asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int ttl(Object key) {
		try {
			return req(Cmd.ttl, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> hgetall(Object key) {
		try {
			return req(Cmd.hgetall, bytes(key)).asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int zremrangebyrank(Object key, Object score_start, Object score_end) {
		try {
			return req(Cmd.zremrangebyrank, bytes(key), bytes(score_start), bytes(score_end)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int zremrangebyscore(Object key, Object score_start, Object score_end) {
		try {
			return req(Cmd.zremrangebyscore, bytes(key), bytes(score_start), bytes(score_end)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public boolean multiZexists(Object key, Object... zkeys) {
		try {
			return req(Cmd.zexists, bytes(key), bytess(zkeys)).asInt() > 0;
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public int multiZsize(Object... keys) {
		try {
			return req(Cmd.zsize, bytess(keys)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int qpushBack(Object key, Object value) {
		try {
			return req(Cmd.qpush_back, bytes(key), bytes(value)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int qpushFront(Object key, Object value) {
		try {
			return req(Cmd.qpush_front, bytes(key), bytes(value)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int qpopBack(Object key) {
		try {
			return req(Cmd.qpop_back, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<String> qpopFront(Object key) {
		try {
			return req(Cmd.qpop_front, bytes(key)).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<String> qrange(Object key, int begin, int limit) {
		try {
			return req(Cmd.qrange, bytes(key), Integer.toString(begin).getBytes(), Integer.toString(limit).getBytes())
					.listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public int expire(Object key, int ttl) {
		try {
			return req(Cmd.expire, bytes(key), Integer.toString(ttl).getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public void close() throws IOException {
		stream.close();
	}

	public int getbit(Object key, int offset) {
		try {
			return req(Cmd.getbit, bytes(key), Integer.toString(offset).getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int setbit(Object key, int offset, byte on) {
		try {
			return req(Cmd.setbit, bytes(key), Integer.toString(offset).getBytes(),
					on == 1 ? "1".getBytes() : "0".getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int countbit(Object key, int start, int size) {
		try {
			return req(Cmd.countbit, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes())
					.asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int strlen(Object key, int start, int size) {
		if (size < 0)
			size = 2000000000;
		try {
			return req(Cmd.strlen, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes())
					.asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int strlen(Object key) {
		try {
			return req(Cmd.strlen, bytes(key)).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public List<KeyValue> hrlist(Object key, Object hkey, int limit) {
		try {
			return req(Cmd.hrlist, bytes(key), bytes(hkey), Integer.toString(limit).getBytes()).asKeyValues();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public boolean zrlist(Object zkey_start, Object zkey_end, int limit) {
		try {
			return req(Cmd.zrlist, bytes(zkey_start), bytes(zkey_end), Integer.toString(limit).getBytes()).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public List<String> qrlist(Object key_start, Object key_end, int limit) {
		try {
			return req(Cmd.qrlist, bytes(key_start), bytes(key_end), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public boolean auth(String passwd) {
		try {
			return req(Cmd.auth, bytes(passwd)).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public int qtrimBack(Object key, int size) {
		try {
			return req(Cmd.qtrim_back, bytes(key), Integer.toString(size).getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public int qtrimFront(Object key, int size) {
		try {
			return req(Cmd.qtrim_front, bytes(key), Integer.toString(size).getBytes()).asInt();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	// -----------------------------------
	public long dbsize() {
		try {
			return req(Cmd.dbsize).asLong();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return 0;
	}

	public boolean qset(Object key, int index, Object value) {
		try {
			return req(Cmd.qset, bytes(key), Integer.toString(index).getBytes(), bytes(value)).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}

	public List<String> qpopBack(Object key, int limit) {
		try {
			return req(Cmd.qpop_back, bytes(key), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public List<String> qpopFront(Object key, int limit) {
		try {
			return req(Cmd.qpop_front, bytes(key), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	// ------------------------

	public List<String> rkeys(Object start, Object end, int limit) {
		try {
			return req(Cmd.rkeys, bytes(start), bytes(end), Integer.toString(limit).getBytes()).listString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return null;
	}

	public String version() {
		try {
			return req(Cmd.version).asString();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
			;
		}
		return null;
	}

	public boolean ping() {
		try {
			return req(Cmd.ping).ok();
		} catch (Exception e) {
			LOGGER.error("ssdb操作发生异常", e);
		}
		return false;
	}
}
