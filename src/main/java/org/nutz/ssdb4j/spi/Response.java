package org.nutz.ssdb4j.spi;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.nutz.ssdb4j.SSDBs;

/**
 * 标准响应,这个类由SSDBStream实例控制生成过程
 *
 */
public class Response {

	public String stat;
	public ArrayList<byte[]> datas = new ArrayList<byte[]>(2);
	public Charset charset = SSDBs.DEFAULT_CHARSET;

	public Response check() {
		if (!ok())
			throw new SSDBException("msg=" + stat + ", values=" + listString());
		return this;
	}

	public boolean ok() {
		return "ok".equals(stat);
	}

	public boolean notFound() {
		return "not_found".equals(stat);
	}

	protected String _string(byte[] data) {
		return new String(data, charset);
	}

	public String asString() {
		if (datas.size() == 0)
			return null;
		return _string(datas.get(0));
	}

	public double asDouble() {
		return Double.parseDouble(asString());
	}

	public int asInt() {
		return Integer.parseInt(asString());
	}

	public long asLong() {
		return Long.parseLong(asString());
	}

	public List<String> listString() {
		List<String> list = new ArrayList<String>();
		for (byte[] data : datas) {
			list.add(_string(data));
		}
		return list;
	}

	public Map<String, Object> map() {
		if (datas.size() % 2 != 0)
			throw new IllegalArgumentException("not key-value pairs");
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		Iterator<byte[]> it = datas.iterator();
		while (it.hasNext()) {
			map.put(_string(it.next()), it.next());
		}
		return map;
	}

	public Map<String, String> mapString() {
		if (datas.size() % 2 != 0)
			throw new IllegalArgumentException("not key-value pairs");
		Map<String, String> map = new LinkedHashMap<String, String>();
		Iterator<byte[]> it = datas.iterator();
		while (it.hasNext()) {
			map.put(_string(it.next()), _string(it.next()));
		}
		return map;
	}

	public String asBlocks(char joint) {
		StringBuilder sb = new StringBuilder();
		for (byte[] block : datas) {
			sb.append(_string(block)).append(joint);
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	public List<KeyValue> asKeyValues() {
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		for (int i = 0; i + 1 < datas.size(); i += 2) {
			String key = _string(datas.get(i));
			String value = _string(datas.get(i + 1));
			keyValues.add(new KeyValue(key, value));
		}
		return keyValues;
	}

	public List<KeyValue> asKeyScores() {
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		for (int i = 0; i + 1 < datas.size(); i += 2) {
			String key = _string(datas.get(i));
			String value = _string(datas.get(i + 1));
			keyValues.add(new KeyValue(key, Long.parseLong(value)));
		}
		return keyValues;
	}
}
