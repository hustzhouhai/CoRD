package org.apache.hadoop.raid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class ECPipeInputStream {
  Configuration _conf;

  private String _coordinatorIp = null;
  private JedisPool _coordinatorJedisPool = null;
  private int _coordinatorJedisPoolSize = 1;

  private byte[] _localIp;
  private JedisPool _localJedisPool = null;
  private int _localJedisPoolSize = 2;

  private boolean _isRemoteCtxInit = false;
  
  private int _packetSize = 0;
  private int _packetCnt = 0;
  private int _blockSize = 0;

  private ArrayBlockingQueue<byte[]> _content = null;

  private String _lostFileName;
  private JedisPool _pipeJedisPool = null;

  private int _currOffsetInBlk = 0;
  private int _currOffsetInPkt = 0;
  private byte[] _currPacket = null;


  public ECPipeInputStream(String filename, Configuration conf) {
    this._lostFileName = filename;
    this._conf = conf;
    
    this._coordinatorIp = conf.get("ecpipe.coordinator");
    System.out.println("ECPipe Coordinator address:" + this._coordinatorIp);
    JedisPoolConfig _coordinatorJedisPoolConfig = new JedisPoolConfig();
    _coordinatorJedisPoolConfig.setMaxTotal(_coordinatorJedisPoolSize);
    _coordinatorJedisPool = new JedisPool(_coordinatorJedisPoolConfig, _coordinatorIp);

    JedisPoolConfig _localJedisPoolConfig = new JedisPoolConfig();
    _localJedisPoolConfig.setMaxTotal(_localJedisPoolSize);
    _localJedisPool = new JedisPool(_localJedisPoolConfig, "127.0.0.1");

    try {
        InetAddress tmpnetaddr = InetAddress.getByName(conf.get("ecpipe.local.addr"));
        _localIp = tmpnetaddr.getAddress();
        for (int i=0; i<4; i++) System.out.println(_localIp[i]);
    } catch (Exception e) {
        System.out.println("Exception here");
    }

    this._packetSize = conf.getInt("ecpipe.packetsize", 32768);
    System.out.println("ECPipe packet size:" + this._packetSize);

    this._packetCnt = conf.getInt("ecpipe.packetcnt", 2048);
    System.out.println("ECPipe packet cnt:" + this._packetCnt);

    this._blockSize = this._packetSize * this._packetCnt;

    this._content = new ArrayBlockingQueue<byte[]>(this._packetCnt);

    System.out.println("ECPipe local address:" + conf.get("ecpipe.local.addr"));
    RaidNode.LOG.info("ECPipe start");
    
    sendRequest(this._lostFileName);
    DataCollector dc = new DataCollector(this._lostFileName, this._packetCnt, this._packetSize);
    dc.start();
  }

  private JedisPool InitRedis(int size, String ip) {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(size);
	JedisPool pool = new JedisPool(config, ip);
	return pool;
  }

  private void sendRequest(String filename) {
    assert (this._coordinatorJedisPool != null) && (this._localJedisPool != null) : "Bad Connection to Redis";
        
    byte[] fName = filename.getBytes();
    int len = fName.length;

	byte[] cmd = new byte[4 + len];
	System.arraycopy(this._localIp, 0, cmd, 0, 4);
    	System.arraycopy(fName, 0, cmd, 4, len);

	try (BinaryJedis jedis = _coordinatorJedisPool.getResource()) {

	  jedis.rpush("dr_requests".getBytes(), cmd);

	  List<byte[]> l = jedis.blpop(0, fName);

	  byte[] tmpip = l.get(1);
	  String ip = "";
	  for(int i=0; i<tmpip.length; i++) {
		ip += (tmpip[i] & 0xFF);
		if(i < tmpip.length - 1) {
		  ip += ".";
		}
	  }
	  System.out.println(ip);
	  this._pipeJedisPool = InitRedis(1, ip);
	  this._isRemoteCtxInit = true;
	} catch (Exception i) {
	  System.out.println("Exception sendRequest");
	}
  }

  private class DataCollector implements Runnable {
    private byte[] _redisKeys;
	private int _packetcnt;
	private int _packetsize;
	private Response[] responses;

	private Thread t;

	DataCollector(String filename, int packetcnt, int packetsize) {
	  _packetcnt = packetcnt;
	  _packetsize = packetsize;
	  _redisKeys = filename.getBytes();
	  responses = new Response[_packetcnt];
	}

	public void run() {
	  long total1 = System.currentTimeMillis();
	  try (BinaryJedis jedis = _pipeJedisPool.getResource()) {
	    Pipeline p = jedis.pipelined();

		while(!_isRemoteCtxInit) 
		  ;

		long read1 = System.currentTimeMillis();
		for(int i=0; i<_packetcnt; i++) {
		  responses[i] = p.blpop(0, _redisKeys);
		}
		p.sync();

		for(int i=0; i<_packetcnt; i++) {
		  List<byte[]> tmp = (List<byte[]>)responses[i].get();
		  byte[] barray = tmp.get(1);
		  try {
		    _content.put(tmp.get(1));
		  } catch (InterruptedException ex) {
		    System.out.println("Exception in collocting data");
		  }
		}
		long read2 = System.currentTimeMillis();
		System.out.println("read time = " + (read2 - read1));
		long total2 = System.currentTimeMillis();
		System.out.println("overall = " + (total2 - total1));
	  }
	}

	public void start() {
	  t = new Thread(this, "DataCollector");
	  t.start();
	}
  }

  public int read (byte[] b, int offset, int len) throws IOException {
    int copied = 0;
	int toRead = Math.min(len, _blockSize - _currOffsetInBlk) ;

 	while (toRead > 0) {
 	  if(_currPacket == null) {
 	    try {
 	      _currPacket = _content.take();
 		} catch (InterruptedException e) {
 		  System.out.println("ECPipeInputStream.read exception");
 		}
 		_currOffsetInPkt = 0;
 
 	  }
 	  if ((_packetSize - _currOffsetInPkt) <= toRead) {
	    System.arraycopy(_currPacket, _currOffsetInPkt, b, copied,
		    (_packetSize - _currOffsetInPkt));
		copied += _packetSize - _currOffsetInPkt;
 		_currOffsetInPkt += _packetSize - _currOffsetInPkt;
 		_currOffsetInPkt = _currOffsetInPkt % _packetSize;
 		_currOffsetInBlk += _packetSize - _currOffsetInPkt;
 		toRead -= _packetSize - _currOffsetInPkt;
		_currPacket = null;
 	  } else {
	    System.arraycopy(_currPacket, _currOffsetInPkt, b, copied,
		    toRead);
		copied += toRead;
		_currOffsetInPkt += toRead;
		_currOffsetInPkt = _currOffsetInPkt % _packetSize;
		_currOffsetInBlk += toRead;
		toRead = 0;
	  }
 	}
	return copied;
  }
}
