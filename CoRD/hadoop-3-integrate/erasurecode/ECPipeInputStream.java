package org.apache.hadoop.hdfs.server.datanode.erasurecode;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.slf4j.Logger;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import org.apache.hadoop.util.Time;



import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class ECPipeInputStream {
    Configuration _conf;
  
    private static final Logger LOG = DataNode.LOG;
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
  
    //private String _lostFileName;
    private String[] _lostFileNames;
    private JedisPool _pipeJedisPool = null;
    private JedisPool[] _cyclJedisPools = null;
  
    private int _currOffsetInPkt = 0;
    private byte[] _currPacket = null;
    private DataNode _dataNode;

    private boolean _usingCycl = false;

    // add by jhli
    private String[] _objectIPs;
  
    public ECPipeInputStream(String filenames[], Configuration conf, DataNode dataNode, int lostNum, int index) {
      this._lostFileNames = filenames;
      this._conf = conf;
      this._dataNode = dataNode;

      this._coordinatorIp = conf.get("ecpipe.coordinator");

      LOG.info("ECPipe Coordinator address:" + this._coordinatorIp);

      JedisPoolConfig _coordinatorJedisPoolConfig = new JedisPoolConfig();
      _coordinatorJedisPoolConfig.setMaxTotal(_coordinatorJedisPoolSize);
      _coordinatorJedisPool = new JedisPool(_coordinatorJedisPoolConfig, _coordinatorIp);
  
      JedisPoolConfig _localJedisPoolConfig = new JedisPoolConfig();
      _localJedisPoolConfig.setMaxTotal(_localJedisPoolSize);
      _localJedisPool = new JedisPool(_localJedisPoolConfig, "127.0.0.1");
  
      try {
          InetAddress tmpnetaddr = InetAddress.getByName(_dataNode.getDatanodeId().getIpAddr());
          LOG.info("ECPipe is running on " + tmpnetaddr);

          _localIp = tmpnetaddr.getAddress();
         // for (int i=0; i<4; i++) 
           // LOG.info(_localIp[i]);
      } catch (Exception e) {
          LOG.info("Exception here");
      }
  
      this._packetSize = conf.getInt("ecpipe.packetsize", 262144);
      LOG.info("ECPipe packet size:" + this._packetSize);
  
      this._packetCnt = conf.getInt("ecpipe.packetcnt", 128);
      LOG.info("ECPipe packet cnt:" + this._packetCnt);
  
      this._blockSize = this._packetSize * this._packetCnt;
  
      this._content = new ArrayBlockingQueue<byte[]>(this._packetCnt);
      
      sendRequest(this._lostFileNames, lostNum, index);

      DataCollector dc = new DataCollector(this._lostFileNames[index], this._packetCnt, this._packetSize, this._objectIPs);
      dc.start();
    }
  
    private JedisPool InitRedis(int size, String ip) {
      JedisPoolConfig config = new JedisPoolConfig();
      config.setMaxTotal(size);
      JedisPool pool = new JedisPool(config, ip);
      return pool;
    }
  
    private void sendRequest(String[] filenames, int lostNum, int index) {
      //measure
      long startTime = Time.monotonicNow();
      assert (this._coordinatorJedisPool != null) && (this._localJedisPool != null) : "Bad Connection to Redis";
          
      // added 
      int totlen = 0;
      for (int i = 0; i < filenames.length; i++) totlen = totlen + filenames[i].getBytes().length;

      int cmdbase = 8; 
      byte[] cmd;
      cmd = new byte[8 + lostNum*4 + totlen];
      byte[] fName_i = filenames[index].getBytes();
      int len_i = fName_i.length;

      System.arraycopy(this._localIp, 0, cmd, 0, 4);
      cmd[cmdbase - 1] = (byte) (index >> 24);
      cmd[cmdbase - 2] = (byte) (index >> 16);
      cmd[cmdbase - 3] = (byte) (index >> 8);
      cmd[cmdbase - 4] = (byte) (index);

      for (int i = 0; i < lostNum; i++) {
        LOG.info("jhli ECPipe: cmdbase = " + cmdbase);
        byte[] fName = filenames[i].getBytes();
        int len = fName.length;
        //System.arraycopy(len, 0,    cmd, cmdbase,     4); 
        cmd[cmdbase + 3] = (byte) (len >> 24);
        cmd[cmdbase + 2] = (byte) (len >> 16);
        cmd[cmdbase + 1] = (byte) (len >> 8);
        cmd[cmdbase + 0] = (byte) len;
        LOG.info("fName: " + filenames[i]);
        LOG.info("len: " + len);
        System.arraycopy(fName, 0,  cmd, cmdbase + 4, len);
        cmdbase += 4 + len;
      }
      /*
      System.arraycopy(this._localIp, 0, cmd, 0, 4);
      System.arraycopy(fName,	      0, cmd, 4, len);
      System.arraycopy(lostNum,	      0, cmd, 4+len, 4);
      */
  
      try (BinaryJedis jedis = _coordinatorJedisPool.getResource()) {
  
        jedis.rpush("dr_requests".getBytes(), cmd);
  
        LOG.info("sendRequest: requesting IP with filename " + filenames[index] + ", length " + len_i + ", i = " + index);
        List<byte[]> l = jedis.blpop(0, fName_i);
        LOG.info("sendRequest: get IP with filename " + filenames[index] + ", length " + len_i + ", i = " + index + ", l.length = " + l.get(1).length);
  
        String ip = "";
        byte[] rsp = l.get(1);
        if (this._usingCycl) { // cycl
          int numOfFetchIPs = 0;
          cmdbase = 0;
          for (int i = 0; i < index; i++) {
            numOfFetchIPs = (int) rsp[cmdbase];
            cmdbase += 8 + numOfFetchIPs * 4;
          }
          numOfFetchIPs = (int) rsp[cmdbase];
          cmdbase += 4;
          LOG.info("cycl: fetchIPs = " + numOfFetchIPs);
          this._objectIPs = new String[numOfFetchIPs];
          this._cyclJedisPools = new JedisPool[numOfFetchIPs];
          for (int i = 0; i < numOfFetchIPs; i++) {
            ip = "";
            for (int j = 0; j < 4; j++) {
              ip += (rsp[cmdbase + j] & 0xFF);
              if (j < 3) {
                ip += ".";
              }
            }
	    cmdbase += 4;
	    LOG.info(ip);
            this._cyclJedisPools[i] = InitRedis(1, ip);
            this._objectIPs[i] = ip;
          }
        }
        else {  // pipe
          if (rsp.length < 4) {
            LOG.info("ERROR: rsp.length < 4");
            return;
          }
          for(cmdbase = 0; cmdbase < 4; cmdbase++) {
            ip += (rsp[cmdbase] & 0xFF);
            if (cmdbase < 3) {
              ip += ".";
            }
          }
          LOG.info(ip);
          //this._pipeJedisPool = InitRedis(1, ip);
	  this._cyclJedisPools = new JedisPool[1];
	  this._cyclJedisPools[0] = InitRedis(1, ip);

          this._objectIPs = new String[1];
          this._objectIPs[0] = ip;
        }
        this._isRemoteCtxInit = true;
      } catch (Exception i) {
        LOG.info("Exception sendRequest");
      }
      long endTime = Time.monotonicNow();
      LOG.info("Measurement: sendRequest(): " + (endTime - startTime));
    }
  
    private class DataCollector implements Runnable {
      private byte[] _redisKeys;
      private int _packetcnt;
      private int _packetsize;
      private String _filename;
      private Response[] responses;
      private String[] _objectIPs;
  
      private Thread t;
  
      DataCollector(String filename, int packetcnt, int packetsize, String[] ips) {
	_filename = filename;
        _packetcnt = packetcnt;
        _packetsize = packetsize;
        _redisKeys = filename.getBytes();
	_objectIPs = ips;
        responses = new Response[_packetcnt];
      }
  
      public void run() {
        long midtime1 = Time.monotonicNow(), midtime2;
        LOG.info("Start to collect the data.");
        long total1 = Time.monotonicNow();

        {
          BinaryJedis[] jedises = new BinaryJedis[_objectIPs.length];
          if (_objectIPs.length != _cyclJedisPools.length) {
            LOG.info("Test jhli: ERROR: _objectIPs and _cyclJedisPools don't share the same length!");
            return;
          }
          int len = _objectIPs.length;
          for (int i = 0; i < len; i++) {
            jedises[i] = _cyclJedisPools[i].getResource();
	    LOG.info("getResource cycl " + i);
          }
          while (!_isRemoteCtxInit);

          long read1 = Time.monotonicNow();

	  LOG.info("cycl starts");

          for (int i = 0; i < _packetCnt; i++) {
	    String newKey = _filename;
	    if (len > 1) { // pipe
	      newKey = newKey + ":" + i;
	    }
            if (i == 0 || i == _packetcnt - 1 || i % (_packetcnt / 4) == 0) {
              LOG.info("DataCollector, run: jhli test: packet " + i + " bytes, waiting, from " + this._objectIPs[i % len] + ", named " + newKey);
            }
            List<byte[]> tmp = jedises[i % len].blpop(0, newKey.getBytes());
            if (i == 0 || i == _packetcnt - 1 || i % (_packetcnt / 4) == 0) {
              midtime2 = Time.monotonicNow();
              LOG.info("DataCollector, run: jhli test: packet " + i + " bytes received from " + this._objectIPs[i % len] + ", takes " + (midtime2 - midtime1));
              midtime1 = Time.monotonicNow();
            }
            try {
              _content.put(tmp.get(1));
            } catch (InterruptedException ex) {
              LOG.info("Exception in collecting data, cycl");
            }
          }
	  long read2 = Time.monotonicNow();
	  LOG.info("read time = " + (read2 - read1));
	  long total2 = Time.monotonicNow();
	  LOG.info("overall = " + (total2 - total1));
        }


      }
  
      public void start() {
        t = new Thread(this, "DataCollector");
        t.start();
      }
    }
  
    public byte[] readFromECPipe (int toReconstruct) throws IOException {
        int copied = 0;
        byte[] targetbuf = new byte[toReconstruct];
	// add by Zuoru: Debug
	//LOG.info("Start to read a packet from ECPipe.");
        if (toReconstruct != _packetSize){
            LOG.info("The size of toread data can not fit the size of packet!");
        }
        if(_currPacket == null) {
          try {
	    _currPacket = _content.take();
          } catch (InterruptedException e) {
                LOG.info("ECPipeInputStream.read exception");
          }
        }
        System.arraycopy(_currPacket, 0, targetbuf, copied, toReconstruct);
        _currPacket = null;
	//LOG.info("Finish the reading of a packet from ECPipe.");
        return targetbuf;
    }

    public int getPacketSize(){
        return this._packetSize;
    }
  }
  
