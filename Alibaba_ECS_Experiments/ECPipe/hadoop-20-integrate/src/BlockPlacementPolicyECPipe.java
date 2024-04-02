/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.util.HostsFileReader;

/**
 * This BlockPlacementPolicy uses a simple heuristic, random placement of
 * the replicas of a newly-created block for all the files in the system, 
 * for the purpose of spreading out the 
 * group of blocks which used by RAID for recovering each other. 
 * This is important for the availability of the blocks. 
 * 
 * Replication of an existing block continues to use the default placement
 * policy.
 * 
 * This simple block placement policy does not guarantee that
 * blocks on the RAID stripe are on different nodes. However, BlockMonitor
 * will periodically scans the raided files and will fix the placement
 * if it detects violation. 
 */
public class BlockPlacementPolicyECPipe extends BlockPlacementPolicyRaid {
  public int srcnode=0;
  public int prynode=0;
  public int srccount=0;
  public int prtcount=0;

  @Override 
  public void initialize(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap, HostsFileReader hostsReader,
	  DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem namesystem) {
	super.initialize(conf, stats, clusterMap,
	    hostsReader, dnsToSwitchMapping, namesystem);
	
	this.srcnode = conf.getInt("num.src.node", 3);
	this.prynode = conf.getInt("num.parity.node", 1);
	FSNamesystem.LOG.info("ECPipe srcnode = " + this.srcnode);
	FSNamesystem.LOG.info("ECPipe debug prynode = " + this.prynode);
  }

  @Override
  protected FileInfo getFileInfo(FSInodeInfo srcINode, String path) throws IOException {
    FileInfo info = super.getFileInfo(srcINode, path);
    if (info.type == FileType.NOT_RAID) {
      return new FileInfo(FileType.SOURCE, Codec.getCodec("rs"));
    }
    return info;
  }
 
  @Override
  public DatanodeDescriptor[] chooseTarget(String srcInode,
		  int numOfReplicas,
		  DatanodeDescriptor writer,
		  List<DatanodeDescriptor> chosenNodes,
		  List<Node> excludesNodes,
		  long blocksize){
    FSNamesystem.LOG.info("ECPipe srcInode = " + srcInode);
	boolean source = true;
	int index;
    if(srcInode.contains("rs")) {
	  source = false;
    }
    
    if(source) {
      index = srccount;
      srccount += 1;
      srccount = srccount % srcnode;
    } else {
      index = srcnode + prtcount;
      prtcount += 1;
      prtcount = prtcount % prynode;
    }
    FSNamesystem.LOG.info("ECPipe index = " + index);
    DatanodeDescriptor[] ret = new DatanodeDescriptor[numOfReplicas];
    for (int i=0; i<numOfReplicas; i++) {
//	  ret[i]=(DatanodeDescriptor)clusterMap.getNext();
    ret[i]=(DatanodeDescriptor)clusterMap.getNext(index);
    }
    return ret;
  }
}
