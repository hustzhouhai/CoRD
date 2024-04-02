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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.util.Time;

/**
 * StripedBlockReconstructor reconstruct one or more missed striped block in
 * the striped block group, the minimum number of live striped blocks should
 * be no less than data block number.
 */
@InterfaceAudience.Private
class StripedBlockReconstructor extends StripedReconstructor
    implements Runnable {

  private StripedWriter stripedWriter;

  StripedBlockReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo) {
    super(worker, stripedReconInfo);

    stripedWriter = new StripedWriter(this, getDatanode(),
        getConf(), stripedReconInfo);
  }

  boolean hasValidTargets() {
    return stripedWriter.hasValidTargets();
  }

  @Override
  public void run() {
    //Measure
    long start = Time.monotonicNow();
    LOG.info("Measurement: starting time: {}", start);
    try {
            
      initDecoderIfNecessary();
      // Measure
      long Reader1 = Time.monotonicNow();
      getStripedReader().init();
      long Reader2 = Time.monotonicNow();
      LOG.info("Measurement: StripedReader(): " + (Reader2 - Reader1));

      stripedWriter.init();
      long Writer1 = Time.monotonicNow();
      LOG.info("Measurement: StripedWriter(): " + (Writer1 - Reader2));
      // add by Zuoru: for test.
      int lostFileNum = stripedWriter.getLostFileName().length;
      LOG.info("Zuoru: the num of failure block in multiple failure: " + lostFileNum );
      String[] lostFileNames = stripedWriter.getLostFileName();
      for(int i = 0; i < lostFileNum; i++){
        LOG.info("Zuoru: Mulitiple Failure: LostFileName(): " + lostFileNames[i]);
      }
      ///
      reconstruct();

      stripedWriter.endTargetBlocks();


      // Currently we don't check the acks for packets, this is similar as
      // block replication.
    } catch (Throwable e) {
      LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
      getDatanode().getMetrics().incrECFailedReconstructionTasks();
    } finally {
      getDatanode().decrementXmitsInProgress(getXmits());
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionTasks();
      metrics.incrECReconstructionBytesRead(getBytesRead());
      metrics.incrECReconstructionRemoteBytesRead(getRemoteBytesRead());
      metrics.incrECReconstructionBytesWritten(getBytesWritten());
      getStripedReader().close();
      stripedWriter.close();
      cleanup();
    }
    //Measure
    long end = Time.monotonicNow();
    LOG.info("Measurement: the total repair time: " + (end - start));
  }

  @Override
  void reconstruct() throws IOException {
    //Measurement
    long startTime = Time.monotonicNow();
    int counter = 0;
    while (getPositionInBlock() < getMaxTargetLength()) {
      DataNodeFaultInjector.get().stripedBlockReconstruction();
      long remaining = getMaxTargetLength() - getPositionInBlock();
      final int toReconstructLen =
          (int) Math.min(getStripedReader().getBufferSize(), remaining);

      long start = Time.monotonicNow();
      // step1: read from minimum source DNs required for reconstruction.
      // The returned success list is the source DNs we do real read from
      getStripedReader().readMinimumSources(toReconstructLen);
      long readEnd = Time.monotonicNow();
      // add by Zuoru: Test

      // step2: decode to reconstruct targets
      reconstructTargets(toReconstructLen);
      long decodeEnd = Time.monotonicNow();

      // step3: transfer data
      if (stripedWriter.transferData2Targets() == 0) {
        String error = "Transfer failed for all targets.";
        throw new IOException(error);
      }
      long writeEnd = Time.monotonicNow();

      // Only the succeed reconstructions are recorded.
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionReadTime(readEnd - start);
      metrics.incrECReconstructionDecodingTime(decodeEnd - readEnd);
      metrics.incrECReconstructionWriteTime(writeEnd - decodeEnd);

      updatePositionInBlock(toReconstructLen);
      counter ++;
      clearBuffers();

      long wholeEnd = Time.monotonicNow();
      LOG.info("A packet takes time: " + (wholeEnd - start) + ", readEnd - start = " + (readEnd - start));
    } 
    long endTime = Time.monotonicNow();
    LOG.info("Measurment: reconstruction time: " + (endTime - startTime));
    LOG.info("Measurment: reconstruction counter: " + counter);
  }

  private void reconstructTargets(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);

    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    // add by Zuoru: Test
    //byte[] mydata = new byte[toReconstructLen];
    //outputs[0] = ByteBuffer.wrap(mydata);
    ///
    long start = System.nanoTime();
    // add by Zuoru:
    LOG.info("Zuoru: the length of inputs: " + inputs.length);
    LOG.info("Zuoru: the length of outputs: " + outputs.length); 
    getDecoder().decode(inputs, erasedIndices, outputs);
    long end = System.nanoTime();
    this.getDatanode().getMetrics().incrECDecodingTime(end - start);

    stripedWriter.updateRealTargetBuffers(toReconstructLen);
  }

  /**
   * Clear all associated buffers.
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();

    stripedWriter.clearBuffers();
  }
}
