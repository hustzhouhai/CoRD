package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;



import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.util.Time;

// add ECPipe
import org.apache.hadoop.hdfs.server.datanode.erasurecode.ECPipeInputStream;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import java.util.Arrays;
///
/**
 * {@link ECPipeReconstructor} reconstruct one missed striped block in the striped block group
 */

 @InterfaceAudience.Private
 class ECPipeReconstructor extends StripedReconstructor
    implements Runnable{

    private StripedWriter stripedWriter;

    // add ECPipe
    private ECPipeInputStream[] ecInputStream;
    private String[] lostFileName;
    private DataNode datanode;
    ///
    // add for multiple failure

    ECPipeReconstructor(ErasureCodingWorker worker,
        StripedReconstructionInfo stripedReconInfo) {
        super(worker, stripedReconInfo);
        stripedWriter = new StripedWriter(this, getDatanode(),
            getConf(), stripedReconInfo);
        // add ECPipe
        this.datanode = worker.getDatanode();
        //
    }

    boolean hasValidTargets() {
        return stripedWriter.hasValidTargets();
    }
    
    private ECPipeInputStream createInputStream(String[] lostFileNames, int lostNum, int index) {
        return new ECPipeInputStream(lostFileNames, getConf(), datanode, lostNum, index);
    }

    @Override
    public void run() {
	//Measure
	long start = Time.monotonicNow();
        try{
            
            int lostFileNum = stripedWriter.getLostFileName().length;
            this.lostFileName = stripedWriter.getLostFileName();
            this.ecInputStream = new ECPipeInputStream[lostFileNum];
            //LOG.info("ECPipe: The lostFileName is " + lostFileName);
            for(int i = 0; i < lostFileNum; i++) {
              this.ecInputStream[i] = createInputStream(lostFileName, lostFileNum, i);
            }

            initDecoderIfNecessary();
            // add by Zuoru: For StripedReader, we just do the init() to maintain 
            // the checksum here.
            // Measure
            long Reader1 = Time.monotonicNow();
            getStripedReader().init();
            long Reader2 = Time.monotonicNow();
	    LOG.info("Measurement: StripedReader(): " + (Reader2 - Reader1));

            stripedWriter.init();
	    long Writer1 = Time.monotonicNow();
	    LOG.info("Measurement: StripedWriter(): " + (Writer1 - Reader2));
                        
            reconstruct();

            stripedWriter.endTargetBlocks();
            

        } catch (Throwable e) {
            LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
            getDatanode().getMetrics().incrECFailedReconstructionTasks();
        } finally {
            getDatanode().decrementXmitsInProgress(getXmits());
            getStripedReader().close();
            stripedWriter.close();
            cleanup();
        }
	long end = Time.monotonicNow();
	LOG.info("Measurement: the total repair time: " + (end - start));
    }

    @Override
    void reconstruct() throws IOException {
	//Measurement
	long startTime = Time.monotonicNow();

        while (getPositionInBlock() < getMaxTargetLength()) {
	    long startTime2 = Time.monotonicNow();
            DataNodeFaultInjector.get().stripedBlockReconstruction();

            final int toReconstructLen = ecInputStream[0].getPacketSize();

	    long t1 = Time.monotonicNow();
            reconstructTargets(toReconstructLen);
	    long t2 = Time.monotonicNow();

            if (stripedWriter.transferData2Targets() == 0){
                String error = "Transfer failed for all targets.";
                throw new IOException(error);
            }

            updatePositionInBlock(toReconstructLen);
      
            clearBuffers();
	    long endTime2 = Time.monotonicNow();
	    //LOG.info("Measurment: A packet takes time: " + (endTime2 - startTime2) + ", reconstructTargets(): " + (t2 - t1));
        }
	long endTime = Time.monotonicNow();
	LOG.info("Measurment: reconstruction time: " + (endTime - startTime));
    }

    private void reconstructTargets(int toReconstructLen) throws IOException {
	long t1 = Time.monotonicNow();
        ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);
	long t2 = Time.monotonicNow();
        //TODO: read from ECPipe , and send it to the outputs.
	      /* Because we just consider the case of a single block loss,
	       *  we can just set the index as 0 here.
        */
        if(outputs.length != ecInputStream.length) {
          LOG.info("Error: the outputs cannot match ecInputStream");
        }
        for(int i = 0; i < ecInputStream.length; i++) {
          byte[] recoverPkt = ecInputStream[i].readFromECPipe(toReconstructLen);
          outputs[i].put(recoverPkt);
	        outputs[i].rewind();
        }
	long t3 = Time.monotonicNow();
        stripedWriter.updateRealTargetBuffers(toReconstructLen);
    }

    private void clearBuffers(){
        stripedWriter.clearBuffers();
    }
    
}

