package org.apache.giraph.examples;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MySSPComputation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /** The shortest paths id */
    public static final LongConfOption SOURCE_ID =
            new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
                    "The shortest paths id");
    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(MySSPComputation.class);

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
        return vertex.getId().get() == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        simulateWorkerFailure();
        if (getSuperstep() == 0) {
            vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
        }
        /*LOG.info("fzhang: superstep:" + getSuperstep() +
                " appAttempt:" + getApplicationAttempt() +
                " myWorkerIndex:" + getMyWorkerIndex());
        if (getSuperstep() == 2) {
            if (0 == getApplicationAttempt()) {
                if (1 == getMyWorkerIndex()) {
                    LOG.info("******************RuntimeException*********************");
                    LOG.info("fzhang: superstep:" + getSuperstep() +
                            " appAttempt:" + getApplicationAttempt() +
                            " myWorkerIndex:" + getMyWorkerIndex());
                    LOG.info("***************************************");
                    throw new RuntimeException("some failure occur in superstep 2");
                }
            }
        }*/
        double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
                    " vertex value = " + vertex.getValue());
        }
        if (minDist < vertex.getValue().get()) {
            vertex.setValue(new DoubleWritable(minDist));
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                double distance = minDist + edge.getValue().get();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Vertex " + vertex.getId() + " sent to " +
                            edge.getTargetVertexId() + " = " + distance);
                }
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
            }
        }
        vertex.voteToHalt();

        try {
//            System.err.println("now Sleep 3 second...");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
