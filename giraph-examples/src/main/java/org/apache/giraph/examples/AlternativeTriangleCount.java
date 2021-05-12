package org.apache.giraph.examples;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class AlternativeTriangleCount extends BasicComputation<
  DoubleWritable, DoubleWritable, FloatWritable, DoubleWritable> {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(AlternativeTriangleCount.class);

  @Override
  public void compute(
    Vertex<DoubleWritable, DoubleWritable, FloatWritable> vertex,
    Iterable<DoubleWritable> messages) throws IOException {

    if (getSuperstep() == 0) {
      for (Edge<DoubleWritable, FloatWritable> edge: vertex.getEdges()) {
        if (edge.getTargetVertexId().compareTo(vertex.getId()) == 1) {
          sendMessage(edge.getTargetVertexId(), vertex.getId());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " sent message " +
              vertex.getId() + " to vertex " + edge.getTargetVertexId());
          }
        }
      }
    }

    /** Second superstep releases messages to message.get() < vertex.getId() < targetVertexId() **/
    if (getSuperstep() == 1) {
      for (DoubleWritable message: messages) {
        for (Edge<DoubleWritable, FloatWritable> edge: vertex.getEdges()) {
          if (edge.getTargetVertexId().compareTo(vertex.getId()) +
            vertex.getId().compareTo(message) == 2) {
            sendMessage(edge.getTargetVertexId(), message);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Vertex " + vertex.getId() + " sent message " +
                message + " to vertex " + edge.getTargetVertexId());
            }
          }
        }
      }
    }
    /** Sends messages to all its neighbours, the messages it receives **/
    if (getSuperstep() == 2) {
      for (DoubleWritable message: messages) {
        sendMessageToAllEdges(vertex, message);
      }
    }

    if (getSuperstep() == 3) {
      double Value = 0.0;
      for (DoubleWritable message: messages) {
        if (vertex.getId().equals(message)) {
          Value += 1.0;
        }
      }
      vertex.setValue(new DoubleWritable(Value));
    }

    vertex.voteToHalt();
  }
}