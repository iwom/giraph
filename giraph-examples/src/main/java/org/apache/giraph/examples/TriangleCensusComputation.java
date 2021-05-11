package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;
import org.apache.log4j.Logger;

public class TriangleCensusComputation extends BasicComputation<DoubleWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  private static Logger LOG = Logger.getLogger(TriangleCensusComputation.class);

  @Override
  public void compute(Vertex<DoubleWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {


    if (getSuperstep() == 0) {
      LOG.info("Starting triangle computation");

      for (Edge<DoubleWritable, FloatWritable> edge : vertex.getEdges()) {
        sendMessage(edge.getTargetVertexId(), vertex.getId());
        LOG.info("Vertex " + vertex.getId() + " sent message " +
          vertex.getId() + " to vertex " + edge.getTargetVertexId());
      }
    }

    if (getSuperstep() == 1) {
      for (DoubleWritable message : messages) {
        for (Edge<DoubleWritable, FloatWritable> edge : vertex.getEdges()) {
          sendMessage(edge.getTargetVertexId(), message);
          LOG.info("Vertex " + vertex.getId() + " sent message " +
            message + " to vertex " + edge.getTargetVertexId());
        }
      }
    }

    if (getSuperstep() == 2) {
      for (DoubleWritable message : messages) {
        sendMessageToAllEdges(vertex, message);
      }
    }

    if (getSuperstep() == 3) {
      double value = 0.0;
      for (DoubleWritable message : messages) {
        if (vertex.getId().equals(message)) {
          value += 1.0;
        }
      }
      LOG.info("Vertex " + vertex.getId() + " is part of " + value + " triangles and has " + vertex.getNumEdges() + " outgoing edges");
      double neighbourPairs = ((double) vertex.getNumEdges() * (vertex.getNumEdges() - 1.0));
      if (neighbourPairs > 0.0) {
        value = (2.0 * value) / neighbourPairs;
      } else {
        value = 0.0;
      }
      vertex.setValue(new DoubleWritable(value));
    }

    vertex.voteToHalt();
  }
}
