package ru.mail.polis.kirpichenkov;

import org.javatuples.Pair;

import java.util.*;

/**
 * Util methods for working with topology
 * @author KirpichenkovPavel
 */
public class TopologyUtil {
  private TopologyUtil() {
  }

  /**
   * Sort nodes in topology to fix their order
   * @param topology set of node urls
   * @return sorted list of node urls
   */
  public static List<String> ordered(Set<String> topology) {
    List<String> nodes = new ArrayList<>(topology);
    Collections.sort(nodes);
    return nodes;
  }

  /**
   * Return N nodes of topology, associated with the given Id, using consistent hashing
   * @param orderedTopology all available nodes
   * @param id key from request
   * @param from requested number of nodes
   * @return collection of N nodes, responsible for the given Id
   * @throws IllegalArgumentException if parameters are incorrect
   */
  public static Collection<String> nodes(List<String> orderedTopology,
                                         String id,
                                         int from) throws IllegalArgumentException {
    if (orderedTopology.size() < 1) {
      throw new IllegalArgumentException("Empty list of nodes in topology");
    }
    if (id.equals("")) {
      throw new IllegalArgumentException("Empty id");
    }
    if (from > orderedTopology.size()) {
      throw new IllegalArgumentException("Not enough replicas");
    }
    int hash = id.hashCode() & Integer.MAX_VALUE;
    int size = orderedTopology.size();
    int start = hash % size;
    Collection<String> results = new ArrayList<>();
    for (int ix = start; ix < start + from; ix++) {
      results.add(orderedTopology.get(ix % size));
    }
    return results;
  }

  public static Pair<Integer, Integer> parseReplicas(String replicas) throws IllegalArgumentException {
    if (!replicas.matches("[1-9][0-9]*/[1-9][0-9]*")) {
      throw new IllegalArgumentException("Request parameters are incorrect");
    }
    String[] fromTo = replicas.split("/");
    int from = Integer.parseInt(fromTo[0]);
    int to = Integer.parseInt(fromTo[1]);
    if (from == 0 || from > to) {
      throw new IllegalArgumentException("Request parameters are incorrect");
    }
    return Pair.with(from, to);
  }

  public static int quorum(int from) {
    return from / 2 + 1;
  }

}
