package ru.mail.polis.kirpichenkov;

import one.nio.http.*;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import ru.mail.polis.KVDao;
import ru.mail.polis.kirpichenkov.Result.Status;

import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Set;

public class OneNioHttpServer extends HttpServer {
  private static final Logger logger = Logger.getLogger(OneNioHttpServer.class);
  private KVDao dao;
  private List<String> topology;
  private String me;

  OneNioHttpServer(HttpServerConfig config, Object... routers) throws IOException {
    super(config, routers);
  }

  void setDao(KVDao dao) {
    this.dao = dao;
  }

  void setTopology(Set<String> topology) {
    this.topology = TopologyUtil.ordered(topology);
    me = findMe(topology);
  }

  /**
   * Try to find this node's url in topology by port number
   * What if nodes are on the same port on different hosts?
   * How to get this host's url?
   * @param topology collection of node urls
   * @return found url or empty string
   */
  private String findMe(Collection<String> topology) {
    return topology
        .stream()
        .filter(url -> {
          String[] parts = url.split(":");
          return Integer.parseInt(parts[parts.length - 1]) == port;
        })
        .findFirst()
        .orElse("");
  }

  /**
   * Entry point for all requests
   * @throws IOException
   */
  public void handleDefault(Request request, HttpSession session) throws IOException {
    logger.debug(String.format("%s %s", methodToString(request), request.getURI()));
    switch (request.getPath()) {
      case "/v0/status":
        handleStatus(session);
        break;
      case "/v0/entity":
        handleEntity(request, session);
        break;
      default:
        sendBadRequest(session);
    }
  }

  private void handleEntity(Request request, HttpSession session) throws IOException {
    Pair<String, Collection<String>> idAndNodes;
    try{
      idAndNodes = processParams(request);
    } catch (IllegalArgumentException ex) {
      sendBadRequest(session);
      return;
    }
    String id = idAndNodes.getValue0();
    Collection<String> nodes = idAndNodes.getValue1();
    // TODO add header for internal requests
    if (true) {
      handleAlone(request, session, id);
    } else {
      collaborate(request, session, id, nodes);
    }
  }

  private Pair<String, Collection<String>> processParams(Request request)
      throws IllegalArgumentException {
    String id = getId(request);
    String replicas = getReplicas(request);
    if (id.isEmpty()) {
      throw new IllegalArgumentException("Empty Id");
    }

    Pair<Integer, Integer> ackFrom;
    if (replicas.isEmpty()) {
      ackFrom = TopologyUtil.quorum(topology.size());
    } else {
      ackFrom = TopologyUtil.parseReplicas(replicas);
    }
    Collection<String> nodes = TopologyUtil.nodes(topology, id, ackFrom.getValue1());
    return Pair.with(id, nodes);
  }

  private void collaborate(Request request,
                           HttpSession session,
                           String Id,
                           Collection<String> nodes) throws IOException {

  }

  private void handleAlone(Request request,
                           HttpSession session,
                           String id) throws IOException {
    switch (request.getMethod()) {
      case Request.METHOD_GET:
        handleGetAlone(session, id);
        break;
      case Request.METHOD_PUT:
        handlePut(request, session, id);
        break;
      case Request.METHOD_DELETE:
        handleDelete(request, session, id);
        break;
      default:
        sendMethodNotAllowed(session);
    }
  }

  private void handleGetAlone(HttpSession session, String id) throws IOException {
    try {
      Result result = innerGet(id);
      switch (result.getStatus()) {
        case OK:
          session.sendResponse(Response.ok(result.getBody()));
          break;
        case ABSENT:
          sendNotFound(session);
          break;
        case DELETED:
          sendNotFound(session);
          break;
      }
    } catch (IOException ex) {
      sendServerError(session);
      logger.error(ex);
    }
  }

  private Result innerGet(String id) throws IOException {
    Result result = new Result();
    try {
      result.setBody(dao.get(id.getBytes()));
      result.setStatus(Status.OK);
    } catch (NoSuchElementException ex) {
      // TODO tombstones
      result.setStatus(Status.ABSENT);
    }
    return result;
  }

  private void handlePut(Request request, HttpSession session, String id) throws IOException {
    try {
      dao.upsert(id.getBytes(), request.getBody());
      session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
    } catch (IOException ex) {
      sendServerError(session);
      logger.error("server error", ex);
    }
  }

  private void handleDelete(Request request, HttpSession session, String id) throws IOException {
    try {
      dao.remove(id.getBytes());
      session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
    } catch (IOException ex) {
      sendServerError(session);
      logger.error("server error", ex);
    }
  }

  private void handleStatus(HttpSession session) throws IOException {
    session.sendResponse(Response.ok("Server is running"));
  }

  private void sendNotFound(HttpSession session) throws IOException {
    session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
  }

  private void sendMethodNotAllowed(HttpSession session) throws IOException {
    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
  }

  private void sendServerError(HttpSession session) throws IOException {
    session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
  }

  private void sendBadRequest(HttpSession session) throws IOException {
    session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
  }

  private String getId(Request request) {
    return getParameter(request, "id");
  }

  private String getReplicas(Request request) {
    return getParameter(request, "replicas");
  }

  private String getParameter(Request request, String name) {
    String param = request.getParameter(String.format("%s=", name));
    if (param == null) {
      return "";
    } else {
      return param;
    }
  }

  private String methodToString(Request request) {
    switch (request.getMethod()) {
      case Request.METHOD_GET:
        return "GET";
      case Request.METHOD_POST:
        return "POST";
      case Request.METHOD_HEAD:
        return "HEAD";
      case Request.METHOD_OPTIONS:
        return "OPTIONS";
      case Request.METHOD_PUT:
        return "PUT";
      case Request.METHOD_DELETE:
        return "DELETE";
      case Request.METHOD_TRACE:
        return "TRACE";
      case Request.METHOD_CONNECT:
        return "CONNECT";
      case Request.METHOD_PATCH:
        return "PATCH";
      default:
        return "";
    }
  }
}
