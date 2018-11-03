package ru.mail.polis.kirpichenkov;

import one.nio.http.*;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static ru.mail.polis.kirpichenkov.Collaboration.entityPath;

public class OneNioHttpServer extends HttpServer {
  private static final Logger logger = Logger.getLogger(OneNioHttpServer.class);
  private InternalDao dao;
  private List<String> topology;
  private String me;

  OneNioHttpServer(HttpServerConfig config, Object... routers) throws IOException {
    super(config, routers);
  }

  void setDao(BasePathGrantingKVDao dao) {
    this.dao = new InternalDao(dao);
  }

  void setTopology(Set<String> topology) {
    this.topology = TopologyUtil.ordered(topology);
    me = findMe(topology);
  }

  /**
   * Try to find this node's url in topology by port number What if nodes are on the same port on
   * different hosts? How to get this host's url?
   *
   * @param topology collection of node urls
   * @return found url or empty string
   */
  private String findMe(Collection<String> topology) {
    return topology
        .stream()
        .filter(
            url -> {
              String[] parts = url.split(":");
              return Integer.parseInt(parts[parts.length - 1]) == port;
            })
        .findFirst()
        .orElse("");
  }

  /**
   * Entry point for all requests
   *
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
    Triplet<String, Integer, Integer> params;
    try {
      params = processParams(request);
    } catch (IllegalArgumentException ex) {
      logger.debug(ex);
      sendBadRequest(session);
      return;
    }
    String id = params.getValue0();
    int acks = params.getValue1();
    int from = params.getValue2();
    Collection<String> nodes = TopologyUtil.nodes(topology, id, from);
    if (Collaboration.isInternal(request)) {
      logger.debug("internal");
      handleAlone(request, session, id);
    } else {
      logger.debug("remote");
      collaborate(request, session, id, nodes, acks);
    }
  }

  /**
   * parse request params
   * @param request incoming request
   * @return entity key, minimal required number of acknowledges, total number of replicas
   * @throws IllegalArgumentException if request contains malformed parameters
   */
  private Triplet<String, Integer, Integer> processParams(Request request)
      throws IllegalArgumentException {
    String id = getId(request);
    String replicas = getReplicas(request);
    if (id.isEmpty()) {
      throw new IllegalArgumentException("Empty Id");
    }

    int acks;
    int from;
    if (replicas.isEmpty()) {
      from = topology.size();
      acks = TopologyUtil.quorum(from);
    } else {
      Pair<Integer, Integer> ackFrom = TopologyUtil.parseReplicas(replicas);
      acks = ackFrom.getValue0();
      from = ackFrom.getValue1();
    }
    return Triplet.with(id, acks, from);
  }

  private void collaborate(
      Request request, HttpSession session, String id, Collection<String> nodes, int acksRequired)
      throws IOException {
    logger.debug("I am " + me);
    List<Result> results = new ArrayList<>();
    for (String nodeUrl : nodes) {
      Result result;
      if (nodeUrl.equals(me)) {
        result = Collaboration.local(request, id, dao);
        logger.debug(
            String.format(
                "Local %s %s%s %s",
                methodToString(request), nodeUrl, entityPath(id), result.getStatus().name()));
      } else {
        result = Collaboration.remote(request, id, nodeUrl);
        logger.debug(
            String.format(
                "Remote %s %s%s %s",
                methodToString(request), nodeUrl, entityPath(id), result.getStatus().name()));
      }
      results.add(result);
    }
    Result result = Collaboration.mergeResults(results, acksRequired);
    Response response =
        result.getStatus() == Result.Status.ERROR
            ? notEnoughReplicas()
            : resultToResponse(request.getMethod(), result);
    session.sendResponse(response);
  }

  private void handleAlone(Request request, HttpSession session, String id) throws IOException {
    Result result = Collaboration.local(request, id, dao);
    Response response = resultToResponse(request.getMethod(), result);
    if (response != null) {
      session.sendResponse(response);
    } else {
      session.sendResponse(serverError());
    }
  }

  private Response resultToResponse(int method, Result result) {
    Response response;
    switch (method) {
      case Request.METHOD_GET:
        response = getResultToResponse(result);
        break;
      case Request.METHOD_PUT:
        response = putResultToResponse(result);
        break;
      case Request.METHOD_DELETE:
        response = deleteResultToResponse(result);
        break;
      default:
        response = notAllowed();
    }
    response.addHeader(Collaboration.TIMESTAMP_HEADER + ": " + result.getTimestamp().toString());
    return response;
  }

  private Response getResultToResponse(Result result) {
    switch (result.getStatus()) {
      case OK:
        return Response.ok(result.getBody());
      case ABSENT:
      case DELETED:
        return notFound();
      case ERROR:
        return serverError();
      default:
        return serverError();
    }
  }

  private Response putResultToResponse(Result result) {
    switch (result.getStatus()) {
      case OK:
        return created();
      case ABSENT:
      case DELETED:
      case ERROR:
        return serverError();
      default:
        return serverError();
    }
  }

  private Response deleteResultToResponse(Result result) {
    switch (result.getStatus()) {
      case OK:
        return accepted();
      case ABSENT:
        return accepted();
      case DELETED:
      case ERROR:
        return serverError();
      default:
        return serverError();
    }
  }

  private void handleStatus(HttpSession session) throws IOException {
    session.sendResponse(Response.ok("Server is running"));
  }

  private Response notFound() {
    return new Response(Response.NOT_FOUND, Response.EMPTY);
  }

  private Response notAllowed() {
    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
  }

  private Response serverError() {
    return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
  }

  private Response badRequest() {
    return new Response(Response.BAD_REQUEST, Response.EMPTY);
  }

  private Response notEnoughReplicas() {
    return new Response(Response.GATEWAY_TIMEOUT, "Not Enough Replicas".getBytes());
  }

  private Response created() {
    return new Response(Response.CREATED, Response.EMPTY);
  }

  private Response accepted() {
    return new Response(Response.ACCEPTED, Response.EMPTY);
  }

  private void sendBadRequest(HttpSession session) throws IOException {
    session.sendResponse(badRequest());
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

  static String methodToString(Request request) {
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
