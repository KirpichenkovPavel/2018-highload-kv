package ru.mail.polis.kirpichenkov;

import one.nio.http.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static ru.mail.polis.kirpichenkov.Collaboration.entityPath;

public class OneNioHttpServer extends HttpServer {
  private static final Logger logger = LogManager.getLogger(OneNioHttpServer.class);
  private InternalDao dao;
  private List<String> topology;
  private String me;
  private ExecutorService threadPool;

  OneNioHttpServer(
      @NotNull final HttpServerConfig config,
      final Object... routers
  ) throws IOException {
    super(config, routers);
  }

  @Override
  public void start() {
    super.start();
    threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void stop() {
    super.stop();
    threadPool.shutdown();
  }

  public void setDao(@NotNull final BasePathGrantingKVDao dao) {
    this.dao = new InternalDao(dao);
  }

  public void setTopology(@NotNull final Set<String> topology) {
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
  @NotNull
  private String findMe(@NotNull final Collection<String> topology) {
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
  public void handleDefault(
      @NotNull final Request request,
      @NotNull final HttpSession session
  ) throws IOException
  {
    logger.debug("{} {}", () -> methodToString(request), request::getURI);
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

  private void handleEntity(
      @NotNull final Request request,
      @NotNull final HttpSession session
  ) throws IOException
  {
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
   *
   * @param request incoming request
   * @return entity key, minimal required number of acknowledges, total number of replicas
   * @throws IllegalArgumentException if request contains malformed parameters
   */
  @NotNull
  private Triplet<String, Integer, Integer> processParams(@NotNull final Request request)
      throws IllegalArgumentException
  {
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
      @NotNull final Request request,
      @NotNull final HttpSession session,
      @NotNull final String id,
      @NotNull final Collection<String> nodes,
      final int acksRequired
  ) throws IOException
  {
    logger.debug("I am {}", me);
    List<Result> results = new ArrayList<>();
    CompletionService<Result> completionService = new ExecutorCompletionService<>(threadPool);
    for (String nodeUrl : nodes) {
      Callable<Result> task = () -> {
        Result result;
        if (nodeUrl.equals(me)) {
          result = Collaboration.local(request, id, dao);
          logger.debug("Local {} {}{} {}",
              () -> methodToString(request),
              () -> nodeUrl,
              () -> entityPath(id),
              () -> result.getStatus().name());
        } else {
          result = Collaboration.remote(request, id, nodeUrl);
          logger.debug("Remote {} {}{} {}",
              () -> methodToString(request),
              () -> nodeUrl,
              () -> entityPath(id),
              () -> result.getStatus().name());
        }
        return result;
      };
      completionService.submit(task);
    }
    int receivedResultsCounter = 0;
    while (receivedResultsCounter < nodes.size()) {
      try{
        Future<Result> wrappedResult = completionService.take();
        results.add(wrappedResult.get());
      } catch (InterruptedException | ExecutionException ex) {
        logger.error("{}\nCause: {}", () -> ex, ex::getCause);
        results.add(Collaboration.error());
      } finally{
        receivedResultsCounter++;
        logger.debug("Received: {}", receivedResultsCounter);
      }
    }
    Result mergeResult = Collaboration.mergeResults(results, acksRequired);
    Response response =
        mergeResult.getStatus() == Result.Status.ERROR
            ? notEnoughReplicas()
            : resultToResponse(request.getMethod(), mergeResult);
    session.sendResponse(response);
  }

  private void handleAlone(
      @NotNull final Request request,
      @NotNull final HttpSession session,
      @NotNull final String id
  ) throws IOException
  {
    Result result = Collaboration.local(request, id, dao);
    Response response = resultToResponse(request.getMethod(), result);
    session.sendResponse(response);
  }

  @NotNull
  private Response resultToResponse(
      final int method,
      @NotNull final Result result
  ) {
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

  @NotNull
  private Response getResultToResponse(@NotNull final Result result) {
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

  @NotNull
  private Response putResultToResponse(@NotNull final Result result) {
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

  @NotNull
  private Response deleteResultToResponse(@NotNull final Result result) {
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

  private void handleStatus(@NotNull final HttpSession session) throws IOException {
    session.sendResponse(Response.ok("Server is running"));
  }

  @NotNull
  private Response notFound() {
    return new Response(Response.NOT_FOUND, Response.EMPTY);
  }

  @NotNull
  private Response notAllowed() {
    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
  }

  @NotNull
  private Response serverError() {
    return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
  }

  @NotNull
  private Response badRequest() {
    return new Response(Response.BAD_REQUEST, Response.EMPTY);
  }

  @NotNull
  private Response notEnoughReplicas() {
    return new Response(Response.GATEWAY_TIMEOUT, "Not Enough Replicas".getBytes());
  }

  @NotNull
  private Response created() {
    return new Response(Response.CREATED, Response.EMPTY);
  }

  @NotNull
  private Response accepted() {
    return new Response(Response.ACCEPTED, Response.EMPTY);
  }

  private void sendBadRequest(@NotNull final HttpSession session) throws IOException {
    session.sendResponse(badRequest());
  }

  @NotNull
  private String getId(@NotNull final Request request) {
    return getParameter(request, "id");
  }

  @NotNull
  private String getReplicas(@NotNull final Request request) {
    return getParameter(request, "replicas");
  }

  @NotNull
  private String getParameter(
      @NotNull final Request request,
      @NotNull final String name
  ) {
    String param = request.getParameter(String.format("%s=", name));
    if (param == null) {
      return "";
    } else {
      return param;
    }
  }

  @NotNull
  static String methodToString(@NotNull final Request request) {
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
