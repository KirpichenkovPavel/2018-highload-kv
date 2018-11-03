package ru.mail.polis.kirpichenkov;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.apache.log4j.Logger;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.Set;

/** @author Pavel Kirpichenkov */
public class KVServiceImpl implements KVService {
  private static final Logger logger = Logger.getLogger(KVServiceImpl.class);
  private BasePathGrantingKVDao dao;
  private HttpServerConfig config;
  private Set<String> topology;
  private OneNioHttpServer server;

  private KVServiceImpl() {}

  private void setDao(BasePathGrantingKVDao dao) {
    this.dao = dao;
  }

  private void setConfig(HttpServerConfig config) {
    this.config = config;
  }

  private void setTopology(Set<String> topology) {
    this.topology = topology;
  }

  /**
   * Get new service instance
   *
   * @param dao {@link KVDao} instance
   * @param port service port number
   * @param topology set of all node urls
   * @return new instance
   */
  public static KVService create(KVDao dao, int port, Set<String> topology) {
    HttpServerConfig config = createConfig(port);
    KVServiceImpl kvService = new KVServiceImpl();
    try{
      kvService.setDao((BasePathGrantingKVDao) dao);
    } catch (ClassCastException ex) {
      throw new RuntimeException("KVDao must implement BasePathGrantingKVDao");
    }
    kvService.setConfig(config);
    kvService.setTopology(topology);
    return kvService;
  }

  /**
   * Instantiate and start a server instance
   */
  public void start() {
    try {
      server = new OneNioHttpServer(this.config);
      server.setDao(dao);
      server.setTopology(topology);
      server.start();
    } catch (IOException e) {
      logger.error(e);
      throw new RuntimeException(e);
    }
    for (AcceptorConfig ac : config.acceptors) {
      logger.debug(String.format("server started at %s:%d", ac.address, ac.port));
    }
  }

  public void stop() {
    if (server != null) {
      server.stop();
      logger.debug("server stopped");
    }
  }

  private static HttpServerConfig createConfig(int port) {
    HttpServerConfig config = new HttpServerConfig();
    AcceptorConfig acceptorConfig = new AcceptorConfig();
    acceptorConfig.port = port;
    config.acceptors = new AcceptorConfig[] {acceptorConfig};
    return config;
  }
}
