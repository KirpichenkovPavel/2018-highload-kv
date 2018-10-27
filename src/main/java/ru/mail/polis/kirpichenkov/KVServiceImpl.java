package ru.mail.polis.kirpichenkov;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.apache.log4j.Logger;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;

/** @author Pavel Kirpichenkov */
public class KVServiceImpl implements KVService {
  private static final Logger logger = Logger.getLogger(KVServiceImpl.class);
  private KVDao dao;
  private HttpServerConfig config;
  private OneNioHttpServer server;

  private KVServiceImpl() {}

  private void setDao(KVDao dao) {
    this.dao = dao;
  }

  private void setConfig(HttpServerConfig config) {
    this.config = config;
  }

  /**
   * Get new service instance
   *
   * @param dao {@link KVDao} instance
   * @param port service port number
   * @return new instance
   * @throws IOException
   */
  public static KVServiceImpl create(KVDao dao, int port) {
    HttpServerConfig config = createConfig(port);
    KVServiceImpl kvService = new KVServiceImpl();
    kvService.setDao(dao);
    kvService.setConfig(config);
    return kvService;
  }

  public void start() {
    try {
      server = new OneNioHttpServer(this.config);
      server.setDao(dao);
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
