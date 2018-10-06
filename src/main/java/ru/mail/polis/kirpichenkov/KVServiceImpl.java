package ru.mail.polis.kirpichenkov;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.server.ServerConfig;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * @author Pavel Kirpichenkov
 */
public class KVServiceImpl extends HttpServer implements KVService {
    private KVDao dao;

    private KVServiceImpl(ServerConfig config) throws IOException {
        super(config);
    }

    /**
     * Get new service instance
     *
     * @param dao  {@link KVDao} instance
     * @param port service port number
     * @return new instance
     * @throws IOException
     */
    public static KVServiceImpl create(KVDao dao, int port) throws IOException {
        ServerConfig config = createConfig(port);
        KVServiceImpl kvService = new KVServiceImpl(config);
        kvService.dao = dao;
        return kvService;
    }

    private static ServerConfig createConfig(int port) {
        ServerConfig config = new ServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    public void handleDefault(Request request, HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "/v0/status":
                handleStatus(session);
                break;
            case "/v0/entity":
                handleEntity(request, session);
                break;
            default:
                sendNotFound(session);
        }
    }

    private void handleEntity(Request request, HttpSession session) throws IOException {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                handleGet(request, session);
                break;
            case Request.METHOD_PUT:
                handlePut(request, session);
                break;
            case Request.METHOD_DELETE:
                handleDelete(request, session);
                break;
            default:
                sendMethodNotAllowed(session);
        }
    }

    private void handleGet(Request request, HttpSession session) throws IOException {
        String id = getId(request);
        if (id.isEmpty()) {
            sendBadRequest(session);
            return;
        }
        try {
            byte[] body = dao.get(id.getBytes());
            session.sendResponse(Response.ok(body));
        } catch (NoSuchElementException ex) {
            sendNotFound(session);
        } catch (IOException ex) {
            sendServerError(session);
        }
    }

    private void handlePut(Request request, HttpSession session) throws IOException {
        String id = getId(request);
        if (id.isEmpty()) {
            sendBadRequest(session);
            return;
        }
        try {
            dao.upsert(id.getBytes(), request.getBody());
            session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
        } catch (IOException ex) {
            sendServerError(session);
        }
    }

    private void handleDelete(Request request, HttpSession session) throws IOException {
        String id = getId(request);
        if (id.isEmpty()) {
            sendBadRequest(session);
            return;
        }
        try {
            dao.remove(id.getBytes());
            session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
        } catch (IOException ex) {
            sendServerError(session);
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
        return request.getParameter("id=");
    }
}
