package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

public class ClusterNode {

  private ClusterNode() {}

  public static void main(String[] args) throws IOException {
    // Temporary storage in the file system
    final File data = Files.createTempDirectory();

    final HashSet<String> topology = new HashSet<>();
    final int PORT;
    final String[] defaultArgs = {"8080"};
    if (args.length == 0) {
      args = defaultArgs;
    }
    try {
      PORT = Integer.parseInt(args[0]);
      for (String arg : args) {
        topology.add("http://localhost:" + Integer.parseInt(arg));
      }
    } catch (NumberFormatException ex) {
      throw new RuntimeException("Incorrect port(s)");
    }

    // Start the storage
    final KVDao dao = KVDaoFactory.create(data);
    final KVService storage = KVServiceFactory.create(PORT, dao, topology);
    storage.start();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  storage.stop();
                  try {
                    dao.close();
                  } catch (IOException e) {
                    throw new RuntimeException("Can't close dao", e);
                  }
                }));
  }
}
