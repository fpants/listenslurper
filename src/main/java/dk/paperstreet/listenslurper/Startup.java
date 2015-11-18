package dk.paperstreet.listenslurper;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;

public class Startup {
    public static void main(String... args) throws IOException, InterruptedException {
        Startup startup = new Startup();
        Client client = startup.startElasticSearch();

        Thread.sleep(1000);

        ElasticsearchAdmin esa = new ElasticsearchAdmin();
        //esa.createIndex(client);

        new Pumper(client).start();

        //client.close();
        //node.close();
    }

    Client startElasticSearch() {
        Settings settings = Settings.settingsBuilder()
                .put("path.home", "c:/jsoe/proj/listenslurper-data")
                .put("http.enabled", "true")
                .build();
        Node node = NodeBuilder.nodeBuilder().settings(settings).data(true).local(true).node();
        node.start();

        Client client = node.client();

        return client;
    }

    static class Pumper extends Thread {
        Client client;

        Pumper(Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            while (true) {
                String mappings = "{\n" +
                        "  \"listen_type\": \"single\",\n" +
                        "  \"payload\": [\n" +
                        "    {\n" +
                        "      \"listened_at\": " + System.currentTimeMillis() / 1000 + ",\n" +
                        "      \"track_metadata\": {\n" +
                        "        \"additional_info\": {\n" +
                        "          \"release_mbid\": \"bf9e91ea-8029-4a04-a26a-224e00a83266\",\n" +
                        "          \"artist_mbids\": [\n" +
                        "            \"db92a151-1ac2-438b-bc43-b82e149ddd50\"\n" +
                        "          ],\n" +
                        "          \"recording_mbid\": \"98255a8c-017a-4bc7-8dd6-1fa36124572b\",\n" +
                        "          \"tags\": [ \"you\", \"just\", \"got\", \"rick rolled!\"]\n" +
                        "        },\n" +
                        "        \"artist_name\": \"Rick Astley\",\n" +
                        "        \"track_name\": \"Never Gonna Give You Up\",\n" +
                        "        \"release_name\": \"Whenever you need somebody\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}";

                client.prepareIndex("listen6", "listen").setSource(mappings).get();
                try {
                    Thread.sleep((long) (Math.random() * 120000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
