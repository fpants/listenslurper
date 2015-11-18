package dk.paperstreet.listenslurper;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ElasticsearchAdmin {
    private static final String INDEX_NAME = "listen6";

    private static final String INDEX_SETTINGS = "C:\\jsoe\\proj\\listenslurper\\config\\elasticsearch_index_settings.json";
    private static final String INDEX_MAPPINGS = "C:\\jsoe\\proj\\listenslurper\\config\\elasticsearch_index_mappings.json";

    public void createIndex(Client client) throws IOException {
        Settings settings = Settings.settingsBuilder().loadFromPath(Paths.get(INDEX_SETTINGS)).build();
        CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).execute().actionGet();

        String mappings = new String(Files.readAllBytes(Paths.get(INDEX_MAPPINGS)));
        PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping().setType("listen").setIndices(INDEX_NAME).setSource(mappings).execute().actionGet();
    }
}
