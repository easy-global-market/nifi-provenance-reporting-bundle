/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.egm.nifi.reporting;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"elasticsearch", "provenance"})
@CapabilityDescription("A provenance reporting task that writes to Elasticsearch")
public class ElasticsearchProvenanceReporter extends AbstractProvenanceReporter {
    public static final PropertyDescriptor ELASTICSEARCH_URL = new PropertyDescriptor
            .Builder().name("Elasticsearch URL")
            .displayName("Elasticsearch URL")
            .description("The address for Elasticsearch")
            .required(true)
            .defaultValue("http://localhost:9200")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_INDEX = new PropertyDescriptor
            .Builder().name("Index")
            .displayName("Index")
            .description("The name of the Elasticsearch index")
            .required(true)
            .defaultValue("nifi")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final Map<String, ElasticsearchClient> esClients = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private ElasticsearchClient getElasticsearchClient(String elasticsearchUrl) throws MalformedURLException {
        if (esClients.containsKey(elasticsearchUrl))
            return esClients.get(elasticsearchUrl);

        URL url = new URL(elasticsearchUrl);
        RestClient restClient = RestClient.builder(new HttpHost(url.getHost(), url.getPort())).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        ElasticsearchClient client = new ElasticsearchClient(transport);
        esClients.put(elasticsearchUrl, client);
        return client;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(ELASTICSEARCH_URL);
        descriptors.add(ELASTICSEARCH_INDEX);
        return descriptors;
    }

    public void indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException {
        final String elasticsearchUrl = context.getProperty(ELASTICSEARCH_URL).getValue();
        final String elasticsearchIndex = context.getProperty(ELASTICSEARCH_INDEX).evaluateAttributeExpressions().getValue();
        final ElasticsearchClient client = getElasticsearchClient(elasticsearchUrl);
        final String id = Long.toString((Long) event.get("event_id"));

        if (!event.containsKey("process_group_name") || !event.containsKey("component_name")) {
            getLogger().warn("Provenance event has no process group or processor, ignoring");
            return;
        }

        final Date eventTimeMillis = (Date) event.get("event_time");

        Map<String, Object> preparedEvent = new HashMap<>();
        preparedEvent.put("event_time", event.get("event_time"));
        preparedEvent.put("event_time_iso", sdf.format(eventTimeMillis));
        preparedEvent.put("component_type", event.get("component_type"));
        preparedEvent.put("component_url", event.get("component_url"));
        preparedEvent.put("component_name", event.get("component_name"));
        preparedEvent.put("process_group_name", event.get("process_group_name"));
        preparedEvent.put("process_group_id", event.get("process_group_id"));
        preparedEvent.put("event_type", event.get("event_type"));
        preparedEvent.put("status", event.get("status"));
        preparedEvent.put("download_input_content_uri", event.get("download_input_content_uri"));
        preparedEvent.put("download_output_content_uri", event.get("download_output_content_uri"));
        preparedEvent.put("view_input_content_uri", event.get("view_input_content_uri"));
        preparedEvent.put("view_output_content_uri", event.get("view_output_content_uri"));
        preparedEvent.put("updated_attributes", objectMapper.writeValueAsString(event.get("updated_attributes")));
        preparedEvent.put("previous_attributes", objectMapper.writeValueAsString(event.get("previous_attributes")));
        if (event.containsKey("details"))
            preparedEvent.put("details", event.get("details"));

        final IndexRequest<Map<String, Object>> indexRequest = new
                IndexRequest.Builder<Map<String, Object>>()
                .index(elasticsearchIndex)
                .id(id)
                .document(preparedEvent)
                .build();
        try {
            client.index(indexRequest);
        } catch (ElasticsearchException ex) {
            getLogger().error("Error while indexing event {}", id, ex);
        }
    }
}
