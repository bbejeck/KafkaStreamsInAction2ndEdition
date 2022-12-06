package bbejeck.spring.streams.container;

import bbejeck.spring.model.LoanAppRollup;
import bbejeck.spring.model.QueryResponse;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.KeyQueryMetadata.NOT_AVAILABLE;

/**
 * User: Bill Bejeck
 * Date: 11/16/22
 * Time: 7:34 PM
 */

@RestController
@RequestMapping("/loan-app-iq")
public class LoanApplicationController {

    @Value("${loan.app.store.name}")
    private String storeName;

    @Value("${application.server}")
    private String applicationServer;
    private final KafkaStreams kafkaStreams;
    private final RestTemplate restTemplate;
    private static final int MAX_RETRIES = 3;
    private final Time time = Time.SYSTEM;

    private static final String BASE_IQ_URL = "http://{host}:{port}/loan-app-iq";

    private HostInfo thisHostInfo;

    private enum HostStatus {
        ACTIVE,
        STANDBY
    }

    @Autowired
    public LoanApplicationController(KafkaStreams kafkaStreams,
                                     RestTemplateBuilder restTemplateBuilder) {
        this.kafkaStreams = kafkaStreams;
        this.restTemplate = restTemplateBuilder.build();
    }


    @PostConstruct
    public void init() {
        String[] parts = applicationServer.split(":");
        thisHostInfo = new HostInfo(parts[0], Integer.parseInt(parts[1]));
    }

    @GetMapping(value = "/{loanType}")
    public QueryResponse<LoanAppRollup> getAggregationKeyQuery(@PathVariable String loanType) {
        KeyQueryMetadata keyMetadata = getKeyMetadata(loanType, Serdes.String().serializer());
        if (keyMetadata == null) {
            return QueryResponse.withError(String.format("ERROR: Unable to get key metadata after %d retries", MAX_RETRIES));
        }
        HostInfo activeHost = keyMetadata.activeHost();
        Set<HostInfo> standbyHosts = keyMetadata.standbyHosts();
        KeyQuery<String, ValueAndTimestamp<LoanAppRollup>> keyQuery = KeyQuery.withKey(loanType);
        QueryResponse<LoanAppRollup> queryResponse = doKeyQuery(activeHost, keyQuery, keyMetadata, loanType, HostStatus.ACTIVE);
        if (queryResponse.hasError() && !standbyHosts.isEmpty()) {
            Optional<QueryResponse<LoanAppRollup>> standbyResponse = standbyHosts.stream()
                    .map(standbyHost -> doKeyQuery(standbyHost, keyQuery, keyMetadata, loanType, HostStatus.STANDBY))
                    .filter(resp -> resp != null && !resp.hasError())
                    .findFirst();
            if (standbyResponse.isPresent()) {
                queryResponse = standbyResponse.get();
            }
        }
        return queryResponse;

    }


    private QueryResponse<LoanAppRollup> doKeyQuery(final HostInfo targetHostInfo,
                                                    final Query<ValueAndTimestamp<LoanAppRollup>> query,
                                                    final KeyQueryMetadata keyMetadata,
                                                    final String loanType,
                                                    final HostStatus hostStatus) {
        QueryResponse<LoanAppRollup> queryResponse;
        if (targetHostInfo.equals(thisHostInfo)) {
            Set<Integer> partitionSet = Collections.singleton(keyMetadata.partition());
            StateQueryResult<ValueAndTimestamp<LoanAppRollup>> stateQueryResult =
                    kafkaStreams.query(StateQueryRequest.inStore(storeName)
                            .withQuery(query)
                            .withPartitions(partitionSet));
            QueryResult<ValueAndTimestamp<LoanAppRollup>> queryResult = stateQueryResult.getOnlyPartitionResult();
            queryResponse = QueryResponse.withResult(queryResult.getResult().value());
        } else {
            String path = "/" + loanType;
            String host = targetHostInfo.host();
            int port = targetHostInfo.port();
            queryResponse = doRemoteRequest(host, port, path);
        }
        return queryResponse.setHostType(hostStatus.name() + "-" + targetHostInfo.host() + ":" + targetHostInfo.port());
    }

    @SuppressWarnings("unchecked")
    private <V> QueryResponse<V> doRemoteRequest(String host, int port, String path) {
        QueryResponse<V> remoteResponse;
        try {
            remoteResponse = restTemplate.getForObject(BASE_IQ_URL + path, QueryResponse.class, host, port);
            if (remoteResponse == null) {
                remoteResponse = QueryResponse.withError("Remote call returned null response");
            }
        } catch (RestClientException exception) {
            remoteResponse = QueryResponse.withError(exception.getMessage());
        }
        return remoteResponse;
    }


    private <K> KeyQueryMetadata getKeyMetadata(K key, Serializer<K> keySerializer) {
        int currentRetries = 0;
        KeyQueryMetadata keyMetadata = kafkaStreams.queryMetadataForKey(storeName, key, keySerializer);
        while (keyMetadata.equals(NOT_AVAILABLE)) {
            if (++currentRetries > MAX_RETRIES) {
                return null;
            }
            time.sleep(2000);
            keyMetadata = kafkaStreams.queryMetadataForKey(storeName, key, keySerializer);
        }
        return keyMetadata;
    }
}
