package org.bf2.cos.connector.camel.it

import groovy.util.logging.Slf4j
import org.bf2.cos.connector.camel.it.support.ContainerImages
import org.bf2.cos.connector.camel.it.support.KafkaConnectorSpec
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import java.util.concurrent.TimeUnit

import static com.github.tomakehurst.wiremock.client.WireMock.*

@Slf4j
class ConnectorIT extends KafkaConnectorSpec {

    static final int PORT = 8080
    static final String SCHEME = 'http'
    static final String HOST = 'rhoc.org'

    static GenericContainer mock

    @Override
    def setupSpec() {
        mock = ContainerImages.WIREMOCK.container()
        mock.withLogConsumer(logger(HOST))
        mock.withNetwork(network)
        mock.withNetworkAliases(HOST)
        mock.withExposedPorts(PORT)
        mock.waitingFor(Wait.forListeningPort())
        mock.start()
    }

    @Override
    def cleanupSpec() {
        closeQuietly(mock)
    }

    def "telegram sink"() {
        given:
        def topic = topic()
        def group = UUID.randomUUID().toString()
        def payload = 'message'
        def token = '/telegram_authorization_token'

        def path = urlPathEqualTo('/bot' + token + '/sendMessage')
        def request = post(path)
        def response = ok().withBody('{}')

        configureFor(SCHEME, mock.getHost(), mock.getMappedPort(PORT))
        stubFor(request.willReturn(response));

        def cnt = connectorContainer('telegram_sink_0.1.json', [
                'kafka_topic'                 : topic,
                'kafka_bootstrap_servers'     : kafka.outsideBootstrapServers,
                'kafka_consumer_group'        : UUID.randomUUID().toString(),
                'telegram_authorization_token': token,
                'telegram_chat_id'            : 'telegram_chat_id',
                'telegram_base_uri'           : SCHEME + '://' + HOST + ':' + PORT
        ])

        cnt.start()

        when:
        kafka.send(topic, payload)

        then:
        def records = kafka.poll(group, topic)
        records.size() == 1
        records.first().value() == payload

        untilAsserted(5, TimeUnit.SECONDS) {
            verify(1, postRequestedFor(path))
        }

        assert findUnmatchedRequests().isEmpty()

        cleanup:
        closeQuietly(cnt)
    }
}
