package ElasticSearchConsumers;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchClient {

    public static RestHighLevelClient createClient(){

        // Setting up credentials
//        String hostname = "sudtyrolmobility-9143737659.us-east-1.bonsaisearch.net";
//        String username = "3j9jjsiat3";
//        String password = "3ruil2aqei";

//        // Setting a credentials provider to supply username/password
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(username, password));
//
//
//        RestClientBuilder builder = RestClient.builder(
//                new HttpHost(hostname, 443, "https"))
//                .setHttpClientConfigCallback(
//                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

}

