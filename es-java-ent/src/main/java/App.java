import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
@SuppressWarnings("deprecation")
public class App {
	@SuppressWarnings("deprecation")
	public static TransportClient client;
	public static final String CLUSTER_NAME = "elasticsearch";
	public static final String HOST_NAME = "localhost";
	public static final String INDEX_NAME = "product";
	public static final String TYPE_NAME = "_doc";
	private static final Logger LOG = LogManager.getLogger(LogApp.class);
	public static String _ID = "1";
	public App() {
		client = createClient();
	}

	public TransportClient createClient() {
		Settings settings = Settings.builder()//.put("cluster.name", CLUSTER_NAME)
				.build();
		// TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(settings).addTransportAddress(
					new TransportAddress(InetAddress.getByName(HOST_NAME),
							9300));
			LOG.info("\nClient successfully created\nNODES:");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			List<DiscoveryNode> nodes = client.listedNodes();
			int i=0;
			for (DiscoveryNode discoveryNode : nodes) {
				System.out.printf("%d : %s\n",++i,discoveryNode);
			}
		} catch (NoNodeAvailableException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			ex.printStackTrace();
		}

		return client;
	}

	public static void main(String args[]) {

		// on startup
		App app = new App();
		app.index();
		//app.get();
		//app.search();
		// app.delete();
		// app.deleteByQuery("brand", "asus1");
		 app.deleteByQueryRequest("brand", "asus1");
		 /*
			 * try { app.sendFile(); } catch (Exception e) { // TODO Auto-generated
			 * catch block e.printStackTrace(); }
			 */

		client.close();
	}

	public void toString(Map<String, Object> json) {
		StringBuilder sb = new StringBuilder();
		sb.append("Brand: " + json.get("brand"));
		sb.append("\tcolor: " + json.get("color"));
		sb.append("\tprovider: " + json.get("provider"));
		sb.append("\tprice: " + json.get("price"));
		System.out.println(sb.toString());
	}
	public void sendFile() throws Exception {

		/*
		 * @String fileName = "D:\\VM\\MOCK_DATA.json";
		 * 
		 * @File jsonFile = new File(fileName);
		 * 
		 * @HttpEntity entity = new FileEntity(jsonFile);
		 * 
		 * @post.setEntity(entity);
		 * 
		 * @HttpClientBuilder clientBuilder = HttpClientBuilder.create();
		 * 
		 * @HttpClient client = clientBuilder.build();
		 * 
		 * HttpResponse response = client.execute(post);
		 * 
		 * @System.out.println("Response: " + response);
		 */
	}

	public void deleteByQuery(String filter, String filterValue) {
		BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client,
				DeleteByQueryAction.INSTANCE)
						.filter(QueryBuilders.matchQuery(filter, filterValue))
						.source(INDEX_NAME).get();
		long deleted = response.getDeleted();
		LOG.info("Deleted item by query: {0}", deleted);
	}
	//execute it as asynchronously, call execute instead of get..
	public void deleteByQueryRequest(String filter, String filterValue) {
		new DeleteByQueryRequestBuilder(client,
			DeleteByQueryAction.INSTANCE)
				.filter(QueryBuilders.matchQuery(filter, filterValue))
				.source(INDEX_NAME)
				.execute(new ActionListener<BulkByScrollResponse>() {   
			        public void onResponse(BulkByScrollResponse response) {
			            long deleted = response.getDeleted(); 
			            //System.out.println("\nSuccessfully delete : "+deleted);
			            LOG.info("\nSuccessfully delete : {0}", deleted);
			        }
			        public void onFailure(Exception e) {
			            //System.out.println("\nDelete failure: "+e.getMessage());
			        	LOG.error("\nDeleted failure : "+e.getMessage());
			        }
				});
	}
	public void delete() {
		DeleteResponse response = client
				.prepareDelete(INDEX_NAME, TYPE_NAME, _ID).get();
		LOG.info("\nDelete Result: %s", response.getId());
	}

	public void search() {
		SearchResponse response = client.prepareSearch(INDEX_NAME)
				.setTypes(TYPE_NAME)
				.setQuery(QueryBuilders.matchQuery("brand", "asus1")).get();
		LOG.info("\nSearch Result: %s", response.toString());
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit sh : hits) {
			// System.out.println(sh);
			Map<String, Object> json = sh.getSourceAsMap();
			toString(json);
		}
	}
	// index noun means -> document
	// index verb means -> inserting
	public IndexResponse index() {
		/*
		 * String json = "{" +
        "\"user\":\"kimchy\"," +
        "\"postDate\":\"2013-01-30\"," +
        "\"message\":\"trying out Elasticsearch\"" +
    		"}";
		 * */
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("brand", "asusxxxx ");
		json.put("color", "grey");
		json.put("provider", "Asus -TR");
		json.put("price", "4500");
		IndexResponse response = client.prepareIndex(INDEX_NAME, TYPE_NAME)
				.setSource(json, XContentType.JSON).get();
		LOG.info(String.format("\nResponse Result:%s ID: %s\n" ,response.getResult().name() ,response.getId()));
		_ID=response.getId();
		return response;
	}

	public void get() {
		GetResponse response = client.prepareGet(INDEX_NAME, TYPE_NAME, _ID)
				.get();
		Map<String, Object> json = response.getSource();
		toString(json);

	}

}
