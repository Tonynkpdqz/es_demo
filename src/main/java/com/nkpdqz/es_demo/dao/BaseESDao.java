package com.nkpdqz.es_demo.dao;

import com.alibaba.fastjson.JSON;
import com.nkpdqz.es_demo.entity.ElasticEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Component
public class BaseESDao {


    @Autowired
    RestHighLevelClient restHighLevelClient;

    public BaseESDao() {
        //restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1",9200,"http")));
    }

    public static final String CREATE_INDEX = "{\n" +
            "    \"properties\": {\n" +
            "      \"id\":{\n" +
            "        \"type\":\"integer\"\n" +
            "      },\n" +
            "      \"userId\":{\n" +
            "        \"type\":\"integer\"\n" +
            "      },\n" +
            "      \"name\":{\n" +
            "        \"type\":\"text\"\n" +
            "      },\n" +
            "      \"url\":{\n" +
            "        \"type\":\"text\",\n" +
            "        \"index\": true\n" +
            "      }\n" +
            "    }\n" +
            "  }";

    public void createIndex(String idxName,String idxSQL){
        try {
            if (!this.indexExist(idxName)){
                System.out.println("index already exist");
                return;
            }
            CreateIndexRequest request = new CreateIndexRequest(idxName);
            buildSetting(request);
            request.mapping(idxSQL, XContentType.JSON);
            CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()){
                System.out.println("初始化失败");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //设置分片,单机
    private void buildSetting(CreateIndexRequest request) {
        request.settings(Settings.builder().put("index.number_of_shards",1)
            .put("index.number_of_replicas",0));
    }

    private boolean indexExist(String idxName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(idxName);
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(true);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return restHighLevelClient.indices().exists(request,RequestOptions.DEFAULT);
    }

    //插入数据
    public void insertOrUpdateOne(String idxName,ElasticEntity entity){
        IndexRequest request = new IndexRequest(idxName);
        request.id(entity.getId());
        request.source(JSON.toJSONString(entity.getData()),XContentType.JSON);
        try {
            restHighLevelClient.index(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //批量插入数据
    public void insertBatch(String idxName, List<ElasticEntity> list){
        BulkRequest request = new BulkRequest();
        list.forEach(item -> request.add(new IndexRequest(idxName).id(item.getId())
        .source(JSON.toJSONString(item.getData()),XContentType.JSON)));
        try {
            restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //批量删除
    public <T> void deleteBatch(String idxName, Collection<T> idList){
        BulkRequest bulkRequest = new BulkRequest();
        idList.forEach(item -> bulkRequest.add(new DeleteRequest(idxName,item.toString())));
        try {
            restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //搜索
    public <T> List<T> search(String idxName, SearchSourceBuilder builder,Class<T> c){
        SearchRequest searchRequest = new SearchRequest(idxName);
        searchRequest.source(builder);
        try {
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            List<T> res = new ArrayList<>(hits.length);
            for (SearchHit hit: hits) {
                res.add(JSON.parseObject(hit.getSourceAsString(),c));
            }
            return res;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    //删除index
    public void deleteIndex(String idxName){
        try {
            restHighLevelClient.indices().delete(new DeleteIndexRequest(idxName),RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteByQuery(String idxName, QueryBuilder builder){
        DeleteByQueryRequest request = new DeleteByQueryRequest(idxName);
        request.setQuery(builder);
        request.setBatchSize(10000);
        request.setConflicts("proceed");
        try {
            restHighLevelClient.deleteByQuery(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
