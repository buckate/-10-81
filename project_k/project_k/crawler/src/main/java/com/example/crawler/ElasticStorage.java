package com.example.crawler;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;

public class ElasticStorage {
    private final RestHighLevelClient client;
    private final String indexName;

    public ElasticStorage(String host, int port, String indexName) {
        this.indexName = indexName;
        this.client = new RestHighLevelClient(
                RestClient.builder(new org.apache.http.HttpHost(host, port, "http")));
    }

    public void close() throws IOException {
        client.close();
    }

    public String computeId(String title, String pubDate) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String base = title + pubDate;
            byte[] hash = digest.digest(base.getBytes());
            return Base64.getUrlEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean exists(String id) throws IOException {
        return client.exists(new org.elasticsearch.action.get.GetRequest(indexName, id), RequestOptions.DEFAULT);
    }

    public void saveArticle(Map<String, Object> doc, String id) throws IOException {
        if (exists(id)) {
            System.out.println("[ElasticStorage] Already exists: " + id);
            return;
        }
        IndexRequest req = new IndexRequest(indexName).id(id).source(doc, XContentType.JSON);
        IndexResponse resp = client.index(req, RequestOptions.DEFAULT);
        System.out.println("[ElasticStorage] Saved: " + resp.getId());
    }

    public void searchByTitle(String title) throws IOException {
        SearchRequest req = new SearchRequest(indexName);
        SearchSourceBuilder src = new SearchSourceBuilder();
        src.query(QueryBuilders.matchQuery("title", title));
        req.source(src);
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
        System.out.println("[ElasticStorage] Found: " + resp.getHits().getTotalHits());
    }

    // AND search
    public void searchByTitleAndAuthor(String title, String author) throws IOException {
        SearchRequest req = new SearchRequest(indexName);
        BoolQueryBuilder bool = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("title", title))
            .must(QueryBuilders.matchQuery("author", author));
        SearchSourceBuilder src = new SearchSourceBuilder().query(bool);
        req.source(src);
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
        System.out.println("[ElasticStorage] Found (AND): " + resp.getHits().getTotalHits());
    }

    // OR search
    public void searchByTitleOrAuthor(String title, String author) throws IOException {
        SearchRequest req = new SearchRequest(indexName);
        BoolQueryBuilder bool = QueryBuilders.boolQuery()
            .should(QueryBuilders.matchQuery("title", title))
            .should(QueryBuilders.matchQuery("author", author))
            .minimumShouldMatch(1);
        SearchSourceBuilder src = new SearchSourceBuilder().query(bool);
        req.source(src);
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
        System.out.println("[ElasticStorage] Found (OR): " + resp.getHits().getTotalHits());
    }

    // Fuzzy search
    public void fuzzySearchInText(String text) throws IOException {
        SearchRequest req = new SearchRequest(indexName);
        SearchSourceBuilder src = new SearchSourceBuilder()
            .query(QueryBuilders.matchQuery("text", text).fuzziness("AUTO"));
        req.source(src);
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
        System.out.println("[ElasticStorage] Fuzzy found: " + resp.getHits().getTotalHits());
    }

    // Aggregation by author
    public void aggregateByAuthor() throws IOException {
        SearchRequest req = new SearchRequest(indexName);
        SearchSourceBuilder src = new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.terms("by_author").field("author.keyword"));
        req.source(src);
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
        Terms byAuthor = resp.getAggregations().get("by_author");
        System.out.println("[ElasticStorage] Publications by author:");
        for (Terms.Bucket bucket : byAuthor.getBuckets()) {
            System.out.println(bucket.getKeyAsString() + ": " + bucket.getDocCount());
        }
    }

    // Aggregation by date
    public void aggregateByDate() throws IOException {
        SearchRequest req = new SearchRequest(indexName);
        SearchSourceBuilder src = new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.dateHistogram("by_date")
                .field("pubDate")
                .calendarInterval(DateHistogramInterval.DAY));
        req.source(src);
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
        var byDate = resp.getAggregations().get("by_date");
        System.out.println("[ElasticStorage] Histogram by date:");
        for (var bucket : ((org.elasticsearch.search.aggregations.bucket.histogram.Histogram) byDate).getBuckets()) {
            System.out.println(bucket.getKeyAsString() + ": " + bucket.getDocCount());
        }
    }
}
