package com.example.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RssCrawler {
    // Класс для представления новости
    public static class Article {
        public String title;
        public String link;
        public String publishedAt;
        public String author;
        public String description;
    }

    // Получить список новостей из RSS
    public List<Article> getNewsList(String rssFeedUrl) throws IOException {
        List<Article> articles = new ArrayList<>();
        Document doc = Jsoup.connect(rssFeedUrl).get();
        Elements items = doc.select("item");
        for (Element item : items) {
            Article art = new Article();
            art.title = item.selectFirst("title").text();
            art.link = item.selectFirst("link").text();
            art.publishedAt = item.selectFirst("pubDate") != null ? item.selectFirst("pubDate").text() : "";
            art.author = item.selectFirst("author") != null ? item.selectFirst("author").text() : "";
            art.description = item.selectFirst("description") != null ? item.selectFirst("description").text() : "";
            articles.add(art);
        }
        return articles;
    }
}
