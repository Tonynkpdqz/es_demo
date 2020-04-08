package com.nkpdqz.es_demo.controller;

import com.nkpdqz.es_demo.dao.BaseESDao;
import com.nkpdqz.es_demo.entity.Book;
import com.nkpdqz.es_demo.entity.ElasticEntity;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.naming.directory.SearchControls;
import java.util.List;

@RestController
@RequestMapping("/es/")
public class Controller {

    @Autowired
    BaseESDao dao;


    @GetMapping("createIndex")
    public void createIndex(){
        dao.createIndex("book_index",BaseESDao.CREATE_INDEX);
    }

    @GetMapping("/{id}")
    public Book getById(@PathVariable int id){
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new TermQueryBuilder("id",id));
        List<Book> res = dao.search("book_index", builder, Book.class);
        if (res.size()>0){
            return res.get(0);
        }else {
            return null;
        }
    }

    @PutMapping("addBook")
    public void putBook(Book book){
        ElasticEntity<Book> entity = new ElasticEntity<>(book.getId().toString(),book);
        dao.insertOrUpdateOne("book_index",entity);
    }

    @GetMapping("testAdd")
    public void putBook2(){
        Book book = new Book(1,1,"nkpdqz");
        ElasticEntity<Book> entity = new ElasticEntity<>(book.getId().toString(),book);
        dao.insertOrUpdateOne("book_index",entity);
    }

}
