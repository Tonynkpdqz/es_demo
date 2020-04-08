package com.nkpdqz.es_demo.entity;

public class ElasticEntity<T> {
    public String id;
    public T data;

    public ElasticEntity() {
    }

    @Override
    public String toString() {
        return "ElasticEntity{" +
                "id='" + id + '\'' +
                ", data=" + data +
                '}';
    }

    public ElasticEntity(String id, T data) {
        this.id = id;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
