package com.banksy.spark_03.pojo;

import java.util.Date;

/**
 * book实体
 * @Author banksy
 * @Data 2022/9/14 9:17 PM
 * @Version 1.0
 **/
public class Book {
    int id;
    int authorId;
    String title;
    Date releaseDate;
    String link;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAuthorId() {
        return authorId;
    }

    public void setAuthorId(int authorId) {
        this.authorId = authorId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }
}