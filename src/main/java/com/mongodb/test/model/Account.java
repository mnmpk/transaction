package com.mongodb.test.model;

import org.bson.codecs.pojo.annotations.BsonId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Account {
    @Id
    @BsonId
    @Field("_id")
    private int id;
    //private ObjectId id;

    @Getter
    private double balance;

    public Account(int id, double balance) {
        this.id = id;
        this.balance = balance;
    }
}
