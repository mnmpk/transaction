package com.mongodb.test.repo;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.mongodb.test.model.Account;

public interface AccountRepository extends MongoRepository<Account, Integer> {
    
}
