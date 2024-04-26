package com.mongodb.test.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.test.model.Stat;
import com.mongodb.test.service.AccountService;

@RestController
public class FlashOrderAPIController {
    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    @Autowired
    private AccountService service;


    @GetMapping("/flash")
    public ResponseEntity<Stat> flashOrderWithCallbackAPITransaction(@RequestParam(defaultValue = "false") boolean batch, @RequestParam(defaultValue = "false") boolean hasError, @RequestParam(required = false) String shard) {
        return new ResponseEntity<>(service.orderMultiple(), HttpStatus.OK);
    }
}
