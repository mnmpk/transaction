package com.mongodb.test.configuration;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.stereotype.Component;

import com.mongodb.MongoException;

@Aspect
@Component
public class TransactionAspect {
    private Logger logger = LoggerFactory.getLogger(getClass());

    // @Around("execution(*
    // com.mongodb.test.service.AsyncAccountService.transferSpring(..))")
    @Around("@annotation(org.springframework.transaction.annotation.Transactional)")
    public Object aroundAdvice(ProceedingJoinPoint joinPoint) throws Throwable {
        Object obj;
        while (true) {
            try {
                obj = joinPoint.proceed();
                break;
            } catch (MongoTransactionException | UncategorizedMongoDbException e) {
                if (((MongoException) e.getCause()).hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    logger.info("TransientTransactionError, aborting transaction and retrying ...");
                    try {
                        Thread.sleep((int) (1000 * Math.random()));
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    continue;
                } else {
                    throw e;
                }
            }
        }
        return obj;
    }
}