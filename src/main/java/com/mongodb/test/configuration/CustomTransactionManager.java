package com.mongodb.test.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.UncategorizedMongoDbException;

import com.mongodb.MongoException;

public class CustomTransactionManager extends MongoTransactionManager {
    private Logger logger = LoggerFactory.getLogger(getClass());

	public CustomTransactionManager() {
        super();
    }
	public CustomTransactionManager(MongoDatabaseFactory dbFactory) {
        super(dbFactory);
    }
    @Override
	protected void doCommit(MongoTransactionObject transactionObject) throws Exception {
        while (true) {
            try {
                super.doCommit(transactionObject);
                logger.info("doCommit success");
                break;
            } catch (UncategorizedMongoDbException e) {                
                if (((MongoException)e.getCause()).hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                    logger.info("UnknownTransactionCommitResult, retrying  commit operation ...");
                    continue;
                } else {
                    logger.info("Exception during commit ...");
                    throw e;
                }
            }
        }
	}
}
