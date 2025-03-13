package com.mongodb.test.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.Updates;
import com.mongodb.test.configuration.MongoTransactional;
import com.mongodb.test.model.Account;
import com.mongodb.test.model.Transfer;
import com.mongodb.test.model.TransferLog;
import com.mongodb.test.repo.AccountRepository;

@Service
public class AsyncAccountService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoClient client;

    @Autowired
    private MongoDatabase database;

    @Value("${settings.collectionName}")
    private String collectionName;

    @Value("${settings.transferLogCollectionName}")
    private String transferLogCollectionName;

    @Value("${settings.transferAmount}")
    private Integer transferAmount;

    @Value("${settings.idPrefix}")
    private int idPrefix;

    @Value("${settings.noOfAccount}")
    private int noOfAccount;

    @Autowired
    private AccountRepository accountRepository;

    @Async
    public CompletableFuture<StopWatch> insertMany(MongoCollection<Account> collection, List<Account> accounts)
            throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        StopWatch sw = new StopWatch();
        sw.start();
        collection.insertMany(accounts);
        sw.stop();
        // logger.info(Thread.currentThread().getName() + " " + accounts.size() + "
        // inserted. takes "
        // + sw.getTotalTimeMillis() + "ms, TPS:" + accounts.size() /
        // sw.getTotalTimeSeconds());
        return CompletableFuture.completedFuture(sw);
    }

    @Async
    public CompletableFuture<Void> callbackTransfer(List<Transfer> transfers, boolean isBatch, boolean hasError,
            String shard) {
        for (Transfer transfer : transfers) {
            final ClientSession clientSession = client.startSession();
            try (clientSession) {
                TransactionOptions txnOptions = TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .readConcern(ReadConcern.MAJORITY)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build();
                TransactionBody<Void> txnBody = () -> {
                    if (isBatch) {
                        transferBatch(clientSession, transfer, shard);
                    } else {
                        transfer(clientSession, transfer, hasError, shard);
                    }
                    return null;
                };
                clientSession.withTransaction(txnBody, txnOptions);
            } catch (RuntimeException e) {
                logger.error("Error during transfer, errorMsg={}, errorCause={}, errorStackTrace={}", e.getMessage(),
                        e.getCause(), e.getStackTrace(), e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> coreTransfer(List<Transfer> transfers, boolean isBatch, boolean hasError,
            String shard) {
        for (Transfer transfer : transfers) {

            UUID tranId = null;
            int retryCount = 0;
            while (true) {
                try {
                    TransactionOptions txnOptions = TransactionOptions.builder()
                            .readPreference(ReadPreference.primary())
                            .readConcern(ReadConcern.MAJORITY)
                            // .readConcern(ReadConcern.SNAPSHOT)
                            .writeConcern(WriteConcern.MAJORITY)
                            .build();
                    try (ClientSession clientSession = client.startSession()) {
                        clientSession.startTransaction(txnOptions);
                        tranId = clientSession.getServerSession().getIdentifier().getBinary("id").asUuid();
                        logger.info("Start Transaction: " + tranId);
                        if (isBatch) {
                            this.transferBatch(clientSession, transfer, shard);
                        } else {
                            this.transfer(clientSession, transfer, hasError, shard);
                        }
                        while (true) {
                            try {
                                clientSession.commitTransaction();
                                logger.info("Transaction committed: " + tranId);
                                break;
                            } catch (MongoException e) {
                                // can retry commit
                                if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                    logger.info("UnknownTransactionCommitResult, retrying " + tranId
                                            + " commit operation ...");
                                    retryCount++;
                                    continue;
                                } else {
                                    logger.info("Exception during commit ...");
                                    throw e;
                                }
                            }
                        }
                    }
                    break;
                } catch (MongoException e) {
                    // logger.info("Transaction aborted. Caught exception during transaction.");
                    if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                        // e.printStackTrace();
                        logger.info("TransientTransactionError, aborting transaction " + tranId + " and retrying ...");
                        retryCount++;
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        continue;
                    } else {
                        throw e;
                    }
                }
            }
            logger.info("Retry count:" + retryCount);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> transfer(List<Transfer> transfers, boolean isBatch, boolean hasError, String shard) {
        for (Transfer transfer : transfers) {
            if (isBatch) {
                this.transferBatch(null, transfer, shard);
            } else {
                this.transfer(null, transfer, hasError, shard);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> transfer(Transfer transfer, boolean isBatch, String shard) {
        if (isBatch) {
            this.transferBatch(null, transfer, shard);
        } else {
            this.transfer(null, transfer, shard);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void transfer(ClientSession clientSession, Transfer transfer) {
        this.transfer(clientSession, transfer, false, null);
    }

    private void transfer(ClientSession clientSession, Transfer transfer, String shard) {
        this.transfer(clientSession, transfer, false, shard);
    }

    private void transfer(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
            MongoCollection<TransferLog> transferLogCollection = database.getCollection(transferLogCollectionName,
                    TransferLog.class);

            if ("hashed".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "HashedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "HashedShard",
                        TransferLog.class);
            } else if ("ranged".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "RangedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "RangedShard",
                        TransferLog.class);
            }

            logger.info(Thread.currentThread().getName() + " Deduct $" + transferAmount + " from account "
                    + t.getFromAccountId());

            Account newBalance;
            if (clientSession != null) {
                newBalance = collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
            } else {
                newBalance = collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
            }

            if (Objects.nonNull(newBalance) && newBalance.getBalance() < 0) {
                logger.warn("Account " + newBalance.getId() + " have not enough balance, skip transfer");

                if (clientSession != null) {
                    collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                            Updates.inc("balance", transferAmount));
                } else {
                    collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()),
                            Updates.inc("balance", transferAmount));
                }
            } else {
                double eachAccountBalance = (double) transferAmount / t.getToAccountId().size();

                List<TransferLog> transferLogList = new ArrayList<>(t.getToAccountId().size());
                for (Integer toAccountId : t.getToAccountId()) {
                    if (hasError && Math.random() > 0.8) {
                        throw new RuntimeException("Unexpected error. Something went wrong");
                    }
                    if (clientSession != null) {
                        collection.updateMany(clientSession, Filters.eq("_id", toAccountId),
                                Updates.inc("balance", eachAccountBalance));
                    } else {
                        collection.updateMany(Filters.eq("_id", toAccountId),
                                Updates.inc("balance", eachAccountBalance));
                    }
                    transferLogList.add(new TransferLog(eachAccountBalance, t.getFromAccountId(), toAccountId));
                }

                if (clientSession != null) {
                    transferLogCollection.insertMany(clientSession, transferLogList);
                } else {
                    transferLogCollection.insertMany(transferLogList);
                }
            }
            sw.stop();
        } catch (Exception e) {
            // logger.error("Exception occur errorMsg={}, errorCause={},
            // errorStackTrace={}", e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferBatch(ClientSession clientSession, Transfer t, String shard) {

        StopWatch sw = new StopWatch();
        sw.start();
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        MongoCollection<TransferLog> transferLogCollection = database.getCollection(transferLogCollectionName,
                TransferLog.class);
        if (shard != null) {
            if ("hashed".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "HashedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "HashedShard",
                        TransferLog.class);
            } else if ("ranged".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "RangedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "RangedShard",
                        TransferLog.class);
            }
        }
        List<UpdateManyModel<Account>> list = new ArrayList<>();
        List<InsertOneModel<TransferLog>> listTransferLog = new ArrayList<>();
        int transferAmount = t.getToAccountId().size();
        Account a = null;
        if (clientSession != null) {
            a = collection.find(clientSession, Filters.eq("_id", t.getFromAccountId())).first();
        } else {
            a = collection.find(Filters.eq("_id", t.getFromAccountId())).first();
        }
        if (a == null || a.getBalance() < transferAmount) {
            logger.info("Account " + (Objects.nonNull(a) ? a.getId() : "a = null")
                    + " have not enough balance, skip transfer");
            sw.stop();
        } else {
            list.add(new UpdateManyModel<>(Filters.eq("_id", t.getFromAccountId()),
                    Updates.inc("balance", -transferAmount)));
            for (Integer id2 : t.getToAccountId()) {
                list.add(new UpdateManyModel<>(Filters.eq("_id", id2), Updates.inc("balance", 1)));
                listTransferLog.add(new InsertOneModel<TransferLog>(new TransferLog(1, t.getFromAccountId(), id2)));
            }
            if (!list.isEmpty()) {
                if (clientSession != null) {
                    collection.bulkWrite(clientSession, list);
                    transferLogCollection.bulkWrite(clientSession, listTransferLog);
                } else {
                    collection.bulkWrite(list);
                    transferLogCollection.bulkWrite(listTransferLog);
                }
            }
            sw.stop();
            // logger.info((clientSession == null ? "" : ("clientSession: " +
            // clientSession.getServerSession().getIdentifier().getBinary("id").asUuid() + "
            // ")) + "Completed transfer, total " + (list.size() + listTransferLog.size()) +
            // " operations takes "
            // + sw.getTotalTimeMillis() + "ms, TPS:" + list.size() /
            // sw.getTotalTimeSeconds());
        }
    }

    public void longTransaction(long waitTime, Transfer transfer) throws InterruptedException {
        UUID tranId = null;
        while (true) {
            try {
                TransactionOptions txnOptions = TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .readConcern(ReadConcern.MAJORITY)
                        // .readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build();
                try (ClientSession clientSession = client.startSession()) {
                    clientSession.startTransaction(txnOptions);
                    tranId = clientSession.getServerSession().getIdentifier().getBinary("id").asUuid();
                    logger.info("Start Transaction: " + tranId);
                    transfer(clientSession, transfer);
                    logger.info("Start waiting for commit");
                    Thread.sleep(waitTime);
                    while (true) {
                        try {
                            clientSession.commitTransaction();
                            logger.info("Transaction committed: " + tranId);
                            break;
                        } catch (MongoException e) {
                            // can retry commit
                            if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                logger.info(
                                        "UnknownTransactionCommitResult, retrying " + tranId + " commit operation ...");
                                continue;
                            } else {
                                logger.error("Exception during commit ...", e);
                                throw e;
                            }
                        }
                    }
                }
                break;
            } catch (MongoException e) {
                // logger.info("Transaction aborted. Caught exception during transaction.");
                if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    // e.printStackTrace();
                    logger.info("TransientTransactionError, aborting transaction " + tranId + " and retrying ...");
                    continue;
                } else {
                    throw e;
                }
            }
        }

        // final ClientSession clientSession = client.startSession();
        // UUID tranId =
        // clientSession.getServerSession().getIdentifier().getBinary("id").asUuid();
        // TransactionOptions txnOptions = TransactionOptions.builder()
        // .readPreference(ReadPreference.primary())
        // .readConcern(ReadConcern.MAJORITY)
        // .writeConcern(WriteConcern.MAJORITY)
        // .build();
        // TransactionBody<String> txnBody = new TransactionBody<String>() {
        // public String execute() {
        // logger.info("Start Transaction: "+tranId);
        // transfer(clientSession, transfers);
        // logger.info("Start waiting for commit");
        // try {
        // Thread.sleep(waitTime);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        // return "Transaction committed: "+tranId;
        // }
        // };
        // try {
        // logger.info(clientSession.withTransaction(txnBody, txnOptions));
        // } catch (RuntimeException e) {
        // logger.error("Error during transfer", e);
        // } finally {
        // clientSession.close();
        // }
    }

    @Async
    public CompletableFuture<Void> flashOrderCore(int[] req) {
        for (int i = 0; i < req.length; i++) {
            final int v = req[i];
            try {
                while (true) {
                    try {
                        TransactionOptions txnOptions = TransactionOptions.builder()
                                .readPreference(ReadPreference.primary())
                                .readConcern(ReadConcern.MAJORITY)
                                // .readConcern(ReadConcern.SNAPSHOT)
                                .writeConcern(WriteConcern.MAJORITY)
                                .build();
                        try (ClientSession clientSession = client.startSession()) {
                            clientSession.startTransaction(txnOptions);
                            try {
                                StopWatch sw = new StopWatch();
                                sw.start();

                                MongoCollection<Document> flashOrderCollection = database.getCollection("flashOrder",
                                        Document.class);
                                MongoCollection<Document> flashProductCollection = database.getCollection(
                                        "flashProduct",
                                        Document.class);
                                Document doc = flashProductCollection.findOneAndUpdate(clientSession,
                                        Filters.eq("sku", "P0001"), Updates.inc("res", v),
                                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
                                flashOrderCollection.insertOne(clientSession, new Document("req", v));
                                logger.info("after flashOrder, res={}, soh={}", doc.getInteger("res"),
                                        doc.getInteger("soh"));
                                /*
                                 * Maximum inventory: soh
                                 * Reserved quota: res
                                 * API Requested quota: req
                                 * Validation: res + req <= soh
                                 * 
                                 * if yes, res = res + req
                                 */
                                if ((doc.getInteger("res")) > doc.getInteger("soh"))
                                    throw new RuntimeException("Not enough inventory");
                                sw.stop();
                                logger.info("flashOrder, t={}, spend={} ms", req, sw.getTotalTimeMillis());
                            } catch (Exception e) {
                                logger.error(
                                        "flashOrder, Exception Occur: t={}, errorMsg={}, errorCause={}, errorStackTrace={}",
                                        req,
                                        e.getMessage(), e.getCause(), e.getStackTrace(), e);
                                throw e;
                            }
                            while (true) {
                                try {
                                    clientSession.commitTransaction();
                                    break;
                                } catch (MongoException e) {
                                    // can retry commit
                                    if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                        logger.info("UnknownTransactionCommitResult, retrying commit operation ...");
                                        continue;
                                    } else {
                                        logger.info("Exception during commit ...");
                                        throw e;
                                    }
                                }
                            }
                        }
                        break;
                    } catch (MongoException e) {
                        // logger.info("Transaction aborted. Caught exception during transaction.");
                        if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                            // e.printStackTrace();
                            logger.info("TransientTransactionError, aborting transaction and retrying ...");
                            try {
                                Thread.sleep(((int) Math.floor(Math.random() * 50) + 51));
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }
            } catch (RuntimeException e) {
                // logger.error("Error during transfer, errorMsg={}, errorCause={},
                // errorStackTrace={}", e.getMessage(),
                // e.getCause(), e.getStackTrace(), e);
            }
        }
        return CompletableFuture.completedFuture(null);

    }

    @Async
    public CompletableFuture<Void> flashOrder(int[] req) {
        for (int i = 0; i < req.length; i++) {
            final int v = req[i];
            final ClientSession clientSession = client.startSession();
            try (clientSession) {
                TransactionOptions txnOptions = TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .readConcern(ReadConcern.MAJORITY)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build();
                TransactionBody<Void> txnBody = () -> {
                    try {
                        StopWatch sw = new StopWatch();
                        sw.start();

                        MongoCollection<Document> flashOrderCollection = database.getCollection("flashOrder",
                                Document.class);
                        MongoCollection<Document> flashProductCollection = database.getCollection("flashProduct",
                                Document.class);
                        Document doc = flashProductCollection.findOneAndUpdate(clientSession,
                                Filters.eq("sku", "P0001"), Updates.inc("res", v),
                                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
                        flashOrderCollection.insertOne(clientSession, new Document("req", v));
                        logger.info("after flashOrder, res={}, soh={}", doc.getInteger("res"), doc.getInteger("soh"));
                        /*
                         * Maximum inventory: soh
                         * Reserved quota: res
                         * API Requested quota: req
                         * Validation: res + req <= soh
                         * 
                         * if yes, res = res + req
                         */
                        if ((doc.getInteger("res")) > doc.getInteger("soh"))
                            throw new RuntimeException("Not enough inventory");
                        sw.stop();
                        logger.info("flashOrder, t={}, spend={} ms", req, sw.getTotalTimeMillis());
                    } catch (Exception e) {
                        logger.error(
                                "flashOrder, Exception Occur: t={}, errorMsg={}, errorCause={}, errorStackTrace={}",
                                req,
                                e.getMessage(), e.getCause(), e.getStackTrace(), e);
                        throw e;
                    }
                    return null;
                };
                clientSession.withTransaction(txnBody, txnOptions);
            } catch (RuntimeException e) {
                // logger.error("Error during transfer, errorMsg={}, errorCause={},
                // errorStackTrace={}", e.getMessage(),
                // e.getCause(), e.getStackTrace(), e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private void transferCase1(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            } else {
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase1, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase1, Exception Occur: t={}, errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase2(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);

            if (clientSession != null) {
                collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
                collection.updateOne(clientSession, Filters.eq("_id", t.getToAccountId().get(0)),
                        Updates.inc("balance", 1));
            } else {
                collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
                collection.updateOne(Filters.eq("_id", t.getToAccountId().get(0)), Updates.inc("balance", 1));
            }

            sw.stop();
            logger.info("transferCase2, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase2, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase3(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        List<Integer> toAccountIdList = new ArrayList<>(1);
        toAccountIdList.add(idPrefix + ((int) Math.floor(Math.random() * noOfAccount) + 1));

        t.setToAccountId(toAccountIdList);
        t.setFromAccountId(idPrefix + ((int) Math.floor(Math.random() * noOfAccount) + 1));
        logger.info("transferCase3, reset to single shard, t={}", t);

        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);

            if (clientSession != null) {
                collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
                collection.updateOne(clientSession, Filters.eq("_id", t.getToAccountId().get(0)),
                        Updates.inc("balance", 1));
            } else {
                collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
                collection.updateOne(Filters.eq("_id", t.getToAccountId().get(0)), Updates.inc("balance", 1));
            }

            sw.stop();
            logger.info("transferCase3, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase3, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase4(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);
            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", 1));
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getToAccountId().get(0), t.getFromAccountId()));
            } else {
                collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", 1));
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase4, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase4, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase5(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);
            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", 1));
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            } else {
                collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", 1));
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase5, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase5, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase6(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);
            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -1));
                collection.updateOne(clientSession, Filters.eq("_id", t.getToAccountId().get(0)),
                        Updates.inc("balance", 1));
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            } else {
                collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -1));
                collection.updateOne(Filters.eq("_id", t.getToAccountId().get(0)), Updates.inc("balance", 1));
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase6, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase6, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase7(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        List<Integer> toAccountIdList = new ArrayList<>(1);
        toAccountIdList.add(idPrefix + ((int) Math.floor(Math.random() * noOfAccount) + 1));

        t.setToAccountId(toAccountIdList);
        t.setFromAccountId(idPrefix + ((int) Math.floor(Math.random() * noOfAccount) + 1));
        logger.info("transferCase7, reset to single shard, t={}", t);

        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);
            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -1));
                collection.updateOne(clientSession, Filters.eq("_id", t.getToAccountId().get(0)),
                        Updates.inc("balance", 1));
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            } else {
                collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -1));
                collection.updateOne(Filters.eq("_id", t.getToAccountId().get(0)), Updates.inc("balance", 1));
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase7, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase7, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase8(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        List<Integer> toAccountIdList = new ArrayList<>(1);
        toAccountIdList.add(idPrefix + ((int) Math.floor(Math.random() * noOfAccount) + 1));

        t.setToAccountId(toAccountIdList);
        t.setFromAccountId(idPrefix + ((int) Math.floor(Math.random() * noOfAccount) + 1));
        logger.info("transferCase8, reset to single shard, t={}", t);

        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);
            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                collection.updateOne(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -1));
                collection.updateOne(clientSession, Filters.eq("_id", t.getToAccountId().get(0)),
                        Updates.inc("balance", 1));
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            } else {
                collection.updateOne(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -1));
                collection.updateOne(Filters.eq("_id", t.getToAccountId().get(0)), Updates.inc("balance", 1));
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase8, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase8, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    private void transferCase10(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        try {
            StopWatch sw = new StopWatch();
            sw.start();

            MongoCollection<Account> collection = database.getCollection(collectionName + "RangedShard", Account.class);
            MongoCollection<TransferLog> transferLogCollection = database
                    .getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);

            if (clientSession != null) {
                collection.updateOne(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -1));
                collection.updateOne(clientSession, Filters.eq("_id", t.getToAccountId().get(0)),
                        Updates.inc("balance", 1));
                transferLogCollection.insertOne(clientSession,
                        new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            } else {
                collection.updateOne(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -1));
                collection.updateOne(Filters.eq("_id", t.getToAccountId().get(0)), Updates.inc("balance", 1));
                transferLogCollection.insertOne(new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0)));
            }

            sw.stop();
            logger.info("transferCase10, t={}, spend={} ms", t, sw.getTotalTimeMillis());
        } catch (Exception e) {
            logger.error("transferCase10, Exception Occur: t={} errorMsg={}, errorCause={}, errorStackTrace={}", t,
                    e.getMessage(), e.getCause(), e.getStackTrace(), e);
            throw e;
        }
    }

    @Async
    public CompletableFuture<Void> transferSpring(List<Transfer> transfers) {
        for (Transfer tr : transfers) {
            Optional<Account> oFromAcct = accountRepository.findById(tr.getFromAccountId().intValue());
            if (oFromAcct.isPresent()) {
                transfer(oFromAcct.get(),
                        tr.getToAccountId().stream().map(i -> accountRepository.findById(i)).toList());
            }else{
                logger.info("acct not found");
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Transactional
    private void transfer(Account fromAcct, List<Optional<Account>> toAccounts) {
        fromAcct.setBalance(fromAcct.getBalance() - (double) toAccounts.size());
        accountRepository.save(fromAcct);
        for (Optional<Account> oA : toAccounts) {
            if (oA.isPresent()) {
                Account toAcct = oA.get();
                toAcct.setBalance(fromAcct.getBalance() + 1);
                accountRepository.save(toAcct);
            }else{
                logger.info("acct not found");
            }
        }
    }

}
