package eu.openminted.content.service.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.service.dao.CorpusBuilderInfoDao;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.content.service.tasks.FetchMetadataTask;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
public class CorpusBuilderExecutionQueueConsumer {
    private static Logger log = Logger.getLogger(CorpusBuilderExecutionQueueConsumer.class.getName());

    private BlockingQueue<String> corpora;

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Autowired
    private CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    private StoreRESTClient storeRESTClient;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolExecutor queueTasksThreadPoolExecutor;

    @org.springframework.beans.factory.annotation.Value("${tempDirectoryPath}")
    private String tempDirectoryPath;

    public void init(BlockingQueue<String> corpora) {

        if (this.corpora == null) this.corpora = corpora;
        if (this.queueTasksThreadPoolExecutor == null)
            this.queueTasksThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        while (true) {
            try {
                String corpusId = corpora.poll(100, TimeUnit.MILLISECONDS);
                if (corpusId != null && !corpusId.isEmpty()) {

                    this.queueTasksThreadPoolExecutor.execute(() -> {
                        if (contentConnectors != null) {
                            CorpusBuilderInfoModel corpusBuilderInfoModel = null;
                            try {
                                corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);
                                if (corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.CANCELED.toString())
                                        || corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.DELETED.toString())
                                        || corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.PROCESSING.toString()))
                                    return;

                                corpusBuilderInfoModel.setStatus(CorpusStatus.PROCESSING.toString());
                                corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.PROCESSING);

                                Collection<Future<?>> futures = new ArrayList<>();
                                Query query = new ObjectMapper().readValue(corpusBuilderInfoModel.getQuery(), Query.class);

                                for (ContentConnector connector : contentConnectors) {
                                    FetchMetadataTask task = new FetchMetadataTask(storeRESTClient, connector, query, tempDirectoryPath, corpusBuilderInfoModel.getArchiveId());
                                    task.setCorpusBuilderInfoDao(corpusBuilderInfoDao);
                                    task.setCorpusId(corpusId);
                                    futures.add(threadPoolExecutor.submit(task));
                                }

                                for (Future<?> future : futures) {
                                    future.get();
                                }
                            } catch (InterruptedException e) {

                                log.info("Thread Interrupted or error in execution");
                                corpusBuilderInfoModel.setStatus(CorpusStatus.CANCELED.toString());
                                corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CANCELED);

                            } catch (Exception ex) {

                                log.error("CorpusBuilderImpl.buildCorpus", ex);
                                if (corpusBuilderInfoModel != null) {
                                    corpusBuilderInfoModel.setStatus(CorpusStatus.CANCELED.toString());
                                    corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CANCELED);
                                }
                            } finally {
                                corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);
                                if (corpusBuilderInfoModel != null
                                        && (!corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.CANCELED.toString())
                                        || corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.DELETED.toString()))) {

                                    storeRESTClient.finalizeArchive(corpusBuilderInfoModel.getArchiveId());

                                    corpusBuilderInfoModel.setStatus(CorpusStatus.CREATED.toString());
                                    corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CREATED);
                                }

                                //TODO: Email to user when corpus is ready which will include the landing page for the corpus
                                new Thread(new JMSProducer()).start();
                                new Thread(new JMSConsumer()).start();
                            }
                        }
                    });
                }

            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }
}
