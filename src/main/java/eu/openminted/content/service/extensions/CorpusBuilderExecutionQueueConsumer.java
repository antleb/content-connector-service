package eu.openminted.content.service.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.service.dao.CorpusBuilderInfoDao;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.content.service.tasks.FetchMetadataTask;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

@Component
public class CorpusBuilderExecutionQueueConsumer {
    private static Logger log = Logger.getLogger(CorpusBuilderExecutionQueueConsumer.class.getName());

    private BlockingQueue<String> queue;

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Autowired
    private CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    private StoreRESTClient storeRESTClient;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolExecutor localThreadPoolExecutor;

    @org.springframework.beans.factory.annotation.Value("${tempDirectoryPath}")
    private String tempDirectoryPath;

    public void init(BlockingQueue<String> queue) {
        if (this.queue == null)
            this.queue = queue;
        if (this.localThreadPoolExecutor == null)
            this.localThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        while (true) {
                try {
                    String corpusId = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (corpusId != null && !corpusId.isEmpty()) {

                        this.localThreadPoolExecutor.execute(()->{
                        if (contentConnectors != null) {
                            CorpusBuilderInfoModel corpusBuilderInfoModel = null;
                            try {
                                corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);

                                Collection<Future<?>> futures = new ArrayList<>();
                                Query query = new ObjectMapper().readValue(corpusBuilderInfoModel.getQuery(), Query.class);

                                for (ContentConnector connector : contentConnectors) {
                                    FetchMetadataTask task = new FetchMetadataTask(storeRESTClient, connector, query, tempDirectoryPath, corpusBuilderInfoModel.getArchiveId());
                                    futures.add(threadPoolExecutor.submit(task));
                                }
                                corpusBuilderInfoModel.setStatus(CorpusStatus.PROCESSING.toString());
                                corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.PROCESSING);
                                for (Future<?> future : futures) {
                                    future.get();
                                }
                                corpusBuilderInfoModel.setStatus(CorpusStatus.CREATED.toString());
                                corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CREATED);
                                //TODO: Email to user when corpus is ready which will include the landing page for the corpus
                            } catch (Exception ex) {
                                log.error("CorpusBuilderImpl.buildCorpus", ex);
                                if (corpusBuilderInfoModel != null) {
                                    corpusBuilderInfoModel.setStatus(CorpusStatus.CANCELED.toString());
                                    corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CANCELED);
                                }
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
