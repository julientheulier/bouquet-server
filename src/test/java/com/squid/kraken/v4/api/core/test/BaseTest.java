package com.squid.kraken.v4.api.core.test;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.annotation.AnnotationServiceBaseImpl;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceBaseImpl;
import com.squid.kraken.v4.api.core.bookmark.BookmarkServiceBaseImpl;
import com.squid.kraken.v4.api.core.client.ClientServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.AuthServiceImpl;
import com.squid.kraken.v4.api.core.customer.CustomerServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.ShortcutServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.StateServiceBaseImpl;
import com.squid.kraken.v4.api.core.dimension.DimensionServiceBaseImpl;
import com.squid.kraken.v4.api.core.domain.DomainServiceBaseImpl;
import com.squid.kraken.v4.api.core.metric.MetricServiceBaseImpl;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectuser.ProjectUserServiceBaseImpl;
import com.squid.kraken.v4.api.core.relation.RelationServiceBaseImpl;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.api.core.usergroup.UserGroupServiceBaseImpl;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.caching.redis.RedisCacheProxy;
import com.squid.kraken.v4.core.analysis.engine.cache.MetaModelObserver;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreManagerFactory;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DataStoreEventBus;
import com.squid.kraken.v4.runtime.RuntimeService;

public class BaseTest {

	protected static Logger logger;
	
    public static final String JUNIT_DB_NAME = "junit";
    
    public static CustomerServiceBaseImpl cs;
    public static ProjectServiceBaseImpl ps;
    public static DomainServiceBaseImpl ds;
    public static MetricServiceBaseImpl ms;
    public static DimensionServiceBaseImpl dims;
    public static AttributeServiceBaseImpl as;
    public static UserServiceBaseImpl us;
    public static AnnotationServiceBaseImpl annotationService;
    public static ProjectUserServiceBaseImpl pus;
    public static RelationServiceBaseImpl rs;
    public static UserGroupServiceBaseImpl ugs;
    public static AuthServiceImpl aus;
    public static ClientServiceBaseImpl clients;
    public static ShortcutServiceBaseImpl shortcutS;
    public static StateServiceBaseImpl stateS;
    public static BookmarkServiceBaseImpl bookmarkS;
    
    static {
    	// set system properties
    	System.setProperty("kraken.config.file", "kraken_v4_config.xml");
        // force using a different log directory
    	if (System.getProperty("logback.configurationFile") == null) {
    		System.setProperty("logback.configurationFile", "logback-junit.xml");
    	}
        logger = LoggerFactory.getLogger(BaseTest.class);
        
        cs = CustomerServiceBaseImpl.getInstance();
        ps = ProjectServiceBaseImpl.getInstance();
        ds = DomainServiceBaseImpl.getInstance();
        ms = MetricServiceBaseImpl.getInstance();
        dims = DimensionServiceBaseImpl.getInstance();
        as = AttributeServiceBaseImpl.getInstance();
        us = UserServiceBaseImpl.getInstance();
        annotationService = AnnotationServiceBaseImpl.getInstance();
        pus = ProjectUserServiceBaseImpl.getInstance();
        rs = RelationServiceBaseImpl.getInstance();
        ugs = UserGroupServiceBaseImpl.getInstance();
        aus = AuthServiceImpl.getInstance();
        clients = ClientServiceBaseImpl.getInstance();
        shortcutS = ShortcutServiceBaseImpl.getInstance();
        stateS = StateServiceBaseImpl.getInstance();
        bookmarkS = BookmarkServiceBaseImpl.getInstance();
    }

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String plugin = System.getProperty("kraken.plugin.dir");
		if (plugin==null || plugin.equals("")) {
			String home = System.getProperty("user.home",".");
			System.setProperty("kraken.plugin.dir", home+"/Drivers");
		}
	}

    public BaseTest() {
    	this(true);// mock startup
    }

    /**
     * Base contructor for tests.
     * Warning : this contructor should be idempotent as it can be called several times when running {@link ServicesTestSuite}
     */
    public BaseTest(boolean isMock) {
        super();
        // force using its own local DB
        KrakenConfig.setProperty("kraken.mongodb.host", "localhost");
        KrakenConfig.setProperty("kraken.mongodb.port", "27017");
        KrakenConfig.setProperty("kraken.mongodb.dbname", JUNIT_DB_NAME);
        // force using a different ehcache directory
        System.setProperty("kraken.ehcache.config", "kraken_v4_junit_ehcache.xml");
        
        if (isMock) {
	        RedisCacheProxy.setMock();
	        // initialize RedisCacheManager (Mock)
	        RedisCacheManager.setMock();
	        RedisCacheManager.getInstance().startCacheManager();
	        
			//RedisCacheManager.getInstance().setConfig(AWSRedisCacheConfig.getDefault());
			//RedisCacheManager.getInstance().startCacheManager();
			
	        // initialize DimensionStore
	        DimensionStoreManagerFactory.initMock();
	        
			// ModelObserver
	        DataStoreEventBus.getInstance().unSubscribe(
					MetaModelObserver.getInstance());
			DataStoreEventBus.getInstance().subscribe(
					MetaModelObserver.getInstance());
        } else {
            //
            // start the runtime
            RuntimeService.startup("0");
        }
    }
    
    public User addUser(String customerId) {
        AppContext ctx = ServiceUtils.getInstance().getRootUserContext(customerId);
        User newUser = new User(new UserPK(ctx.getCustomerId()), "user1"+customerId, "Password");
        us.store(ctx, newUser);
        return newUser;
    }
    
    public void given(String message) {
    	logger.info("==== GIVEN "+message);
    }
    
    public void when(String message) {
    	logger.info("==== WHEN "+message);
    }
    
    public void then(String message) {
    	logger.info("==== THEN "+message);
    }

}

