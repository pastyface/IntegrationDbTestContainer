package com.example.testcontainers

import com.github.dockerjava.api.DockerClient
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.hibernate.SessionFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.datasource.DelegatingDataSource
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.Network

grails.util.Environment
javax.annotation.PreDestroy

/**
 * This class uses Testcontainers to manage a MySQL container for integration testing.
 * It allows creating a snapshot of the test database after bootstrapping so tests can reset
 * the database to a consistent state quickly between runs.
 */
@Slf4j
class IntegrationDbTestContainer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    static final String REPO_NAME = 'test-db-snapshot'
    static final String IMAGE_TAG = 'latest'
    static final String IMAGE_NAME = "$REPO_NAME:$IMAGE_TAG"
    static final String DB_NAME = "testdb"
    static final String DB_USER = "root"
    static final String DB_PASS = "password"

    static IntegrationDbTestContainer INSTANCE

    boolean deleteImage
    boolean forceRefresh
    boolean newImage = true

    GenericContainer mySQLContainer
    Network sharedNetwork

    ApplicationContext applicationContext

    IntegrationDbTestContainer() {
        deleteImage = System.getProperty('integrationDB.deleteImage') as boolean
        forceRefresh = System.getProperty('integrationDB.forceRefresh') as boolean

        if(Environment.current == Environment.TEST) {
            if (forceRefresh) {
                log.info "Forcing the creation of a new MySQL container"
                createNewMySQLContainer()
            } else {
                DockerClient dockerClient = DockerClientFactory.instance().client()
                def images = dockerClient.listImagesCmd().withImageNameFilter(IMAGE_NAME).exec()

                if (images) {
                    log.info "Using existing pre-populated MySQL container - use -DintegrationDB.forceRefresh to recreate"
                    mySQLContainer = new GenericContainer<>(IMAGE_NAME).withExposedPorts(3306)
                    System.setProperty('dataSource.dbCreate', 'none')
                    newImage = false
                } else {
                    log.info "No existing image found, creating new MySQL container..."
                    createNewMySQLContainer()
                }
            }
        }
    }

    void initialize(ConfigurableApplicationContext applicationContext) {
        this.applicationContext = applicationContext
        INSTANCE = this

        if(Environment.current == Environment.TEST) {
            mySQLContainer.start()
            configureDataSource(false)
        }
    }

    private void configureDataSource(boolean updatePoolUrl) {
        int mappedPort = mySQLContainer.getMappedPort(3306)
        String host = mySQLContainer.getHost()
        String jdbcUrl = "jdbc:mysql://${host}:${mappedPort}/${DB_NAME}?useUnicode=true&noAccessToProcedureBodies=true&serverTimezone=Europe/London&characterEncoding=utf8&collationConnection=utf8mb4_0900_ai_ci"
        System.setProperty("dataSource.url", jdbcUrl)
        System.setProperty("dataSource.username", DB_USER)
        System.setProperty("dataSource.password", DB_PASS)

        if (updatePoolUrl) {
            def dataSource = applicationContext.getBean('dataSource')
            while (dataSource instanceof DelegatingDataSource) {
                dataSource = dataSource.targetDataSource
            }
            if (dataSource.getPool() != null) {
                dataSource.poolProperties.url = jdbcUrl
                dataSource.getPool().purge()
            }
        }
    }

    private void createNewMySQLContainer() {
        mySQLContainer = new MySQLContainer<>("mysql:8.0")
                .withDatabaseName(DB_NAME)
                .withUsername(DB_USER)
                .withPassword(DB_PASS)
                .withNetworkAliases("mysql")
                .withUrlParam("useUnicode", "true")
                .withUrlParam("noAccessToProcedureBodies", "true")
                .withUrlParam("serverTimezone", "Europe/London")
                .withUrlParam("characterEncoding", "utf8")
                .withUrlParam("collationConnection", "utf8mb4_0900_ai_ci")
        mySQLContainer.addParameter('TC_MY_CNF' ,'/testcontainer/conf.d')
        System.setProperty('dataSource.dbCreate', 'update')
    }

    void flushData(Sql sql) {
        if(newImage) {
            log.info "Flushing test data..."
            sql.execute("FLUSH TABLES WITH READ LOCK")
            sql.execute("SET GLOBAL innodb_fast_shutdown = 0")
            log.info "Finished flushing"
        }
    }

    String createDatabaseSnapshot() {
        if(newImage) {
            try {
                log.info "Creating snapshot image..."
                def containerId = mySQLContainer.getContainerId()

                DockerClient dockerClient = DockerClientFactory.instance().client()
                String imageId = dockerClient.commitCmd(containerId)
                        .withRepository(REPO_NAME)
                        .withTag(IMAGE_TAG)
                        .withLabels(['org.testcontainers': deleteImage as String])
                        .exec()

                resetDatabase()

                log.info "Snapshot image saved: $imageId"
                return imageId
            } catch (Exception e) {
                throw new RuntimeException("Failed to create database snapshot", e)
            }
        }
    }

    void resetDatabase() {
        log.info "Resetting the database"
        if (mySQLContainer != null) {
            mySQLContainer.stop()
        }

        mySQLContainer = new GenericContainer<>(IMAGE_NAME)
                .withImagePullPolicy { false }
                .withExposedPorts(3306)
                .withNetworkAliases("mysql")

        mySQLContainer.start()
        configureDataSource(true)
        clearCache()
    }

    void clearCache() {
        SessionFactory sessionFactory = applicationContext.getBean("sessionFactory")
        sessionFactory.cache.evictAllRegions()
        def interceptor = applicationContext.getBean("persistenceInterceptor")
        interceptor.flush()
        interceptor.clear()
    }

    @PreDestroy
    void cleanup() {
        if(Environment.current == Environment.TEST && mySQLContainer != null) {
            mySQLContainer.stop()
            mySQLContainer.close()
        }
    }
}
