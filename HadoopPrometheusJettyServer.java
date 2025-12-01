package org.apache.hadoop.metrics2.sink;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ShutdownHookManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Embedded Jetty server for Prometheus metrics export.
 * Lifecycle managed by Hadoop Metrics2 framework.
 */
public class HadoopPrometheusJettyServer {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopPrometheusJettyServer.class);
  
  private static final String DEFAULT_PORT_KEY = "prometheus.metrics.port";
  private static final int DEFAULT_PORT = 9090;
  private static final String METRICS_PATH = "/metrics";
  private static final String SINK_PREFIX = "prometheus";
  
  private final int port;
  private final String contextName;
  private final PrometheusMetricsSink metricsSink;
  private Server jettyServer;

  /**
   * Creates a new instance. Server is not started until start() is called.
   * @param conf Hadoop configuration
   * @param contextName Component name (e.g., namenode, datanode)
   * @throws IllegalArgumentException if configuration is invalid
   */
  public HadoopPrometheusJettyServer(Configuration conf, String contextName) {
    this.contextName = Objects.requireNonNull(contextName, "Component context name is required");
    Configuration confCopy = new Configuration(Objects.requireNonNull(conf, "Hadoop configuration is required"));
    
    // Determine port priority: component-specific > global sink > global default
    this.port = resolvePort(confCopy);
    LOG.info("Resolved Prometheus port for {}: {} (key: {}.sink.{}.port)", 
             contextName, port, contextName, SINK_PREFIX);
    
    this.metricsSink = createMetricsSink(confCopy);
    registerShutdownHook();
  }

  private int resolvePort(Configuration conf) {
    // Priority 1: component-specific sink config
    String componentKey = contextName + ".sink." + SINK_PREFIX + ".port";
    int port = conf.getInt(componentKey, -1);
    if (port > 0) {
      LOG.debug("Using component-specific port configuration: {}={}", componentKey, port);
      return port;
    }
    
    // Priority 2: global sink config
    String globalKey = SINK_PREFIX + ".port";
    port = conf.getInt(globalKey, -1);
    if (port > 0) {
      LOG.debug("Using global sink port configuration: {}={}", globalKey, port);
      return port;
    }
    
    // Priority 3: default global key
    port = conf.getInt(DEFAULT_PORT_KEY, DEFAULT_PORT);
    LOG.debug("Using default port configuration: {}={}", DEFAULT_PORT_KEY, port);
    return port;
  }

  private PrometheusMetricsSink createMetricsSink(Configuration conf) {
    try {
      // Extract subset configuration using Hadoop utilities
      Map<String, String> props = conf.getPropsWithPrefix(contextName + ".sink." + SINK_PREFIX + ".");
      MapConfiguration mapConf = new MapConfiguration(new HashMap<>(props));
      SubsetConfiguration sinkConf = new SubsetConfiguration(mapConf, "", ".");
      
      PrometheusMetricsSink sink = new PrometheusMetricsSink();
      sink.init(sinkConf);
      LOG.debug("Successfully initialized PrometheusMetricsSink for {}", contextName);
      return sink;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Failed to initialize PrometheusMetricsSink for %s. " +
                       "Check configuration in hadoop-metrics2.properties", contextName), e);
    }
  }

  private void registerShutdownHook() {
    ShutdownHookManager.get().addShutdownHook(
        () -> {
          try {
            stop();
          } catch (Exception e) {
            LOG.error("Error during shutdown of {} Prometheus server", contextName, e);
          }
        },
        Integer.MAX_VALUE // Run last
    );
    LOG.debug("Registered shutdown hook for {} Prometheus server", contextName);
  }

  public synchronized void start() throws IOException {
    if (isRunning()) {
      LOG.warn("Prometheus server for {} is already running on port {}", contextName, port);
      return;
    }

    jettyServer = new Server(port);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new MetricsServlet(metricsSink)), METRICS_PATH);
    jettyServer.setHandler(context);

    try {
      jettyServer.start();
      LOG.info("Started Prometheus metrics endpoint for {}: http://{}:{}{}", 
               contextName, getListenAddress(), port, METRICS_PATH);
    } catch (Exception e) {
      if (e instanceof java.net.BindException) {
        throw new IOException(
            String.format("Port %d is already in use. Failed to start %s Prometheus server. " +
                         "Check configuration or stop conflicting process.", port, contextName), e);
      }
      throw new IOException("Failed to start Jetty server for " + contextName, e);
    }
  }

  private String getListenAddress() {
    return "0.0.0.0".equals(jettyServer.getURI().getHost()) ? "localhost" : jettyServer.getURI().getHost();
  }

  public synchronized void stop() {
    if (jettyServer == null || jettyServer.isStopped()) {
      LOG.debug("Prometheus server for {} is already stopped", contextName);
      return;
    }

    try {
      jettyServer.stop();
      LOG.info("Stopped Prometheus server for {} on port {}", contextName, port);
    } catch (Exception e) {
      LOG.error("Error stopping Prometheus server for {} on port {}", contextName, port, e);
    } finally {
      jettyServer = null;
    }
  }

  public boolean isRunning() {
    return jettyServer != null && jettyServer.isStarted() && !jettyServer.isStopping();
  }

  public int getPort() {
    return port;
  }

  public String getContextName() {
    return contextName;
  }

  private static class MetricsServlet extends HttpServlet {
    private final PrometheusMetricsSink sink;

    MetricsServlet(PrometheusMetricsSink sink) {
      this.sink = Objects.requireNonNull(sink);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setContentType("text/plain; version=0.0.4; charset=utf-8");
      resp.setStatus(HttpServletResponse.SC_OK);
      
      try (Writer writer = resp.getWriter()) {
        sink.writeMetrics(writer);
      } catch (Exception e) {
        LOG.error("Failed to write metrics for request from {}", req.getRemoteAddr(), e);
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  // Component-specific factory methods
  public static HadoopPrometheusJettyServer forNameNode(Configuration conf) {
    return new HadoopPrometheusJettyServer(conf, "namenode");
  }

  public static HadoopPrometheusJettyServer forDataNode(Configuration conf) {
    return new HadoopPrometheusJettyServer(conf, "datanode");
  }

  public static HadoopPrometheusJettyServer forResourceManager(Configuration conf) {
    return new HadoopPrometheusJettyServer(conf, "resourcemanager");
  }

  public static HadoopPrometheusJettyServer forNodeManager(Configuration conf) {
    return new HadoopPrometheusJettyServer(conf, "nodemanager");
  }
}