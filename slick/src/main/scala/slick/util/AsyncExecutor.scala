package slick.util

import java.io.Closeable
import java.lang.management.ManagementFactory
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import javax.management.{ InstanceNotFoundException, ObjectName }

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** A connection pool for asynchronous execution of blocking I/O actions.
  * This is used for the asynchronous query execution API on top of blocking back-ends like JDBC. */
trait AsyncExecutor extends Closeable {
  /** An ExecutionContext for running Futures. */
  def executionContext: ExecutionContext
  /** Shut the thread pool down and try to stop running computations. The thread pool is
    * transitioned into a state where it will not accept any new jobs. */
  def close(): Unit

}

object AsyncExecutor extends Logging {
  /** Create an [[AsyncExecutor]] with a thread pool suitable for blocking
    * I/O. New threads are created as daemon threads.
    *
    * @param name A prefix to use for the names of the created threads.
    * @param numThreads The number of threads in the pool. */
  def apply(name: String, numThreads: Int): AsyncExecutor = apply(name, numThreads, numThreads)

  /** Create an [[AsyncExecutor]] with a thread pool suitable for blocking
    * I/O. New threads are created as daemon threads.
    *
    * @param name A prefix to use for the names of the created threads.
    * @param minThreads The number of core threads in the pool.
    * @param maxConnections The maximum number of threads in the pool.
    *                       The maximum number of configured connections for the connection pool.
    *                       The underlying ThreadPoolExecutor will not pick up any more work when all connections are in use.
    *                       It will resume as soon as a connection is released again to the pool
    *                       Default is Integer.MAX_VALUE which is only ever a good choice when not using connection pooling
    * @param keepAliveTime when the number of threads is greater than
    *        the core, this is the maximum time that excess idle threads
    *        will wait for new tasks before terminating.
    * @param registerMbeans If set to true, register an MXBean that provides insight into the current
    *        queue and thread pool workload. */
  def apply(name: String, minThreads: Int, maxConnections: Int = Integer.MAX_VALUE, keepAliveTime: Duration = 1.minute, registerMbeans: Boolean = false): AsyncExecutor = new AsyncExecutor {
    @volatile private[this] lazy val mbeanName = new ObjectName(s"slick:type=AsyncExecutor,name=$name");

    // Before init: 0, during init: 1, after init: 2, during/after shutdown: 3
    private[this] val state = new AtomicInteger(0)

    @volatile private[this] var executor: ThreadPoolExecutor = _

    lazy val executionContext = {
      if(!state.compareAndSet(0, 1))
        throw new IllegalStateException("Cannot initialize ExecutionContext; AsyncExecutor already shut down")
      val queue: BlockingQueue[Runnable] = new LinkedBlockingQueue[Runnable]
      val tf = new DaemonThreadFactory(name + "-")
      executor = new ThreadPoolExecutor(minThreads, maxConnections, keepAliveTime.toMillis, TimeUnit.MILLISECONDS, queue, tf)
      if(registerMbeans) {
        try {
          val mbeanServer = ManagementFactory.getPlatformMBeanServer
          if(mbeanServer.isRegistered(mbeanName))
            logger.warn(s"MBean $mbeanName already registered (AsyncExecutor names should be unique)")
          else {
            logger.debug(s"Registering MBean $mbeanName")
            mbeanServer.registerMBean(new AsyncExecutorMXBean {
              def getMaxQueueSize = -1
              def getQueueSize = queue.size()
              def getMaxThreads = maxConnections
              def getActiveThreads = executor.getActiveCount
            }, mbeanName)
          }
        } catch { case NonFatal(ex) => logger.error("Error registering MBean", ex) }
      }
      if(!state.compareAndSet(1, 2)) {
        unregisterMbeans()
        executor.shutdownNow()
        throw new IllegalStateException("Cannot initialize ExecutionContext; AsyncExecutor shut down during initialization")
      }
      new ExecutionContextExecutor {
        override def reportFailure(t: Throwable): Unit = loggingReporter(t)

        override def execute(command: Runnable): Unit = {
          executor.execute(command)
        }
      }
    }

    private[this] def unregisterMbeans(): Unit = if(registerMbeans) {
      try {
        val mbeanServer = ManagementFactory.getPlatformMBeanServer
        logger.debug(s"Unregistering MBean $mbeanName")
        try mbeanServer.unregisterMBean(mbeanName) catch { case _: InstanceNotFoundException => }
      } catch { case NonFatal(ex) => logger.error("Error unregistering MBean", ex) }
    }

    def close(): Unit = if(state.getAndSet(3) == 2) {
      unregisterMbeans()
      executor.shutdownNow()
      if(!executor.awaitTermination(30, TimeUnit.SECONDS))
        logger.warn("Abandoning ThreadPoolExecutor (not yet destroyed after 30 seconds)")
    }
  }

  def default(name: String, maxConnections: Int): AsyncExecutor =
    apply(
      name,
      minThreads = 20,
      maxConnections = maxConnections
    )

  def default(name: String = "AsyncExecutor.default"): AsyncExecutor =
    apply(name, numThreads = 20)

  private class DaemonThreadFactory(namePrefix: String) extends ThreadFactory {
    private[this] val group = Option(System.getSecurityManager).fold(Thread.currentThread.getThreadGroup)(_.getThreadGroup)
    private[this] val threadNumber = new AtomicInteger(1)

    def newThread(r: Runnable): Thread = {
      val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
      if(!t.isDaemon) t.setDaemon(true)
      if(t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
      t
    }
  }

  /** An Executor which spawns a new daemon thread for each command. It is useful for wrapping
    * synchronous `close` calls for asynchronous `shutdown` operations. */
  private[slick] val shutdownExecutor: Executor = new Executor {
    def execute(command: Runnable): Unit = {
      val t = new Thread(command)
      t.setName("shutdownExecutor")
      t.setDaemon(true)
      t.start()
    }
  }

  val loggingReporter: Throwable => Unit = (t: Throwable) => {
    logger.warn("Execution of asynchronous I/O action failed", t)
  }
}

/** The information that is exposed by an [[AsyncExecutor]] via JMX. */
trait AsyncExecutorMXBean {
  /** Get the configured maximum queue size (0 for direct hand-off, -1 for unlimited) */
  def getMaxQueueSize: Int
  /** Get the current number of DBIOActions in the queue (waiting to be executed) */
  def getQueueSize: Int
  /** Get the configured maximum number of database I/O threads */
  def getMaxThreads: Int
  /** Get the number of database I/O threads that are currently executing a task */
  def getActiveThreads: Int
}
