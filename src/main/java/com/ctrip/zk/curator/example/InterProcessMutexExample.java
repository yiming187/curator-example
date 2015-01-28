package com.ctrip.zk.curator.example;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class InterProcessMutexExample {
	private static final int QTY = 5;
	private static final int REPETITIONS = QTY * 10;
	private static final String PATH = "/examples/locks";

	public static void main(String[] args) throws Exception {
		final FakeLimitedResource resource = new FakeLimitedResource();
		ExecutorService service = Executors.newFixedThreadPool(QTY);
		final String zkConnString = "127.0.0.1:2181";
		try {
			for (int i = 0; i < QTY; ++i) {
				final int index = i;
				Callable<Void> task = new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						CuratorFramework client = CuratorFrameworkFactory
								.newClient(zkConnString,
										new ExponentialBackoffRetry(1000, 3));
						try {
							client.start();
							final ExampleClientThatLocks example = new ExampleClientThatLocks(
									client, PATH, resource, "Client " + index);
							for (int j = 0; j < REPETITIONS; ++j) {
								example.doWork(10, TimeUnit.SECONDS);
							}
						} catch (Throwable e) {
							e.printStackTrace();
						} finally {
							CloseableUtils.closeQuietly(client);
						}
						return null;
					}
				};
				service.submit(task);
			}
			service.shutdown();
			service.awaitTermination(10, TimeUnit.MINUTES);
		} finally {
		}
	}
}

class ExampleClientThatLocks {
	private final InterProcessMutex lock;
	private final FakeLimitedResource resource;
	private final String clientName;

	public ExampleClientThatLocks(CuratorFramework client, String lockPath,
			FakeLimitedResource resource, String clientName) {
		this.resource = resource;
		this.clientName = clientName;
		lock = new InterProcessMutex(client, lockPath);
	}

	public void doWork(long time, TimeUnit unit) throws Exception {
		if (!lock.acquire(time, unit)) {
			throw new IllegalStateException(clientName
					+ " could not acquire the lock");
		}
		try {
			System.out.println(clientName + " has the lock");
			resource.use(); // access resource exclusively
		} finally {
			System.out.println(clientName + " releasing the lock");
			lock.release(); // always release the lock in a finally block
		}
	}

	public static void main(String[] args) {

	}
}

class FakeLimitedResource {
	private final AtomicBoolean inUse = new AtomicBoolean(false);

	public void use() throws InterruptedException {
		// 真实环境中我们会在这里访问/维护一个共享的资源
		// 这个例子在使用锁的情况下不会非法并发异常IllegalStateException
		// 但是在无锁的情况由于sleep了一段时间，很容易抛出异常
		if (!inUse.compareAndSet(false, true)) {
			throw new IllegalStateException(
					"Needs to be used by one client at a time");
		}
		try {
			Thread.sleep((long) (3 * Math.random()));
		} finally {
			inUse.set(false);
		}
	}
}
