package com.ctrip.zk.curator.example;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

public class LeaderSelectorExample {
	private static final int CLIENT_QTY = 10;
	private static final String PATH = "/examples/leader";

	public static void main(String[] args) throws Exception {
		List<CuratorFramework> clients = Lists.newArrayList();
		List<ExampleClient> examples = Lists.newArrayList();
		String zkConnString = "127.0.0.1:2181";
		try {
			for (int i = 0; i < CLIENT_QTY; ++i) {
				CuratorFramework client = CuratorFrameworkFactory.newClient(
						zkConnString, new ExponentialBackoffRetry(1000, 3));
				clients.add(client);
				ExampleClient example = new ExampleClient(client, PATH,
						"Client #" + i);
				examples.add(example);
				client.start();
				example.start();
			}

			System.out.println("Press enter/return to quit\n");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} finally {
			System.out.println("Shutting down...");
			for (ExampleClient exampleClient : examples) {
				CloseableUtils.closeQuietly(exampleClient);
			}
			for (CuratorFramework client : clients) {
				CloseableUtils.closeQuietly(client);
			}
		}
	}
}

class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable {
	private final String name;
	private final LeaderSelector leaderSelector;
	private final AtomicInteger leaderCount = new AtomicInteger();

	public ExampleClient(CuratorFramework client, String path, String name) {
		this.name = name;
		leaderSelector = new LeaderSelector(client, path, this);
		leaderSelector.autoRequeue();
	}

	public void start() throws IOException {
		leaderSelector.start();
	}

	@Override
	public void close() throws IOException {
		leaderSelector.close();
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		final int waitSeconds = (int) (5 * Math.random()) + 1;
		System.out.println(name + " is now the leader. Waiting " + waitSeconds
				+ " seconds...");
		System.out.println(name + " has been leader "
				+ leaderCount.getAndIncrement() + " time(s) before.");
		try {
			Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
		} catch (InterruptedException e) {
			System.err.println(name + " was interrupted.");
			Thread.currentThread().interrupt();
		} finally {
			System.out.println(name + " relinquishing leadership.\n");
		}
	}
}