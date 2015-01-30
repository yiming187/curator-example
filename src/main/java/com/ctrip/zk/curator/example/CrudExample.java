package com.ctrip.zk.curator.example;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

public class CrudExample {
	public static void main(String[] args) {
		String zkConnString = "127.0.0.1:2181";
		CuratorFramework client = null;
		try {
			client = CuratorFrameworkFactory.newClient(zkConnString,
					new ExponentialBackoffRetry(1000, 3));
			client.start();
			if (client.checkExists().forPath("/crud") != null) {
				delete(client, "/crud");
			}
			create(client, "/crud/a", "a is a node".getBytes());
			System.out.println("/crud/a : "
					+ new String(client.getData().forPath("/crud/a")));

			String nodeBPath = createEphemeralSequential(client, "/crud/b",
					"b is ephemeral node".getBytes());
			System.out.println("/crud/b : "
					+ new String(client.getData().forPath(nodeBPath)));

			setDataAsync(client, "/crud/a", "a is async updated".getBytes());

			setDataAsyncWithCallback(client, new BackgroundCallback() {

				@Override
				public void processResult(CuratorFramework client,
						CuratorEvent event) throws Exception {
					System.out.println("setDataAsyncWithCallback: " + event);
				}

			}, "/crud/a", "a is callback updated".getBytes());

			List<String> watched = watchedGetChildren(client, "/crud");
			System.out.println(watched);

			createEphemeral(client, "/crud/c", "c is a new node".getBytes());
			watched = watchedGetChildren(client, "/crud");
			System.out.println(watched);

			delete(client, "/crud");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			CloseableUtils.closeQuietly(client);
		}
	}

	public static void create(CuratorFramework client, String path,
			byte[] payload) throws Exception {
		// this will create the given ZNode with the given data
		client.create().creatingParentsIfNeeded().forPath(path, payload);
	}

	public static void createEphemeral(CuratorFramework client, String path,
			byte[] payload) throws Exception {
		// this will create the given EPHEMERAL ZNode with the given data
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL).forPath(path, payload);
	}

	public static String createEphemeralSequential(CuratorFramework client,
			String path, byte[] payload) throws Exception {
		// this will create the given EPHEMERAL-SEQUENTIAL ZNode with the given
		// data using Curator protection.
		return client.create().creatingParentsIfNeeded().withProtection()
				.withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
				.forPath(path, payload);
	}

	public static void setData(CuratorFramework client, String path,
			byte[] payload) throws Exception {
		// set data for the given node
		client.setData().forPath(path, payload);
	}

	public static void setDataAsync(CuratorFramework client, String path,
			byte[] payload) throws Exception {
		// this is one method of getting event/async notifications
		CuratorListener listener = new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client,
					CuratorEvent event) throws Exception {
				System.out.println("setDataAsync: " + event);
			}
		};
		client.getCuratorListenable().addListener(listener);
		// set data for the given node asynchronously. The completion
		// notification
		// is done via the CuratorListener.
		client.setData().inBackground().forPath(path, payload);
	}

	public static void setDataAsyncWithCallback(CuratorFramework client,
			BackgroundCallback callback, String path, byte[] payload)
			throws Exception {
		// this is another method of getting notification of an async completion
		client.setData().inBackground(callback).forPath(path, payload);
	}

	public static void delete(CuratorFramework client, String path)
			throws Exception {
		// delete the given node
		client.delete().deletingChildrenIfNeeded().forPath(path);
	}

	public static void guaranteedDelete(CuratorFramework client, String path)
			throws Exception {
		// delete the given node and guarantee that it completes
		client.delete().guaranteed().forPath(path);
	}

	public static List<String> watchedGetChildren(CuratorFramework client,
			String path) throws Exception {
		/**
		 * Get children and set a watcher on the node. The watcher notification
		 * will come through the CuratorListener (see setDataAsync() above).
		 */
		return client.getChildren().watched().forPath(path);
	}

	public static List<String> watchedGetChildren(CuratorFramework client,
			String path, Watcher watcher) throws Exception {
		/**
		 * Get children and set the given watcher on the node.
		 */
		return client.getChildren().usingWatcher(watcher).forPath(path);
	}

}
