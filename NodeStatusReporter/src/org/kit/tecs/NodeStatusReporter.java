package org.kit.tecs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NodeStatusReporter {

	private static NodeProbe nodeProbe;
	private static ZKConnector zooCon;
	private static String zkHosts;
	private static String zkHeadNode;
	private static String nodename;
	private static int ival;
	private static OperationMode currentOpMode;
	private static OperationMode updatedOpMode;
	private static StatusUpdater statusUpdater;

	public NodeStatusReporter() {
		statusUpdater = new StatusUpdater();
		currentOpMode = OperationMode.unreachable;
		updatedOpMode = OperationMode.unreachable;
		
	}

	private static void npConnect() {

		nodeProbe = new NodeProbe("127.0.0.1", 7199, null, null);

		while (true) {

			try {

				nodeProbe.connect();
				
				updatedOpMode = retrieveOperationMode();
				updateStatus(currentOpMode, updatedOpMode);
				
				break;
			} catch (IOException e) {

				System.out
						.println("NodeProbe Connection unavailable! Retry in 15 seconds...");
					
			}

			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
			}

		}

	}

	private static void zkConnect() {

		zooCon = new ZKConnector();

		while (true) {
			
				try {
					zooCon.connect(zkHosts);
					break;
					
				} catch (IOException e) {
					
					System.out
					.println("ZooKeeper Connection unavailable! Retry in 15 seconds... Please make sure to provide a valid hostlist as agrs[0]: <zkHost:zkPort>[,<zkHost:zkPort>]");
				} catch (InterruptedException e) {
					
					System.out
					.println("ZooKeeper Connection unavailable! Retry in 15 seconds... Please make sure to provide a valid hostlist as agrs[0]: <zkHost:zkPort>[,<zkHost:zkPort>]");
				}
				
			

				
		

			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
			}

		}

	}

	public static OperationMode retrieveOperationMode() {

		String opModeString = "";

		try {

			opModeString = nodeProbe.getOperationMode();

		} catch (Exception e) {

		}

		OperationMode returnOpMode = OperationMode.unreachable;
		
		
		if(opModeString.equalsIgnoreCase("JOINING")) {
			returnOpMode = OperationMode.joining;
		}
		if(opModeString.equalsIgnoreCase("NORMAL")) {
			returnOpMode = OperationMode.normal;
		}
		if(opModeString.equalsIgnoreCase("MOVING")) {
			returnOpMode = OperationMode.moving;
		}
		if(opModeString.equalsIgnoreCase("LEAVING")) {
			returnOpMode = OperationMode.leaving;
		}
		if(opModeString.equalsIgnoreCase("DECOMMISSIONED")) {
			returnOpMode = OperationMode.decommissioned;
		}
		if(opModeString.equalsIgnoreCase("")) {
			returnOpMode = OperationMode.unreachable;
		}
		
		
		return returnOpMode;
		
		
		

	
		

	}

	private static void setStatus(OperationMode _opMode) {

		zooCon.createNode(zkHeadNode + "/" + _opMode + "/" + nodename,
				getLocalhostString(), false, true);

	}

	private static void revokeStatus(OperationMode _opMode) {

		zooCon.deleteNode(zkHeadNode + "/" + _opMode + "/" + nodename);

	}

	public static void updateStatus(OperationMode _currentOpMode,
			OperationMode _updatedOpMode) {

		if (_currentOpMode != null) {
			revokeStatus(_currentOpMode);
		}

		setStatus(_updatedOpMode);

		
	}

	public static String getLocalhostString() {

		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println("Cannot retrieve localhost address!");
			return null;
		}

	}

	public static void main(String[] args) {

		NodeStatusReporter nsr = new NodeStatusReporter();

		if (args.length < 2) {
			System.out
					.println("NodeStatusReporter <zkHost:zkPort>[,<zkHost:zkPort>] <zkHeadNode> [<nodename>] [<interval>]");
		}

		zkHosts = args[0];
		zkHeadNode = args[1];

		if (args.length > 2) {
			nodename = args[2];
		}
		if (args.length > 3) {
			ival = Integer.parseInt(args[3]);
		}

		zkConnect();
		

		zooCon.checkAndCreatePath(zkHeadNode + "/" + "all", true);
		zooCon.checkAndCreatePath(zkHeadNode + "/" + "joining", true);
		zooCon.checkAndCreatePath(zkHeadNode + "/" + "normal", true);
		zooCon.checkAndCreatePath(zkHeadNode + "/" + "moving", true);
		zooCon.checkAndCreatePath(zkHeadNode + "/" + "leaving", true);
		zooCon.checkAndCreatePath(zkHeadNode + "/" + "decommissioned", true);
		zooCon.checkAndCreatePath(zkHeadNode + "/" + "unreachable", true);

		zooCon.createNode(zkHeadNode + "/" + "all" + "/" + nodename,
				getLocalhostString(), false, false);
		zooCon.createNode(zkHeadNode + "/" + "unreachable" + "/" + nodename,
				getLocalhostString(), false, false);

		npConnect(); 
		
				
		statusUpdater.run();

	}

	class StatusUpdater extends Thread {

		public void run() {

			int _ival = ival * 1000;

			while (true) {

				try {
					this.sleep(_ival);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				updatedOpMode = retrieveOperationMode();

				if (!updatedOpMode.equals(currentOpMode)) {
					updateStatus(currentOpMode, updatedOpMode);
				}

				if (currentOpMode.equals(OperationMode.unreachable)) {
					npConnect();
				}

				currentOpMode = updatedOpMode;

			}

		}

	}

}
