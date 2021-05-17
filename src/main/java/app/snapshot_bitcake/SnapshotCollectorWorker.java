package app.snapshot_bitcake;

import app.AppConfig;
import app.CausalBroadcastShared;
import servent.message.Message;
import servent.message.snapshot.AcharyaBadrinathAskAmountMessage;
import servent.message.snapshot.NaiveAskAmountMessage;
import servent.message.util.MessageUtil;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main snapshot collector class. Has support for Naive, Chandy-Lamport
 * and Lai-Yang snapshot algorithms.
 * 
 * @author bmilojkovic
 *
 */
public class SnapshotCollectorWorker implements SnapshotCollector {

	private volatile boolean working = true;
	
	private final AtomicBoolean collecting = new AtomicBoolean(false);
	
	private final Map<String, Integer> collectedNaiveValues = new ConcurrentHashMap<>();
	private final Map<String, Object[]> collectedAcharyaBadrinathValues = new ConcurrentHashMap<>();

	private SnapshotType snapshotType = SnapshotType.NAIVE;
	
	private BitcakeManager bitcakeManager;

	public SnapshotCollectorWorker(SnapshotType snapshotType) {
		this.snapshotType = snapshotType;

		switch (snapshotType) {
			case NAIVE -> bitcakeManager = new NaiveBitcakeManager();
			case ACHARYA_BADRINATH -> bitcakeManager = new AcharyaBadrinathManager();
			case NONE -> {
				AppConfig.timestampedErrorPrint("Making snapshot collector without specifying type. Exiting...");
				System.exit(0);
			}
		}
	}
	
	@Override
	public BitcakeManager getBitcakeManager() {
		return bitcakeManager;
	}
	
	@Override
	public void run() {
		while(working) {
			
			/*
			 * Not collecting yet - just sleep until we start actual work, or finish
			 */
			while (!collecting.get()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (!working) {
					return;
				}
			}

			Map<Integer, Integer> vectorClock;
			Message askMessage;
			
			/*
			 * Collecting is done in three stages:
			 * 1. Send messages asking for values
			 * 2. Wait for all the responses
			 * 3. Print result
			 */
			
			//1 send asks
			switch (snapshotType) {
			case NAIVE:
				vectorClock = new ConcurrentHashMap<>(CausalBroadcastShared.getVectorClock());

				askMessage = new NaiveAskAmountMessage(
						AppConfig.myServentInfo,
						null,
						null,
						vectorClock
				);
				
				for (Integer neighbor : AppConfig.myServentInfo.getNeighbors()) {
					askMessage = askMessage.changeReceiver(neighbor);
					
					MessageUtil.sendMessage(askMessage);
				}

				collectedNaiveValues.put("node"+AppConfig.myServentInfo.getId(), bitcakeManager.getCurrentBitcakeAmount());

				// Increment clock for original sender
				CausalBroadcastShared.incrementClock(AppConfig.myServentInfo.getId());

				break;

			case ACHARYA_BADRINATH:
				vectorClock = new ConcurrentHashMap<>(CausalBroadcastShared.getVectorClock());

				askMessage = new AcharyaBadrinathAskAmountMessage(
						AppConfig.myServentInfo,
						null,
						null,
						vectorClock
				);

				for (Integer neighbor : AppConfig.myServentInfo.getNeighbors()) {
					askMessage = askMessage.changeReceiver(neighbor);

					MessageUtil.sendMessage(askMessage);
				}


				addAcharyaBadrinathSnapshotInfo(
					"node"+AppConfig.myServentInfo.getId(),
					bitcakeManager.getCurrentBitcakeAmount(),
					CausalBroadcastShared.getSendTransactions(),
					CausalBroadcastShared.getReceivedTransactions()
				);

				// Increment clock for original sender
				CausalBroadcastShared.incrementClock(AppConfig.myServentInfo.getId());
				break;

			case NONE:
				//Shouldn't be able to come here. See constructor. 
				break;
			}
			
			//2 wait for responses or finish
			boolean waiting = true;
			while (waiting) {
				switch (snapshotType) {
				case NAIVE:
					if (collectedNaiveValues.size() == AppConfig.getServentCount()) {
						waiting = false;
					}
					break;

				case ACHARYA_BADRINATH:
					if (collectedAcharyaBadrinathValues.size() == AppConfig.getServentCount()) {
						waiting = false;
					}
					break;

				case NONE:
				//Shouldn't be able to come here. See constructor.
				break;
				}
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				if (!working) {
					return;
				}
			}
			
			//print
			int sum;
			switch (snapshotType) {
			case NAIVE:
				sum = 0;
				for (Entry<String, Integer> itemAmount : collectedNaiveValues.entrySet()) {
					sum += itemAmount.getValue();
					AppConfig.timestampedStandardPrint(
							"Info for " + itemAmount.getKey() + " = " + itemAmount.getValue() + " bitcake");
				}
				
				AppConfig.timestampedStandardPrint("System bitcake count: " + sum);

				collectedNaiveValues.clear(); //reset for next invocation
				break;

			case ACHARYA_BADRINATH:
				sum = 0;
				for (Entry<String, Object[]> itemAmount : collectedAcharyaBadrinathValues.entrySet()) {
					Object[] payload = itemAmount.getValue();

					int totalAmount = (int) payload[0];
					List<Message> sendTransactions = (List<Message>) payload[1];

					sum += totalAmount;
					AppConfig.timestampedStandardPrint(
							"Info for " + itemAmount.getKey() + " = " + totalAmount + " bitcake");


					for (Message sendTransaction : sendTransactions) {
						payload = collectedAcharyaBadrinathValues.get("node" + sendTransaction.getOriginalReceiverInfo().getId());
						List<Message> receivedTransactions = (List<Message>) payload[2];

						boolean exist = false;
						for (Message receivedTransaction : receivedTransactions) {
							if (sendTransaction.getMessageId() == receivedTransaction.getMessageId()) {
								exist = true;
								break;
							}
						}

						if (!exist) {
							AppConfig.timestampedStandardPrint(
									"Info for unprocessed transaction: " + sendTransaction.getMessageText() + " bitcake");

							int amountNumber = Integer.parseInt(sendTransaction.getMessageText());

							sum += amountNumber;
						}
					}
				}

				AppConfig.timestampedStandardPrint("System bitcake count: " + sum);

				collectedAcharyaBadrinathValues.clear(); //reset for next invocation
				break;

			case NONE:
				//Shouldn't be able to come here. See constructor. 
				break;
			}
			collecting.set(false);
		}

	}
	
	@Override
	public void addNaiveSnapshotInfo(String snapshotSubject, int amount) {
		collectedNaiveValues.put(snapshotSubject, amount);
	}

	@Override
	public void addAcharyaBadrinathSnapshotInfo(String snapshotSubject, int amount,
				List<Message> sendTransactions, List<Message> receivedTransactions) {

		List<Message> sendTrx = new CopyOnWriteArrayList<>(sendTransactions);
		List<Message> receivedTrx = new CopyOnWriteArrayList<>(receivedTransactions);

		Object[] payload = new Object[]{
				amount,
				sendTrx,
				receivedTrx
		};

		collectedAcharyaBadrinathValues.put(snapshotSubject, payload);
	}

	@Override
	public void startCollecting() {
		boolean oldValue = this.collecting.getAndSet(true);
		
		if (oldValue) {
			AppConfig.timestampedErrorPrint("Tried to start collecting before finished with previous.");
		}
	}
	
	@Override
	public void stop() {
		working = false;
	}

}
