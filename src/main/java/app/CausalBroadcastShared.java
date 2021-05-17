package app;

import app.snapshot_bitcake.BitcakeManager;
import app.snapshot_bitcake.SnapshotCollector;
import servent.handler.MessageHandler;
import servent.handler.NullHandler;
import servent.handler.TransactionHandler;
import servent.handler.snapshot.NaiveAskAmountHandler;
import servent.handler.snapshot.NaiveTellAmountHandler;
import servent.message.BasicMessage;
import servent.message.Message;
import servent.message.snapshot.NaiveTellAmountMessage;
import servent.message.util.MessageUtil;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class contains shared data for the Causal Broadcast implementation:
 * <ul>
 * <li> Vector clock for current instance
 * <li> Commited message list
 * <li> Pending queue
 * </ul>
 * As well as operations for working with all of the above.
 * 
 * @author bmilojkovic
 *
 */
public class CausalBroadcastShared {

	private static final Map<Integer, Integer> vectorClock = new ConcurrentHashMap<>();
	private static final List<Message> commitedCausalMessageList = new CopyOnWriteArrayList<>();
	private static final Queue<Message> pendingMessages = new ConcurrentLinkedQueue<>();
	private static final Object pendingMessagesLock = new Object();
	private static SnapshotCollector snapshotCollector;

	private static final Set<String> receivedNaiveAsk = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	/*
	 * Thread pool for executing the handlers. Each client will get it's own handler thread.
	 */
	private static final ExecutorService threadPool = Executors.newWorkStealingPool();

	public static void setSnapshotCollector(SnapshotCollector sc) {
		snapshotCollector = sc;
	}

	public static SnapshotCollector getSnapshotCollector() {
		return snapshotCollector;
	}

	public static void initializeVectorClock(int serventCount) {
		for(int i = 0; i < serventCount; i++) {
			vectorClock.put(i, 0);
		}
	}
	
	public static void incrementClock(int serventId) {
		vectorClock.computeIfPresent(serventId, (key, oldValue) -> oldValue+1);
	}
	
	public static Map<Integer, Integer> getVectorClock() {
		return vectorClock;
	}
	
	public static List<Message> getCommitedCausalMessages() {
		List<Message> toReturn = new CopyOnWriteArrayList<>(commitedCausalMessageList);
		
		return toReturn;
	}
	
	public static void addPendingMessage(Message msg) {
		pendingMessages.add(msg);
	}
	
	public static void commitCausalMessage(Message newMessage) {
		AppConfig.timestampedStandardPrint("Committing " + newMessage);
		commitedCausalMessageList.add(newMessage);
		incrementClock(newMessage.getOriginalSenderInfo().getId());

		checkPendingMessages();
	}
	
	private static boolean otherClockGreater(Map<Integer, Integer> clock1, Map<Integer, Integer> clock2) {
		if (clock1.size() != clock2.size()) {
			throw new IllegalArgumentException("Clocks are not same size how why");
		}
		
		for(int i = 0; i < clock1.size(); i++) {
			if (clock2.get(i) > clock1.get(i)) {
				return true;
			}
		}
		
		return false;
	}

	public static void checkPendingMessages() {
		boolean gotWork = true;
		
		while (gotWork) {
			gotWork = false;
			
			synchronized (pendingMessagesLock) {
				Iterator<Message> iterator = pendingMessages.iterator();
				
				Map<Integer, Integer> myVectorClock = getVectorClock();
				while (iterator.hasNext()) {
					Message pendingMessage = iterator.next();
					BasicMessage basicMessage = (BasicMessage)pendingMessage;

					if (!otherClockGreater(myVectorClock, basicMessage.getSenderVectorClock())) {
						gotWork = true;

						AppConfig.timestampedStandardPrint("Committing " + pendingMessage);
						commitedCausalMessageList.add(pendingMessage);
						incrementClock(pendingMessage.getOriginalSenderInfo().getId());

						MessageHandler messageHandler = new NullHandler(basicMessage);

						switch (basicMessage.getMessageType()) {
							case TRANSACTION:
								if (basicMessage.getOriginalReceiverInfo().getId() == AppConfig.myServentInfo.getId()) {
									messageHandler = new TransactionHandler(basicMessage, snapshotCollector.getBitcakeManager());
								}
								break;
							case NAIVE_ASK_AMOUNT:
								boolean didPut = receivedNaiveAsk.add(basicMessage.getOriginalSenderInfo().getId() + ":" + AppConfig.myServentInfo.getId() + ":" + basicMessage.getMessageId());

								if (didPut) {
									messageHandler = new NaiveAskAmountHandler(basicMessage, snapshotCollector);
								}
								break;
							case NAIVE_TELL_AMOUNT:
								if (basicMessage.getOriginalReceiverInfo().getId() == AppConfig.myServentInfo.getId()) {
									messageHandler = new NaiveTellAmountHandler(basicMessage, snapshotCollector);
								}
								break;
						}

						threadPool.submit(messageHandler);

						iterator.remove();
						
						break;
					}
				}
			}
		}
		
	}

}
