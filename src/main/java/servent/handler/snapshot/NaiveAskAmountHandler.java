package servent.handler.snapshot;

import app.AppConfig;
import app.CausalBroadcastShared;
import app.snapshot_bitcake.BitcakeManager;
import app.snapshot_bitcake.SnapshotCollector;
import servent.handler.MessageHandler;
import servent.message.Message;
import servent.message.MessageType;
import servent.message.snapshot.NaiveTellAmountMessage;
import servent.message.util.MessageUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NaiveAskAmountHandler implements MessageHandler {

	private final Message clientMessage;
	private final SnapshotCollector snapshotCollector;
	
	public NaiveAskAmountHandler(Message clientMessage, SnapshotCollector snapshotCollector) {
		this.clientMessage = clientMessage;
		this.snapshotCollector = snapshotCollector;
	}

	@Override
	public void run() {
		if (clientMessage.getMessageType() == MessageType.NAIVE_ASK_AMOUNT) {
			BitcakeManager bitcakeManager = snapshotCollector.getBitcakeManager();
			int currentAmount = bitcakeManager.getCurrentBitcakeAmount();

			Map<Integer, Integer> vectorClock = new ConcurrentHashMap<>(CausalBroadcastShared.getVectorClock());

			Message tellMessage = new NaiveTellAmountMessage(
					AppConfig.myServentInfo, clientMessage.getOriginalSenderInfo(),
					null, vectorClock, currentAmount);

			for (int neighbor : AppConfig.myServentInfo.getNeighbors()) {
				//Same message, different receiver, and add us to the route table.
				MessageUtil.sendMessage(tellMessage.changeReceiver(neighbor).makeMeASender());
			}

			CausalBroadcastShared.commitCausalMessage(tellMessage);
		} else {
			AppConfig.timestampedErrorPrint("Ask amount handler got: " + clientMessage);
		}

	}

}
