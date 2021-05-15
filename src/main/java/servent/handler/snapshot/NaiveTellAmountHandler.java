package servent.handler.snapshot;

import app.AppConfig;
import app.snapshot_bitcake.SnapshotCollector;
import servent.handler.MessageHandler;
import servent.message.Message;
import servent.message.MessageType;

public class NaiveTellAmountHandler implements MessageHandler {

	private Message clientMessage;
	private SnapshotCollector snapshotCollector;
	
	public NaiveTellAmountHandler(Message clientMessage, SnapshotCollector snapshotCollector) {
		this.clientMessage = clientMessage;
		this.snapshotCollector = snapshotCollector;
	}

	@Override
	public void run() {
		if (clientMessage.getMessageType() == MessageType.NAIVE_TELL_AMOUNT) {
			int neighborAmount = Integer.parseInt(clientMessage.getMessageText());

			snapshotCollector.addNaiveSnapshotInfo(
					"node"+String.valueOf(clientMessage.getOriginalSenderInfo().getId()), neighborAmount);
		} else {
			AppConfig.timestampedErrorPrint("Tell amount handler got: " + clientMessage);
		}

	}

}
