package servent.handler.snapshot;

import app.AppConfig;
import app.snapshot_bitcake.BitcakeManager;
import servent.handler.MessageHandler;
import servent.message.Message;
import servent.message.MessageType;
import servent.message.snapshot.NaiveTellAmountMessage;
import servent.message.util.MessageUtil;

public class NaiveAskAmountHandler implements MessageHandler {

	private Message clientMessage;
	private BitcakeManager bitcakeManager;
	
	public NaiveAskAmountHandler(Message clientMessage, BitcakeManager bitcakeManager) {
		this.clientMessage = clientMessage;
		this.bitcakeManager = bitcakeManager;
	}

	@Override
	public void run() {
		if (clientMessage.getMessageType() == MessageType.NAIVE_ASK_AMOUNT) {
			int currentAmount = bitcakeManager.getCurrentBitcakeAmount();
			
			Message tellMessage = new NaiveTellAmountMessage(
					clientMessage.getReceiverInfo(), clientMessage.getOriginalSenderInfo(), currentAmount);
			
			MessageUtil.sendMessage(tellMessage);
		} else {
			AppConfig.timestampedErrorPrint("Ask amount handler got: " + clientMessage);
		}

	}

}
