package servent.message.snapshot;

import app.ServentInfo;

import servent.message.BasicMessage;
import servent.message.MessageType;

import java.util.Map;

public class NaiveAskAmountMessage extends BasicMessage {

	private static final long serialVersionUID = -2134483210691179901L;

	public NaiveAskAmountMessage(ServentInfo sender, ServentInfo receiver, ServentInfo neighbor,
								 Map<Integer, Integer> senderVectorClock) {
		super(MessageType.NAIVE_ASK_AMOUNT, sender, receiver, neighbor, senderVectorClock);
	}
}
