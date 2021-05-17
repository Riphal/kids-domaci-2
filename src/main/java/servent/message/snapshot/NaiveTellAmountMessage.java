package servent.message.snapshot;

import app.ServentInfo;
import servent.message.BasicMessage;
import servent.message.MessageType;

import java.util.Map;

public class NaiveTellAmountMessage extends BasicMessage {

	private static final long serialVersionUID = -296602475465394852L;

	public NaiveTellAmountMessage(ServentInfo sender, ServentInfo receiver, ServentInfo neighbor,
								  Map<Integer, Integer> senderVectorClock, int amount) {
		super(MessageType.NAIVE_TELL_AMOUNT, sender, receiver, neighbor, senderVectorClock, String.valueOf(amount));
	}
}
