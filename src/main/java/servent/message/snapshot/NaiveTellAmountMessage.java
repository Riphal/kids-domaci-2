package servent.message.snapshot;

import app.ServentInfo;
import servent.message.BasicMessage;
import servent.message.MessageType;

public class NaiveTellAmountMessage extends BasicMessage {

	private static final long serialVersionUID = -296602475465394852L;

	public NaiveTellAmountMessage(ServentInfo sender, ServentInfo receiver, int amount) {
		super(MessageType.NAIVE_TELL_AMOUNT, sender, receiver, String.valueOf(amount));
	}
}
