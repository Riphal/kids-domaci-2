package servent.message.snapshot;

import app.ServentInfo;
import servent.message.BasicMessage;
import servent.message.MessageType;

public class NaiveAskAmountMessage extends BasicMessage {

	private static final long serialVersionUID = -2134483210691179901L;

	public NaiveAskAmountMessage(ServentInfo sender, ServentInfo receiver) {
		super(MessageType.NAIVE_ASK_AMOUNT, sender, receiver);
	}
}
