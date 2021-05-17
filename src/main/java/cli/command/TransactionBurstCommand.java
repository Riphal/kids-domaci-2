package cli.command;

import app.AppConfig;
import app.CausalBroadcastShared;
import app.ServentInfo;

import app.snapshot_bitcake.SnapshotCollector;
import servent.message.Message;
import servent.message.TransactionMessage;
import servent.message.util.MessageUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionBurstCommand implements CLICommand {

	private static final int TRANSACTION_COUNT = 3;
	private static final int BURST_WORKERS = 3;
	private static final int MAX_TRANSFER_AMOUNT = 5;
	
	private final SnapshotCollector snapshotCollector;
	
	public TransactionBurstCommand(SnapshotCollector snapshotCollector) {
		this.snapshotCollector = snapshotCollector;
	}
	
	private class TransactionBurstWorker implements Runnable {
		
		@Override
		public void run() {
			for (int i = 0; i < TRANSACTION_COUNT; i++) {
				ServentInfo receiverInfo = AppConfig.getInfoById((int)(Math.random() * AppConfig.getServentCount()));

				while (receiverInfo.getId() == AppConfig.myServentInfo.getId()) {
					receiverInfo = AppConfig.getInfoById((int)(Math.random() * AppConfig.getServentCount()));
				}

				int amount = 1 + (int)(Math.random() * MAX_TRANSFER_AMOUNT);

				Map<Integer, Integer> vectorClock = new ConcurrentHashMap<>(CausalBroadcastShared.getVectorClock());

				Message transactionMessage = new TransactionMessage(
						AppConfig.myServentInfo,
						receiverInfo,
						null,
						vectorClock,
						amount,
						snapshotCollector.getBitcakeManager()
				);

				for (int neighbor : AppConfig.myServentInfo.getNeighbors()) {
					//Same message, different receiver, and add us to the route table.
					MessageUtil.sendMessage(transactionMessage.changeReceiver(neighbor).makeMeASender());
				}

				// reduce our bitcake count then send the message
				transactionMessage.sendEffect();

				CausalBroadcastShared.commitCausalMessage(transactionMessage);
			}
		}
	}
	
	@Override
	public String commandName() {
		return "transaction_burst";
	}

	@Override
	public void execute(String args) {
		for (int i = 0; i < BURST_WORKERS; i++) {
			Thread t = new Thread(new TransactionBurstWorker());
			
			t.start();
		}
	}

	
}
