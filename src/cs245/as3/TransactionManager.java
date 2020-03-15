package cs245.as3;

import java.util.ArrayList;
import java.util.HashMap;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;


/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * Heap dump testing : java -Xmx1400m -Xms1400m -XX:+HeapDumpOnOutOfMemoryError -jar target/tests.jar
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	static class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private StorageManager _sm;
	private LogManager _lm;

	// Data strcutures for callback
	private Map<Long, Transaction> allTxnsMap;
	PriorityQueue<Integer> txnStart;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
		allTxnsMap = new HashMap();
		txnStart = new PriorityQueue<>();
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this._sm = sm;
		this._lm = lm;
		wakeUpFromCrash();
	}

	private void wakeUpFromCrash() {
		//allTxnsMap = TransactionUtils.deserializeEntireLog(this._lm, txnStart);
		int rawlogsize = _lm.getLogEndOffset();
		if(rawlogsize%128!=0) throw new RuntimeException("Log should always be multiples of 128");
		int loglen = rawlogsize/128;

		for(int i=_lm.getLogTruncationOffset(); i<rawlogsize; i+=128){
			byte[] logmsgArr = _lm.readLogRecord(i, 128);
			LogMsg log = LogMsg.deserialize(logmsgArr);
			if(log.isWriteLog()){
				Transaction txn = allTxnsMap.get(log.getTxnid());
				if(txn==null){
					//first time
					txn = new Transaction(log, i);
					txnStart.offer(i);
				}
				txn.writeLogEncountered(log);
				allTxnsMap.put(txn.getTxnid(), txn);

			} else if(log.isCommitLog()){
				Transaction txn = allTxnsMap.get(log.getTxnid());
				/*if(txn==null){
					txn = new Transaction(log,i);
					txnStart.offer(i);
				}*/
				txn.commitLogEncountered(log);
				allTxnsMap.put(txn.getTxnid(), txn);
			}
		} //for
		//allTxnsMap = TransactionUtils.deserializeEntireLog(this._lm, txnStart);


		List<Transaction> committedTxns = allTxnsMap.values().stream().filter(txn -> txn.isCommitted()).collect(Collectors.toList());
		committedTxns.sort((Transaction a, Transaction b)-> Long.compare(a.getTxnid(), b.getTxnid()));

		for(Transaction comTxn : committedTxns){
			for(TaggedValue tv: comTxn.getLatestValues()){
				latestValues.put(tv.tag, new TaggedValue(0, tv.value));
				_sm.queueWrite(tv.tag, comTxn.getTxnid(), tv.value);
			}
		}
		/*committedTxns.forEach(txn -> latestValues.putAll(txn.getLatestValues()));

		//Queue all committed writes to disk
		for(long key: latestValues.keySet()){
			//Start pushing to dick
			TaggedValue tv = latestValues.get(key);
			_sm.queueWrite(txnId, tv.tag, tv.value);
		}*/

		//TODO: "End" txn
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txnid) {
		//LogMsg startLog = new LogMsg((byte) 1, txnid);
		//_lm.appendLogRecord(startLog.serialize());
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read()
	 * calls until the transaction making the write commits. For simplicity, we will not make reads
	 * to this same key from txID itself after we make a write to the key.
	 */
	public void write(long txID, long key, byte[] value) {
		//LogMsg writeLog = new LogMsg((byte) 2, txID, key, value);
		//_lm.appendLogRecord(writeLog.serialize());

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		Map<Long, TaggedValue> pushTheseToDisk = new HashMap<>();

		// Modify in memory data structure
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {

			//Record this txn
			Transaction thistxn = new Transaction(txID, _lm.getLogEndOffset());
			allTxnsMap.put(txID, thistxn);
			txnStart.offer(_lm.getLogEndOffset());

			for (WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag = 0; //TODO: Change this
				TaggedValue tv = new TaggedValue(tag, x.value);
				latestValues.put(x.key, tv);

				// log this
				LogMsg writeLog = new LogMsg((byte) 2, txID, x.key, x.value);
				_lm.appendLogRecord(writeLog.serialize());

				//update internal data struct
				allTxnsMap.get(txID).writeLogEncountered(writeLog);
			}

		}

		//log commit
		LogMsg commitLog = new LogMsg((byte) 3, txID);
		_lm.appendLogRecord(commitLog.serialize());
		allTxnsMap.get(txID).commitLogEncountered(commitLog);

		//start pushing to disk
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				_sm.queueWrite(x.key, txID, x.value);
			}
			writesets.remove(txID);
		}

	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		//LogMsg abortLog = new LogMsg((byte) 4, txID);
		//_lm.appendLogRecord(abortLog.serialize());
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long txnId, byte[] persisted_value) {
		// update once txn has been completed
		Transaction tobj = allTxnsMap.get(txnId);
		if (tobj == null) return;
		tobj.flush(key, persisted_value);
		allTxnsMap.put(txnId, tobj);
		if (!tobj.flushedToDisk) return;
		txnStart.remove(tobj.getBegin());
		if (txnStart.isEmpty())
			_lm.setLogTruncationOffset(_lm.getLogEndOffset());
		else
			_lm.setLogTruncationOffset(txnStart.peek());
	}
}
