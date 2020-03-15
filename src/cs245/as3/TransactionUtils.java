package cs245.as3;

import cs245.as3.interfaces.LogManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;


public class TransactionUtils {

  public static Map<Long, Transaction> deserializeEntireLog(LogManager lm, PriorityQueue<Integer> txnStart){
    Map<Long, Transaction> allTxnsMap = new HashMap<>();
    int rawlogsize = lm.getLogEndOffset();
    if(rawlogsize%128!=0) throw new RuntimeException("Log should always be multiples of 128");
    int loglen = rawlogsize/128;
    for(int i=0; i<loglen; ++i){
      byte[] logmsgArr = lm.readLogRecord(i*128, 128);
      LogMsg log = LogMsg.deserialize(logmsgArr);
      if(log.isWriteLog()){
        Transaction txn = allTxnsMap.get(log.getTxnid());
        if(txn==null){
          txn = new Transaction(log, i);
          allTxnsMap.put(txn.getTxnid(), txn);
          txnStart.offer(i);
        }
        txn.writeLogEncountered(log);
      } else if(log.isCommitLog()){
        Transaction txn = allTxnsMap.get(log.getTxnid());
        if(txn==null){
          txn = new Transaction(log,i);
          allTxnsMap.put(txn.getTxnid(), txn);
          txnStart.offer(i);
        }
        txn.commitLogEncountered(log);
      }
    } //for
    return allTxnsMap;
  }
}
