package cs245.as3;

import cs245.as3.driver.LogManagerImpl;
import cs245.as3.interfaces.LogManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;


public class TestTxnUtils {

  public static void main(String[] args) {
    testTemplate(100, 1000);
  }

  private static void testTemplate(int numTxns, int maximumNumWritesForAnyTxn) {
    Map<Long, List<LogMsg>> txn2Logmsgs = createTransactions(numTxns, maximumNumWritesForAnyTxn);
    LogManager lm = new LogManagerImpl();
    appendAllTxnsToLog(lm, txn2Logmsgs);

    // lm is ready
    System.out.println("Starting deserilization");
    Map<Long, Transaction> deserialized = TransactionUtils.deserializeEntireLog(lm);
    testIfDeserializedEqualsOriginal(deserialized, txn2Logmsgs);
  }

  private static Map<Long, List<LogMsg>> createTransactions(int numTxns, int maximumNumWritesForAnyTxn) {
    Random random = new Random();
    Map<Long, List<LogMsg>> txn2Logmsgs = new HashMap<>();
    for (int i = 0; i < numTxns; ++i) {
      int numWritesForThisTxn = random.nextInt(maximumNumWritesForAnyTxn);
      List<LogMsg> msgs =
          createTransactionLogMessages(i, numWritesForThisTxn); //each log message of size 100 (1 start, 98 writes, 1 commit)
      txn2Logmsgs.put((long) i, msgs);
    }
    return txn2Logmsgs;
  }

  private static void appendAllTxnsToLog(LogManager lm, Map<Long, List<LogMsg>> txn2Logmsgs) {
    Random random = new Random();
    int numTxns = txn2Logmsgs.size();
    Map<Long, List<LogMsg>> copy = deepCopy(txn2Logmsgs);
    long nextTxn = 0;

    while (!copy.keySet().isEmpty()) {
      long thistxn = nextTxn;
      nextTxn = (nextTxn + 1) % numTxns;

      List<LogMsg> allMsgsOfTxn = copy.get(thistxn);
      if (allMsgsOfTxn == null) {
        continue;
      }
      if (allMsgsOfTxn.isEmpty()) {
        System.out.println("Txn"+thistxn+" message queue is fully drained");
        copy.remove(thistxn);
        continue;
      }
      int num_msgs2drain = Math.max(random.nextInt(allMsgsOfTxn.size()),1); //drain atleast 1 message per iteration
      for (int i = 0; i < num_msgs2drain; ++i) {
        lm.appendLogRecord(allMsgsOfTxn.get(i).serialize());
      }
      allMsgsOfTxn = allMsgsOfTxn.subList(num_msgs2drain, allMsgsOfTxn.size());
      if (allMsgsOfTxn.isEmpty()) {
        System.out.println("Txn"+thistxn+" message queue is fully drained");
        copy.remove(thistxn);
      }
      copy.put((long) thistxn, allMsgsOfTxn);

      System.out.println(
          "Drained " + num_msgs2drain + " logs from txn" + thistxn + ", map size=" + copy.keySet().size());
    }
  }

  private static void testIfDeserializedEqualsOriginal(Map<Long, Transaction> deserialized, Map<Long, List<LogMsg>> originalTxn2Logmsgs) {
    Assert.assertEquals(originalTxn2Logmsgs.keySet().size(), deserialized.keySet().size());
    for (long txnid : originalTxn2Logmsgs.keySet()) {
      List<LogMsg> orig = originalTxn2Logmsgs.get(txnid);
      List<LogMsg> converted = deserialized.get(txnid).getOrderedLogMessages();
      Assert.assertNotEquals(converted, null);
      Assert.assertEquals(orig.size(), converted.size());
      for (int i = 0; i < orig.size(); ++i) {
        Assert.assertEquals(orig.get(i), converted.get(i));
      }
      System.out.println("Test passed for txn " + txnid);
    }
  }

  private static Map<Long, List<LogMsg>> deepCopy(Map<Long, List<LogMsg>> orig) {
    Map<Long, List<LogMsg>> map = new HashMap<>();
    for (long key : orig.keySet()) {
      List<LogMsg> copy = new ArrayList<>();
      copy.addAll(orig.get(key));
      map.put(key, copy);
    }
    return map;
  }

  private static List<LogMsg> createTransactionLogMessages(int txnid, int numWrites) {
    // Create write log msgs for txn1
    List<LogMsg> msgs = new ArrayList<>();
    msgs.add(new LogMsg((byte) 1, txnid)); //start
    for (int i = 1; i <= numWrites; ++i) {
      msgs.add(createRandomWriteLogOnThisTxn(txnid));
    }
    msgs.add(new LogMsg((byte) 3, txnid)); //commit
    return msgs;
  }

  private static LogMsg createRandomWriteLogOnThisTxn(long txnid) {
    Random random = new Random();
    byte[] val = new byte[random.nextInt(101)];
    random.nextBytes(val);
    long key = random.nextLong();
    LogMsg writeLog = new LogMsg((byte) 2, txnid, key, val);
    return writeLog;
  }
}
