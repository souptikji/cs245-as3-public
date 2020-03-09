package cs245.as3;

import java.util.Random;
import org.junit.Assert;


public class TestLogWriteUtils {

  public static void main(String[] args) {
    Random random = new Random();
    int numTests = 100;
    // Write messages
    for (int i = 0; i < numTests; ++i) {
      int valLen = random.nextInt(101);
      LogMsg writeLog = createRandomWriteLog(valLen);
      testTemplate(writeLog);
    }
    // Start messages
    for (int i = 0; i < numTests; ++i) {
      LogMsg startLog = createRandomStartLog();
      testTemplate(startLog);
    }

    // Commit messages
    for (int i = 0; i < numTests; ++i) {
      LogMsg commitLog = createRandomCommitLog();
      testTemplate(commitLog);
    }
  }

  private static void testTemplate(LogMsg logMsg) {
    byte[] serialized = logMsg.serialize();
    Assert.assertEquals("Actual length is " + serialized.length, serialized.length, 128);
    LogMsg deserealizedLog = LogMsg.deserialize(serialized);
    Assert.assertEquals("Equality failed", logMsg.equals(deserealizedLog), true);
    System.out.println("Passed");
  }

  private static LogMsg createRandomWriteLog(int valLength) {
    Random random = new Random();
    byte[] val = new byte[valLength];
    random.nextBytes(val);
    long key = random.nextLong();
    long txnid = random.nextLong();
    LogMsg writeLog = new LogMsg((byte) 2, txnid, key, val);
    return writeLog;
  }

  private static LogMsg createRandomStartLog() {
    Random random = new Random();
    long txnid = random.nextLong();
    LogMsg logMsg = new LogMsg((byte) 1, txnid);
    return logMsg;
  }

  private static LogMsg createRandomCommitLog() {
    Random random = new Random();
    long txnid = random.nextLong();
    LogMsg logMsg = new LogMsg((byte) 3, txnid);
    return logMsg;
  }
}
