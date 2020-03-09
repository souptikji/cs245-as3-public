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
      WriteLog writeLog = createRandomWriteLog(valLen);
      testTemplate(writeLog);
    }
    // Start messages
    for (int i = 0; i < numTests; ++i) {
      WriteLog startLog = createRandomStartLog();
      testTemplate(startLog);
    }

    // Commit messages
    for (int i = 0; i < numTests; ++i) {
      WriteLog commitLog = createRandomCommitLog();
      testTemplate(commitLog);
    }
  }

  private static void testTemplate(WriteLog writeLog) {
    byte[] serialized = writeLog.serialize();
    Assert.assertEquals("Actual length is " + serialized.length, serialized.length, 128);
    WriteLog deserealizedLog = WriteLog.deserialize(serialized);
    Assert.assertEquals("Equality failed", writeLog.equals(deserealizedLog), true);
    System.out.println("Passed");
  }

  private static WriteLog createRandomWriteLog(int valLength) {
    Random random = new Random();
    byte[] val = new byte[valLength];
    random.nextBytes(val);
    long key = random.nextLong();
    long txnid = random.nextLong();
    WriteLog writeLog = new WriteLog((byte) 2, txnid, key, val);
    return writeLog;
  }

  private static WriteLog createRandomStartLog() {
    Random random = new Random();
    long txnid = random.nextLong();
    WriteLog writeLog = new WriteLog((byte) 1, txnid);
    return writeLog;
  }

  private static WriteLog createRandomCommitLog() {
    Random random = new Random();
    long txnid = random.nextLong();
    WriteLog writeLog = new WriteLog((byte) 3, txnid);
    return writeLog;
  }
}
