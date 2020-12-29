import org.junit.Assert;
import org.junit.Test;

import java.nio.file.FileSystemException;

public class SparkApplicationTest {

    @Test
    public void ConvertMetricWrongLengthTest() {
        String csvRow = "1";
        Assert.assertNull(SparkApplication.ConvertMetric(csvRow));
    }

    @Test
    public void ConvertMetricWrongTypeTest() {
        String csvRow = "0, zzz, 0";
        Assert.assertNull(SparkApplication.ConvertMetric(csvRow));
    }

    @Test
    public void ConvertMetricNullTest() {
        String csvRow = null;
        Assert.assertNull(SparkApplication.ConvertMetric(csvRow));
    }

    @Test(expected = IllegalArgumentException.class)
    public void RunJobZeroScaleTest() {
        SparkApplication.RunJob("local[4]", "", "", 0);
    }
}
