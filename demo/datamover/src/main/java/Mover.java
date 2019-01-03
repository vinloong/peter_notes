import org.joda.time.DateTime;

public class Mover {

    public static void main(String[] args) {

        EsHelper es = new EsHelper();

        es.initHelper("anxin-cloud", "anxinyun-m1:9300,anxinyun-n1:9300,anxinyun-n2:9300");

        DateTime dt1 = new DateTime(2018, 12, 26, 0, 0);
        DateTime dt2 = new DateTime(2019, 01, 04, 0, 0);
        int hourDelay = 7 * 24;
        int structId = 194;

        es.migrateRawData("73fa4fbb-6ed1-426c-8e84-13670588f871", "eb71d42c-772e-4de0-8e1a-0dfcbf3c1025", structId, dt1, dt2, hourDelay);
        es.migrateRawData("a003fb16-a029-4f59-909b-22417dbc5312", "bff1c82a-6483-411f-a7e5-f539c751e70c", structId, dt1, dt2, hourDelay);
        es.migrateRawData("dbe4d5f5-a238-4089-83f4-331bbd04c971", "c95ccc34-f091-40ef-953e-3260d8f51959", structId, dt1, dt2, hourDelay);

        // 水
        es.migrateThemeData(215, 549, structId, dt1, dt2, hourDelay);
        // 电
        es.migrateThemeData(216, 550, structId, dt1, dt2, hourDelay);
        // 人
        es.migrateThemeData(211, 541, structId, dt1, dt2, hourDelay);

        es.migrateAggData(215, 549, dt1, dt2, hourDelay);
        es.migrateAggData(216, 550, dt1, dt2, hourDelay);
        es.migrateAggData(211, 541, dt1, dt2, hourDelay);

        System.out.println("finished");
    }
}