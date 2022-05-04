package arthur.dy.lee.controller;

import arthur.dy.lee.client.HBaseClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
public class HbaseController {

    private HBaseClient hBaseClient;

    @Autowired
    public HbaseController(HBaseClient hBaseClient) {
        this.hBaseClient = hBaseClient;
    }

    private final static String TABLE       = "quick-hbase-table";
    private final static String TABLE_FAM_1 = "quick";
    private final static String TABLE_FAM_2 = "hbase";

    @PostMapping("/create")
    public ResponseEntity createTable() throws Exception {
        hBaseClient.createTable(TABLE, TABLE_FAM_1, TABLE_FAM_2);
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @PutMapping("/insert")
    public ResponseEntity insertOrUpdate() throws Exception {
        hBaseClient.insertOrUpdate(TABLE, "1", TABLE_FAM_1, "speed", "1km/h");
        hBaseClient.insertOrUpdate(TABLE, "1", TABLE_FAM_1, "feel", "better");
        hBaseClient.insertOrUpdate(TABLE, "1", TABLE_FAM_2, "action", "create table");
        hBaseClient.insertOrUpdate(TABLE, "1", TABLE_FAM_2, "time", "2019年07月20日17:52:53");
        hBaseClient.insertOrUpdate(TABLE, "1", TABLE_FAM_2, "user", "admin");
        hBaseClient.insertOrUpdate(TABLE, "2", TABLE_FAM_2, "user", "admin");
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @GetMapping("/find-by-time")
    public String findByTime() throws Exception {
        String result = hBaseClient.getValue(TABLE, "1", TABLE_FAM_2, "time");
        return result;
    }

    @GetMapping("/find-one")
    public String findOne() throws Exception {
        String result = hBaseClient.selectOneRow(TABLE, "1");
        return result;
    }

    @GetMapping("/find-table")
    public String scanTable() throws Exception {
        String result = hBaseClient.scanTable(TABLE, "{FILTER=>\"PrefixFilter('2019')\"");
        return result;
    }
}
