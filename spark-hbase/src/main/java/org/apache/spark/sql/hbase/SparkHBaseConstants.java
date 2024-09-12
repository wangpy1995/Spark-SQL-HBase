package org.apache.spark.sql.hbase;

public interface SparkHBaseConstants {
    enum NAMESPACE implements SparkHBaseConstants {}

    enum TABLE_CONSTANTS implements SparkHBaseConstants {
        COLUMN_QUALIFIER_SPLITTER(":"),
        ROW_KEY("row_key");

        private final String value;

        TABLE_CONSTANTS(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    enum COLUMN_FAMILY implements SparkHBaseConstants {}

    enum QUALIFIER implements SparkHBaseConstants {}
}
