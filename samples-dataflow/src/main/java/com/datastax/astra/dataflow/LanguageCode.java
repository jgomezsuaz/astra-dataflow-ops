package com.datastax.astra.dataflow;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.api.services.bigquery.model.TableRow;

import java.io.Serializable;

/**
 * DTO for Language Code.
 */
@Table(name = LanguageCode.TABLE_NAME)
public class LanguageCode implements Serializable {

    /** Constants for mapping. */
    public static final String TABLE_NAME      = "languages";
    public static final String COLUMN_CODE     = "code";
    public static final String COLUMN_LANGUAGE = "language";

    @PartitionKey
    @Column(name = COLUMN_CODE)
    private String code;

    @Column(name = COLUMN_LANGUAGE)
    private String language;

    /**
     * Constructor
     */
    public LanguageCode() {
    }

    /**
     * Full-fledged constructor
     */
    public LanguageCode(String code, String language) {
        this.code = code;
        this.language = language;
    }

    /**
     * Helping generate the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static LanguageCode fromCsvRow(String csvRow) {
        String[] chunks = csvRow.split(",");
        return new LanguageCode(chunks[0], chunks[1]);
    }

    /**
     * Convert to CSV Row.
     *
     * @return
     *      csv Row.
     */
    public String toCsvRow() {
        return code + "," + language;
    }

    /**
     * Read From BigQuery table.
     *
     * @param row
     *      current big query row
     * @return
     *      current bean.
     */
    public static LanguageCode fromBigQueryTableRow(TableRow row) {
        return new LanguageCode((String) row.get("code"), (String) row.get("language"));
    }

    /**
     * Convert to BigQuery TableRow.
     * @return
     *      big query table row
     */
    public TableRow toBigQueryTableRow() {
        TableRow row = new TableRow();
        row.set("code", this.code);
        row.set("language", this.language);
        return row;
    }

    /**
     * Helping generate the Target Table if it does not exist.
     *
     * @return
     *      create statement
     */
    public static String cqlCreateTable() {
        return SchemaBuilder.createTable(TABLE_NAME)
                .addPartitionKey(COLUMN_CODE, DataType.text())
                .addColumn(COLUMN_LANGUAGE, DataType.text())
                .ifNotExists().toString();
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
        return code;
    }

    /**
     * Set value for code
     *
     * @param code
     *         new value for code
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * Gets language
     *
     * @return value of language
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Set value for language
     *
     * @param language
     *         new value for language
     */
    public void setLanguage(String language) {
        this.language = language;
    }


}